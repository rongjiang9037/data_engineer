import boto3
import re
import pandas as pd
import numpy as np


from airflow.models import Variable

def extract_label_desp_data(**kwargs):
    ## get parameters
    s3_bucket_name = kwargs["params"]["S3_BUCKET_NAME"]
    label_desp_key_path = kwargs["params"]["LABEL_DESP_PATH"]
    output_key_path = kwargs["params"]["LABEL_DESP_STAGING_PATH"]
    AWS_KEY = Variable.get("AWS_KEY")
    AWS_SECRET = Variable.get("AWS_SECRET")

    ## read data from S3
    s3 = boto3.resource('s3',
                        aws_access_key_id=AWS_KEY,
                        aws_secret_access_key= AWS_SECRET)



    obj = s3.Object(s3_bucket_name, label_desp_key_path)
    data_str = obj.get()['Body'].read()
    data_str = str(data_str).split(';')

    for desp in data_str:
        if 'I94PORT' in desp:
            i94_port = desp
        if 'I94CIT & I94RES' in desp:
            i94_country = desp
        if 'I94MODE' in desp:
            i94_mode = desp
        if 'I94ADDR' in desp:
            i94_state = desp
        if 'I94VISA' in desp:
            i94_visa = desp

    ## clean raw data and save them as a DataFrame
    ### 1. port data
    i94_port = i94_port.replace("INT''L FALLS, MN", "INTL FALLS, MN")
    i94_port = re.sub(r'\\t|\\r|\\n',' ',i94_port)
    i94_port = re.sub('[^A-Za-z0-9= \']+', '', i94_port)
    port_info_list = re.findall(re.compile("\'([A-Z0-9 ]+)\' *= *\'([A-Za-z0-9 ]+[A-Z0-9a-z]+)"), i94_port)
    port_info_list = [list(x)+['port'] for x in port_info_list]

    ### 2. country data
    region_list = re.findall(re.compile("([0-9]+) {1,2}=  {1,2}\'([A-Za-z0-9 ,\(\)\.:\/-]+)\'"), i94_country)
    region_list = [list(x) + ['country'] for x in region_list]

    ### 3. i94 mode
    i94mode_list= re.findall(re.compile("([1-9]) = \'([a-zA-Z ]+)\'"), i94_mode)
    i94mode_list = [list(x) + ['i94_mode'] for x in i94mode_list]

    ### 4.  state
    state_info_list = re.findall(re.compile("\'([A-Z9]{2})\'=\'([a-zA-Z \.]+)\'"), i94_state)
    state_info_list = [list(x) + ['state'] for x in state_info_list]

    ### 5. visa
    visa_list = re.findall(re.compile("([1-3]) = ([A-Za-z]+)"), i94_visa)
    visa_list = [list(x) + ['visa'] for x in visa_list]

    ### combine and save as DataFrame to S3
    combined_list = port_info_list + region_list + i94mode_list + visa_list + state_info_list
    df = pd.DataFrame(combined_list, columns=['code', 'value', 'type'])
    output_path = "s3://{}/{}".format(s3_bucket_name, output_key_path)
    df.to_csv(output_path, index=False)


def process_port(df_port, df_states):
    """
    this function processes port table after extraction.
    :param desp: a string, contains all port description from label description sas file
    :param df_states: DataFrame format, contains US 50 states name and abbr
    :return:
    """
    states_list = df_states['code'].unique()
    def is_us(x):
        if len(np.intersect1d(np.array(x.split(' ')), states_list)) > 0:
            return True
        else:
            return False

    def get_state(x):
        find_state = np.intersect1d(np.array(x.split(' ')), states_list)
        if len(find_state) > 0:
            return find_state[0]
        else:
            return 'NA'

    def get_address(x):
        find_state = np.intersect1d(np.array(x.split(' ')), states_list)
        if len(find_state) > 0:
            state = find_state[0]
            x = x.replace(state, '').strip()
            x = x.replace(',', '')
            return x
        else:
            return x

    df_port.rename(columns={"code":"port_code", "value":"port_add"}, inplace=True)
    df_port.drop(["type"], axis=1, inplace=True)

    ## process format and outlier
    df_port['port_code'] = df_port['port_code'].str.replace("'", "")
    df_port['port_add'] = df_port['port_add'].str.replace("'", "").str.strip()

    # process the outlier
    no_port_idx = df_port['port_add'].str.contains('No PORT Code')
    df_port.loc[no_port_idx, 'port_add'] = 'NA, NA'

    collapsed_idx = df_port['port_add'].str.contains('Collapsed')
    df_port.loc[collapsed_idx, 'port_add'] = 'NA, NA'

    unknown_idx = df_port['port_add'].str.contains('UNIDENTIFED')
    df_port.loc[unknown_idx, 'port_add'] = 'NA, NA'

    unknown_idx = df_port['port_add'].str.contains('UNKNOWN')
    df_port.loc[unknown_idx, 'port_add'] = 'NA, NA'

    df_port.loc[df_port['port_add'].str.contains('#INTL'), 'port_add'] = 'BELLINGHAM, WASHINGTON WA'
    df_port.loc[df_port['port_add'].str.contains('PASO DEL NORTE,TX'), 'port_add'] = 'PASO DEL NORTE, TX'

    ## apply function to filter US port, state, address info etc
    df_port = df_port[df_port['port_add'].apply(lambda x: is_us(x))]
    print(df_port)
    df_port['state'] = df_port['port_add'].apply(lambda x: get_state(x))
    df_port['address'] = df_port['port_add'].apply(lambda x: get_address(x))
    df_port = df_port[['port_code', 'state', 'address']]
    df_port = df_port.rename(columns={'port_code': 'port_key'})

    return df_port



def process_country(df):
    """
    This function extracts country info.
    :param str_country:
    :return:
    """
    df.rename(columns={"code":"region_key", "value":"region_name"}, inplace=True)
    df.drop(["type"], axis=1, inplace=True)
    return df

def process_i94_mode(df):
    """
    This function extracts i94 mode info.
    :param str_i94_mode:
    :return:
    """
    df.rename(columns={"code":"i94mode_key", "value":"i94mode"}, inplace=True)
    df.drop(["type"], axis=1, inplace=True)
    return df


def process_state(df):
    """
    This function extracts state info.
    :param str_state:
    :return:
    """
    df.rename(columns={"code":"state_key", "value":"state"}, inplace=True)
    df.drop(["type"], axis=1, inplace=True)
    return df


def process_i94_visa(df):
    """
    This function extracts visa info.
    :param visa_str:
    :return:
    """
    df.rename(columns={"code":"visa_key", "value":"visa_broad_type"}, inplace=True)
    df.drop(["type"], axis=1, inplace=True)
    return df
