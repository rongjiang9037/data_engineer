import re
import pandas as pd
import numpy as np

def get_all_desp_str(label_description_path):
    """
    This function reads all de desp functions and return a long string.
    :param label_description_path:
    :return:
    """
    ## read port description data
    res = []
    with open(label_description_path, 'r') as file_object:
        for line in file_object:
            if len(line.strip()) > 0:
                res.append(line.strip())
    i94_desp = ''.join(res).split(';')
    return i94_desp


def extract_data_from_desp(label_description_path, key_word):
    """
    This function gets port description data.

    :param label_description_path: S3 path where the label description is stored
    :param key_word: the key word used to extract corresponding data
    :return: data in string format
    """
    i94_desp = get_all_desp_str(label_description_path)

    # process port description data
    for desp in i94_desp:
        if key_word in desp:
            return desp
    return None

def process_port(desp, df_states):
    """
    this function processes port table after extraction.
    :param desp: a string, contains all port description from label description sas file
    :param df_states: DataFrame format, contains US 50 states name and abbr
    :return:
    """
    states_list = df_states['Abbreviation'].unique()
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

    ## preprocess "desp" string and save it as a DataFrame
    desp = desp.replace("INT''L FALLS, MN", "INTL FALLS, MN")
    desp = desp.replace("\t\t", "\t")
    port_info = re.findall(re.compile("\'([A-Z0-9]+\'\\t=\\t\'.+)\'"), desp)[0]

    port_info_list = port_info.split("''")
    port_info_list = [x.split("\t=\t") for x in port_info_list]

    df_port = pd.DataFrame(port_info_list, columns=['port_code', 'port_add'])

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
    df_port['state'] = df_port['port_add'].apply(lambda x: get_state(x))
    df_port['address'] = df_port['port_add'].apply(lambda x: get_address(x))
    df_port = df_port[['port_code', 'state', 'address']]
    df_port = df_port.rename(columns={'port_code': 'port_key'})

    return df_port



def process_country(str_country):
    """
    This function extracts country info.
    :param str_country:
    :return:
    """
    region_list = re.findall(re.compile("([0-9]+) {1,2}=  {1,2}\'([A-Za-z0-9 ,\(\)\.:\/-]+)\'"), str_country)
    df_region = pd.DataFrame(region_list, columns=['region_key', 'region_name'])
    return df_region

def process_i94_mode(str_i94_mode):
    """
    This function extracts i94 mode info.
    :param str_i94_mode:
    :return:
    """
    i94mode_list= re.findall(re.compile("([1-9]) = \'([a-zA-Z ]+)\'"), str_i94_mode)
    df_i94mode = pd.DataFrame(i94mode_list, columns=['i94mode_key', 'i94mode'])
    return df_i94mode


def process_state(str_state):
    """
    This function extracts state info.
    :param str_state:
    :return:
    """
    state_info_list = re.findall(re.compile("\'([A-Z9]{2})\'=\'([a-zA-Z \.]+)\'"), str_state)
    df_state = pd.DataFrame(state_info_list, columns=['state_code', 'state'])
    return df_state


def process_i94_visa(visa_str):
    """
    This function extracts visa info.
    :param visa_str:
    :return:
    """
    visa_list = re.findall(re.compile("([1-3]) = ([A-Za-z]+)"), visa_str)
    df_visa = pd.DataFrame(visa_list, columns=['visa_key', 'visa_broad_type'])
    return df_visa
