import re
import pandas as pd
import numpy as np


def get_port(label_description_path):
    """
    This function gets port description data.

    :param label_description_path: S3 path where the label description is stored
    :return: data in string format
    """
    ## read port description data
    res = []
    with open(label_description_path, 'r') as file_object:
        for line in file_object:
            if len(line.strip()) > 0:
                res.append(line.strip())

    i94_desp = ''.join(res).split(';')
    # process port description data
    for desp in i94_desp:
        if 'I94PORT' in desp:
            return desp
    return None


def port_etl(desp, df_states):
    """
    this function is the ETL for port table.
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
