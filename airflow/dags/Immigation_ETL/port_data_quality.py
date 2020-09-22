import pandas as pd

def is_empty(port_output_path):
    """
    This function checks if the data is empty
    :param port_output_path:
    :return:
    """
    df = pd.read_csv(port_output_path)
    if df.empty:
        return True
    else:
        return False



