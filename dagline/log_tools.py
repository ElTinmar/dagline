import pandas as pd
import seaborn as sns
import re
from typing import List

def parse_logs(filename: str) -> List:
    # TODO write the proper regex
    log_entry = re.compile('<date> <time> <process_id> <process_name> <loglevel> <num> <receive_time> <process_time> <send_time> <total_time>')
    with open(filename, 'r') as f:
        content = f.read()
        entries = re.findall(log_entry, content)
    return entries


def plot_logs(filename: str) -> None:
    
    # get entries using regexp
    entries = parse_logs(filename)

    # convert entries to pandas dataframe
    data = pd.DataFrame(entries)

    # boxplot by process
    sns.boxplot(data=data,x='process_name', y='receive_time')
    sns.boxplot(data=data,x='process_name', y='process_time')
    sns.boxplot(data=data,x='process_name', y='send_time')
    sns.boxplot(data=data,x='process_name', y='total_time')