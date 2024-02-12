import pandas as pd
import seaborn as sns
import re
from typing import List

def parse_logs(filename: str) -> List:
    # TODO write the proper regex
    log_entry = re.compile(r"(?P<date>\d+-\d+-\d+)\s+(?P<time>\d+:\d+:\d+,\d+)\s+(?P<process_id>Process-\d+)\s+(?P<process_name>\w+)\s+(?P<loglevel>\w+)\s+(?P<num>#\d+\s,)\s+(?P<receive_time>receive_time: \d+\.\d+,)\s+(?P<process_time>process_time: \d+\.\d+,)\s+(?P<send_time>send_time: \d+\.\d+,)\s+(?P<total_time>total_time: \d+\.\d+)")
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