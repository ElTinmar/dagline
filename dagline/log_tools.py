import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt  
import re
from typing import List, Dict

def parse_logs(filename: str) -> List[Dict]:

    log_entry = re.compile(r"""
        (?P<datetime>\d+-\d+-\d+ \s+ \d+:\d+:\d+,\d+) \s+
        (?P<process_id>Process-\d+) \s+
        (?P<process_name>\w+) \s+
        (?P<loglevel>\w+) \s+
        [#](?P<num>\d+) \s,\s+
        receive_time:\s (?P<receive_time>\d+\.\d+) ,\s+
        process_time:\s (?P<process_time>\d+\.\d+) ,\s+
        send_time:\s (?P<send_time>\d+\.\d+) ,\s+
        total_time:\s (?P<total_time>\d+\.\d+)
        """, re.VERBOSE)
    
    with open(filename, 'r') as f:
        content = f.read()
        entries = [e.groupdict() for e in log_entry.finditer(content)]

    return entries


def plot_logs(filename: str) -> None:
    
    # get entries using regexp
    entries = parse_logs(filename)

    # convert entries to pandas dataframe
    data = pd.DataFrame(entries)
    data = data.astype({
        'datetime': 'datetime64[ns]',
        'process_id': 'str',
        'process_name': 'str',
        'loglevel': 'str',
        'num': 'int64',
        'receive_time': 'float32',
        'process_time': 'float32',
        'send_time': 'float32',
        'total_time': 'float32',
    })

    # boxplot by process
    fig, axes = plt.subplots(1, 4, figsize=(8,2))
    sns.boxplot(ax=axes[0], data=data, x='process_name', y='receive_time')
    sns.boxplot(ax=axes[1], data=data, x='process_name', y='process_time')
    sns.boxplot(ax=axes[2], data=data, x='process_name', y='send_time')
    sns.boxplot(ax=axes[3], data=data, x='process_name', y='total_time')
    plt.show()
