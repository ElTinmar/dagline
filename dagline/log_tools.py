import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt  
import re
from typing import List, Dict, Optional

#TODO remove ylabel and use title instead

def parse_logs(filename: str) -> List[Dict]:

    log_entry = re.compile(r"""
        (?P<datetime>\d+-\d+-\d+ \s+ \d+:\d+:\d+,\d+) \s+
        (?P<process_id>Process-\d+) \s+
        (?P<process_name>(\w|\.)+) \s+
        (?P<loglevel>\w+) \s+
        [#](?P<num>\d+) \s,\s+
        t_start:\s (?P<t_start>\d+\.\d+) ,\s+
        receive_data_time:\s (?P<receive_data_time>\d+\.\d+) ,\s+
        process_data_time:\s (?P<process_data_time>\d+\.\d+) ,\s+
        send_data_time:\s (?P<send_data_time>\d+\.\d+) ,\s+
        receive_metadata_time:\s (?P<receive_metadata_time>\d+\.\d+) ,\s+
        process_metadata_time:\s (?P<process_metadata_time>\d+\.\d+) ,\s+
        send_metadata_time:\s (?P<send_metadata_time>\d+\.\d+) ,\s+
        total_time:\s (?P<total_time>\d+\.\d+) ,\s+
        t_stop:\s (?P<t_stop>\d+\.\d+)
        """, re.VERBOSE)
    
    with open(filename, 'r') as f:
        content = f.read()
        entries = [e.groupdict() for e in log_entry.finditer(content)]

    return entries


def plot_logs(filename: str, outlier_thresh: Optional[float] = None) -> None:
    
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
        't_start': 'float64',
        'receive_data_time': 'float64',
        'process_data_time': 'float64',
        'send_data_time': 'float64',
        'receive_metadata_time': 'float64',
        'process_metadata_time': 'float64',
        'send_metadata_time': 'float64',
        'total_time': 'float64',
        't_stop': 'float64'
    })

    if outlier_thresh:
        data = data[data['receive_data_time']<outlier_thresh] 

    # boxplot by process
    fig, axes = plt.subplots(1, 4, figsize=(8,2))
    for id, y in enumerate(['receive_data_time', 'process_data_time', 'send_datatime', 'total_time']):
        ax = axes[id]
        g = sns.boxplot(ax=ax, data=data, x='process_name', y=y)
        g.set_title(y)
        g.set(ylabel=None)
        ax.tick_params(axis='x', rotation=90)
    
    plt.show()
