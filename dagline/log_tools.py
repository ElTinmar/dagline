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
        receive_time:\s (?P<receive_time>\d+\.\d+) ,\s+
        process_time:\s (?P<process_time>\d+\.\d+) ,\s+
        send_time:\s (?P<send_time>\d+\.\d+) ,\s+
        total_time:\s (?P<total_time>\d+\.\d+) ,\s+
        timestamp:\s (?P<timestamp>\d+\.\d+)
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
        'receive_time': 'float64',
        'process_time': 'float64',
        'send_time': 'float64',
        'total_time': 'float64',
        'timestamp': 'float64'
    })

    if outlier_thresh:
        data = data[data['receive_time']<outlier_thresh] 

    # boxplot by process
    fig, axes = plt.subplots(1, 4, figsize=(8,2))
    for id, y in enumerate(['receive_time', 'process_time', 'send_time', 'total_time']):
        ax = axes[id]
        g = sns.boxplot(ax=ax, data=data, x='process_name', y=y)
        g.set_title(y)
        g.set(ylabel=None)
        ax.tick_params(axis='x', rotation=90)
    
    plt.show()
