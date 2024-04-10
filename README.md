# dagline
DAG multiprocessing pipeline

Works only on linix for two reasons:
    - multiprocessing with fork
    - system-wide time.perf_counter only has good resolution on linux across processes
    
```
pip install git+https://github.com/ElTinmar/dagline.git@main
```
