from abc import ABC, abstractmethod
from multiprocessing import Event, Process, Barrier
from typing  import Any, Optional, Dict
import time
from itertools import cycle
from queue import Empty, Full
from enum import Enum
import cProfile
import pstats

from multiprocessing_logger import Logger
from ipc_tools import QueueLike

class receive_strategy(Enum):
    '''
    POLL: All queues convey the same type of data. Cycle through queues until one is ready to retrieve data.
    COLLECT: Wait for data from all queues to arrive and label the data by queue.
             Queues can transmit different types of data.
    '''

    POLL = 1
    COLLECT = 2

class send_strategy(Enum):
    '''
    DISPATCH: All queues convey the same type of data. Cycle through queues until one is ready to send data.
    BROADCAST: Send to all queues by name. Queues can transmit different types of data.
    '''

    DISPATCH = 1
    BROADCAST = 2

# TODO: Have workers also create a QT window and event loop in a separate process 
# implement an IPC system between the worker loop and GUI to communicate 
class WorkerNode(ABC):

    def __init__(
            self, 
            name: str, 
            logger: Logger,
            logger_queues: Logger,
            send_block: bool = False,
            send_timeout: Optional[float] = None,
            send_strategy: send_strategy = send_strategy.DISPATCH, 
            receive_block: bool = True,
            receive_timeout: Optional[float] = 10.0,
            receive_strategy: receive_strategy = receive_strategy.POLL,
            profile: bool = False
        ) -> None:
        
        super().__init__()
        self.stop_event = Event()
        self.logger = logger
        self.logger_queues = logger_queues
        self.barrier = None
        self.name = name
        self.iteration = 0
        self.receive_queues = []
        self.receive_queue_names = []
        self.receive_queues_iterator = None
        self.send_queues = []
        self.send_queue_names = []
        self.send_queues_iterator = None
        self.send_block = send_block
        self.send_timeout = send_timeout
        self.send_strategy = send_strategy
        self.receive_block = receive_block
        self.receive_timeout = receive_timeout
        self.receive_queues_iterator = None
        self.receive_strategy = receive_strategy
        self.profile = profile
        self.profiler = cProfile.Profile()
        self.local_logger = self.logger.get_logger(self.name)

    def set_barrier(self, barrier: Barrier) -> None:
        self.barrier = barrier

    def register_receive_queue(self, queue: QueueLike, name: str):
        if queue  not in self.receive_queues:  # should I enforce that?
            self.receive_queues.append(queue)
            self.receive_queue_names.append(name)
            self.receive_queues_iterator = cycle(zip(self.receive_queue_names, self.receive_queues))

    def register_send_queue(self, queue: QueueLike, name: str):
        if queue  not in self.send_queues: # should I enforce that?
            self.send_queues.append(queue)
            self.send_queue_names.append(name)
            self.send_queues_iterator = cycle(zip(self.send_queue_names, self.send_queues))
    
    def main_loop(self):

        self.initialize()
        self.synchronize_workers() 

        while not self.stop_event.is_set():
            t_start = time.perf_counter_ns()
            t0_ns = time.monotonic_ns()
            data = self.receive()
            t1_ns = time.monotonic_ns() 
            results = self.work(data)
            t2_ns = time.monotonic_ns()
            self.send(results)
            t3_ns = time.monotonic_ns()
            self.iteration += 1
            t_stop = time.perf_counter_ns()
            self.log_timings(self.iteration,t_start,t0_ns,t1_ns,t2_ns,t3_ns,t_stop)
            
        self.cleanup()

    def log_timings(self, iteration: int, t_start: int, t0_ns: int, t1_ns: int, t2_ns: int, t3_ns: int, t_stop: int):
        
        t_start_ms = t_start * 1e-6
        receive_time_ms = (t1_ns - t0_ns) * 1e-6
        process_time_ms = (t2_ns - t1_ns) * 1e-6
        send_time_ms = (t3_ns - t2_ns) * 1e-6
        total_time_ms = (t3_ns - t0_ns) * 1e-6
        t_stop_ms = t_stop * 1e-6

        self.local_logger.info(f'''
            #{iteration} ,
            t_start: {t_start_ms},
            receive_time: {receive_time_ms}, 
            process_time: {process_time_ms}, 
            send_time: {send_time_ms},
            total_time: {total_time_ms},
            t_stop: {t_stop_ms}
        ''')

    def initialize(self) -> None:
        '''initialize resources at the beginning of the loop in a new process'''

        # initialize loggers
        self.logger.configure_emitter()
        self.logger_queues.configure_emitter()

        if self.profile:
            self.profiler.enable()

    def synchronize_workers(self) -> None:
        if self.barrier:
            self.barrier.wait()
        print(f'{self.name} initialized. starting work...')


    def cleanup(self) -> None:
        '''cleans resources at the end'''
        if self.profile:
            self.profiler.disable()
            ps = pstats.Stats(self.profiler)
            ps.dump_stats(self.name + '.prof')
        print(f'{self.name} closing...')

    def receive(self) -> Optional[Any]:
        '''receive data'''
        if self.receive_strategy == receive_strategy.COLLECT:
            return self.collect()
        elif self.receive_strategy == receive_strategy.POLL:
            return self.poll()
        
    def collect(self) -> Dict:
        '''Each receive queue must receive data'''

        data = {}
        for name, queue in zip(self.receive_queue_names, self.receive_queues):
            data[name] = queue.get(block=self.receive_block, timeout=self.receive_timeout)
        return data
    
    def poll(self) -> Optional[Any]:
        '''Use if all queues are equivalent. Return data from the first queue that is ready'''

        if self.receive_queues_iterator is not None:

            if self.receive_timeout is None:
                deadline = float('inf')
            else:
                deadline = time.monotonic() + self.receive_timeout

            for name, queue in self.receive_queues_iterator:
                
                if time.monotonic() > deadline:
                    return None
                
                try:
                    return queue.get_nowait()
                except Empty:
                    pass

                # sleep a bit ?

    def send(self, data: Optional[Any]) -> None:
        '''sends data'''

        if data is not None:
            if self.send_strategy == send_strategy.BROADCAST:
                self.broadcast(data)
            elif self.send_strategy == send_strategy.DISPATCH:
                self.dispatch(data)

    def broadcast(self, data_dict: Optional[Dict]) -> None:
        '''send data to all queues with proper names'''

        if data_dict is not None:

            for name, queue in zip(self.send_queue_names, self.send_queues):      
                if name in data_dict:
                    queue.put(data_dict[name], block=self.send_block, timeout=self.send_timeout)

    def dispatch(self, data: Any) -> None:
        '''Use if all queues are equivalent. Send data alternatively to each queue'''

        if self.send_queues_iterator is not None:

            if self.send_timeout is None:
                deadline = float('inf')
            else:
                deadline = time.monotonic() + self.send_timeout

            for name, queue in self.send_queues_iterator:
                
                if time.monotonic() > deadline:
                    return None
                
                try:
                    return queue.put_nowait(data)
                except Full:
                    pass

                # sleep a bit ?

    @abstractmethod
    def work(self, data: Any) -> Any:
        '''does the actual processing'''
        
    def start(self):
        '''start the loop in a separate process'''
        self.process = Process(target = self.main_loop)
        self.process.start()
        
    def stop(self):
        '''stop the loop and join process'''
        self.stop_event.set()
        self.process.join()
    
    def kill(self):
        '''stop the loop and join process'''
        self.stop_event.set()
        self.process.terminate()
