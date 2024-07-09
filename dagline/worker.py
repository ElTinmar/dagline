from abc import ABC, abstractmethod
from dataclasses import dataclass
from multiprocessing import Event, Process, Barrier
from typing  import Any, Optional, Dict, Iterator
import time
from itertools import cycle
from queue import Empty, Full
from enum import Enum
import cProfile
import pstats
from multiprocessing_logger import Logger
from ipc_tools import QueueLike

@dataclass
class Timing:
    start_absolute_ns: int = 0
    start_relative_ns: int = 0
    receive_data_relative_ns: int = 0
    process_data_relative_ns: int = 0
    send_data_relative_ns: int = 0
    receive_metadata_relative_ns: int = 0
    process_metadata_relative_ns: int = 0
    send_metadata_relative_ns: int = 0
    stop_absolute_ns: int = 0

    @property
    def t_start_ms(self):
        return self.start_absolute_ns * 1e-6

    @property
    def t_stop_ms(self):
        return self.stop_absolute_ns * 1e-6
    
    @property
    def receive_data_time_ms(self):
        return (self.receive_data_relative_ns - self.start_relative_ns) * 1e-6
    
    @property
    def process_data_time_ms(self):
        return (self.work_relative_ns - self.receive_data_relative_ns) * 1e-6
    
    @property
    def send_data_time_ms(self):
        return (self.send_data_relative_ns - self.work_relative_ns) * 1e-6

    @property
    def receive_metadata_time_ms(self):
        return (self.send_data_relative_ns - self.work_relative_ns) * 1e-6
    
    @property
    def process_metadata_time_ms(self):
        return (self.send_data_relative_ns - self.work_relative_ns) * 1e-6
    
    @property
    def send_metadata_time_ms(self):
        return (self.send_data_relative_ns - self.work_relative_ns) * 1e-6
    
    @property
    def total_time_ms(self):
        return (self.stop_absolute_ns - self.start_absolute_ns) * 1e-6
    
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
    BROADCAST = 2 # TODO this is maybe a bad name, think of something else

#TODO: data and metadata methods share a lot of duplicated code. Can I do better without 
# sacrificing readability ? 
class WorkerNode(ABC):

    def __init__(
            self, 
            name: str, 
            logger: Logger,
            logger_queues: Logger,
            send_data_block: bool = False,
            send_data_timeout: Optional[float] = None,
            send_data_strategy: send_strategy = send_strategy.DISPATCH, 
            receive_data_block: bool = True, # TODO maybe wether to block and timeout duration should be defined on a per queue basis
            receive_data_timeout: Optional[float] = 10.0,
            receive_data_strategy: receive_strategy = receive_strategy.POLL,
            send_metadata_block: bool = False,
            send_metadata_timeout: Optional[float] = None,
            send_metadata_strategy: send_strategy = send_strategy.BROADCAST, 
            receive_metadata_block: bool = False,
            receive_metadata_timeout: Optional[float] = None,
            receive_metadata_strategy: receive_strategy = receive_strategy.COLLECT,
            profile: bool = False
        ) -> None:
        
        super().__init__()
        self.stop_event = Event()
        self.barrier = None
        self.name = name
        self.iteration = 0

        self.logger = logger
        self.logger_queues = logger_queues
        self.local_logger = self.logger.get_logger(self.name)

        self.receive_data_queues = []
        self.receive_data_queue_names = []
        self.receive_data_queues_iterator = None
        self.receive_data_block = receive_data_block
        self.receive_data_timeout = receive_data_timeout
        self.receive_data_strategy = receive_data_strategy

        self.send_data_queues = []
        self.send_data_queue_names = []
        self.send_data_queues_iterator = None
        self.send_data_block = send_data_block
        self.send_data_timeout = send_data_timeout
        self.send_data_strategy = send_data_strategy

        self.receive_metadata_queues = []
        self.receive_metadata_queue_names = []
        self.receive_metadata_queues_iterator = None
        self.receive_metadata_block = receive_metadata_block
        self.receive_metadata_timeout = receive_metadata_timeout
        self.receive_metadata_strategy = receive_metadata_strategy

        self.send_metadata_queues = []
        self.send_metadata_queue_names = []
        self.send_metadata_queues_iterator = None
        self.send_metadata_block = send_metadata_block
        self.send_metadata_timeout = send_metadata_timeout
        self.send_metadata_strategy = send_metadata_strategy

        self.profile = profile
        self.profiler = cProfile.Profile()

    def set_barrier(self, barrier: Barrier) -> None:
        self.barrier = barrier

    def register_receive_data_queue(self, queue: QueueLike, name: str):
        if queue not in self.receive_data_queues:  # should I enforce that?
            self.receive_data_queues.append(queue)
            self.receive_data_queue_names.append(name)
            self.receive_data_queues_iterator = cycle(zip(self.receive_data_queue_names, self.receive_data_queues))

    def register_send_data_queue(self, queue: QueueLike, name: str):
        if queue not in self.send_data_queues: # should I enforce that?
            self.send_data_queues.append(queue)
            self.send_data_queue_names.append(name)
            self.send_data_queues_iterator = cycle(zip(self.send_data_queue_names, self.send_data_queues))

    def register_receive_metadata_queue(self, queue: QueueLike, name: str):
        if queue not in self.receive_metadata_queues:  # should I enforce that?
            self.receive_metadata_queues.append(queue)
            self.receive_metadata_queue_names.append(name)
            self.receive_metadata_queues_iterator = cycle(zip(self.receive_metadata_queue_names, self.receive_metadata_queues))

    def register_send_metadata_queue(self, queue: QueueLike, name: str):
        if queue not in self.send_metadata_queues: # should I enforce that?
            self.send_metadata_queues.append(queue)
            self.send_metadata_queue_names.append(name)
            self.send_metadata_queues_iterator = cycle(zip(self.send_metadata_queue_names, self.send_metadata_queues))

    def main_loop(self):

        self.initialize()
        self.synchronize_workers() 

        timing = Timing()

        while not self.stop_event.is_set():
            self.iteration += 1

            ## START TIMER ----------------------------------------------
            timing.start_absolute_ns = time.perf_counter_ns()
            timing.start_relative_ns = time.monotonic_ns()

            ## DATA -----------------------------------------------------
            data = self.receive()
            timing.receive_data_relative_ns = time.monotonic_ns() 

            results = self.work(data)
            timing.process_data_relative_ns = time.monotonic_ns()

            self.send(results)
            timing.send_data_relative_ns = time.monotonic_ns()

            ## METADATA --------------------------------------------------
            metadata = self.receive_metadata()
            timing.receive_metadata_relative_ns = time.monotonic_ns()

            results_md = self.process_metadata(metadata)
            timing.process_metadata_relative_ns = time.monotonic_ns()

            self.send_metadata(results_md)
            timing.send_metadata_relative_ns = time.monotonic_ns()

            ## STOP TIMER -------------------------------------------------
            timing.stop_absolute_ns = time.perf_counter_ns()

            ## LOG TIMINGS ------------------------------------------------
            self.log_timings(self.iteration,timing)
            
        self.cleanup()

    def log_timings(self, iteration: int, timing: Timing):

        self.local_logger.info(f'''
            #{iteration} ,
            t_start: {timing.t_start_ms},
            receive_data_time: {timing.receive_data_time_ms}, 
            process_data_time: {timing.process_data_time_ms}, 
            send_data_time: {timing.send_data_time_ms},
            receive_metadata_time: {timing.receive_metadata_time_ms}, 
            process_metadata_time: {timing.process_metadata_time_ms}, 
            send_metadata_time: {timing.send_metadata_time_ms},
            total_time: {timing.total_time_ms},
            t_stop: {timing.t_stop_ms}
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
        if self.receive_data_strategy == receive_strategy.COLLECT:
            return self.collect(
                self.receive_data_queue_names,
                self.receive_data_queues,
                self.receive_data_block,
                self.receive_data_timeout
            )
        elif self.receive_data_strategy == receive_strategy.POLL:
            return self.poll(
                self.receive_data_queues_iterator,
                self.receive_data_timeout
            )

    def receive_metadata(self) -> Optional[Any]:
        '''receive metadata'''
        if self.receive_metadata_strategy == receive_strategy.COLLECT:
            return self.collect(
                self.receive_metadata_queue_names,
                self.receive_metadata_queues,
                self.receive_metadata_block,
                self.receive_metadata_timeout
            )
        elif self.receive_metadata_strategy == receive_strategy.POLL:
            return self.poll(
                self.receive_metadata_queues_iterator,
                self.receive_metadata_timeout
            )
    
    # static method    
    def collect(
            self,
            receive_queue_names: list, 
            receive_queues: list, 
            receive_block:bool, 
            receive_timeout: Optional[float]
        ) -> Dict:
        '''Each receive queue must receive data'''

        data = {}
        for name, queue in zip(receive_queue_names, receive_queues):
            data[name] = queue.get(block=receive_block, timeout=receive_timeout)
        return data
    
    # static method
    def poll(
            self,
            receive_queues_iterator: Optional[Iterator],
            receive_timeout: Optional[float],

        ) -> Optional[Any]:
        '''Use if all queues are equivalent. Return data from the first queue that is ready'''

        if receive_queues_iterator is not None:

            if receive_timeout is None:
                deadline = float('inf')
            else:
                deadline = time.monotonic() + receive_timeout

            for name, queue in receive_queues_iterator:
                
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
            if self.send_data_strategy == send_strategy.BROADCAST:
                self.broadcast(
                    data,
                    self.send_data_queue_names,
                    self.send_data_queues,
                    self.send_data_block,
                    self.send_data_timeout
                    )
            elif self.send_data_strategy == send_strategy.DISPATCH:
                self.dispatch(
                    data,
                    self.send_data_queues_iterator,
                    self.send_data_timeout
                )

    def send_metadata(self, metadata: Optional[Any]) -> None:
        '''sends data'''

        if metadata is not None:
            if self.send_metadata_strategy == send_strategy.BROADCAST:
                self.broadcast(
                    metadata,
                    self.send_metadata_queue_names,
                    self.send_metadata_queues,
                    self.send_metadata_block,
                    self.send_metadata_timeout
                    )
            elif self.send_metadata_strategy == send_strategy.DISPATCH:
                self.dispatch(
                    metadata,
                    self.send_metadata_queues_iterator,
                    self.send_metadata_timeout
                )

    # static method
    def broadcast(
            self, 
            data_dict: Optional[Dict],
            send_queue_names: list,
            send_queues: list,
            send_block: bool,
            send_timeout: Optional[float]
        ) -> None:
        '''send data to all queues with proper names'''

        if data_dict is not None:

            for name, queue in zip(send_queue_names, send_queues):      
                if name in data_dict:
                    queue.put(data_dict[name], block=send_block, timeout=send_timeout)

    # static method
    def dispatch(
            self, 
            data: Any,
            send_queues_iterator: Optional[Iterator],
            send_timeout: Optional[bool]
        ) -> None:
        '''Use if all queues are equivalent. Send data alternatively to each queue'''

        if send_queues_iterator is not None:

            if send_timeout is None:
                deadline = float('inf')
            else:
                deadline = time.monotonic() + send_timeout

            for name, queue in send_queues_iterator:
                
                if time.monotonic() > deadline:
                    return None
                
                try:
                    return queue.put_nowait(data)
                except Full:
                    pass

                # sleep a bit ?

    @abstractmethod
    def process_data(self, data: Any) -> Any:
        '''does the actual processing'''

    @abstractmethod
    def process_metadata(self, metadata: Any) -> Any:
        '''handles and generate metadata'''
        
    def start(self):
        '''start the loop in a separate process'''
        self.process = Process(target = self.main_loop)
        self.process.start()
        
    def stop(self):
        '''stop the loop and join process'''
        self.stop_event.set()
        self.process.join() # this may hang if queues are not empty
    
    def kill(self):
        '''stop the loop and join process'''
        self.stop_event.set()
        self.process.terminate() # stop even if queues are not empty
