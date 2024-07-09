import time
from numpy.typing import NDArray
import numpy as np
import cv2
from ipc_tools import ObjectRingBuffer2, QueueMP
from dagline import WorkerNode, ProcessingDAG
from typing import Tuple, Dict, Optional
from PyQt5.QtWidgets import QApplication, QLabel, QWidget, QPushButton, QHBoxLayout
from multiprocessing_logger import Logger

HEIGHT = 2048
WIDTH = 2048

class Gui(WorkerNode):

    def initialize(self) -> None:
        super().initialize()
        self.app = QApplication([])
        self.window = QWidget()
        self.label = QLabel()
        self.button = QPushButton("Black")
        self.button.setCheckable(True)
        self.button.toggled.connect(self.on_press)
        self.toggled = False
        self.layout = QHBoxLayout(self.window)
        self.layout.addWidget(self.label)
        self.layout.addWidget(self.button)
        self.window.show()

    def on_press(self):
        self.toggled = True

    def process_data(self, data: None) -> NDArray:
        self.app.processEvents()

    def process_metadata(self, metadata: Dict) -> Optional[Dict]:
        # receive
        text = metadata['gui_info']
        if text:
            self.label.setText(text)

        # send only one message when button is toggled, otherwise queue gets full
        if self.toggled:
            res = {}
            res['gui_command'] = self.button.isChecked()
            self.toggled = False
            return res       
        else:
            return None

class Sender(WorkerNode):

    def __init__(self, fps:int=30, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.index = 0
        self.state = False
        self.prev_time = 0
        self.fps = fps

    def initialize(self) -> None:
        super().initialize()
        self.start_time = time.perf_counter() 

    def process_data(self, data: None) -> NDArray:
        self.index += 1
        if self.state:
            image = np.zeros((HEIGHT,WIDTH), dtype=np.uint8)
        else:
            image = np.random.randint(0,255,(HEIGHT,WIDTH), dtype=np.uint8)
        timestamp = time.perf_counter() - self.start_time
        while timestamp-self.prev_time < 1/self.fps:
            timestamp = time.perf_counter() - self.start_time
            time.sleep(0.01)
        self.prev_time = timestamp
        return (self.index, timestamp, image)
    
    def process_metadata(self, metadata: Dict) -> Dict:
        # receive
        state = metadata['gui_command']
        if state is not None: 
            self.state = state

        # send
        res = {}
        res['gui_info'] = f'frame #{self.index}' 
        return res

class Receiver(WorkerNode):

    def initialize(self) -> None:
        super().initialize()
        cv2.namedWindow('receiver')
    
    def cleanup(self) -> None:
        super().cleanup()
        cv2.destroyAllWindows()

    def process_data(self, data: NDArray) -> None:
        index, timestamp, image = data
        cv2.imshow('receiver', image)
        cv2.waitKey(1)

    def process_metadata(self, metadata: None) -> None:
        pass
    
dt_uint8_gray = np.dtype([
    ('index', int, (1,)),
    ('timestamp', float, (1,)), 
    ('image', np.uint8, (HEIGHT,WIDTH))
])

def serialize_image(buffer: NDArray, obj: Tuple[int, float, NDArray]) -> None:
    index, timestamp, image = obj 
    buffer['index'] = index
    buffer['timestamp'] = timestamp
    buffer['image'] = image

def deserialize_image(arr: NDArray) -> Tuple[int, float, NDArray]:
    index = arr['index'].item()
    timestamp = arr['timestamp'].item()
    image = arr[0]['image']
    return (index, timestamp, image)

if __name__ == '__main__':

    worker_logger = Logger('workers.log', Logger.INFO)
    queue_logger = Logger('queues.log', Logger.INFO)

    # create workers
    g = Gui(name='gui', logger=worker_logger, logger_queues=queue_logger)
    s = Sender(fps=60, name='sender', logger=worker_logger, logger_queues=queue_logger)
    r = Receiver(name='receiver', logger=worker_logger, logger_queues=queue_logger)

    # create IPC
    q0 = ObjectRingBuffer2(
        num_items = 100,
        data_type = dt_uint8_gray,
        serialize = serialize_image,
        deserialize = deserialize_image,
        name = 'images'
    )
    q1 = QueueMP()
    q2 = QueueMP()

    # create DAG
    dag = ProcessingDAG()
    dag.connect_data(
        sender=s,
        receiver=r,
        queue=q0,
        name='image_queue'
    )
    dag.connect_metadata(
        sender=s,
        receiver=g,
        queue=q1,
        name='gui_info'
    )
    dag.connect_metadata(
        sender=g,
        receiver=s,
        queue=q2,
        name='gui_command'
    )

    #run DAG
    #worker_logger.start()
    #queue_logger.start()
    dag.start()
    time.sleep(20)
    dag.kill()
    #worker_logger.stop()
    #queue_logger.stop()