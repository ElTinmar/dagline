import time
from numpy.typing import NDArray
import numpy as np
import cv2
from ipc_tools import ObjectRingBuffer2, QueueMP
from dagline import WorkerNode, ProcessingDAG
from typing import Tuple
from PyQt5.QtWidgets import QApplication, QLabel

HEIGHT = 2048
WIDTH = 2048

class Gui(WorkerNode):

    def initialize(self) -> None:
        super().initialize()
        self.app = QApplication([])
        self.label = QLabel()
        self.label.show()

    def process_data(self, data: None) -> NDArray:
        self.app.process_events()

    def process_metadata(self, metadata: None) -> None:
        self.label.setText(metadata)

class Sender(WorkerNode):

    def initialize(self) -> None:
        super().initialize()
        self.index = 0
        self.start_time = time.perf_counter() 

    def process_data(self, data: None) -> NDArray:
        self.index += 1
        timestamp = time.time.perf_counter() - self.start_time
        return (self.index, timestamp, np.random.randint(0,255,(HEIGHT,WIDTH), dtype=np.uint8))
    
    def process_metadata(self, metadata: None) -> str:
        return f'frame #{self.index}'

class Receiver(WorkerNode):

    def initialize(self) -> None:
        super().initialize()
        cv2.namedWindow('receiver')
    
    def cleanup(self) -> None:
        super().cleanup()
        cv2.destroyAllWindows()

    def process_data(self, data: NDArray) -> None:
        cv2.imshow('receiver', data)
        cv2.waitKey(1)

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

    # create workers
    g = Gui()
    s = Sender()
    r = Receiver()

    # create IPC
    q0 = ObjectRingBuffer2(
        num_items = 100,
        data_type = dt_uint8_gray,
        serialize = serialize_image,
        deserialize = deserialize_image,
        name = 'images'
    )
    q1 = QueueMP()

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
        name='gui_queue'
    )

    #run DAG
    dag.start()
    time.sleep(10)
    dag.stop()