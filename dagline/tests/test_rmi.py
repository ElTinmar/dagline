from multiprocessing import Process, Event, Queue
from queue import Empty
import time

class A(Process):
    def __init__(self, stop: Event, queue, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.stop = stop
        self.queue = queue

    def hello(self):
        print('hello')

    def handle(self,message):
        if message == 'hello':
            self.hello()

    def run(self):  
        while not self.stop.is_set():
            try:
                message = self.queue.get(block=False)
                self.handle(message)
            except Empty:
                pass
            time.sleep(0.1)
        print('job finished')

def rmi(queue):
    queue.put('hello')

if __name__ == '__main__':

    stop = Event()
    q = Queue()

    a = A(stop,q)
    a.start()
    time.sleep(2)
    rmi(q)
    time.sleep(2)
    stop.set()
    a.join()
