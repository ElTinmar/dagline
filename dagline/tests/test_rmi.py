from multiprocessing import Process, Event, Queue
from queue import Empty
import time

class A(Process):
    def __init__(self, stop: Event, queue, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.stop = stop
        self.queue = queue

    def hello(self, who, punctuation: str):
        print('hello' + who + punctuation)

    def handle(self, message, args, kwargs):
        if message == 'hello':
            self.hello(*args, **kwargs)

    def run(self):  
        while not self.stop.is_set():
            try:
                message, arg, kwargs = self.queue.get(block=False)
                self.handle(message, arg, kwargs)
            except Empty:
                pass
            time.sleep(0.1)
        print('job finished')

def rmi(queue):
    method = 'hello'
    args = [' world']
    kwargs = {'punctuation': '!'}
    message = (method,args,kwargs)
    queue.put(message)

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
