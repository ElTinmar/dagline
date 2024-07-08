from multiprocessing import Process, Event, Queue
from queue import Empty
import time

class A(Process):
    def __init__(self, stop: Event, queue_in, queue_out, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.stop = stop
        self.queue_in = queue_in
        self.queue_out = queue_out

    def hello(self, who, punctuation: str):
        print('hello' + who + punctuation)

    def handle(self, message, args, kwargs):
        if message == 'hello':
            self.hello(*args, **kwargs)
            self.queue_out.put('OK')

    def run(self):  
        while not self.stop.is_set():
            try:
                message, arg, kwargs = self.queue_in.get(block=False)
                self.handle(message, arg, kwargs)
            except Empty:
                pass
            time.sleep(0.1)
        print('job finished')

def rmi(queue_in, queue_out):
    method = 'hello'
    args = [' world']
    kwargs = {'punctuation': '!'}
    message = (method,args,kwargs)
    queue_in.put(message)
    print(queue_out.get())

if __name__ == '__main__':

    stop = Event()
    q_in = Queue()
    q_out = Queue()

    a = A(stop,q_in,q_out)
    a.start()
    time.sleep(2)
    rmi(q_in,q_out)
    time.sleep(2)
    stop.set()
    a.join()
