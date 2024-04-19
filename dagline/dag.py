from .worker import WorkerNode
from ipc_tools import QueueLike
from multiprocessing import Barrier

# TODO make a dag widget that shows buffer size and FPS in real time ?
#class QueueLikeMonitorWidget(QWidget):
#    pass 

# TODO have ProcessingDAG handle logging and plotting of logs ?

class ProcessingDAG():

    def __init__(self):
        self.nodes = []
        self.edges = []
        

    def connect(self, sender: WorkerNode, receiver: WorkerNode, queue: QueueLike, name: str):
        sender.register_send_queue(queue, name)
        receiver.register_receive_queue(queue, name)

        if sender not in self.nodes:
            self.nodes.append(sender)

        if receiver not in self.nodes:
            self.nodes.append(receiver)

        self.edges.append((sender, receiver, queue, name))

    def start(self):
        # TODO start from leave to root

        barrier = Barrier(len(self.nodes))
        for node in self.nodes:
            node.set_barrier(barrier)
            node.start()

    def stop(self):
        # TODO stop from root to leaves
        for node in self.nodes:
            node.stop()
