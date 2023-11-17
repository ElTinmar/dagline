from .worker import WorkerNode
from ipc_tools import QueueLike

# TODO make a dag widget that shows buffer size and FPS in real time ?

class DAG():

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
        for node in self.nodes:
            node.start()

    def stop(self):
        # TODO stop from root to leaves
        for node in self.nodes:
            node.kill()