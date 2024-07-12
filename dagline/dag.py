from .worker import WorkerNode
from ipc_tools import QueueLike
from multiprocessing import Barrier

class ProcessingDAG():

    def __init__(self):
        self.nodes = []
        self.data_edges = []
        self.metadata_edges = []

    def connect_data(self, sender: WorkerNode, receiver: WorkerNode, queue: QueueLike, name: str):
        sender.register_send_data_queue(queue, name)
        receiver.register_receive_data_queue(queue, name)

        if sender not in self.nodes:
            self.nodes.append(sender)

        if receiver not in self.nodes:
            self.nodes.append(receiver)

        self.data_edges.append((sender, receiver, queue, name))

    def connect_metadata(self, sender: WorkerNode, receiver: WorkerNode, queue: QueueLike, name: str):
        sender.register_send_metadata_queue(queue, name)
        receiver.register_receive_metadata_queue(queue, name)

        if sender not in self.nodes:
            self.nodes.append(sender)

        if receiver not in self.nodes:
            self.nodes.append(receiver)

        self.metadata_edges.append((sender, receiver, queue, name))

    def start(self):
        # TODO start from leave to root

        print(self.nodes)
        print(self.data_edges)
        print(self.metadata_edges)

        barrier = Barrier(len(self.nodes))
        for node in self.nodes:
            node.set_barrier(barrier)
            print(f'starting node {node.name}')
            node.start()

    def stop(self):
        # TODO stop from root to leaves
        for node in self.nodes:
            print(f'stopping node {node.name}')
            node.stop()

    def kill(self):
        # TODO stop from root to leaves
        for node in self.nodes:
            print(f'stopping node {node.name}')
            node.kill()