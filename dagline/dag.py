from .worker import WorkerNode
from ipc_tools import QueueLike, MonitoredQueue, ModifiableRingBuffer
from multiprocessing import Barrier

class ProcessingDAG():

    def __init__(self):
        self.nodes = []
        self.data_edges = []
        self.metadata_edges = []

    def add_node(self, node: WorkerNode):
        '''add isolated node'''
        self.nodes.append(node)

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
        barrier = Barrier(len(self.nodes)+1)

        for node in self.nodes:
            node.set_barrier(barrier)
            print(f'starting node {node.name}')
            node.start()

        barrier.wait()

    def stop(self):
        for node in self.nodes:
            print(f'stopping node {node.name}')
            node.stop()
        
        for sender, receiver, queue, name in self.data_edges:
            if isinstance(queue, MonitoredQueue):
                base_queue = queue.queue
                if isinstance(base_queue, ModifiableRingBuffer):
                    print(f"Name: {name}, freq: {queue.get_average_freq()}, lost: {base_queue.num_lost_item.value}")
                else:
                    print(f"Name: {name}, freq: {queue.get_average_freq()}")


    def kill(self):
        # TODO stop from root to leave
        for node in self.nodes:
            print(f'killing node {node.name}')
            node.kill()
