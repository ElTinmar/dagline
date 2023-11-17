from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtWidgets import QProgressBar, QHBoxLayout, QVBoxLayout, QWidget, QPushButton, QLabel
from .dag import ProcessingDAG

class DAGWidget(QWidget):

    def __init__(self, dag: ProcessingDAG, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.dag = dag
        self.declare_components()
        self.layout_components()

        self.timer = QTimer()
        self.timer.timeout.connect(self.main)
        self.timer.setInterval(33)
        self.timer.start()

    def declare_components(self):
        self.start_button = QPushButton('start', self)
        self.start_button.clicked.connect(self.dag.start)

        self.stop_button = QPushButton('stop', self)
        self.stop_button.clicked.connect(self.dag.stop)

        self.progress_bars = []
        self.sender_name = []
        self.receiver_name = []
        self.queue_name = []
        self.fps_in_label = []
        self.fps_out_label = []

        for sender, receiver, queue, name in self.dag.edges:
            
            sender_name = QLabel()
            sender_name.setText(sender.name)
            pbar = QProgressBar(self)
            receiver_name = QLabel()
            receiver_name.setText(receiver.name)
            queue_name = QLabel()
            fps_in = QLabel()
            fps_out = QLabel()

            self.sender_name.append(sender_name) 
            self.progress_bars.append(pbar)
            self.receiver_name.append(receiver_name)
            self.fps_in_label.append(fps_in)
            self.fps_out_label.append(fps_out)
            self.queue_name.append(queue_name)

    def layout_components(self):

        mainlayout = QVBoxLayout(self)

        buttonlayout = QHBoxLayout()
        buttonlayout.addWidget(self.start_button)
        buttonlayout.addWidget(self.stop_button)

        mainlayout.addLayout(buttonlayout)

        for i in range(len(self.dag.edges)):
            row = QHBoxLayout()
            row.addWidget(self.queue_name[i])
            row.addWidget(self.sender_name[i])
            row.addWidget(self.fps_in_label[i])
            row.addWidget(self.progress_bars[i])
            row.addWidget(self.fps_out_label[i])
            row.addWidget(self.receiver_name[i])
            
            mainlayout.addLayout(row)

    def main(self):
        for i in range(len(self.dag.edges)):
            sender, receiver, queue, name = self.dag.edges[i]
            fps_in, fps_out = queue.get_freqs()
            self.progress_bars[i].setValue(queue.qsize())
            self.fps_in_label[i].setText(str(fps_in))
            self.fps_out_label[i].setText(str(fps_out))

