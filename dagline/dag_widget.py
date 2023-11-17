from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtWidgets import QProgressBar, QHBoxLayout, QVBoxLayout, QWidget, QPushButton
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
        pass

    def layout_components(self):
        pass

    def main(self):
        pass
