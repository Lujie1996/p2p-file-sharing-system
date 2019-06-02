# Run as a background thread to periodically check the correctness of successor's data

from threading import Thread
from utils import *
import time
import random

PRINT = False

class Checker(Thread):
    def __init__(self, node):
        Thread.__init__(self)
        self.node = node
        self.config = parse_config()

    def run(self):
        while True:
            low = self.config['interval_lower_bound']
            high = self.config['interval_upper_bound']
            sleep_time = random.randint(low * 1000, high * 1000) / 1000.0
            time.sleep(sleep_time)

            if len(self.node.storage) != 0:
                self.node.check_local()