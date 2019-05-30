# Run as a background thread to periodically check the correctness of current node's finger table

from threading import Thread
import time
import random


def parse_config():
    config = dict()
    config['interval_upper_bound'] = 4
    config['interval_lower_bound'] = 2
    config['M'] = 5
    return config


class FixFigure(Thread):

    def __init__(self, node):
        Thread.__init__(self)
        self.node = node
        self.config = parse_config()

    def run(self):
        while True:
            self.fix_finger_table()
            low = self.config['interval_lower_bound']
            high = self.config['interval_upper_bound']
            sleep_time = random.randint(low * 1000, high * 1000) / 1000.0
            time.sleep(sleep_time)

    def fix_finger_table(self):
        M = self.config['M']
        for i in range(2, M + 1):
            # The successor of current node (case i == 1) is not checked, since it can be only checked in stabilizer
            ith_entry_id = (self.node.id + (2 ** (i-1))) % (2 ** M)
            successor_id, successor_addr = self.node.find_successor_local(ith_entry_id)
            if successor_id == -1:
                print('{}: [fix_fingure]: find_successor_local() failed. ith_entry_id: {}'.format(self.node.id, ith_entry_id))
            else:
                self.node.update_kth_finger_table_entry(successor_id, successor_addr)