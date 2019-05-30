# Run as a background thread to periodically detect the correctness of its successor

from threading import Thread
import random
import time


def parse_config():
    config = dict()
    config['interval_upper_bound'] = 4
    config['interval_lower_bound'] = 2
    config['M'] = 5
    return config


class Stabilize(Thread):

    def __init__(self, node):
        Thread.__init__(self)
        self.node = node
        self.config = parse_config()

    def run(self):
        while True:
            suc_pre_id, suc_pre_addr = self.node.get_successors_predecessor()
            if suc_pre_id == -1:
                print('{}: [stabilize]: get_successors_predecessor() failed. Successor failed to call other RPC'
                      .format(self.node.id))
            elif suc_pre_id == -2:
                print('{}: [stabilize]: get_successors_predecessor() failed. Successor itself has failed'
                      .format(self.node.id))
                self.node.delete_successor()
            elif suc_pre_addr == self.node.successor[0]:
                # successor does not have a predecessor yet, notify it
                self.node.notify_successor()
            else:
                if suc_pre_id != self.node.id:
                    self.node.update_kth_finger_table_entry(1, suc_pre_id, suc_pre_addr)

            low = self.config['interval_lower_bound']
            high = self.config['interval_upper_bound']
            sleep_time = random.randint(low * 1000, high * 1000) / 1000.0
            time.sleep(sleep_time)
