# Run as a background thread to periodically detect the correctness of its successor

from threading import Thread
import random
import time
from utils import *

PRINT = False


class Stabilize(Thread):

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

            try:
                suc_pre_id, suc_pre_addr = self.node.get_successors_predecessor()
                if PRINT:
                    print('[stabilize] {}: get_successors_predecessor() returned {} at {}'.format(self.node.id, suc_pre_id, suc_pre_addr))
            except:
                suc_pre_id = -1

            if suc_pre_id == -1:
                if PRINT:
                    print('ERROR [stabilize] #{}: get_successors_predecessor() failed. Successor itself has failed. Delete it.'
                      .format(self.node.id))
                self.node.remove_chord_node_from_tracker(self.node.successor[1])
                self.node.delete_successor()
                self.node.notify_successor(type='leave')
            elif suc_pre_addr == self.node.successor[0]:
                # successor does not have a predecessor yet, notify it
                self.node.notify_successor(type='join')
            else:
                if suc_pre_id != self.node.id:
                    if PRINT:
                        print('[stabilize] {}: will update successor to {}'.format(self.node.id, suc_pre_id))
                    self.node.update_kth_finger_table_entry(0, suc_pre_id, suc_pre_addr)
                    if PRINT:
                        print('[stabilize] {}: successor has been changed to {}'.format(self.node.id, self.node.successor))
