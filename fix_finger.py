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


class FixFinger(Thread):

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

            self.fix_finger_table()

    def fix_finger_table(self):
        M = self.config['M']
        for i in range(2, M+1):
            # The successor of current node (case i == 1) is not checked, since it can be only checked in stabilizer
            ith_entry_id = (self.node.id + (2 ** (i-1))) % (2 ** M)
            successor_id, successor_addr = self.node.find_successor_local(ith_entry_id)
            if successor_id == -1:
                print('ERROR [fix_finger] #{}: find_successor_local() failed, find_successor returned -1. ith_entry:{} ith_entry_id: {}'
                      .format(self.node.id, i, ith_entry_id))
            elif successor_id == -2:
                print('ERROR [fix_finger] #{}: find_successor_local() failed. find_successor returned -2. ith_entry:{} ith_entry_id: {}'
                      .format(self.node.id, i, ith_entry_id))
            else:
                if successor_id != self.node.finger_table[i-1][1][0]:
                    self.node.update_kth_finger_table_entry(i-1, successor_id, successor_addr)
                    print("[fix_finger] nodeId:{} update the {}th finger table entry {} with {}, {}"
                      .format(self.node.id, i, ith_entry_id, successor_id, successor_addr))
