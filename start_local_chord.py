from node import LocalChordCluster, serve_join
from utils import *
import tracker
import threading
import time


def start_chord():
    input_str = str(input('Specify N to initialize the system with N nodes, or type \'j\' to join an existing system\n'))
    if input_str == 'j':
        print('Here is a list of addresses that will not conflict with each other in Chord ring')
        unique_addr = get_unique_addr_list(15)
        for i in range(15):
            print('{}: {}'.format(i, unique_addr[i]))

        local_addr = str(input('Select the address of current node by index\n'))
        dest_addr = str(input('Select the address of an existing node to connect to by index\n'))
        local_addr = unique_addr[int(local_addr)]
        dest_addr = unique_addr[int(dest_addr)]

        serve_join(local_addr, dest_addr)
        print('Node has started at {} connected to {}'.format(local_addr, dest_addr))
    else:
        # start tracker server
        tracker_server = threading.Thread(target=tracker.start_server, args=(TRACKER_ADDR,))
        tracker_server.start()

        time.sleep(1)
        print('Starting chord nodes...')

        number_of_nodes = int(input_str)
        if number_of_nodes <= 0:
            print('Start at least one node')
        elif number_of_nodes >= 100:
            print('Too many nodes to start')
        else:
            addr_list = get_unique_addr_list(number_of_nodes)
            print(addr_list)
            LocalChordCluster(addr_list).start()


if __name__ == '__main__':
    start_chord()

