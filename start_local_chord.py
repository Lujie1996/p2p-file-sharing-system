# import Node
# import Nodes


class Node:
    def __init__(self, local_addr, connect_to):
        print(local_addr, connect_to)

    def start(self):
        pass


class Nodes:
    def __init__(self, addr_list):
        print(addr_list)

    def start(self):
        pass


def start_chord():
    input_str = str(input('Specify N to initialize the system with N nodes, or type \'j\' to join an existing system\n'))
    if input_str == 'j':
        local_addr = str(input('Input the address of current node (IP:Port)\n'))
        dest_addr = str(input('Input the address of an existing node to connect to (IP:Port)\n'))
        if local_addr.count(':') != 1 or dest_addr.count(':') != 1:
            print('Wrong address')
        else:
            Node(local_addr, dest_addr).start()
            print('Node has started at {} connected to {}'.format(local_addr, dest_addr))
    else:
        number_of_nodes = int(input_str)
        if number_of_nodes <= 0:
            print('Start at least one node')
        elif number_of_nodes >= 100:
            print('Too many nodes to start')
        else:
            ip = 'localhost'
            start_port = 50000
            addr_list = list()
            for i in range(number_of_nodes):
                addr = (ip + ':' + str(start_port + i))
                addr_list.append(addr)
            Nodes(addr_list)


if __name__ == '__main__':
    start_chord()

