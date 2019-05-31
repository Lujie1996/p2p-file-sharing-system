from node import Node, LocalChordCluster
import hashlib


M = 5


def get_hash_value(s):
    hash = hashlib.sha1()
    hash.update(str(s).encode())
    # print('hash value of {} is: {}'.format(s, int(hash.hexdigest(), 16) % (2 ** M)))
    return int(hash.hexdigest(), 16) % (2 ** M)


def get_unique_addr_list(n):
    unique = dict()
    ip = 'localhost'
    port = 50000

    while len(unique) != n:
        addr = (ip + ':' + str(port))
        port += 1
        hashed = get_hash_value(addr)
        if hashed not in unique:
            unique[hashed] = addr
    return unique


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
            addr_list = list(get_unique_addr_list(number_of_nodes).values())
            print(addr_list)
            LocalChordCluster(addr_list).start()


if __name__ == '__main__':
    start_chord()

