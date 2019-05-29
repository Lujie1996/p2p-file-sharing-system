from threading import Thread
#from utils import get_hash_value

M = 5
def get_hash_value():
    pass

class Node(Thread):
    def __init__(self, local_addr, connect_to=None):
        super().__init__()
        self.addr = local_addr
        self.connect_to = connect_to
        self.id = get_hash_value(local_addr)
        self.predecessor = None
        self.successor = None
        self.finger_table = [] # [(key, [successor_id, successor_address(ip:port)]]

    def set_predecessor(self, predecessor):
        self.predecessor = predecessor

    def set_successor(self, successor):
        self.successor = successor

    def run(self):
        if len(self.finger_table) == 0:
            self.init_finger_table()

        # TODO: initial the stabilize thread

        # TODO: initial the fix_finger_table thread

    def init_finger_table(self):
        for i in range(0, M):
            key = self.id + 2 ** i
            # initially setting to be -1, ""
            l = [-1, ""]
            self.finger_table.append((key, l))

    def update_kth_finger_table_entry(self, k, successor_id, successor_addr):
        entry = self.finger_table[k]
        entry[1][0] = successor_id
        entry[1][1] = successor_addr

    def init_finger_table_with_nodes_info(self, id_addr_map):
        self.init_finger_table()
        node_identifiers = sorted(id_addr_map.keys())
        id_pos = node_identifiers.find(self.id) # position of this node in the nodes ring
        if id_pos == -1:
            return

        j = id_pos + 1
        for i in range(0, M):
            key = self.id + 2 ** i
            while j < len(node_identifiers):
                if node_identifiers[j] >= key:
                    successor_id = node_identifiers[j]
                    self.update_kth_finger_table_entry(i, successor_id, id_addr_map[successor_id])
                    break
                j += 1


class LocalChordCluster:
    def __init__(self, addr_list, K=5):
        self.addr_list = addr_list
        self.K = K

    def start(self):
        # get the hash value by their addr(ip:port)
        # TODO: need to consider the hash collision situations?
        id_addr_map = dict()
        for addr in self.addr_list:
            node_id = get_hash_value(addr)
            id_addr_map[node_id] = addr
        node_identifiers = sorted(id_addr_map.keys())

        for i, node_id in enumerate(node_identifiers):
            node = Node(node_id, id_addr_map[node_id])
            pre_id = node_identifiers[i - 1] if i > 0 else node_identifiers[-1]
            node.set_predecessor(id_addr_map[pre_id])
            succ_id = node_identifiers[i + 1] if i < len(node_identifiers) - 1 else node_identifiers[0]
            node.set_successor(id_addr_map[succ_id])

            node.init_finger_table_with_nodes_info(id_addr_map)
            node.start()















