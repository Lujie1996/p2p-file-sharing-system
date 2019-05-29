import sys
from threading import Thread
import random
import chord_service_pb2
import chord_service_pb2_grpc
import grpc
import hashlib

M = 5

def get_hash_value(s):
    hash = hashlib.sha3_256()
    hash.update(s.encode())
    return int(hash.hexdigest()) % (2 ** M)


class Node(Thread):
    def __init__(self, local_addr, contact_to=None):
        super().__init__()
        self.addr = local_addr
        self.contact_to = contact_to
        self.id = get_hash_value(local_addr)
        self.predecessor = None
        self.successor = None
        self.finger_table = [] # [(key, [successor_id, successor_address(ip:port)])]

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

    def join(self):
        # join in a chord ring by connecting to the contact_to node
        if not self.contact_to:
            return

        with grpc.insecure_channel(self.contact_to) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            new_request = self.generate_find_successor_request(self.id, 0)
            try:
                # TODO: read timeout value from config.txt
                response = stub.find_successor(new_request, timeout=20)
                self.join_in_chord_ring(response)
            except Exception:
                self.logger.info("(Node#{})Timeout error when find_successor to {}".format(self.id, self.contact_to))

    def join_in_chord_ring(self, response):
        self.set_successor(response.addr)
        # update the first entry in the finger table
        self.finger_table[0][1][0] = response.id
        self.finger_table[0][1][1] = response.addr

        # TODO: update the predecessor of the node

        # TODO: notify the successor


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

    def find_successor(self, request, context):
        if request is None:
            return chord_service_pb2.FindSuccessorResponse(successorId=-1, pathlen=-1, addr=self.addr)

        if request.id == self.id:
            return chord_service_pb2.FindSuccessorResponse(successorId=self.id, pathlen=request.pathlen, addr=self.addr)
        elif self.id < request.id <= self.successor:
            return chord_service_pb2.FindSuccessorResponse(successorId=self.successor, pathlen=request.pathlen+1, addr=self.addr)
        else:
            next_id, next_address = self.closest_preceding_node(request.id)
            if self.id == next_id:  # There is only one node in chord ring
                return chord_service_pb2.FindSuccessorResponse(successorId=self.id, pathlen=request.pathlen, addr=self.addr)

            with grpc.insecure_channel(next_address) as channel:
                stub = chord_service_pb2_grpc.ChordStub(channel)
                new_request = self.generate_find_successor_request(request.id, request.pathlen + 1)
                try:
                    response = stub.find_successor(new_request, timeout=20)
                except Exception:
                    #self.logger.info("(Node#{})Timeout error when find_successor to {}".format(self.id, next_id))
                    return chord_service_pb2.FindSuccessorResponse(successorId=-1, pathlen=-1, addr=self.addr)

            return response

    def closest_preceding_node(self, id):
        i = 0
        while i < len(self.finger_table) - 1:
            if id <= self.finger_table[i][1][0]:
                break
            if self.id >= self.finger_table[i][1][0]:
                if id <= self.finger_table[i][1][0] + 2 ** M:
                    break
            i += 1

        return self.finger_table[i][1]

    def generate_find_successor_request(self, id, pathlen):
        request = chord_service_pb2.FindSuccessorRequest()
        request.id = id
        request.pathlen = pathlen
        return request


class LocalChordCluster:
    def __init__(self, addr_list):
        self.addr_list = addr_list

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
