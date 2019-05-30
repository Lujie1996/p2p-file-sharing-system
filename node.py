import grpc
import hashlib
from threading import Thread
import chord_service_pb2
import chord_service_pb2_grpc
from logging import Logger, StreamHandler, Formatter


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
        self.predecessor = None  # (id, addr)
        self.successor = None  # (id, addr)
        self.finger_table = []  # [(key, [successor_id, successor_address(ip:port)])]
        self.logger = self.set_log()

    def set_log(self):
        logger = Logger(-1)
        # logger.addHandler(FileHandler("{}_{}.log".format(PERSISTENT_PATH_PREFIC, self.name)))
        ch = StreamHandler()
        ch.setFormatter(Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(ch)
        logger.setLevel("WARNING")
        return logger

    def set_predecessor(self, id, addr):
        self.predecessor = (id, addr)

    def set_successor(self, id, addr):
        self.successor = (id, addr)

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

    def _join(self):
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
        self.set_successor(response.successorId, response.addr)
        # update the first entry in the finger table
        self.finger_table[0][1][0] = response.successorId
        self.finger_table[0][1][1] = response.addr

        # update the predecessor of the node
        # notify the successor
        with grpc.insecure_channel(response.addr) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            find_predecessor_req = chord_service_pb2.FindPredecessorRequest()
            notify_req = chord_service_pb2.NotifyRequest(predecessorId=self.id, addr=self.addr)
            try:
                find_predecessor_res = stub.find_predecessor(find_predecessor_req, timeout=20)
                if find_predecessor_res is not None:
                    self.predecessor = (find_predecessor_res.predecessorId, find_predecessor_res.addr)
                notify_res = stub.notify(notify_req, timeout=20)
            except Exception:
                self.logger.error("Node#{} error when notify or find_predecessor to {}".format(self.id, response.addr))

    def notify_successor(self):
        # used to contact successor and notify the existence of current node



    # RPC
    def notify(self, request, context):
        if request is None or request.predecessorId is None:
            return chord_service_pb2.NotifyResponse(result=-1)

        if not self.predecessor or (request.predecessorId > self.predecessor[0] and request.predecessorId < self.id):
            self.predecessor = (request.predecessorId, request.addr)
            return chord_service_pb2.NotifyResponse(result=1)

    def init_finger_table_with_nodes_info(self, id_addr_map):
        self.init_finger_table()
        node_identifiers = sorted(id_addr_map.keys())
        id_pos = node_identifiers.index(self.id) # position of this node in the nodes ring
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

    def delete_successor(self):
        # TODO: delete the successor from finger table
        pass

    # RPC
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
                    # self.logger.info("(Node#{})Timeout error when find_successor to {}".format(self.id, next_id))
                    return chord_service_pb2.FindSuccessorResponse(successorId=-1, pathlen=-1, addr=self.addr)

            return response

    def find_successor_local(self, id):
        # Different from find_successor(), this function is not a RPC and it starts to find successor of id
        request = self.generate_find_successor_request(id, 0)

        with grpc.insecure_channel(self.addr) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            try:
                response = stub.find_successor(request, timeout=20)
                return response.successorId, response.addr
            except Exception:
                return -1, str(-1)
                print('find_successor_local() failed at RPC')

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

    # RPC
    def get_predecessor(self, request, context):
        if not self.predecessor:
            # when node does not have a predecessor yet, return the node itself
            return chord_service_pb2.GetPredeccessorResponse(id=self.id, addr=self.addr)
        else:
            return chord_service_pb2.GetPredeccessorResponse(id=self.predecessor[0], addr=self.predecessor[1])

    def get_successors_predecessor(self):
        with grpc.insecure_channel(self.successor[1]) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            try:
                request = chord_service_pb2.GetPredeccessorRequest()
                response = stub.get_predecessor(request, timeout=20)
                return response.id, response.addr
            except Exception:
                print('get_successors_predecessor() failed at RPC')
                return -1, str(-1)


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
            node.set_predecessor(pre_id, id_addr_map[pre_id])
            succ_id = node_identifiers[i + 1] if i < len(node_identifiers) - 1 else node_identifiers[0]
            node.set_successor(succ_id, id_addr_map[succ_id])

            node.init_finger_table_with_nodes_info(id_addr_map)
            node.start()
