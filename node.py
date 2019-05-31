import sys
import grpc
import time
import hashlib
from concurrent import futures
import chord_service_pb2
import chord_service_pb2_grpc
from logging import Logger, StreamHandler, Formatter
from fix_finger import FixFigure
from stabilize import Stabilize


M = 5
_ONE_DAY_IN_SECONDS = 60 * 60 * 24

def get_hash_value(s):
    hash = hashlib.sha1()
    hash.update(str(s).encode())
    # print('hashed value for {} is: {}'.format(s, int(hash.hexdigest(), 16) % (2 ** M)))
    return int(hash.hexdigest(), 16) % (2 ** M)


class Node(chord_service_pb2_grpc.ChordServicer):
    def __init__(self, local_addr, contact_to=None, initial_id_addr_map=None):
        self.addr = local_addr
        self.contact_to = contact_to
        self.id = get_hash_value(local_addr)
        self.predecessor = None  # (id, addr)
        self.successor = None  # (id, addr)
        self.finger_table = []  # [(key, [successor_id, successor_address(ip:port)])]
        self.initial_id_addr_map = initial_id_addr_map
        self.logger = self.set_log()
        self.only_main_thread = True
        self.fix_fingure = FixFigure(self)
        self.stabilize = Stabilize(self)
        self.run()

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
        # initialize the finger table and set the predecessor as well as successor
        self.initialize_with_node_info()
        if not self.only_main_thread:
            self.fix_fingure.start()
            self.stabilize.start()
        print('[node] #{}: finger table: {}'.format(self.id, self.finger_table))

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
        with grpc.insecure_channel(response.addr) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            find_predecessor_req = chord_service_pb2.GetPredecessorRequest()
            try:
                find_predecessor_res = stub.get_predecessor(find_predecessor_req, timeout=20)
                if find_predecessor_res is not None:
                    self.predecessor = (find_predecessor_res.predecessorId, find_predecessor_res.addr)
            except Exception:
                self.logger.error("Node#{} error when find_predecessor to {}".format(self.id, response.addr))

        self.notify_successor()

    def notify_successor(self):
        # used to contact successor and notify the existence of current node
        if self.successor is None:
            return
        with grpc.insecure_channel(self.successor[1]) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            notify_req = chord_service_pb2.NotifyRequest(predecessorId=self.id, addr=self.addr)
            try:
                notify_res = stub.notify(notify_req, timeout=20)
            except Exception:
                self.logger.error("Node#{} rpc error when notify to {}".format(self.id, self.successor[0]))

    # RPC
    def notify(self, request, context):
        if request is None or request.predecessorId is None:
            return chord_service_pb2.NotifyResponse(result=-1)

        if not self.predecessor or (request.predecessorId > self.predecessor[0] and request.predecessorId < self.id):
            self.predecessor = (request.predecessorId, request.addr)
            return chord_service_pb2.NotifyResponse(result=1)

    def initialize_with_node_info(self):
        self.init_finger_table()
        if self.initial_id_addr_map is None:
            return
        node_identifiers = sorted(self.initial_id_addr_map.keys())
        id_pos = node_identifiers.index(self.id)  # position of this node in the nodes ring
        if id_pos == -1:
            return

        # set the predecessor and successor by the initial node
        pre_id = node_identifiers[id_pos - 1] if id_pos > 0 else node_identifiers[-1]
        self.set_predecessor(pre_id, self.initial_id_addr_map[pre_id])
        succ_id = node_identifiers[id_pos + 1] if id_pos < len(node_identifiers) - 1 else node_identifiers[0]
        self.set_successor(succ_id, self.initial_id_addr_map[succ_id])

        # initialize the finger table
        node_size = len(node_identifiers)
        for k in range(node_size):
            node_identifiers.append(node_identifiers[k] + 2 ** M)

        j = id_pos + 1
        for i in range(0, M):
            key = (self.id + 2 ** i) % (2 ** M)
            while j < len(node_identifiers):
                if node_identifiers[j] >= key:
                    successor_id = node_identifiers[j] % (2 ** M)
                    self.update_kth_finger_table_entry(i, successor_id, self.initial_id_addr_map[successor_id])
                    break
                j += 1

    def delete_successor(self):
        # delete the successor from finger table and fill in with the most recent successor after that
        if self.successor is None:
            return -1

        next_suc = None
        suc_id = self.successor[0]
        # find the closest successor
        for i in range(0, M):
            key, suc_info = self.finger_table[i]
            if suc_info[0] is not None and suc_info[0] > suc_id:
                next_suc = suc_info
                break

        for i in range(0, M):
            key, suc_info = self.finger_table[i]
            if suc_info[0] == suc_id:
                # replace this successor with next possible successor, otherwise to set it to be None
                if next_suc is not None:
                    suc_info[0] = next_suc[0]
                    suc_info[1] = next_suc[1]
                else:
                    suc_info[0] = None
                    suc_info[1] = None
            else:
                break
        return 0

    # RPC
    def find_successor(self, request, context):
        if request is None or request.id < 0 or request.pathlen < 0:
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
                # if this RPC is fine, but it fails to call next RPC, the return is -1
            except Exception:
                print('[node] #{}: find_successor_local() failed at RPC'.format(self.id))
                return -2, str(-2)
                # return -2 when this RPC went wrong

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
            return chord_service_pb2.GetPredecessorResponse(id=self.id, addr=self.addr)
        else:
            return chord_service_pb2.GetPredecessorResponse(id=self.predecessor[0], addr=self.predecessor[1])

    def get_successors_predecessor(self):
        with grpc.insecure_channel(self.successor[1]) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            try:
                request = chord_service_pb2.GetPredecessorRequest()
                response = stub.get_predecessor(request, timeout=20)
                return response.id, response.addr
            except Exception as e:
                print('[node] #{} get_successors_predecessor() failed at RPC'.format(self.id))
                print('-------------------------------------------------------------------------------')
                print(str(e))
                print('-------------------------------------------------------------------------------')
                return -1, str(-1)


class LocalChordCluster():
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
            serve(id_addr_map[node_id], id_addr_map)


def serve(addr, id_addr_map):
    print("starting rpc server: {}".format(addr))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=200))
    chord_service_pb2_grpc.add_ChordServicer_to_server(Node(addr, id_addr_map), server)

    server.add_insecure_port(addr)
    try:
        server.start()
    except Exception as e:
        print('Server start failed!')
        print(str(e))
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    addr = sys.argv[1]
    serve(addr)