import sys
import grpc
import time
import threading
from concurrent import futures
import chord_service_pb2
import chord_service_pb2_grpc
import p2p_service_pb2
import p2p_service_pb2_grpc
from logging import Logger, StreamHandler, Formatter
from fix_finger import FixFinger
from stabilize import Stabilize
from checker import Checker
from utils import *


_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_R = 4


class Node(chord_service_pb2_grpc.ChordServicer):
    def __init__(self, local_addr, contact_to, initial_id_addr_map=None):
        self.addr = local_addr
        self.contact_to = contact_to
        self.id = get_hash_value(local_addr)
        self.predecessor = None  # (id, addr)
        self.successor = None  # (id, addr)
        self.finger_table = []  # [(key, [successor_id, successor_address(ip:port)])]
        self.initial_id_addr_map = initial_id_addr_map
        self.logger = self.set_log()
        self.only_main_thread = False # debug use
        self.fix_finger = FixFinger(self)
        self.stabilize = Stabilize(self)
        self.checker = Checker(self)
        self.storage = dict() # key:[len, seq_num, [addrs]] seq_num increases by 1 everytime update
        self.storage_lock = threading.Lock()
        self.tracker_addr = TRACKER_ADDR
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
        if self.initial_id_addr_map is not None:
            self.initialize_with_node_info()
        else:
            self._join()

        if not self.only_main_thread:
            self.fix_finger.start()
            self.stabilize.start()
            self.checker.start()
        print('[node] #{}: finger table: {}; successor: {}; predecessor: {}'
              .format(self.id, self.finger_table, self.successor, self.predecessor))

    def get_get_request(self, keys_to_fetch):
        get_req = chord_service_pb2.GetRequest()
        for key in keys_to_fetch:
            get_req.keys.append(key)
        return get_req

    def fetch_data_from_predecessor(self, data_to_fetch):
        # print('nodeId{} fetches data from its predecessorId{}'.format(self.id, self.predecessor[0]))
        if len(data_to_fetch) == 0:
            return 0
        try:
            with grpc.insecure_channel(self.predecessor[1]) as channel:
                stub = chord_service_pb2_grpc.ChordStub(channel)
                get_request = self.get_get_request(data_to_fetch)
                res = stub.get(get_request)
                if res.result == 0:
                    # reuse function with same logic
                    print('***fetch_data_from_predecessor() invokes update_storage()function***')
                    self.update_storage(res, -1)
        except Exception as e:
            print("[Fetch Failed] #{} when fetching data from node {}".format(self.id, self.predecessor[0]))
            return -1

        return 0

    # RPC
    def check(self, request, context):
        # RPC called by predecessor to check and update all replicate data and delete extra replicas
        # TODO: (important) make sure the check process is done after notify when joining in,
        # so that the predecessor points to right node
        if request.pairs is None:
            return chord_service_pb2.CheckResponse(result=0)

        if not self.predecessor:
            return chord_service_pb2.CheckResponse(result=-1)

        data_to_fetch = []
        for one_pair in request.pairs:
            # currently addr is not set in the request
            key = one_pair.key
            if one_pair.len == 0:
                if key in self.storage:
                    with self.storage_lock:
                        self.storage.pop(key)
            elif key not in self.storage or one_pair.seq_num != self.storage[key][1]:
                data_to_fetch.append(key)
            elif self.storage[key][0] != one_pair.len:
                with self.storage_lock:
                    self.storage[key][0] = one_pair.len

        # fetch the missing data from predecessor
        # TODO: try the asynchronous fetch using background thread or another thread
        # print('the number of data to be fetched:{}'.format(len(data_to_fetch)))

        if len(data_to_fetch) == 0:
            return chord_service_pb2.CheckResponse(result=0)

        ret = self.fetch_data_from_predecessor(data_to_fetch)

        """
        if self.id == 16:
            print('+++++++++++++++++++++++++++++++'+ str(self.id) + ':' + str(time.time()) + '++++++++++++++++++++++++++++++++')
        """

        return chord_service_pb2.CheckResponse(result=ret)

    # RPC
    def get(self, request, context):
        # RPC for getting values of multiple keys
        # storage : {key: [len, seq, [ip_addr]]}
        get_res = chord_service_pb2.GetResponse()
        get_res.result = 0
        for key in request.keys:
            pair = get_res.pairs.add()
            if key not in self.storage:
                continue
            vals = self.storage[key]
            pair.key = key
            pair.len = vals[0]
            pair.seq_num = vals[1]
            for addr in vals[2]:
                pair.addrs.append(addr)
        return get_res

    # RPC
    def put(self, request, context):
        # RPC for putting (key,values) to current nodes
        # here we only store in the first node and then check() thread will
        # periodically replicate data to the replication chain

        if request.pairs is None:
            return chord_service_pb2.PutResponse(result=0)

        """
        if self.id == 11:
            print('*******************************'+ str(self.id) + ':' + str(time.time()) + '*************************************')
        """

        print('pairs received at put(): {}'.format(str(request.pairs)))
        for pair in request.pairs:
            # TODO: ADD LOCK TO EACH KEY and exception process
            if pair.key not in self.storage:
                # initial the values for the key. [len, seq, [ip_addr]]
                with self.storage_lock:
                    self.storage[pair.key] = list()
                    self.storage[pair.key].append(_R)  # len
                    self.storage[pair.key].append(1)   # seq_num
                    self.storage[pair.key].append(list())  # addrs
                    for addr in pair.addrs:
                        self.storage[pair.key][2].append(addr)
            else:
                # add current ip to the addr
                # addr_list = self.storage[pair.key][2]
                is_change = False
                for addr in pair.addrs:
                    if addr not in self.storage[pair.key][2]:
                        with self.storage_lock:
                            self.storage[pair.key][2].append(addr)
                        is_change = True
                if is_change:
                    with self.storage_lock:
                        self.storage[pair.key][1] = self.storage[pair.key][1] + 1 # seq_num += 1

        return chord_service_pb2.PutResponse(result=0)

    def init_finger_table(self):
        for i in range(0, M):
            key = self.id + 2 ** i
            # initially setting to be -1, ""
            l = [-1, ""]
            self.finger_table.append((key, l))

    def update_kth_finger_table_entry(self, k, successor_id, successor_addr):
        # print('*****NOW UPDATE FINGER ENTRY*****')
        entry = self.finger_table[k]
        entry[1][0] = successor_id
        entry[1][1] = successor_addr

        if k == 0:
           self.set_successor(successor_id, successor_addr)
        # print('node {} updated finger table is: {}'.format(self.id, str(self.finger_table)))

    def _join(self):
        # join in a chord ring by connecting to the contact_to node
        if not self.contact_to:
            print("No contact_to is specified!")
            return

        print("contact to {} when node {} join in".format(self.contact_to, self.id))
        self.init_finger_table()
        with grpc.insecure_channel(self.contact_to) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            new_request = self.generate_find_successor_request(self.id, 0)
            try:
                # TODO: read timeout value from config.txt
                response = stub.find_successor(new_request, timeout=20)
                self.join_in_chord_ring(response)
            except Exception as e: 
                print("Node#{})Timeout error when find_successor to {}".format(self.id, self.contact_to))
                self.logger.info("(Node#{})Timeout error when find_successor to {}".format(self.id, self.contact_to))

    def join_in_chord_ring(self, response):
        # print("get successor: {} when join in the ring".format(str(response)))
        self.set_successor(response.successorId, response.addr)
        print('join_in_chord_ring(): the successor is: {} at {}'.format(response.successorId, response.addr))
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
                    self.set_predecessor(find_predecessor_res.id, find_predecessor_res.addr)
                print("get predecessor response is {}".format(str(find_predecessor_res)))
            except Exception as e: 
                self.logger.error("%%%%%Node#{} error when find_predecessor to {}".format(self.id, response.addr))

        self.notify_successor(type='join')

    def notify_successor(self, type):
        # used to contact successor and notify the existence of current node
        if self.successor is None:
            return

        print('In notify_successor  |  successorId:{}  |  succesorAddr:{}'.format(self.successor[0], self.successor[1]))
        with grpc.insecure_channel(self.successor[1]) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            notify_req = chord_service_pb2.NotifyRequest(predecessorId=self.id, addr=self.addr)
            try:
                if type == 'join':
                    notify_res = stub.notify_at_join(notify_req, timeout=20)
                    self.update_storage(notify_res)
                    self.add_chord_node_to_tracker(self.addr)
                if type == 'leave':
                    notify_res = stub.notify_at_leave(notify_req, timeout=20)
            except Exception as e:
                print(str(e))
                self.logger.error("Node#{} rpc error when notify to {}".format(self.id, self.successor[0]))

    def add_chord_node_to_tracker(self, add_addr):
        with grpc.insecure_channel(self.tracker_addr) as channel:
            stub = p2p_service_pb2_grpc.P2PStub(channel)
            request = p2p_service_pb2.AddChordNodeRequest(addr=add_addr)
            try:
                response = stub.rpc_add_chord_node(request)
                if response.result == -1:
                    print('[notify tracker add node failed] nodeId{} add_chord_node_to_tracker new nodeId{} failed'.format(self.id, self.successor[0]))
            except Exception:
                self.logger.error("Node#{} rpc error when add node{} to tracker".format(self.id, self.successor[0]))

    def remove_chord_node_from_tracker(self, remove_addr):
        with grpc.insecure_channel(self.tracker_addr) as channel:
            stub = p2p_service_pb2_grpc.P2PStub(channel)
            request = p2p_service_pb2.RemoveChordNodeRequest(addr=remove_addr) # current successor is the leaved node
            try:
                response = stub.rpc_remove_chord_node(request)
                if response.result == -1:
                    print('[notify tracker remove node failed] nodeId{} remove_chord_node_from_tracker removed nodeId{} failed'.format(self.id, self.successor[0]))
            except Exception:
                self.logger.error("Node#{} rpc error when remove node{} from tracker".format(self.id, self.successor[0]))

    # RPC
    def notify_at_join(self, request, context):
        print("node {} received notify to set predecessor to {}".format(self.id, request.predecessorId))
        if request is None or request.predecessorId is None:
            return chord_service_pb2.NotifyResponse(result=-1)

        if self.predecessor is None:
            self.predecessor = (request.predecessorId, request.addr)
            response = self.generate_notify_response()
            return response

        # predecessor_id_offset = find_offset(self.predecessor[0], self.id)
        # request_predecessor_id_offset = find_offset(request.predecessorId, self.id)

        self.predecessor = (request.predecessorId, request.addr)
        response = self.generate_notify_response()
        return response

    def notify_at_leave(self, request, context):
        print("node {} received notify to set predecessor to {}".format(self.id, request.predecessorId))
        if request is None or request.predecessorId is None:
            return chord_service_pb2.NotifyResponse(result=-1)

        for key in self.storage: #key:[len, seq_num, [addrs]]
            val = self.storage[key]
            if val[0] != _R:
                self.storage[key][0] = val[0] + 1

        if self.predecessor is None:
            self.set_predecessor(request.predecessorId, request.addr)
            return chord_service_pb2.NotifyResponse(result=0)

        # predecessor_id_offset = find_offset(self.predecessor[0], self.id)
        # request_predecessor_id_offset = find_offset(request.predecessorId, self.id)

        self.predecessor = (request.predecessorId, request.addr)
        return chord_service_pb2.NotifyResponse(result=0)

    def generate_notify_response(self):
        response = chord_service_pb2.NotifyResponse()
        response.result = 0
        # TODO: traversing dictionary needs to be locked?
        for key, value in self.storage.items():  # value = [len, seq_num, [addrs]]
            if value[0] == _R:
                # successor_id, successor_addr = self.find_successor_local(int(key, 16) % (2 ** M))
                short_key = int(key, 16) % (2 ** M)
                pre_self_offset = find_offset(self.predecessor[0], self.id)
                if find_offset(short_key, self.id) < pre_self_offset:
                    continue
            to_pair = response.pairs.add()
            to_pair.key = key
            to_pair.len = value[0]
            to_pair.seq_num = value[1]
            for addr in value[2]:
                to_pair.addrs.append(addr)
        return response

    def generate_check_request(self):
        request = chord_service_pb2.CheckRequest()
        with self.storage_lock:
            for key, value in self.storage.items():  # value = [len, seq_num, [addrs]]
                to_pair = request.pairs.add()
                to_pair.key = key
                to_pair.len = value[0] - 1
                to_pair.seq_num = value[1]
        return request

    def update_storage(self, notify_res, len_bias=0):
        # print("[NOTIFY RESPONSE]: {}".format(str(notify_res)))
        if notify_res.pairs is None:
            return

        print('data to be updated in storage:{}'.format(str(notify_res.pairs)))

        for pair in notify_res.pairs:
            with self.storage_lock:
                self.storage[pair.key] = list()
                self.storage[pair.key].append(pair.len + len_bias)
                self.storage[pair.key].append(pair.seq_num)
                self.storage[pair.key].append(list())
                for addr in pair.addrs:
                    self.storage[pair.key][2].append(addr)

    def initialize_with_node_info(self):
        self.init_finger_table()
        self.add_chord_node_to_tracker(self.addr)
        if self.initial_id_addr_map is None:
            return
        node_identifiers = sorted(self.initial_id_addr_map.keys())
        # [id1, id2, id3, ...]

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
            key = self.id + 2 ** i
            while j < len(node_identifiers):
                if node_identifiers[j] >= key:
                    successor_id = node_identifiers[j] % (2 ** M)
                    self.update_kth_finger_table_entry(i, successor_id, self.initial_id_addr_map[successor_id])
                    break
                j += 1

    def delete_successor(self):
        # delete the successor from finger table and fill in with the most recent successor after that
        print('*****NOW DELETE SUCCESSOR*****')
        if self.successor is None:
            return -1

        next_suc = None
        suc_id = self.successor[0]
        # find the closest successor
        for i in range(0, M):
            key, suc_info = self.finger_table[i]
            if suc_info[0] is not None and suc_info[0] != suc_id:
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

        print('{} finger table is {}'.format(self.id, self.finger_table))
        self.set_successor(self.finger_table[0][1][0], self.finger_table[0][1][1])
        print('{} successor is: {}'.format(self.id, self.successor))
        return 0
    
    # RPC
    def find_successor(self, request, context):
        # print("in find_successor---self.id:{}  self.addr:{}".format(self.id, self.addr))
        # print('[Find successor] node #{} looks for id {}, length is {}'.format(self.id, request.id, request.pathlen))
        # print('[Print Node] node #{} successor: {} predecessor:{}'.format(self.id, str(self.successor), str(self.predecessor)))
        # TODO: differentiate between 1. successor failed; 2. nodes in the path other than sucessor failed
        if request is None or request.id < 0 or request.pathlen < 0:
            return chord_service_pb2.FindSuccessorResponse(successorId=-1, pathlen=-1, addr=self.addr)

        search_id_offset = find_offset(self.id, request.id)
        successor_id_offset = find_offset(self.id, self.successor[0])

        if request.id == self.id:
            return chord_service_pb2.FindSuccessorResponse(successorId=self.id, pathlen=request.pathlen, addr=self.addr)
        elif search_id_offset <= successor_id_offset:
            return chord_service_pb2.FindSuccessorResponse(successorId=self.successor[0], pathlen=request.pathlen+1, addr=self.successor[1])
        else:
            next_id, next_address = self.closest_preceding_node(request.id)
            if self.id == next_id:  # There is only one node in chord ring
                return chord_service_pb2.FindSuccessorResponse(successorId=self.id, pathlen=request.pathlen, addr=self.addr)

            with grpc.insecure_channel(next_address) as channel:
                stub = chord_service_pb2_grpc.ChordStub(channel)
                new_request = self.generate_find_successor_request(request.id, request.pathlen + 1)
                try:
                    response = stub.find_successor(new_request, timeout=20)
                except Exception as e:
                    print('next_address:{}  request_id:{}  succesor_id:{}  2nd RPC failed'.format(next_address, request.id, self.successor[0]))
                    # self.logger.info("(Node#{})Timeout error when find_successor to {}".format(self.id, next_id))
                    return chord_service_pb2.FindSuccessorResponse(successorId=-1, pathlen=-1, addr=self.addr)

            return response

    def find_successor_local(self, id):
        # Different from find_successor(), this function is not a RPC and it starts to find successor of id
        request = self.generate_find_successor_request(id, 0)

        # print("\n start: in find_successor_local  self.id:{}  self.addr:{}".format(self.id, self.addr))

        with grpc.insecure_channel(self.addr) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            try:
                response = stub.find_successor(request, timeout=20)
                # print('{} looks for id {}, return is {}'.format(self.id, request.id, response.successorId))
                # print('end: {} looks for id {}, return is {}'.format(self.id, request.id, response.successorId))
                return response.successorId, response.addr
                # if this RPC is fine, but it fails to call next RPC, the return is -1
            except Exception as e:
                print('[node] #{}: find_successor_local() failed at RPC'.format(self.id))
                return -2, str(-2)
                # return -2 when this RPC went wrong

    def check_local(self):
        check_request = self.generate_check_request()
        with grpc.insecure_channel(self.successor[1]) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            try:
                res = stub.check(check_request)
            except Exception as e:
                print(str(e))
                print('[check] #{} check_local() failed at RPC'.format(self.id))
                #return -1, -1

    def closest_preceding_node(self, id):
        search_id_offset = find_offset(self.id, id)

        for i in range(M - 1, 0, -1):
            # 5 4 3 2 1
            ith_finger = self.finger_table[i][1]  # [id, address]
            if ith_finger is None or ith_finger[0] == -1:
                continue

            ith_finger_id = ith_finger[0]
            ith_finger_addr = ith_finger[1]

            ith_finger_offset = find_offset(self.id, ith_finger_id)

            # print("self.id:{}...id:{}.....search_id_offset:{}..ith_finger_offset:{}".format(self.id, id, search_id_offset, ith_finger_offset))

            if ith_finger_offset > 0 and ith_finger_offset < search_id_offset:
                # TODO: Check if it is alive
                # query = query_to_address(ith_finger, Message.get_json_dump("areyoualive"))
                # if query["subject"] == "iamalive":

                if ith_finger_id == -1:
                    print('ERROR [node] {}, finger table is:{}'.format(self.id, str(self.finger_table)))
                # print('node {} looks for the closest_preceding_node of {}, return {}'.format(self.id, id, str(ith_finger_id)))
                return ith_finger_id, ith_finger_addr

        return self.finger_table[0][1]

    def generate_find_successor_request(self, id, pathlen):
        request = chord_service_pb2.FindSuccessorRequest()
        request.id = id
        request.pathlen = pathlen
        return request

    # RPC
    def get_predecessor(self, request, context):

        # print("node {} received RPC call to get predecessor {}".format(self.id, str(self.predecessor)))

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
                print('[node] #{} get_successors_predecessor() failed at RPC. Current successor is: {}.'.format(self.id, self.successor))
                # print('-------------------------------------------------------------------------------')
                # print(str(e))
                # print('-------------------------------------------------------------------------------')
                return -1, str(-1)

    # RPC
    def get_configuration(self, request, context):
        response = chord_service_pb2.GetConfigurationResponse()
        print(self.predecessor, self.successor)
        response.predecessorId = self.predecessor[0]
        response.successorId = self.successor[0]
        for e in self.finger_table:
            entry = response.table.add()
            entry.id = int(e[0])
            entry.successor_id = int(e[1][0])
            entry.addr = str(e[1][1])
        response.storage = str(self.storage)
        return response


class LocalChordCluster():
    def __init__(self, addr_list):
        self.addr_list = addr_list

    def start(self):
        # get the hash value by their addr(ip:port)
        # TODO: need to consider the hash collision situations?
        id_addr_map = dict()
        for addr in self.addr_list:
            node_id = get_hash_value(addr)
            print(node_id)
            id_addr_map[node_id] = addr
        node_identifiers = sorted(id_addr_map.keys())

        for i, node_id in enumerate(node_identifiers):
            thread = threading.Thread(target=serve, args=(id_addr_map[node_id], id_addr_map))
            thread.start()
            # time.sleep(0.1)
            print('Node {} started at {}...'.format(node_id, id_addr_map[node_id]))


def serve(addr, id_addr_map=None):
    print("starting rpc server: {}".format(addr))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=200))
    chord_service_pb2_grpc.add_ChordServicer_to_server(Node(addr, None, id_addr_map), server)

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


def serve_join(addr, connect_to):
    print("starting rpc server: {}".format(addr))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20000))
    chord_service_pb2_grpc.add_ChordServicer_to_server(Node(addr, contact_to=connect_to), server)

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
    serve(addr, None)
