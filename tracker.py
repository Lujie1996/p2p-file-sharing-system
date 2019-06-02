import sys
import grpc
import time
import random
from concurrent import futures
import p2p_service_pb2
import p2p_service_pb2_grpc
import utils

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class Tracker(p2p_service_pb2_grpc.P2PServicer):

    def __init__(self, addr):
        self.ip = addr.split(':')[0]
        self.port = int(addr.split(':')[1])
        self.storage = dict()  # filename -> hashed_value_of_file
        self.chord_nodes = set()

    def rpc_register_file(self, request, context):
        # request.filename, request.hashed_value_of_file
        # return RegisterFileResponse {
        #   int32 result: -1 for fail, 0 for succeeded
        #   string entrance_addr: address of a node in Chord
        # }
        self.storage[request.filename] = request.hashed_value_of_file
        response = p2p_service_pb2.RegisterFileResponse(result=0)
        return response

    def rpc_look_up_file(self, request, context):
        # request.filename
        # return LookUpFileResponse {
        #   int32 result: -1 for failed, -2 for not found, 0 for succeeded
        #   string hashed_value_of_file
        #   string entrance_addr: address of a node in Chord
        # }
        if request.filename not in self.storage:
            return p2p_service_pb2.LookUpFileResponse(result=-2)
        hashed_value_of_file = self.storage[request.filename]

        entrance_addr = random.choice(list(self.chord_nodes))
        return p2p_service_pb2.LookUpFileResponse(result=0, hashed_value_of_file=hashed_value_of_file, entrance_addr=entrance_addr)

    def rpc_add_chord_node(self, request, context):
        self.chord_nodes.add(request.addr)
        return p2p_service_pb2.AddChordNodeResponse(result=0)

    def rpc_remove_chord_node(self, request, context):
        node_addr = request.addr
        if node_addr not in self.chord_nodes:
            return p2p_service_pb2.RemoveChordNodeResponse(result=-1)
        self.chord_nodes.remove(node_addr)
        return p2p_service_pb2.RemoveChordNodeResponse(result=0)


def start_server(addr):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=200))
    p2p_service_pb2_grpc.add_P2PServicer_to_server(Tracker(addr), server)

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


if '__name__' == '__main__':
    addr = utils.TRACKER_ADDR
    start_server(addr)