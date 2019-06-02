import sys
import grpc
import time
import random
import hashlib
import threading
from concurrent import futures
import chord_service_pb2
import chord_service_pb2_grpc
import utils

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class Tracker(p2p_service_pb2_grpc.P2PServicer):

    def __init__(self, addr):
        self.ip = addr.split(':')[0]
        self.port = int(addr.split(':')[1])
        self.storage = dict()  # filename -> hashed_value_of_file

    def rpc_register_file(self, request, context):
        # request.filename, request.hashed_value_of_file
        # return RegisterFileResponse {
        #   int32 result: 0 for fail, 1 for succeeded
        #   string entrance_addr: address of a node in Chord
        # }
        pass

    def rpc_look_up_file(self, request, context):
        # request.filename
        # return LookUpFileResponse {
        #   int32 result: 0 for failed, -1 for not found, 1 for succeeded
        #   string hashed_value_of_file
        #   string entrance_addr: address of a node in Chord
        # }
        pass


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