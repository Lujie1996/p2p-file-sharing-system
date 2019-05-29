import sys
from threading import Thread
import random
import chord_service_pb2
import chord_service_pb2_grpc
import grpc

M = 5

class Node(Thread):
    def find_successor(self, request, context):
        if request is None:
            return chord_service_pb2.FindSuccessorResponse(successorId=-1,pathlen=-1)

        if request.id == self.id:
            return chord_service_pb2.FindSuccessorResponse(successorId=self.id,pathlen=request.pathlen)
        elif self.id < request.id <= self.successor:
            return chord_service_pb2.FindSuccessorResponse(successorId=self.successor,pathlen=request.pathlen+1)
        else:
            next_id, next_address = self.closest_preceding_node(request.id)
            if self.id == next_id:  # There is only one node in chord ring
                return chord_service_pb2.FindSuccessorResponse(successorId=self.id, pathlen=request.pathlen)

            with grpc.insecure_channel(next_address) as channel:
                stub = chord_service_pb2_grpc.ChordStub(channel)
                new_request = self.generate_find_successor_request(request.id, request.pathlen + 1)
                try:
                    response = stub.find_successor(new_request, timeout=20)
                except Exception:
                    #self.logger.info("(Node#{})Timeout error when find_successor to {}".format(self.id, next_id))
                    return chord_service_pb2.FindSuccessorResponse(successorId=-1, pathlen=-1)

            return response

    def closest_preceding_node(self, id):
        i = 0
        while i < len(self.finger_table) - 1:
            if id <= self.finger_table[i][0]:
                break
            if self.id >= self.finger_table[i][0]:
                if id <= self.finger_table[i][0] + 2 ** M:
                    break
            i += 1

        return self.finger_table[i]

    def generate_find_successor_request(self, id, pathlen):
        request = chord_service_pb2.FindSuccessorRequest()
        request.id = id
        request.pathlen = pathlen
        return request


