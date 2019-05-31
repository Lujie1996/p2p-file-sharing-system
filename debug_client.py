import grpc
import sys
import chord_service_pb2
import chord_service_pb2_grpc
from utils import *


def get_finger_table_of_all_nodes(number_of_nodes):
    addr_list = get_unique_addr_list(number_of_nodes)
    print(addr_list)
    for addr in addr_list:
        print('--------------------------------------------------------')
        print('Node #{} at {}'.format(get_hash_value(addr), addr))
        get_finger_table(addr)

def get_finger_table(addr):
    with grpc.insecure_channel(addr) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            try:
                request = chord_service_pb2.GetFingerTableRequest()
                response = stub.get_finger_table(request, timeout=20)
                for entry in response.table:
                    print('{} -> [{}, {}]'.format(entry.id, entry.successor_id, entry.addr))
            except Exception as e:
                print(e)

def start(args):
   debug_type = args[1]
   if debug_type == "one_finger_table":
      addr = args[2]
      get_finger_table(addr)
   elif debug_type == "all":
      number_of_nodes = int(input('Number of nodes in Chord:\n'))
      get_finger_table_of_all_nodes(number_of_nodes)
      
   
if __name__ == "__main__":
   start(sys.argv)
