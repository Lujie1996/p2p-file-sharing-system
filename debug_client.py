import grpc
import sys
import chord_service_pb2
import chord_service_pb2_grpc
from utils import *


def get_configuration_of_all_nodes(number_of_nodes):
    addr_list = get_unique_addr_list(number_of_nodes)
    print(addr_list)
    for addr in addr_list:
        print('--------------------------------------------------------')
        print('Node #{} at {}'.format(get_hash_value(addr), addr))
        get_node_configuration(addr)


def get_node_configuration(addr):
    with grpc.insecure_channel(addr) as channel:
        stub = chord_service_pb2_grpc.ChordStub(channel)
        try:
            request = chord_service_pb2.GetConfigurationRequest()
            response = stub.get_configuration(request, timeout=20)
            print('--------------------------------------------------------')
            print('nodeId:{}  |   predecessorId:{}  |  successorId:{}'.format(get_hash_value(addr), response.predecessorId, response.successorId))
            for entry in response.table:
                print('{} -> [{}, {}]'.format(entry.id, entry.successor_id, entry.addr))
            print(response.storage)
        except Exception as e:
            print('Exception:\n' + str(e))


def start():
    command = str(input('Input \'all\' to see all nodes, or node index to see that node\n'))
    if command == "all":
        number_of_nodes = int(input('Number of nodes in Chord:\n'))
        get_configuration_of_all_nodes(number_of_nodes)
    else:
        try:
            node_id = int(command)
            addr_list = get_unique_addr_list(20)
            get_node_configuration(addr_list[node_id])
        except:
            print('Invalid command')


if __name__ == "__main__":
   start()
