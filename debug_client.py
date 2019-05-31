import grpc
import chord_service_pb2
import chord_service_pb2_grpc
import hashlib


M = 5


def get_hash_value(s):
    hash = hashlib.sha1()
    hash.update(str(s).encode())
    # print('hashed value for {} is: {}'.format(s, int(hash.hexdigest(), 16) % (2 ** M)))
    return int(hash.hexdigest(), 16) % (2 ** M)

M = 5

ip = 'localhost'
start_port = 50000
addr_list = list()
for i in range(M):
    addr = (ip + ':' + str(start_port + i))
    addr_list.append(addr)

for addr in addr_list:
    print('--------------------------------------------------------')
    print('Node #{} at {}'.format(get_hash_value(addr), addr))
    with grpc.insecure_channel(addr) as channel:
        stub = chord_service_pb2_grpc.ChordStub(channel)
        try:
            request = chord_service_pb2.GetFingerTableRequest()
            response = stub.get_finger_table(request, timeout=20)
            for entry in response.table:
                print('{} -> [{}, {}]'.format(entry.id, entry.successor_id, entry.addr))
        except Exception as e:
            print(e)
