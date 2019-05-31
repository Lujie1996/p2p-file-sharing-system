import grpc
import chord_service_pb2
import chord_service_pb2_grpc

M = 5

ip = 'localhost'
start_port = 50000
addr_list = list()
for i in range(M):
    addr = (ip + ':' + str(start_port + i))
    addr_list.append(addr)

for addr in addr_list:
    with grpc.insecure_channel(addr) as channel:
        stub = chord_service_pb2_grpc.ChordStub(channel)
        try:
            request = chord_service_pb2.GetFingerTableRequest()
            response = stub.get_finger_table(request, timeout=20)
            print(response.finger_table)
        except Exception as e:
            print(e)
