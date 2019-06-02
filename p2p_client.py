import grpc
import random
import chord_service_pb2
import chord_service_pb2_grpc
import p2p_service_pb2
import p2p_service_pb2_grpc
import utils


class Client:

    def __init__(self, addr):
        self.addr = addr
        self.tracker_addr = utils.TRACKER_ADDR
        self.entrance_addr = ''
        self.local_files = dict()  # hashed_value_of_file -> file; note: the stored file is of type: byte
        # uploaded files are stored in self.local_files, only these files can  be downloaded by other peers

    def start(self):
        while True:
            print('------------------------------------')
            command = str(input('Choose your operation:\n'
                                '1 for upload\n'
                                '2 for download\n'
                                '3 for put\n'
                                '4 for get\n'
                                '5 for debug\n'
                                'or any other input to exit\n'))
            print('------------------------------------\n')
            if command == '1':
                filename = str(input('Filename:\n'))
                # upload file
                self.upload(filename)
            elif command == '2':
                filename = str(input('Filename:\n'))
                # download file
                self.download(filename)
            elif command == '3':
                # put
                s = str(input('Input key,value separated by \',\'\n'))
                key, value = s.split(',')
                hashed_value_of_file = utils.sha1(key)
                result = self.put(hashed_value_of_file, value)
                if result == 0:
                    print('Succeeded')
                else:
                    print('Failed')
            elif command == '4':
                # get
                key = str(input('Input key:\n'))
                hashed_value_of_file = utils.sha1(key)
                result, addr_list = self.get(hashed_value_of_file)
                if result == 0:
                    print('Succeeded')
                    print(str(addr_list))
                else:
                    print('Failed')
            elif command == '5':
                self.show_debug_info()
            else:
                return

    def put(self, hashed_value_of_file, fileholder_addr):
        # return 0 for succeeded, -1 for failed

        self.check_and_fill_entrance()
        # step1: find_successor(hashed_value_of_file)
        result, successor_addr = self.find_successor(hashed_value_of_file)
        if result == -1:
            return -1

        print('find_successor return: {}, {}'.format(result, successor_addr))
        # step2: put the (hashed_value_of_file, fileholder_addr) to addr_to_put
        with grpc.insecure_channel(successor_addr) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            req = chord_service_pb2.PutRequest()
            pair = req.pairs.add()
            pair.key = hashed_value_of_file
            pair.addrs.append(fileholder_addr)
            try:
                response = stub.put(req, timeout=20)
            except Exception:
                print("RPC failed from put()")
                return -1
        if response.result == -1:
            return -1

        return 0

    def get(self, hashed_value_of_file):
        # return {int result: 0 for succeeded, -1 for failed; list<string> addr_list}

        self.check_and_fill_entrance()
        # step1: find_successor(hashed_value_of_file)
        result, successor_addr = self.find_successor(hashed_value_of_file)
        if result == -1:
            return -1, list()

        # step2: get addr_list from Chord
        with grpc.insecure_channel(successor_addr) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            req = chord_service_pb2.GetRequest()
            req.keys.append(hashed_value_of_file)
            try:
                response = stub.get(req, timeout=20)
            except Exception:
                print("RPC failed from get()")
                return -1, list()
        if response.result == -1:
            return -1, list()

        addr_list = list()
        for addr in response.pairs[0].addrs:
            addr_list.append(addr)
        return 0, addr_list

    def find_successor(self, hashed_value_of_file):
        # return: {int result: -1 for failed, 0 for succeeded; string successor_addr}
        key = int(hashed_value_of_file, 16) % (2 ** utils.M)
        print('key is: {}'.format(key))
        print('entrance_addr: {}'.format(self.entrance_addr))
        with grpc.insecure_channel(self.entrance_addr) as channel:
            stub = chord_service_pb2_grpc.ChordStub(channel)
            request = chord_service_pb2.FindSuccessorRequest(id=key, pathlen=1)
            try:
                response = stub.find_successor(request, timeout=20)
            except Exception:
                print("RPC failed from find_successor()")
                return -1, ''
        return 0, response.addr

    def check_and_fill_entrance(self):
        if self.entrance_addr:
            return

        with grpc.insecure_channel(self.tracker_addr) as channel:
            stub = p2p_service_pb2_grpc.P2PStub(channel)
            request = p2p_service_pb2.GetEntranceRequest()
            try:
                response = stub.rpc_get_entrance(request, timeout=20)
            except Exception:
                print("RPC failed from check_and_fill_entrance()")
                return -1, list()
        self.entrance_addr = response.entrance_addr

    def upload(self, filename):
        try:
            with open(filename, 'rb') as f:
                lines = f.readlines()
        except Exception as e:
            print('Cannot open file')
            return
        file = b''
        for line in lines:
            file += line

        hashed_value_of_file = utils.sha1(file)

        # step 1: Register (filename, hashed_value_of_file) at tracker
        with grpc.insecure_channel(self.tracker_addr) as channel:
            stub = p2p_service_pb2_grpc.P2PStub(channel)
            request = p2p_service_pb2.RegisterFileRequest(filename=filename, hashed_value_of_file=hashed_value_of_file)
            try:
                response = stub.rpc_register_file(request, timeout=20)
            except Exception:
                print("RPC failed")
                return
        if response.result == -1:
            print('Failed while registering to tracker')
            return

        # step2: Tracker gives fileholder the addr of a node in Chord (as the entrance of Chord)
        #  this is piggybacked in the return of RPC register_file
        self.entrance_addr = response.entrance_addr

        # step3: Fileholder put(hashed_value_of_file, fileholder_addr) to Chord
        result = self.put(hashed_value_of_file=hashed_value_of_file, fileholder_addr=self.addr)
        # response: {int32 result: -1 for failed, 0 for succeeded;}

        if result == -1:
            print('Failed while putting local address to Chord')
            return

        # store this file into local memory storage
        self.local_files[hashed_value_of_file] = file

        print('Upload succeeded!')

    def download(self, filename):
        # step1: Client contacts tracker with the filename it wants to obtain, tracker returns hashed_value_of_file,
        #        as well as a node in Chord (as the entrance of Chord)
        with grpc.insecure_channel(self.tracker_addr) as channel:
            stub = p2p_service_pb2_grpc.P2PStub(channel)
            request = p2p_service_pb2.LookUpFileRequest(filename=filename)
            try:
                response = stub.rpc_look_up_file(request, timeout=20)
            except Exception:
                print("RPC failed")
                return
        if response.result == -1:
            print('Failed while looking up file on tracker')
            return
        elif response.result == -2:
            print('File not found')
            return
        hashed_value_of_file = response.hashed_value_of_file
        self.entrance_addr = response.entrance_addr

        # step2: Client contacts the entrance, get a list of fileholder_addr
        result, addr_list = self.get(hashed_value_of_file=hashed_value_of_file)
        # response: {int32 result: -1 for failed, 0 for succeeded; repeated Address addr_list}

        if result == -1:
            print('Failed while getting fileholder\'s addresses from Chord')
            return
        addr_list = list()
        for addr in addr_list:
            addr_list.append(addr)

        # Client picks one addr randomly and contacts it to download.
        # This client can also register in Chord as a fileholder
        download_addr = random.choice(addr_list)
        with grpc.insecure_channel(download_addr) as channel:
            stub = p2p_service_pb2_grpc.P2PStub(channel)
            request = p2p_service_pb2.DownloadRequest(hashed_value_of_file=hashed_value_of_file)
            try:
                response = stub.rpc_download(request, timeout=20)
                # response: {int32 result: -1 for file not found, 0 for succeeded; string file}
            except Exception:
                print("RPC failed")
                return
        if response.result == -1:
            print('File not found')
            return
        file = response.file
        file = file.encode()  # string to bytes

        # write the file into local file
        try:
            with open(filename, 'wb') as f:
                f.write(file)
        except Exception as e:
            print('Failed while storing the downloaded file to local')
            return

        print('Download succeeded!')

    def show_debug_info(self):
        with grpc.insecure_channel(self.tracker_addr) as channel:
            stub = p2p_service_pb2_grpc.P2PStub(channel)
            request = p2p_service_pb2.GetDebugRequest()
            try:
                response = stub.rpc_get_debug(request, timeout=20)
            except Exception:
                print("RPC failed")
                return
        print(response.debug_info)

    #  peer node calls this.
    def rpc_download(self, request, context):
        # request.hashed_value_of_file
        # return DownloadResponse {
        #   int32 result: 0 for fail, -1 for file not found, 1 for succeeded
        #   string file
        # }
        hashed_value_of_file = request.hashed_value_of_file
        if hashed_value_of_file not in self.local_files:
            response = p2p_service_pb2.DownloadResponse(result=-1)
            return response
        file = self.local_files[hashed_value_of_file]
        file = file.decode()  # convert bytes to string
        response = p2p_service_pb2.DownloadResponse(result=0, file=file)
        return response


if __name__ == '__main__':
    addr = str(input(
        'Indicate the address to start the client (ip:port) or just indicate the port number to start on local\n'))
    if ':' not in addr:
        addr = 'localhost:' + addr
    Client(addr).start()