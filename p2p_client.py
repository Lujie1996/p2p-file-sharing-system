
import grpc
import time
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

    def start(self):
        while True:
            command = str(input('Choose your operation: 1 for upload, 2 for download, any other input to exit\n'))
            filename = str(input('Filename:\n'))
            if command == '1':
                # upload file
                self.upload(filename)
            elif command == '2':
                # download file
                self.download(filename)
            else:
                return

    def put(self, hashed_value_of_file, fileholder_addr):
        pass

    def get(self, hashed_value_of_file):
        pass

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
        if response.result == 0:
            print('Failed while registering to tracker')
            return

        # step2: Tracker gives fileholder the addr of a node in Chord (as the entrance of Chord)
        #  this is piggybacked in the return of RPC register_file
        self.entrance_addr = response.entrance_addr

        # step3: Fileholder put(hashed_value_of_file, fileholder_addr) to Chord
        result = self.put(hashed_value_of_file=hashed_value_of_file, fileholder_addr=self.addr)
        # response: {int32 result: 0 for failed, 1 for succeeded;}

        if result == 0:
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
        if response.result == 0:
            print('Failed while looking up file on tracker')
            return
        elif response.result == -1:
            print('File not found')
            return
        hashed_value_of_file = response.hashed_value_of_file
        entrance_addr = response.entrance_addr

        # step2: Client contacts the entrance, get a list of fileholder_addr
        result, addr_list = self.get(hashed_value_of_file=hashed_value_of_file)
        # response: {int32 result: 0 for failed, 1 for succeeded; repeated Address addr_list}

        if result == 0:
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
                # response: {int32 result: 0 for failed, -1 for file not found, 1 for succeeded; string file}
            except Exception:
                print("RPC failed")
                return
        if response.result == 0:
            print('Failed while downloading the file')
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
        response = p2p_service_pb2.DownloadResponse(result=1, file=file)
        return response


if '__name__' == '__main__':
    addr = str(input('Indicate the address to start the client (ip:port)\n'))
    Client(addr).start()