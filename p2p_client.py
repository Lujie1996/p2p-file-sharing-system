import sys
import grpc
import time
import random
import hashlib
import threading
from concurrent import futures
import chord_service_pb2
import chord_service_pb2_grpc
from logging import Logger, StreamHandler, Formatter
from fix_finger import FixFinger
from stabilize import Stabilize
from utils import *


class Client:

    def __init__(self, addr):
        self.ip, self.port = addr.split(':')


    def start(self):
        while True:
            command = str(input('Choose your operation: 1 for upload, 2 for download\n'))
            if command == '1':
                filename = str(input('Filename:\n'))
            elif command == '2':
                filename =


    def upload(self):

    def download(self):


if '__name__' == '__main__':
    addr = str(input('Indicate the address to start the client (ip:port)\n'))
    Client(addr).start()