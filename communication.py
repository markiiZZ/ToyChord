import socket
import logging
import sys

class Communication(object):

    def __init__(self, PORT):
        self.PORT=int(PORT)

        self.comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.comm_socket.connect(('127.0.0.1', self.PORT))
        print("connect ok")

    def socket_comm(self,message):
        try:
            print("edw krasarei")
            self.comm_socket.send(message.encode())
            print(message)
            print("oxi edw")
        except socket.error:
            logging.error('socket: SEND MESSAGE FAIL')
            sys.exit()
        try:
            self.answer = self.comm_socket.recv(1024)
        except socket.error:
            logging.error('socket: RECEIVE MESSAGE FAIL')
            sys.exit()
        else:
            return self.answer.decode()
