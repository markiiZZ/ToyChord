import socket
import logging
import sys

class Communication(object):

    def __init__(self, PORT):
        self.PORT=int(PORT)

        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect(('127.0.0.1', self.PORT))
        print("connect ok")

    def socket_comm(self,message):
        try:
            self.client_socket.send(message)
        except socket.error:
            logging.error('client: SEND MESSAGE FAIL')
            sys.exit()
