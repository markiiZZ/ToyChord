import socket
import logging
import sys

class Communication(object):

    def __init__(self, PORT):
        self.PORT=int(PORT)

        self.comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.comm_socket.connect(('127.0.0.1', self.PORT))
        #print("connect ok")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        try:
            message = 'quit'
            self.comm_socket.send(message.encode())
        except socket.error:
            logging.error('CLOSURE WAS UNSUCCESSFUL')
            sys.exit()
        else:
            self.comm_socket.recv(1024)
            self.comm_socket.close()


    def __exit__(self, exc_type, exc_val, exc_tb):
        print("EXIT WITH AS")
        self.close()
        return False

    def __enter__(self):
        return self

    def close(self):
        try:
            self.comm_socket.send(b'quit')
        except socket.error:
            logging.error('client: CLOSURE WAS UNSUCCESSFUL')
            sys.exit()
        else:
            self.comm_socket.recv(1024)
            self.comm_socket.close()

    def socket_comm(self,message):
        try:
            #print("edw krasarei")
            self.comm_socket.send(message.encode())
            #print(message)
            #print("oxi edw")
        except socket.error:
            logging.error('socket: SEND MESSAGE FAIL')
            print("shit1")
            sys.exit()
        try:
            #print("hi")
            self.answer = self.comm_socket.recv(1024)
            #print("good")
        except socket.error:
            logging.error('socket: RECEIVE MESSAGE FAIL')
            print("shit2")
            sys.exit()
        else:
            print("ok i guess from socket comm")
            return self.answer.decode()

    #def socket_close(self):
    #    self.comm_socket.close()
    #    print("socket closed")
