import socket
import logging
import sys

class Communication(object):
    """Create a socket between two servers to exchange messages"""
    def __init__(self, ADDRESS):
        token = ADDRESS.split('&')
        self.IP = token[0]
        self.PORT = int(token[1])
        self.comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.comm_socket.connect((self.IP, self.PORT))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __enter__(self):
        return self

    def close(self):
        """Close the socket safely"""
        try:
            self.comm_socket.send(b'quit')
        except socket.error:
            logging.error('Closure was unsuccessful')
            sys.exit()
        else:
            self.comm_socket.recv(1024)
            self.comm_socket.close()

    def socket_comm(self,message):
        """Send a message via the socket and wait for the answer"""
        try:
            self.comm_socket.send(message.encode())
        except socket.error:
            logging.error('Send message was unsuccessful')
            sys.exit()

        try:
            self.answer = self.comm_socket.recv(1024)
        except socket.error:
            logging.error('Receive message was unsuccessful')
            sys.exit()
        else:
            return self.answer.decode()
