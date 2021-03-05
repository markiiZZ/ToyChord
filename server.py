#!/usr/bin/env python

#import Queue
import sys
import threading
import time
import logging
import socket

from hashlib import sha1

logging.basicConfig(filename='debug.log', level=logging.ERROR)

class Server(object):
    """Server class
    Implements all the stuff that a DHT server should do"""
    def __init__(self, ip_addr, port, master):
        """Every new implemented method must be added to the dictionary
        with 'key' a unique identifier as below"""
        self.operations = {
                           'del': self.__del__,
                           }
#        self.close = False LATER
        self.ip_addr = ip_addr
        self.port = str(port)
        print(self.port)
        self.pair = '{};{}'.format(ip_addr, port)
        print(self.pair)
        self.poop = (self.ip_addr, self.port)
        print(self.poop)
        m = sha1()
        for s in self.poop:
            m.update(s.encode())
        self.hash = m.hexdigest()
        print(m)

        print(self.hash)

        self.data = {}
#        self.replication = 0 LATER
        self.m_port = master
        self.data_lock = threading.Lock()
#        self.thread_list = [] MAYBE THROW AWAY
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.sock.bind((self.ip_addr, int(self.port)))
        except socket.error as msg:
            logging.error('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()
        self.port = self.sock.getsockname()[1]
        #self.sock.settimeout(1) #CHECK
        self.sock.listen(10)    #CHECK
        print("success")
#        self.neighbors = Neighbors(self.hash, self.port, self.hash, self.port) LATER

        self.message_queues = {}  # servers' reply messages

    def __del__(self):
        """Destructor"""
        self.sock.close()

    def connection(self):
        conn, _ = self.sock.accept()
        print("accept ok")
        data = conn.recv(1024)
        print(data)

class Client(object):

    def __init__(self, PORT):
        self.PORT=int(PORT)

        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect(('127.0.0.1', self.PORT))
        print("connect ok")

    def communication(self,message):
        try:
            self.client_socket.send(message)
        except socket.error:
            logging.error('client: SEND MESSAGE FAIL')
            sys.exit()
