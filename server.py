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
        self.methods = {
                           'del': self.__del__
                        #   'join': self.__join
                       }
#        self.close = False LATER
        self.ip_addr = ip_addr
        self.port = str(port)
        self.main_port = master
        self.pair = (self.ip_addr, self.port)

        m = sha1()
        for s in self.pair:
            m.update(s.encode())
        self.hash = m.hexdigest()

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
#        self.port = self.sock.getsockname()[1]
        #self.sock.settimeout(1) #CHECK
        self.sock.listen(10)    #CHECK
        print("success")
        self.adjacent = Adjacent(self.hash, self.port, self.hash, self.port) 

        self.message_queues = {}  # servers' reply messages

    def __del__(self):
        """Destructor"""
        self.sock.close()

    def connection(self):
        conn, _ = self.sock.accept()
        print("accept ok")
        data = conn.recv(1024)
        print(data)

class Bootstrap(Server):
    def __init__(self, ip_addr, port):
        self.network_size = 1
        super(Bootstrap, self).__init__(ip_addr, port, port)
        #replication LATER

def discover_adjacent(hash, port):
    sock = Communication(port)
    a = sock.socket_comm('join:' + hash).split(':')
    return int(a[0]), a[1], int(a[2]), a[3]

def send_message(port, message):
    sock = Communication(port)
    return sock.socket_comm(message)
