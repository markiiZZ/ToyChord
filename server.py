#!/usr/bin/env python

#import Queue
import sys
import time
import logging
import socket
import queue
import threading

from hashlib import sha1
from queue import Queue

from adjacent import Adjacent
from communication import Communication

logging.basicConfig(filename='debug.log', level=logging.ERROR)

#EXOUME THEWRHSEI OTI EXOUN IP:LOCALHOST------PROSOXH!!!

class Server(object):
    """Server class
    Implements all the stuff that a DHT server should do"""
    def __init__(self, ip_addr, port, master):
        """Every new implemented method must be added to the dictionary
        with 'key' a unique identifier as below"""
        self.methods = {
                           'join': self.join,
                           'front': self.update_succ,
                           'back': self.update_pred,
                           'get_from_succ': self.get_from_succ,
                           'update': self.update,
                           'insert': self.insert
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
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.sock.bind((self.ip_addr, int(self.port)))
        except socket.error as msg:
            logging.error('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()

        self.sock.settimeout(1) #CHECK
        self.sock.listen(10)    #CHECK
        print("success")
        self.adjacent = Adjacent(self.hash, self.port, self.hash, self.port)

        self.message_queues = {}  # servers' reply messages

    #def __del__(self):
        #"""Destructor"""
        #self.sock.close()

    def bad_request(self):
        self.message_queues[sock].put('Invalid server command')

    def connection(self):
        while True:
            try:
                connect, _ = self.sock.accept()
            except socket.timeout:
                pass
            else:
                self.message_queues[connect] = Queue()
                threading.Thread(target = self.socket_thread, args = (connect,)).start()
    #things about close

    def socket_thread(self, sock):
        while True:
            try:
                data = sock.recv(1024).decode()
                print(data)
                if not data:
                    break
                else:
                    command = self.methods.get(data.split(':')[0])
                    if (command == None):
                        self.bad_request(data, sock)
                    else:
                        command(data, sock)
            except socket.error:
                logging.error('Data recv failed')
                break
            else:
                try:
                    answer = self.message_queues[sock].get_nowait()
                except queue.Empty:
                    pass
                else:
                    sock.send(answer.encode())
            #things about close

    def place_here(self, hashing):
        #mallon swsto
        return (self.adjacent.pred_hash < hashing <= self.hash) | (hashing <= self.hash <= self.adjacent.pred_hash) | (self.hash <= self.adjacent.pred_hash <= hashing)

    def update_pred(self, data, sock):
        _, pred_port, pred_hash = data.split(':')
        self.adjacent.update_adjacent(int(pred_port), pred_hash, 0)
        self.message_queues[sock].put('Updated')

    def update_succ(self, data, sock):
        _, succ_port, succ_hash = data.split(':')
        self.adjacent.update_adjacent(int(succ_port), succ_hash, 1)
        self.message_queues[sock].put('Updated')

    def join_DHT(self):
        pred_port, pred_hash, succ_port, succ_hash = discover_adjacent(self.hash, self.main_port)
        print(pred_port, pred_hash, succ_port, succ_hash)
        self.adjacent.notify_adjacent(pred_port, pred_hash, self.port, self.hash, 0)
        self.adjacent.notify_adjacent(succ_port, succ_hash, self.port, self.hash, 1)
        self.adjacent.send_adjacent('get_from_succ', 1)

    def join(self, data, sock):
        _, hashing = data.split(':')
        if self.place_here(hashing):
            adj =  self.adjacent.get_adjacent(0) + ':' + str(self.port) + ':' + self.hash
        else:
            adj = self.adjacent.send_adjacent(data, 1)
        self.message_queues[sock].put(adj)

    def get_from_succ(self, data, sock):
        #with the adding of replication -> needs modification
        #_, song = data.split(':') in the basic retreival you take everything from the succ
        self.data_lock.acquire()
        if(not self.data):
            print("adeios")
        for (key, value) in self.data.items():
            if not self.place_here(key):
                #kai alla orismata sthn update gia replication
                message = 'update:{}:{}'.format(key, value[0], value[1])
                #mhpws de xreiazetai to thread?
                threading.Thread(target = send_adjacent, args = (message, 0)).start()
                #ayta pou kanei me to None:None einai oti koitazei posoi exoun to kathe tragoudi
                #apo tous epomenous sto ring kai apo ton teleutaio pou to exei to diagrafei wste
                #to plhthos twn antigrafwn meta thn eisagwgh tou neou komvou na diathreitai
                #XRHSIMO GIA REPLICATION
                del self.data[key]
        self.message_queues[sock].put('DONE')
        self.data_lock.release()

    def update(self, data, sock):
        #replication -> xamos
        _, hash_key, key, value = data.split(':')
        #logging
        self.data_lock.acquire()
        #if (self.data.get(hash_key) != (key,value)):
        self.data[hash_key] = (key, value)
        self.data_lock.release()
        #messages sta queues xreiazontai sthn parousa fash?

    def insert(self, data, sock):
        _, key, value = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        #logging
        self.data_lock.acquire()
        print("mphka")
        if (self.data.get(hash_key) == (key, value)):
            self.data_lock.release()
        elif self.place_here(hash_key):
            self.data[hash_key] = (key,value)
            self.data_lock.release()
             #pragmata me REPLICATION
        else:
            self.data_lock.release()
            #gt na to valw se queue?
            self.adjacent.send_adjacent(data, 1)
        print(self.port, self.data[hash_key])






class Bootstrap(Server):
    def __init__(self, ip_addr, port):
        self.network_size = 1
        super(Bootstrap, self).__init__(ip_addr, port, port)
        #replication LATER

    def join(self, data, sock):
        self.network_size += 1
        super(Bootstrap, self).join(data, sock)
        #add the replication to the queue

def discover_adjacent(hash, port):
    socket = Communication(port)
    a = socket.socket_comm('join:' + hash).split(':')
    return int(a[0]), a[1], int(a[2]), a[3]

def send_message(port, message):
    socket = Communication(port)
    return socket.socket_comm(message)
