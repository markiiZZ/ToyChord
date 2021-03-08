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
                           'insert': self.insert,
                           'quit': self.quit,
                           'query_all': self.query_all,
                           'print_node_songs': self.print_node_songs,
                           'query': self.query,
                           'query_forward': self.query_forward,
                           'delete': self.delete,
                           'delete_forward': self.delete_forward,
                           'depart': self.depart,
                           'DHT_ends': self.DHT_ends,
                           'overlay':self.overlay,
                           'overlay_forward':self.overlay_forward
                       }
        self.terminates = False
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
        self.thread_list = []

    #def __del__(self):
        #"""Destructor"""
        #self.sock.close()
    def quit(self, data, sock):
        """Quits"""
        #print("CLOSE MAN")
        self.message_queues[sock].put('CLOSE')

    def bad_request(self, data, sock):
        self.message_queues[sock].put('Invalid server command')

    def main_loop(self):
        while True:
            self.connection()
            #print("jjo")
            if self.terminates:
                time.sleep(1)
                #print("vgainw")
                return

    def connection(self):
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
                #print("asf")
                data = sock.recv(1024).decode()
                #print(data)
                #print("noun")
                #print(data)
                if not data:
                    #print("iuihu")
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
                    #print(answer)
                    sock.send(answer.encode())
                    if answer == 'CLOSE':
                        del self.message_queues[sock]
                        sock.close()
                        return
            #things about close

    def DHT_ends(self, data, sock):
        send_message(self.main_port, 'DHT_ends')

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
        #if(not self.data):
            #print("adeios")
        entries_to_del = []
        for (key, value) in self.data.items():
            if not self.place_here(key):
                #kai alla orismata sthn update gia replication
                message = 'update:{}:{}:{}'.format(key, value[0], value[1])
                #mhpws de xreiazetai to thread?
                threading.Thread(target = self.adjacent.send_adjacent, args = (message, 0)).start()
                #ayta pou kanei me to None:None einai oti koitazei posoi exoun to kathe tragoudi
                #apo tous epomenous sto ring kai apo ton teleutaio pou to exei to diagrafei wste
                #to plhthos twn antigrafwn meta thn eisagwgh tou neou komvou na diathreitai
                #XRHSIMO GIA REPLICATION
                entries_to_del.append(key)
        for key in entries_to_del:
            del self.data[key]
        self.data_lock.release()
        self.message_queues[sock].put('DONE')

    def update(self, data, sock):
        #replication -> xamos
        _, hash_key, key, value = data.split(':')
        #logging
        self.data_lock.acquire()
        #if (self.data.get(hash_key) != (key,value)):
        self.data[hash_key] = (key, value)
        self.data_lock.release()
        self.message_queues[sock].put('DONE')
        #messages sta queues xreiazontai sthn parousa fash?

    def insert(self, data, sock):
        _, key, value = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        #logging
        self.data_lock.acquire()
        #print("mphka")
        #efoson to dictionary den uposthrizei duplicates isws einai peritto to if
        if (self.data.get(hash_key) == (key, value)):
            self.data_lock.release()
            #print("already exists")
        elif self.place_here(hash_key):
            self.data[hash_key] = (key,value)
            self.data_lock.release()
            #print(self.port, self.data[hash_key])
            self.message_queues[sock].put('DONE')
             #pragmata me REPLICATION
        else:
            #print("stelnw geitona")
            self.data_lock.release()
            #gt na to valw se queue?
            #POLY XRHSIMO TELIKA
            self.message_queues[sock].put(self.adjacent.send_adjacent(data, 1))

    def print_node_songs(self, data, sock):
        _, hash_of_first = data.split(':')
        if (hash_of_first == self.hash):
            self.message_queues[sock].put('DONE')
        else:
            self.data_lock.acquire()
            print(self.port) #isws axreiasto
            for (key, value) in self.data.items():
                print(value)
            self.data_lock.release()
            self.message_queues[sock].put(self.adjacent.send_adjacent(data, 1))

    def query_all(self, data, sock):
        self.data_lock.acquire()
        print(self.port) #isws axreiasto
        for (key, value) in self.data.items():
            print(value)
        self.data_lock.release()
        if (self.adjacent.succ_hash != self.hash):
            self.message_queues[sock].put(self.adjacent.send_adjacent('print_node_songs:{}'.format(self.hash), 1))
        else:
            self.message_queues[sock].put('DONE')

    def query_forward(self, data, sock):
        _, key, hash_of_first = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        self.data_lock.acquire()
        if (self.data.get(hash_key, None)!= None):
            print(self.data[hash_key][1])
            self.data_lock.release()
            self.message_queues[sock].put('DONE')
        else:
            if (hash_of_first == self.hash):
                self.data_lock.release()
                print('This song does not exist')
                self.message_queues[sock].put('FAIL')
            else:
                self.data_lock.release()
                self.message_queues[sock].put(self.adjacent.send_adjacent(data, 1))

    def query(self, data, sock):
        _, key = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        self.data_lock.acquire()
        if (self.data.get(hash_key, None)!= None):
            print(self.data[hash_key][1])
            self.data_lock.release()
            self.message_queues[sock].put('DONE')
        else:
            self.data_lock.release()
            self.message_queues[sock].put(self.adjacent.send_adjacent('query_forward:{}:{}'.format(key, self.hash), 1))

    def delete_forward(self, data, sock):
        _, key, hash_of_first = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        self.data_lock.acquire()
        if (self.data.get(hash_key, None)!= None):
            del self.data[hash_key]
            self.data_lock.release()
            self.message_queues[sock].put('DONE')
        else:
            if (hash_of_first == self.hash):
                self.data_lock.release()
                print('This song does not exist')
                self.message_queues[sock].put('FAIL')
            else:
                self.data_lock.release()
                self.message_queues[sock].put(self.adjacent.send_adjacent(data, 1))

    def delete(self, data, sock):
        _, key = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        self.data_lock.acquire()
        if (self.data.get(hash_key, None)!= None):
            del self.data[hash_key]
            self.data_lock.release()
            self.message_queues[sock].put('DONE')
        else:
            self.data_lock.release()
            self.message_queues[sock].put(self.adjacent.send_adjacent('delete_forward:{}:{}'.format(key, self.hash), 1))


    def depart(self, data, sock):
        self.data_lock.acquire()
        for (key, value) in self.data.items():
            self.adjacent.send_adjacent('update:{}:{}:{}'.format(key, value[0], value[1]), 1)
        self.data_lock.release()
        self.adjacent.send_adjacent('front:{}:{}'.format(self.adjacent.succ_port, self.adjacent.succ_hash), 0)
        self.adjacent.send_adjacent('back:{}:{}'.format(self.adjacent.pred_port, self.adjacent.pred_hash), 1)
        send_message(self.main_port, 'depart')
        self.terminates = True
        self.message_queues[sock].put('DONE')

    def overlay_forward(self, data, sock):
        _, hash_of_first, curr_overlay = data.split(':')
        if (hash_of_first == self.hash):
            print(curr_overlay)
            self.message_queues[sock].put('DONE')
        else:
            data = data + '->' + self.port
            self.message_queues[sock].put(self.adjacent.send_adjacent(data, 1))
            self.message_queues[sock].put('DONE')

    def overlay(self, data, sock):
        self.message_queues[sock].put(self.adjacent.send_adjacent('overlay_forward:{}:{}'.format(self.hash, self.port), 1))
        self.message_queues[sock].put('DONE')
















class Bootstrap(Server):
    def __init__(self, ip_addr, port):
        self.network_size = 1
        super(Bootstrap, self).__init__(ip_addr, port, port)
        #replication LATER

    def join(self, data, sock):
        self.network_size += 1
        super(Bootstrap, self).join(data, sock)
        #add the replication to the queue

    def depart(self, data, sock):
        self.network_size -= 1
        self.message_queues[sock].put('DONE')

    def DHT_ends(self, data, sock):
        #print("hiuhih")
        self.network_size = 0
        self.terminates = True
        self.message_queues[sock].put('DONE')

def discover_adjacent(hash, port):
    with Communication(port) as socket:
       a = socket.socket_comm('join:' + hash).split(':')
    return int(a[0]), a[1], int(a[2]), a[3]

def send_message(port, message):
    with Communication(port) as socket:
       return socket.socket_comm(message)
