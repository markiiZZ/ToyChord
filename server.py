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
                           'query_forward_E': self.query_forward_E,
                           'query_forward_L': self.query_forward_L,
                           'delete': self.delete,
                           'delete_replicas': self.delete_replicas,
                           'depart': self.depart,
                           'DHT_ends': self.DHT_ends,
                           'overlay':self.overlay,
                           'overlay_forward':self.overlay_forward,
                           'RM':self.RM,
                           'remove':self.remove,
                           'search':self.search,
                           'reply':self.reply,
                           'update_replicas':self.update_replicas,
                           'get_network_size':self.get_network_size
                       }
        self.terminates = False
        self.ip_addr = str(ip_addr)
        self.port = str(port)
        self.main_port = master
        self.pair = (self.ip_addr, self.port)
        self.address = self.ip_addr + '&' + self.port

        m = sha1()
        for s in self.pair:
            m.update(s.encode())
        self.hash = m.hexdigest()

        self.data = {}
        self.consistency = 'RANDOM'
        self.replicas = 0
        self.data_lock = threading.Lock()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self.ip_addr, int(self.port)))
        except socket.error as msg:
            logging.error('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()

        self.sock.settimeout(1) #CHECK
        self.sock.listen(10)    #CHECK
        print("success")
        self.adjacent = Adjacent(self.hash, self.address, self.hash, self.address)

        self.message_queues = {}  # servers' reply messages

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

    def get_network_size(self, data, sock):
        send_message(self.main_port, 'get_network_size')

    def place_here(self, hashing):
        #mallon swsto
        return (self.adjacent.pred_hash < hashing <= self.hash) | (hashing <= self.hash <= self.adjacent.pred_hash) | (self.hash <= self.adjacent.pred_hash <= hashing)

    def update_pred(self, data, sock):
        _, pred_port, pred_hash = data.split(':')
        self.adjacent.update_adjacent(pred_port, pred_hash, 0)
        self.message_queues[sock].put('Updated')

    def update_succ(self, data, sock):
        _, succ_port, succ_hash = data.split(':')
        self.adjacent.update_adjacent(succ_port, succ_hash, 1)
        self.message_queues[sock].put('Updated')

    def join_DHT(self):
        pred_port, pred_hash, succ_port, succ_hash, self.consistency, self.replicas = discover_adjacent(self.hash, self.main_port)
        #print(pred_port, pred_hash, succ_port, succ_hash, self.consistency, self.replicas)
        self.adjacent.notify_adjacent(pred_port, pred_hash, self.address, self.hash, 0)
        self.adjacent.notify_adjacent(succ_port, succ_hash, self.address, self.hash, 1)
        self.adjacent.send_adjacent('get_from_succ:', 1)
        #if (self.replicas == self.network_size -1):
            #for (key, value) in self.data.items():
                #if self.belongs_here(key):
                    #print("sumvainei")
                    #send.adjacent.send_adjacent('remove:{}'.format(key), 0)

    def join(self, data, sock):
        _, hashing = data.split(':')
        if self.place_here(hashing):
            adj = self.adjacent.get_adjacent(0) + ':' + str(self.address) + ':' + self.hash
        else:
            adj = self.adjacent.send_adjacent(data, 1)
        self.message_queues[sock].put(adj)

    def remove(self, data, sock):
        _, hash_key = data.split(':')
        self.data_lock.acquire()
        del self.data[hash_key]
        self.data_lock.release()
        self.message_queues[sock].put('DONE')

    def RM(self, data, sock):
        _, hash_key = data.split(':')
        if self.place_here(hash_key):
            #self.message_queues[sock].put(self.adjacent.send_adjacent('DONE'))
            self.message_queues[sock].put(self.adjacent.send_adjacent('remove:{}'.format(hash_key), 0))
        else:
            #threading.Thread(target = self.adjacent.send_adjacent, args = ('RM:{}'.format(hash_key), 1)).start()
            self.message_queues[sock].put(self.adjacent.send_adjacent('RM:{}'.format(hash_key), 1))

    def search(self, data, sock):
        _,hash_key = data.split(':')
        key, value = self.data.get(hash_key, (None, None))
        if key != None:
            if self.adjacent.send_adjacent('search:{}'.format(key), 1) == 'None:None':
                del self.data[hash_key]
        self.message_queues[sock].put('{}:{}'.format(key, value))

    def get_from_succ(self, data, sock):
        network_size = send_message(self.main_port, 'get_network_size')
        self.data_lock.acquire()
        entries_to_del = []
        if (self.replicas > int(network_size)-1):
            for (key, value) in self.data.items():
                message = 'update:{}:{}:{}'.format(key, value[0], value[1])
                threading.Thread(target = self.adjacent.send_adjacent, args = (message, 0)).start()
            self.data_lock.release()
            self.message_queues[sock].put('DONE')
        elif (self.replicas == int(network_size)-1):
            for (key, value) in self.data.items():
                if not self.place_here(key):
                    message = 'update:{}:{}:{}'.format(key, value[0], value[1])
                    threading.Thread(target = self.adjacent.send_adjacent, args = (message, 0)).start()
                    #if not self.adjacent.pred_port.place_here(key):
                    threading.Thread(target = self.adjacent.send_adjacent, args = ('RM:{}'.format(key), 1)).start()
                    #self.message_queues[sock].put(self.adjacent.send_adjacent('RM:{}'.format(key), 1))
            self.data_lock.release()
            self.message_queues[sock].put('DONE')
        else:
            for (key, value) in self.data.items():
                if not self.place_here(key):
                    message = 'update:{}:{}:{}'.format(key, value[0], value[1])
                    threading.Thread(target = self.adjacent.send_adjacent, args = (message, 0)).start()
                    if self.adjacent.send_adjacent('search:{}'.format(key), 1) == 'None:None':
                        entries_to_del.append(key)
            for key in entries_to_del:
                del self.data[key]
            self.data_lock.release()
            self.message_queues[sock].put('DONE')

    def reply(self, data, sock):
        _, key, value = data.split(':')
        self.message_queues[sock].put('{}:{}'.format(key, value))

    def update(self, data, sock):
        _, hash_key, key, value = data.split(':')
        self.data_lock.acquire()
        if(self.data.get(hash_key, None) == None):
             self.data[hash_key] = (key, value)
             self.data_lock.release()
             self.message_queues[sock].put('DONE')
        else:
            self.data_lock.release()
            self.message_queues[sock].put(self.adjacent.send_adjacent(data, 1))

    def update_replicas(self, data, sock):
        _, hash_key, key, value, replicas, responsible, address = data.split(':')
        if (self.consistency == 'E'):
            if (replicas == '0' or responsible == self.hash):
                self.message_queues[sock].put('DONE')
            else:
                self.data_lock.acquire()
                #oi komvoi stous opoious analogei replica tou tragoudiou logika de tha to exoun
                self.data[hash_key] = (key, value)
                replicas = str(int(replicas)-1)
                self.data_lock.release()
                if (replicas == '0'):
                    self.message_queues[sock].put('DONE')
                else:
                    self.message_queues[sock].put(self.adjacent.send_adjacent('update_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, replicas, responsible, address), 1))
        else:
            if (replicas == '0'):
                #print("bhioo")
                self.message_queues[sock].put('{}:{}'.format(key, value))
            elif (responsible == self.hash):
                #("fefwf")
                send_message(address, 'reply:{}:{}'.format(key, value))
                self.message_queues[sock].put('DONE')
            else:
                #print("mphka")
                self.data_lock.acquire()
                self.data[hash_key] = (key, value)
                replicas = str(int(replicas)-1)
                self.data_lock.release()
                if (replicas == '0'):
                    send_message(address, 'reply:{}:{}'.format(key, value))
                    self.message_queues[sock].put('DONE')
                else:
                    self.message_queues[sock].put(self.adjacent.send_adjacent('update_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, replicas, responsible, address), 1))


    def insert(self, data, sock):
        _, key, value, address = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        #logging
        self.data_lock.acquire()
        #print("mphka")

        # vgazw thn periptwsh ayth wste na ksekinaw thn anazhthsh apo ton prwteuonta
        # gia na glitwsw periptwseis opws px delete, to idio tragoudi kai insert pali to idio
        #kai na mhn exei diadothei to delete se ayto ton komvo o opoios periexei antigrafo --> tote
        #de tha kanei insert to tragoudi
        #if (self.data.get(hash_key) == (key, value)):
            #self.data_lock.release()
            #self.message_queues[sock].put('DONE')
        if self.place_here(hash_key):
            if (self.data.get(hash_key) == (key, value)):
                self.data_lock.release()
                self.message_queues[sock].put('DONE')
            else:
                self.data[hash_key] = (key,value)
                #print("geia")
                self.data_lock.release()
                if (self.consistency == 'E'):
                    self.message_queues[sock].put('{}:{}'.format(key, value))
                    self.adjacent.send_adjacent('update_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, self.replicas - 1, self.hash, address), 1)
                else:
                    #print("bhbjhb")
                    self.message_queues[sock].put(self.adjacent.send_adjacent('update_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, self.replicas - 1, self.hash, address), 1))
            #print(self.port, self.data[hash_key])
        else:
            self.data_lock.release()
            self.message_queues[sock].put(self.adjacent.send_adjacent(data, 1))

    def print_node_songs(self, data, sock):
        #_, hash_of_first = data.split(':')
        hash_of_first = data.split(':')[1]
        if (hash_of_first == self.hash):
            self.message_queues[sock].put(data)
        else:
            self.data_lock.acquire()
            songs =  []
            #print(self.port) #isws axreiasto
            for (key, value) in self.data.items():
                songs.append(value)
            #print(str(songs))
            data = data + ':' + str(self.address) + ':' + str(songs)
            self.data_lock.release()
            self.message_queues[sock].put(self.adjacent.send_adjacent(data, 1))

    def query_all(self, data, sock):
        self.data_lock.acquire()
        #print(self.port) #isws axreiasto
        songs = []
        for (key, value) in self.data.items():
            songs.append(value)
        self.data_lock.release()
        if (self.adjacent.succ_hash != self.hash):
            data = self.adjacent.send_adjacent('print_node_songs:{}:{}:{}'.format(self.hash, self.address, songs), 1)
            #print(data.split(':'))
            #print(data.split(':')[2])
            #print(data.split(':')[3])
            answer = data.split(':')
            for i in answer[2:]:
                print(i)
            self.message_queues[sock].put(data)
        else:
            print(self.address)
            print(songs)
            self.message_queues[sock].put('DONE')

    def query_forward_E(self, data, sock):
        _, key, hash_of_first = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        self.data_lock.acquire()
        value = self.data.get(hash_key, (None, None))
        self.data_lock.release()
        if (value != (None, None)):
            self.message_queues[sock].put('{}:{}'.format(self.address, value[1]))
        else:
            if (hash_of_first == self.hash): #paizei pio apodotika me thn place_here
                self.message_queues[sock].put('This song does not exist')
            else:
                self.message_queues[sock].put(self.adjacent.send_adjacent(data, 1))

    def query_forward_L(self, data, sock):
        _, key, replicas = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        self.data_lock.acquire()
        value = self.data.get(hash_key, (None, None))
        self.data_lock.release()
        if (replicas != '-1'):
            if int(replicas) > 1:
                replicas = str(int(replicas)-1)
                self.message_queues[sock].put(self.adjacent.send_adjacent('query_forward_L:{}:{}'.format(key, replicas), 1))
            else:
                if(value == (None, None)):
                    self.message_queues[sock].put('This song does not exist')
                else:
                    self.message_queues[sock].put('{}:{}'.format(self.address, value[1]))
        elif self.place_here(hash_key):
            if (self.replicas > 1):
                replicas = str(self.replicas-1)
                self.message_queues[sock].put(self.adjacent.send_adjacent('query_forward_L:{}:{}'.format(key, replicas), 1))
            else:
                self.message_queues[sock].put('{}:{}'.format(self.address, value[1]))
        else:
            self.message_queues[sock].put(self.adjacent.send_adjacent('query_forward_L:{}:{}'.format(key, replicas), 1))

    def query(self, data, sock):
        _, key = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        self.data_lock.acquire()
        value = self.data.get(hash_key, (None, None))
        #print(value)
        #print(value[0])
        #print(value[1])
        self.data_lock.release()
        if (self.consistency == 'E'):
            if (value != (None, None)):
                print('{}:{}'.format(self.address, value[1]))
                self.message_queues[sock].put('{}:{}'.format(self.address, value[1]))
            else:
                answer = self.adjacent.send_adjacent('query_forward_E:{}:{}'.format(key, self.hash), 1)
                print(answer)
                self.message_queues[sock].put(answer)
        else:
            if (self.place_here(hash_key)):
                if self.replicas > 1:
                    if (self.adjacent.succ_hash != self.hash):
                        replicas = str(self.replicas-1)
                        answer = self.adjacent.send_adjacent('query_forward_L:{}:{}'.format(key, replicas), 1)
                        print(answer)
                        self.message_queues[sock].put(answer)
                    else:
                        print('{}:{}'.format(self.address, value[1]))
                        self.message_queues[sock].put('{}'.format(value[1]))
                else:
                    print('{}:{}'.format(self.address, value[1]))
                    self.message_queues[sock].put('{}'.format(value[1]))
            else:
                answer = self.adjacent.send_adjacent('query_forward_L:{}:{}'.format(key, '-1'), 1)
                print(answer)
                self.message_queues[sock].put(answer)

    """def delete_forward(self, data, sock):
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
                self.message_queues[sock].put(self.adjacent.send_adjacent(data, 1))"""

    """def delete(self, data, sock):
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
            self.message_queues[sock].put(self.adjacent.send_adjacent('delete_forward:{}:{}'.format(key, self.hash), 1))"""

    def delete_replicas(self, data, sock):
        _, hash_key, key, value, replicas, responsible, address = data.split(':')
        if (self.consistency == 'E'):
            if (replicas == '0' or responsible == self.hash):
                self.message_queues[sock].put('DONE')
            else:
                self.data_lock.acquire()
                #oi komvoi stous opoious analogei replica tou tragoudiou logika de tha to exoun
                del self.data[hash_key]
                replicas = str(int(replicas)-1)
                self.data_lock.release()
                if (replicas == '0'):
                    self.message_queues[sock].put('DONE')
                else:
                    self.message_queues[sock].put(self.adjacent.send_adjacent('delete_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, replicas, responsible, address), 1))
        else:
            if (replicas == '0'):
                #print("bhioo")
                self.message_queues[sock].put('{}:{}'.format(key, value))
            elif (responsible == self.hash):
                #("fefwf")
                send_message(address, 'reply:{}:{}'.format(key, value))
                self.message_queues[sock].put('DONE')
            else:
                #print("mphka")
                self.data_lock.acquire()
                del self.data[hash_key]
                replicas = str(int(replicas)-1)
                self.data_lock.release()
                if (replicas == '0'):
                    send_message(address, 'reply:{}:{}'.format(key, value))
                    self.message_queues[sock].put('DONE')
                else:
                    self.message_queues[sock].put(self.adjacent.send_adjacent('delete_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, replicas, responsible, address), 1))

    def delete(self, data, sock):
        _, key, address = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
       #logging
       #print("mphka")

       # vgazw thn periptwsh ayth wste na ksekinaw thn anazhthsh apo ton prwteuonta
       # gia na glitwsw periptwseis opws px delete, to idio tragoudi kai insert pali to idio
       #kai na mhn exei diadothei to delete se ayto ton komvo o opoios periexei antigrafo --> tote
       #de tha kanei insert to tragoudi
       #if (self.data.get(hash_key) == (key, value)):
           #self.data_lock.release()
           #self.message_queues[sock].put('DONE')
        if self.place_here(hash_key):
            self.data_lock.acquire()
            (key, value) = self.data.pop(hash_key, (None, None))
            #print((key, value))
            self.data_lock.release()
            if ((key, value) == (None, None)):
                print('This song does not exist')
                self.message_queues[sock].put('DONE')
            else:
                if (self.consistency == 'E'):
                    self.message_queues[sock].put('{}:{}'.format(key, value))
                    self.adjacent.send_adjacent('delete_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, self.replicas - 1, self.hash, address), 1)
                else:
                   #print("bhbjhb")
                    self.message_queues[sock].put(self.adjacent.send_adjacent('delete_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, self.replicas - 1, self.hash, address), 1))
           #print(self.port, self.data[hash_key])
        else:
            self.message_queues[sock].put(self.adjacent.send_adjacent(data, 1))

    def depart(self, data, sock):
        network_size = send_message(self.main_port, 'get_network_size')
        if(self.replicas > int(network_size)-1):
             pass
        else:
            self.send_data_forward()
        self.adjacent.send_adjacent('front:{}:{}'.format(self.adjacent.succ_port, self.adjacent.succ_hash), 0)
        self.adjacent.send_adjacent('back:{}:{}'.format(self.adjacent.pred_port, self.adjacent.pred_hash), 1)
        send_message(self.main_port, 'depart')
        self.terminates = True
        self.message_queues[sock].put('DONE')

    def send_data_forward(self):
        self.data_lock.acquire()
        for (key, value) in self.data.items():
            self.adjacent.send_adjacent('update:{}:{}:{}'.format(key, value[0], value[1]), 1)


    def overlay_forward(self, data, sock):
        _, hash_of_first, curr_overlay = data.split(':')
        if (hash_of_first == self.hash):
            print(curr_overlay)
            self.message_queues[sock].put('DONE')
        else:
            data = data + '->' + self.address
            self.message_queues[sock].put(self.adjacent.send_adjacent(data, 1))
            self.message_queues[sock].put('DONE')

    def overlay(self, data, sock):
        self.message_queues[sock].put(self.adjacent.send_adjacent('overlay_forward:{}:{}'.format(self.hash, self.address), 1))
        self.message_queues[sock].put('DONE')



class Bootstrap(Server):
    def __init__(self, ip_addr, port, consistency, replicas):
        address = ip_addr + '&' + port
        super(Bootstrap, self).__init__(ip_addr, port, address)
        self.network_size = 1
        self.consistency = consistency
        self.replicas = int(replicas)
        #replication LATER

    def join(self, data, sock):
        self.network_size += 1
        super(Bootstrap, self).join(data, sock)
        self.message_queues[sock].put(self.message_queues[sock].get() + ':' + self.consistency + ':' + str(self.replicas))

    def depart(self, data, sock):
        self.network_size -= 1
        self.message_queues[sock].put('DONE')

    def DHT_ends(self, data, sock):
        #print("hiuhih")
        self.network_size = 0
        self.terminates = True
        self.message_queues[sock].put('DONE')

    def get_network_size(self, data, sock):
        self.message_queues[sock].put(str(self.network_size))

def discover_adjacent(hash, port):
    with Communication(port) as socket:
       a = socket.socket_comm('join:' + hash).split(':')
    return a[0], a[1], a[2], a[3], a[4], int(a[5])

def send_message(port, message):
    with Communication(port) as socket:
       return socket.socket_comm(message)
