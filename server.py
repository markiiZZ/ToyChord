#!/usr/bin/env python

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

class Server(object):
    """The server class implements join, insert, query, delete, depart and the relevant methods"""

    def __init__(self, ip_addr, port, master):
        """These are the methods that are called from the nodes, identified from a "key" of the following
           dictionary, which is passed to its node through part of a message by another node or directly
           from the cmd"""
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
                           'get_network_size':self.get_network_size,
                           'send_to_succ':self.send_to_succ,
                           'reply_query':self.reply_query
                       }
        #becomes true if the node departs
        self.terminates = False
        self.ip_addr = str(ip_addr)
        self.port = str(port)
        #address of the bootstrap
        self.main_address = master
        self.pair = (self.ip_addr, self.port)
        self.address = self.ip_addr + '&' + self.port

        m = sha1()
        for s in self.pair:
            m.update(s.encode())
        #hash of the node
        self.hash = m.hexdigest()

        #songs that are allocated to this node
        self.data = {}
        #initialize consistency to a random value until the bootstrap updates it
        self.consistency = 'RANDOM'
        #initialize number of replicas to a random value until the bootstrap updates it
        self.replicas = 0
        self.data_lock = threading.Lock()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind((self.ip_addr, int(self.port)))
        except socket.error as msg:
            logging.error('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()

        self.sock.settimeout(1)
        self.sock.listen(10)
        print("success")
        self.adjacent = Adjacent(self.hash, self.address, self.hash, self.address)

        # queue that stores the messages
        self.message_q = {}

    def quit(self, data, sock):
        """Quits"""
        self.message_q[sock].put('CLOSE')

    def bad_request(self, data, sock):
        self.message_q[sock].put('Invalid server command')

    def main_loop(self):
        """This method loops until the node departs"""
        while True:
            self.connection()
            if self.terminates:
                time.sleep(1)
                return

    def connection(self):
        """The node accepts connections"""
        try:
            connect, _ = self.sock.accept()
        except socket.timeout:
            pass
        else:
            #initialize the message queue
            self.message_q[connect] = Queue()
            threading.Thread(target = self.socket_thread, args = (connect,)).start()

    def socket_thread(self, sock):
        """"The node accepts messages through its socket from other servers and calls the appropriate
           method according to the first word of the message which is a key to a certain method above.
           It sends the answer or the result of the method, which is stored to the message queues, to
           the other end of the socket that connected with it though the "Communication class"""
        while True:
            try:
                #receive the message
                data = sock.recv(1024).decode()
                if not data:
                    break
                else:
                    #call the corresponding method
                    command = self.methods.get(data.split(':')[0])
                    #if it is not in the dectionary call the 'bad_request' method
                    if (command == None):
                        self.bad_request(data, sock)
                    else:
                        command(data, sock)
            except socket.error:
                logging.error('Failed to receive message')
                break
            else:
                try:
                    #get the answer from the queue
                    answer = self.message_q[sock].get_nowait()
                except queue.Empty:
                    pass
                else:
                    #send it to the other end of the socket so that the instance of the server
                    #that called us unblocks
                    sock.send(answer.encode())
                    #if the received message is 'CLOSE', the socket closes
                    if answer == 'CLOSE':
                        del self.message_q[sock]
                        sock.close()
                        return

    def DHT_ends(self, data, sock):
        """This method is called when the DHT ends (bootstrap departs)"""
        send_message(self.main_address, 'DHT_ends')

    def get_network_size(self, data, sock):
        """This method is sent from a node to the bootstrap so that it gets informed of the size of the network"""
        send_message(self.main_address, 'get_network_size')

    def place_here(self, hashing):
        """Method that determines if a node belongs in a certain position (through its hashing)
           or a song belongs to a specific node"""
        return (self.adjacent.pred_hash < hashing <= self.hash) | (hashing <= self.hash <= self.adjacent.pred_hash) | (self.hash <= self.adjacent.pred_hash <= hashing)


    """ update_pred -- update_succ -- join_DHT -- join -- discover_adjacent(class Server)
        join (class Bootstrap) -- notify_adjacent -- update_adjacent (class Adjacent)
                                    implement JOIN

        remove -- RM -- search -- update -- get_from_succ
                      ensure that the new node gets the correct songs from its successor"""


    def update_pred(self, data, sock):
        """Called from the notify_adjacent method of the Adjacent class to inform a node
           that the new node is its predecessor"""
        _, pred_addr, pred_hash = data.split(':')
        self.adjacent.update_adjacent(pred_addr, pred_hash, 0)
        self.message_q[sock].put('Updated')

    def update_succ(self, data, sock):
        """Called from the notify_adjacent method of the Adjacent class to inform a node
           that the new node is its successor"""
        _, succ_addr, succ_hash = data.split(':')
        self.adjacent.update_adjacent(succ_addr, succ_hash, 1)
        self.message_q[sock].put('Updated')

    def join_DHT(self):
        """Called by a node when it joins the DHT"""
        #Calls the discover_adjacent method to learn about the address and the hashing of the back and next node
        #and the type of consistency(linear/eventual) and the number of replicas needed, through the bootstrap
        pred_addr, pred_hash, succ_addr, succ_hash, self.consistency, self.replicas = discover_adjacent(self.hash, self.main_address)
        #Notifies the back/next node that the new node is now their front/back node respectively
        self.adjacent.notify_adjacent(pred_addr, pred_hash, self.address, self.hash, 0)
        self.adjacent.notify_adjacent(succ_addr, succ_hash, self.address, self.hash, 1)
        #sends the get_from_succ key (that maps to the get_from_succ method) to the next node to get the
        #corresponding songs
        self.adjacent.send_adjacent('get_from_succ:', 1)

    def join(self, data, sock):
        """Allocates the node in the DHT"""
        #When a new node joins, this method is called for the first time from the bootstrap and if the new
        #node does not get to be its predecessor, the message is sent to the next node and is recursively called
        #from the nodes until the place of the new node is found
        _, hashing = data.split(':')
        if self.place_here(hashing):
            adj = self.adjacent.get_adjacent(0) + ':' + str(self.address) + ':' + self.hash
        else:
            adj = self.adjacent.send_adjacent(data, 1)
        self.message_q[sock].put(adj)

    def remove(self, data, sock):
        """This method deletes a song from the node that calls it (helping og get_from_succ)"""
        _, hash_key = data.split(':')
        self.data_lock.acquire()
        del self.data[hash_key]
        self.data_lock.release()
        self.message_q[sock].put('DONE')

    def RM(self, data, sock):
        """This method finds the responsible node of a song and deletes it from its predecessor through the remove method
           (helping of get_from_succ)"""
        _, hash_key = data.split(':')
        if self.place_here(hash_key):
            self.message_q[sock].put(self.adjacent.send_adjacent('remove:{}'.format(hash_key), 0))
        else:
            self.message_q[sock].put(self.adjacent.send_adjacent('RM:{}'.format(hash_key), 1))

    def search(self, data, sock):
        """For a node that has a song, searches if the successor does not have it (the node had the last
         replica) and if so it erases it from that node"""
        _,hash_key = data.split(':')
        key, value = self.data.get(hash_key, (None, None))
        if key != None:
            if self.adjacent.send_adjacent('search:{}'.format(key), 1) == 'None:None':
                del self.data[hash_key]
        self.message_q[sock].put('{}:{}'.format(key, value))

    def update(self, data, sock):
       """This method adds a new song to a node's data if it does not already exist"""
       _, hash_key, key, value = data.split(':')
       self.data_lock.acquire()
       if(self.data.get(hash_key, None) == None):
            self.data[hash_key] = (key, value)
            self.data_lock.release()
            self.message_q[sock].put('DONE')
       else:
           self.data_lock.release()
           self.message_q[sock].put(self.adjacent.send_adjacent(data, 1))

    def get_from_succ(self, data, sock):
        """Through this method, the new node that joins, takes the corresponding songs from its successor"""
        #Asks from the bootstrap the size of the network to enter the right case
        network_size = send_message(self.main_address, 'get_network_size')
        self.data_lock.acquire()
        entries_to_del = []
        #if the number of replicas is greater than the network size, before the entrance of the node,
        #then the new node gets all the songs from the successor (all the nodes have obviously all songs
        #of the DHT in this case)for every song of the successor, the update message is sent to the new node,
        # which adds each song to it through the corresponging method
        if (self.replicas > int(network_size)-1):
            for (key, value) in self.data.items():
                message = 'update:{}:{}:{}'.format(key, value[0], value[1])
                threading.Thread(target = self.adjacent.send_adjacent, args = (message, 0)).start()
            self.data_lock.release()
            self.message_q[sock].put('DONE')
        #if the number of replicas was equal to the network size, before the entrance of the new node,
        #then the successor(that has all songs of the DHT as explained above) passes every song to the new node(through update)
        #except of the songs that he is responsible for (the replicas reach up to the pred of the new node).
        #for every song ((key),(value)) the RM method is called, that finds the responsible node for every song and
        #deletes it from its predecessor (through the remove method) to keep the right number of replicas after the new join
        elif (self.replicas == int(network_size)-1):
            for (key, value) in self.data.items():
                if not self.place_here(key):
                    message = 'update:{}:{}:{}'.format(key, value[0], value[1])
                    threading.Thread(target = self.adjacent.send_adjacent, args = (message, 0)).start()
                    #if not self.adjacent.pred_addr.place_here(key):
                    threading.Thread(target = self.adjacent.send_adjacent, args = ('RM:{}'.format(key), 1)).start()
                    #self.message_q[sock].put(self.adjacent.send_adjacent('RM:{}'.format(key), 1))
            self.data_lock.release()
            self.message_q[sock].put('DONE')
        #if the number of replicas is smaller than the network size, before the entrance of the new node,
        #then the successor of the new node, sends the "update" message to the new node for the songs that he is
        #not responsible for (for the songs that he is responsible for, the new node cannot have a replica of them
        #because the "else" condition would not be valid).Then for every song that is passed to the new node, the "search" message is sent
        #to the next nodes. If a node has the song but its successor doesnt have it, then this means that the successor
        #had the last replica and now the node must have the last replica because a new node entered the DHT. So the song
        #will be deleted from him
        else:
            for (key, value) in self.data.items():
                if not self.place_here(key):
                    message = 'update:{}:{}:{}'.format(key, value[0], value[1])
                    threading.Thread(target = self.adjacent.send_adjacent, args = (message, 0)).start()
                    #this is activated if the successor of the new node had the last replica of the song,
                    #then it will be deleted from him and the new node will have the last replica
                    if self.adjacent.send_adjacent('search:{}'.format(key), 1) == 'None:None':
                        entries_to_del.append(key)
            for key in entries_to_del:
                del self.data[key]
            self.data_lock.release()
            self.message_q[sock].put('DONE')


    """reply -- update -- update_replicas --insert implement INSERT"""


    def reply(self, data, sock):
        """Used to reply directly to the caller"""
        _, key, value = data.split(':')
        self.message_q[sock].put('{}:{}'.format(key, value))

    def update_replicas(self, data, sock):
        """This method is called to add/update replicas of a new song that enters the DHT"""
        _, hash_key, key, value, replicas, responsible, address = data.split(':')
        #if the consistency is eventual
        if (self.consistency == 'E'):
            #if the circle of the Chord has occured or the numbers of replicas to be updated is "0" then
            #the method returns through a queue message
            if (replicas == '0' or responsible == self.hash):
                self.message_q[sock].put('DONE')
            #else the nodes that need to store a replica, add the song to their data and decrement the number of replicas
            #that have to be updated. if this number reaches zero it returns
            else:
                self.data_lock.acquire()
                self.data[hash_key] = (key, value)
                replicas = str(int(replicas)-1)
                self.data_lock.release()
                if (replicas == '0'):
                    self.message_q[sock].put('DONE')
                else:
                    self.message_q[sock].put(self.adjacent.send_adjacent('update_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, replicas, responsible, address), 1))
        #if the consistency is linear
        else:
            #if number of replicas is one then no replicas should be added
            if (replicas == '0'):
                send_message(address, 'reply:{}:{}'.format(key, value))
                self.message_q[sock].put('DONE')
            #if the circle of the Chord has occured, a reply message is sent to the caller (because all replicas are updated)
            #and the method returns
            elif (responsible == self.hash):
                send_message(address, 'reply:{}:{}'.format(key, value))
                self.message_q[sock].put('DONE')
            #else same steps are followed as the eventual case but the reply message is sent to the caller when the
            #number of replicas reaches zero
            else:
                self.data_lock.acquire()
                self.data[hash_key] = (key, value)
                replicas = str(int(replicas)-1)
                self.data_lock.release()
                if (replicas == '0'):
                    send_message(address, 'reply:{}:{}'.format(key, value))
                    self.message_q[sock].put('DONE')
                else:
                    self.message_q[sock].put(self.adjacent.send_adjacent('update_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, replicas, responsible, address), 1))

    def insert(self, data, sock):
        """This method is responsible for insertion of a new song to the DHT"""
        _, key, value, address = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        self.data_lock.acquire()
        #if this node is responsible for the new song
        if self.place_here(hash_key):
            #if he already has the (key,value) pair --> no update
            if (self.data.get(hash_key) == (key, value)):
                self.data_lock.release()
                self.message_q[sock].put('DONE')
            #else he adds the song to its data (or updates the song that had the same key)
            #and follows the right steps for each type of consistency
            else:
                self.data[hash_key] = (key,value)
                self.data_lock.release()
                #if the consistency is eventual, then the answer is passed to the caller and then the
                #replicas are updated through the update_replicas method
                if (self.consistency == 'E'):
                    send_message(address, 'reply:{}:{}'.format(key, value))
                    message = 'update_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, self.replicas - 1, self.hash, address)
                    threading.Thread(target = self.adjacent.send_adjacent, args = (message, 1)).start()
                    self.message_q[sock].put('DONE')

                #else the update of the replicas is implemented before the return of the method
                else:
                    self.message_q[sock].put(self.adjacent.send_adjacent('update_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, self.replicas - 1, self.hash, address), 1))
        #the method is called recursively by the nodes until the responsible node is found
        else:
            self.data_lock.release()
            self.message_q[sock].put(self.adjacent.send_adjacent(data, 1))




    """print_node_songs -- query_all implement  QUERY * """


    def print_node_songs(self, data, sock):
        """Every node prints its songs"""
        #The second element of the message is the hash of the node that implements the query *
        hash_of_first = data.split(':')[1]
        #if the circle of the Chord has occured the method returns the list with addresses and songs
        if (hash_of_first == self.hash):
            self.message_q[sock].put(data)
        #else its node adds its songs and address to the message and sends it to its successor until the circle has occured
        else:
            self.data_lock.acquire()
            songs = []
            for (key, value) in self.data.items():
                songs.append(value)
            data = data + ':' + str(self.address) + ':' + str(songs)
            self.data_lock.release()
            self.message_q[sock].put(self.adjacent.send_adjacent(data, 1))

    def query_all(self, data, sock):
        """This method is called with the query * request"""
        self.data_lock.acquire()
        songs = []
        #the node that implements the "query *" adds its data to a list
        for (key, value) in self.data.items():
            songs.append(value)
        self.data_lock.release()
        #if he is not the only node in the DHT he sends "print_node_song" message with its hash, address and songs to the next node
        if (self.adjacent.succ_hash != self.hash):
            data = self.adjacent.send_adjacent('print_node_songs:{}:{}:{}'.format(self.hash, self.address, songs), 1)
            #the "data" contains the address of the node with its songs that the print_node_songs method returns
            #the node prints the data
            answer = data.split(':')
            for i in answer[2:]:
                print(i)
            self.message_q[sock].put(data)
        #if the node is the only node in the DHT it prints its address and songs
        else:
            print(self.address)
            print(songs)
            self.message_q[sock].put('DONE')


    """query -- query_forward_E -- query_forward_L -- reply_query implement QUERY KEY according to consistency (linear/eventual)
        ALONG WITH THE VALUE, WE ALSO PRINT THE ADDRESS OF THE NODE THAT GAVE THE ANSWER """

    def reply_query(self,data,sock):
        _, addr, value = data.split(':')
        print('{}:{}'.format(addr, value))
        self.message_q[sock].put('DONE')

    def query_forward_E(self, data, sock):
        """This method is called for the "query key" case when the consistency is eventual"""
        _, key, hash_of_first, address = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        self.data_lock.acquire()
        value = self.data.get(hash_key, (None, None))
        self.data_lock.release()
        #if the closest node to the node that implements the query has the song, it simply returns it
        if (value != (None, None)):
            send_message(address, 'reply_query:{}:{}'.format(self.address, value[1]))
            self.message_q[sock].put('DONE')
        #else if a circle has occured, the song has not been found so it does not exist. If not, the method is called from
        #the next nodes and if a node finds it, the method returns with the song
        else:
            if (hash_of_first == self.hash):
                send_message(address, 'reply_query:{}:{}'.format(self.address, 'This song does not exist'))
                self.message_q[sock].put('DONE')
            else:
                self.message_q[sock].put(self.adjacent.send_adjacent(data, 1))

    def query_forward_L(self, data, sock):
        """This method is called for the "query key" case when the consistency is linear"""
        _, key, replicas, address = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        self.data_lock.acquire()
        value = self.data.get(hash_key, (None, None))
        self.data_lock.release()
        #if the song has passed through the node that is responsible for it
        if (replicas != '-1'):
            #if we haven't reached the last replica yet, call the method from the next node
            if int(replicas) > 1:
                replicas = str(int(replicas)-1)
                self.message_q[sock].put(self.adjacent.send_adjacent('query_forward_L:{}:{}:{}'.format(key, replicas, address), 1))
            #else if the last replica exists, we return the song, else we reply "This song does not exist"
            else:
                if(value == (None, None)):
                    send_message(address,'reply_query:{}:{}'.format(self.address, 'This song does not exist'))
                    self.message_q[sock].put('DONE')
                else:
                    send_message(address,'reply_query:{}:{}'.format(self.address, value[1]))
                    self.message_q[sock].put('DONE')
        #if we find the node that is responsible for the song
        elif self.place_here(hash_key):
            #if the number of replicas is greater than one we call the method from the next node with the correct number of replicas
            #to find the last replica of the song (if it exists)
            if (self.replicas > 1):
                replicas = str(self.replicas-1)
                self.message_q[sock].put(self.adjacent.send_adjacent('query_forward_L:{}:{}:{}'.format(key, replicas, address), 1))
            #else this node has the only replica and it returns its value along with the address (if it exists)
            else:
                if(value == (None, None)):
                    send_message(address,'reply_query:{}:{}'.format(self.address, 'This song does not exist'))
                    self.message_q[sock].put('DONE')
                else:
                    send_message(address,'reply_query:{}:{}'.format(self.address, value[1]))
                    self.message_q[sock].put('DONE')
        #if we haven't reached the responsible node of the song we pass the message to the next node
        else:
            self.message_q[sock].put(self.adjacent.send_adjacent('query_forward_L:{}:{}:{}'.format(key, replicas, address), 1))

    def query(self, data, sock):
        """This method implements QUERY KEY according to the consistency (linear/eventual)
           It returns the value of the (key, value) pair along with the node that returns it
           so that the difference of the linear and eventual case is visible"""
        _, key = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        self.data_lock.acquire()
        value = self.data.get(hash_key, (None, None))
        self.data_lock.release()
        #if the consistency is eventual
        if (self.consistency == 'E'):
            #if the node that implements the query has the song
            if (value != (None, None)):
                #it prints the (key, value) pair that corresponds to the song and returns
                print('{}:{}'.format(self.address, value[1]))
                self.message_q[sock].put('{}:{}'.format(self.address, value[1]))
            else:
                #else it sends a "query_forward_message" to its successor to find the closest node that has a replica
                #(even with a stale value)and prints the answer of the method
                self.message_q[sock].put(self.adjacent.send_adjacent('query_forward_E:{}:{}:{}'.format(key, self.hash, self.address), 1))
        #if the consistency is linear
        else:
            #if the node that implements the query is responsible for the song
            if (self.place_here(hash_key)):
                #if the number of replicas is greater than one
                if self.replicas > 1:
                    #if this node is not the only node in the DHT, it sends the query_foreard_L message to its successor
                    #so that it gets the value of the last replica of the song and prints the answer of the method
                    if (self.adjacent.succ_hash != self.hash):
                        replicas = str(self.replicas-1)
                        self.message_q[sock].put(self.adjacent.send_adjacent('query_forward_L:{}:{}:{}'.format(key, replicas, self.address), 1))
                    #if the node is alone in the DHT, it simply prints the (key, value) pair if it exists
                    #else "This song does not exist" message
                    else:
                        if (value != (None, None)):
                            print('{}:{}'.format(self.address, value[1]))
                            self.message_q[sock].put('{}'.format(value[1]))
                        else:
                            print('{}:{}'.format(self.address, 'This song does not exist'))
                            self.message_q[sock].put('This song does not exist')
                #same if the number of replicas is one
                else:
                    if (value != (None, None)):
                        print('{}:{}'.format(self.address, value[1]))
                        self.message_q[sock].put('{}'.format(value[1]))
                    else:
                        print('{}:{}'.format(self.address, 'This song does not exist'))
                        self.message_q[sock].put('This song does not exist')
            #if the node that implements the query is not responsible for the song then the nodes recursively
            #call this method until the outer if of the linear case, is activated
            else:
                self.message_q[sock].put(self.adjacent.send_adjacent('query_forward_L:{}:{}:{}'.format(key, '-1', self.address), 1))


    """delete -- delete_replicas implement DELETE"""

    """"The DELETE implementation is almost symmetric to the INSERT"""


    def delete_replicas(self, data, sock):
        _, hash_key, key, value, replicas, responsible, address = data.split(':')
        if (self.consistency == 'E'):
            if (replicas == '0' or responsible == self.hash):
                self.message_q[sock].put('DONE')
            else:
                self.data_lock.acquire()
                del self.data[hash_key]
                replicas = str(int(replicas)-1)
                self.data_lock.release()
                if (replicas == '0'):
                    self.message_q[sock].put('DONE')
                else:
                    self.message_q[sock].put(self.adjacent.send_adjacent('delete_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, replicas, responsible, address), 1))
        else:
            if (replicas == '0'):
                send_message(address, 'reply:{}:{}'.format(key, value))
                self.message_q[sock].put('DONE')
            elif (responsible == self.hash):
                send_message(address, 'reply:{}:{}'.format(key, value))
                self.message_q[sock].put('DONE')
            else:
                self.data_lock.acquire()
                del self.data[hash_key]
                replicas = str(int(replicas)-1)
                self.data_lock.release()
                if (replicas == '0'):
                    send_message(address, 'reply:{}:{}'.format(key, value))
                    self.message_q[sock].put('DONE')
                else:
                    self.message_q[sock].put(self.adjacent.send_adjacent('delete_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, replicas, responsible, address), 1))

    def delete(self, data, sock):
        _, key, address = data.split(':')
        key1 = key.encode()
        hash_key = sha1(key1).hexdigest()
        if self.place_here(hash_key):
            self.data_lock.acquire()
            (key, value) = self.data.pop(hash_key, (None, None))
            self.data_lock.release()
            if ((key, value) == (None, None)):
                send_message(address,'reply_query:{}:{}'.format(self.address, 'This song does not exist'))
                self.message_q[sock].put('DONE')
            else:
                if (self.consistency == 'E'):
                    send_message(address, 'reply:{}:{}'.format(key, value))
                    message = 'delete_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, self.replicas - 1, self.hash, address)
                    threading.Thread(target = self.adjacent.send_adjacent, args = (message, 1)).start()
                    self.message_q[sock].put('DONE')
                else:
                    self.message_q[sock].put(self.adjacent.send_adjacent('delete_replicas:{}:{}:{}:{}:{}:{}'.format(hash_key, key, value, self.replicas - 1, self.hash, address), 1))
        else:
            self.message_q[sock].put(self.adjacent.send_adjacent(data, 1))


    """depart -- send_data_forward -- update implement DEPART"""


    def send_to_succ(self):
        """Retrieve the songs from a departing node"""
        #For each song, through the update method, the first node that does not have the song, gets it
        #and becomes its last replica
        self.data_lock.acquire()
        for (key, value) in self.data.items():
            self.adjacent.send_adjacent('update:{}:{}:{}'.format(key, value[0], value[1]), 1)
        self.data_lock.release()

    def depart(self, data, sock):
        """This method implements the depart of a random node (not the bootstrap)"""
        network_size = send_message(self.main_address, 'get_network_size')
        #if the number of replicas is greater than the network size, before the depart of the node,
        #then the songs of the DHT do not have to be reallocated(all the nodes already contained all songs)
        if(self.replicas > int(network_size)-1):
             pass
        #else the node sends its songs to its successor
        else:
            self.send_to_succ()
        self.adjacent.send_adjacent('front:{}:{}'.format(self.adjacent.succ_addr, self.adjacent.succ_hash), 0)
        self.adjacent.send_adjacent('back:{}:{}'.format(self.adjacent.pred_addr, self.adjacent.pred_hash), 1)
        #The depart message is sent to the bootstrap to update the network size and the termination value becomes true
        send_message(self.main_address, 'depart')
        self.terminates = True
        self.message_q[sock].put('DONE')


    """overlay_forward -- overlay implement overlay"""


    """The folliwing 2 methods implement the overlay request"""
    def overlay_forward(self, data, sock):
        _, hash_of_first, curr_overlay = data.split(':')
        if (hash_of_first == self.hash):
            print(curr_overlay)
            self.message_q[sock].put('DONE')
        else:
            data = data + '->' + self.address
            self.message_q[sock].put(self.adjacent.send_adjacent(data, 1))
            self.message_q[sock].put('DONE')

    def overlay(self, data, sock):
        self.message_q[sock].put(self.adjacent.send_adjacent('overlay_forward:{}:{}'.format(self.hash, self.address), 1))
        self.message_q[sock].put('DONE')



class Bootstrap(Server):
    """Bootstrap class - accesses the methods of the base class Server"""

    def __init__(self, ip_addr, port, consistency, replicas):
        """Initialization of the bootstrap server, which enters the DHT first"""
        address = ip_addr + '&' + port

        super(Bootstrap, self).__init__(ip_addr, port, address)
        #Initializes the size of the network
        self.network_size = 1
        #The consistency and the number of replicas are given from the user and passed as variables
        #of the bootstrap
        self.consistency = consistency
        self.replicas = int(replicas)

    def join(self, data, sock):
        """Every time a node join the DHT, bootstrap updates the size of the network and passes the
           type of consistency, number of replicas to the new node through the message queue"""
        self.network_size += 1
        super(Bootstrap, self).join(data, sock)
        self.message_q[sock].put(self.message_q[sock].get() + ':' + self.consistency + ':' + str(self.replicas))

    def depart(self, data, sock):
        """Every time a node departs, bootstrap updates the network size"""
        self.network_size -= 1
        self.message_q[sock].put('DONE')

    def DHT_ends(self, data, sock):
        """The DHT ends when the bootstrap departs (assuming last of all)"""
        self.network_size = 0
        print("Bye")
        self.terminates = True
        self.message_q[sock].put('DONE')

    def get_network_size(self, data, sock):
        """Informs a certain node about the network size"""
        self.message_q[sock].put(str(self.network_size))


"""Called from a node that joins a DHT so that a socket with the bootstrap is created to find the
   successor and predecessor of a new node"""
def discover_adjacent(hash, address):
    with Communication(address) as socket:
       a = socket.socket_comm('join:' + hash).split(':')
    return a[0], a[1], a[2], a[3], a[4], int(a[5])

"""Called from a node to communicate with a node that is not adjacent to it"""
def send_message(address, message):
    with Communication(address) as socket:
       return socket.socket_comm(message)
