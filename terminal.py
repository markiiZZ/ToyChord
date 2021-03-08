#!/usr/bin/env python3

import cmd2
import sys
import time
from server import *
from communication import Communication

class ToyChord(cmd2.Cmd):

    def do_greet(self, person):
        """greet [person]
        Greet the named person"""
        if person:
            print("hi,", person)
        else:
            print('hi')

    def do_bootstrap(self, port):
        """bootstrap enters the DHT"""
        self.node = Bootstrap("127.0.0.1", port)
        threading.Thread(target = self.node.connection, args = ()).start()
        #sys.exit() #mallon

    def do_join(self, port):
        self.node = Server("127.0.0.1", port, 5000)
        self.node.join_DHT()
        my_thread = threading.Thread(target = self.node.connection, args = ()).start()
        #sys.exit()

    def do_insert(self, line):
        key = line.split(', ')[0]
        value = line.split(', ')[1]
        port = self.node.port
        with Communication(port) as sock:
            sock.socket_comm('insert:{}:{}'.format(key, value))

    def do_query(self, key):
        port = self.node.port
        if (key == '*'):
            with Communication(port) as sock:
                sock.socket_comm('query_all')
        else:
            with Communication(port) as sock:
                sock.socket_comm('query:{}'.format(key))

    def do_delete(self, key):
        port = self.node.port
        with Communication(port) as sock:
            sock.socket_comm('delete:{}'.format(key))

    def do_depart(self):
        port = self.port
        with Communication(port) as sock:
            sock.socket_comm('depart')
        my_thread.join()



    def do_print(self, random):
        port = self.bootstr.port
        print(port)

    def do_exit(self, line):
        return True

def main():

    toy = ToyChord()
    try:
        toy.cmdloop()
    except KeyboardInterrupt:
        toy.do_exit(None)

if __name__ == '__main__':
    main()
