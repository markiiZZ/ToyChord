#!/usr/bin/env python3

import cmd2
import sys
import time
from server import Server, Client
#from intermediate import Intermediate

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
        self.bootstr = Bootstrap("127.0.0.1", port)

    def do_fake(self, port):
        pelatis = Client(port)
        pelatis.communication(b'hello world')

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
