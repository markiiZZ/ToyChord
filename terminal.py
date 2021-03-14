#!/usr/bin/env python3

import cmd2
import argparse
from cmd2 import with_argument_list, ansi
import sys
import time
import click
from pyfiglet import Figlet
from server import *
from communication import Communication
from multiprocessing import Process

class ToyChord(cmd2.Cmd):

    CMD_CAT_TOYCHORD = 'ToyChord Commands'
    def __init__(self):
        """Initialize the base class as well as this one"""
        super().__init__()
        self.prompt = 'ToyChord@ntua$ '
        self.allow_style = ansi.STYLE_TERMINAL
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        self.ip_addr = s.getsockname()[0]
        s.close()

    @with_argument_list
    def do_bootstrap(self, line):
        """Bootstrap enters the DHT"""
        consistency = line[0]
        replicas = line[1]
        port = line[2]
        self.node = Bootstrap(self.ip_addr, port, consistency, replicas)
        self.my_Process = Process(target = self.node.main_loop, args = ())
        self.my_Process.start()
        #sys.exit() #mallon

    def do_join(self, port):
        """A node (not bootstrap) joins the DHT"""
        self.node = Server(self.ip_addr, port, "192.168.1.3&5000")
        self.node.join_DHT()
        self.my_Process = Process(target = self.node.main_loop, args = ())
        self.my_Process.start()
        #sys.exit()

    @with_argument_list
    def do_insert(self, line):
        """Insert a new (key, value) pair"""
        #key = line.split(', ')[0]
        #value = line.split(', ')[1]
        port = self.node.port
        address = self.ip_addr + '&' + port
        with Communication(address) as sock:
            sock.socket_comm('insert:{}:{}:{}'.format(line[0], line[1], address))

    def do_query(self, key):
        """Search a specific (key, value) pair or all of them"""
        port = self.node.port
        address = self.ip_addr + '&' + port
        if (key == '*'):
            with Communication(address) as sock:
                sock.socket_comm('query_all')
        else:
            with Communication(address) as sock:
                sock.socket_comm('query:{}'.format(key))

    def do_delete(self, key):
        """Delete an existing (key, value) pair"""
        port = self.node.port
        address = self.ip_addr + '&' + port
        with Communication(address) as sock:
            sock.socket_comm('delete:{}:{}'.format(key, address))

    def do_depart(self, line):
        """A node departs from DHT"""
        port = self.node.port
        address = self.ip_addr + '&' + port
        if (address == "192.168.1.3&5000"):
            with Communication(address) as sock:
                sock.socket_comm('DHT_ends')
        else:
            with Communication(address) as sock:
                sock.socket_comm('depart')
        self.my_Process.join()

    def do_overlay(self, line):
        """Display DHT topology"""
        port = self.node.port
        address = self.ip_addr + '&' + port
        with Communication(address) as sock:
            sock.socket_comm('overlay')

    def do_exit(self, line):
        return True

    cmd2.categorize((do_bootstrap,
            do_join,
            do_insert,
            do_query,
            do_delete,
            do_depart,
            do_overlay), CMD_CAT_TOYCHORD)

def main():

    f = Figlet(font='slant')
    click.echo(f.renderText('ToyChord'))
    click.echo(" Georgia Stavropoulou \n")
    click.echo(" Nikoleta-Markela Iliakopoulou \n")
    click.echo(" Stefanos-Stamatis Achlatis \n")

    toy = ToyChord()
    try:
        toy.cmdloop()
    except KeyboardInterrupt:
        toy.do_exit(None)

if __name__ == '__main__':
    main()
