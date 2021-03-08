import sys
import logging

from communication import Communication

class Adjacent(object):

    def __init__(self, pred_hash, pred_port, succ_hash, succ_port):
        self.pred_hash = pred_hash
        self.pred_port = pred_port
        self.succ_hash = succ_hash
        self.succ_port = succ_port

    def notify_adjacent(self, new_port, new_hash, my_port, my_hash, flag):
        if (flag == 0): #0 is for pred
            with Communication(new_port) as sock:
               sock.socket_comm('front:' + str(my_port) + ':' + my_hash)
            self.pred_hash = new_hash
            self.pred_port = new_port
        else:
            with Communication(new_port) as sock:
               sock.socket_comm('back:' + str(my_port) + ':' + my_hash)
            self.succ_hash = new_hash
            self.succ_port = new_port
    #logging

    def update_adjacent(self, new_port, new_hash, flag):
        if (flag == 0):
            self.pred_hash = new_hash
            self.pred_port = new_port
        else:
            self.succ_hash = new_hash
            self.succ_port = new_port

    def send_adjacent(self, data, flag):
        if (flag == 0):
            with Communication(self.pred_port) as sock:
                return sock.socket_comm(data)
        else:
            with Communication(self.succ_port) as sock:
                return sock.socket_comm(data)

    def get_adjacent(self, flag):
        if(flag == 0):
            return str(self.pred_port) + ':' + self.pred_hash
        else:
            return str(self.succ_port) + ':' + self.succ_hash
