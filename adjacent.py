import sys
import logging

from communication import Communication

class Adjacent(object):
    """Implementation of the adjacent nodes of a server"""

    def __init__(self, pred_hash, pred_addr, succ_hash, succ_addr):
        """Initialize the adjacent nodes after "join" of a server"""
        self.pred_hash = pred_hash
        self.pred_addr = pred_addr
        self.succ_hash = succ_hash
        self.succ_addr = succ_addr

    def notify_adjacent(self, new_addr, new_hash, my_addr, my_hash, flag):
        """Inform an adjacent node that you are its successor/predecessor"""
        if (flag == 0): #0 is for pred and 1 for succ
            with Communication(new_addr) as sock:
               sock.socket_comm('front:' + str(my_addr) + ':' + my_hash)
            self.pred_hash = new_hash
            self.pred_addr = new_addr
        else:
            with Communication(new_addr) as sock:
               sock.socket_comm('back:' + str(my_addr) + ':' + my_hash)
            self.succ_hash = new_hash
            self.succ_addr = new_addr

    def update_adjacent(self, new_addr, new_hash, flag):
        """Update your adjacent nodes when a new node joins"""
        if (flag == 0):
            self.pred_hash = new_hash
            self.pred_addr = new_addr
        else:
            self.succ_hash = new_hash
            self.succ_addr = new_addr

    def send_adjacent(self, data, flag):
        """Communicate with the previous or next node"""
        if (flag == 0):
            with Communication(self.pred_addr) as sock:
                return sock.socket_comm(data)
        else:
            with Communication(self.succ_addr) as sock:
                return sock.socket_comm(data)

    def get_adjacent(self, flag):
        """Return your adjacent nodes"""
        if(flag == 0):
            return str(self.pred_addr) + ':' + self.pred_hash
        else:
            return str(self.succ_addr) + ':' + self.succ_hash
