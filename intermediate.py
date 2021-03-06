from multiprocessing import Process, Queue
#from client import Client
from server import Server, Bootstrap

class Intermediate(object):
    def __init__(self, ip_addr, port):
        self.commands = {
                          'join':self.join
                        }
        self.nodes = {}
        self.ip_addr_bootstr = ip_addr
        self.bootstr_port = port
        self.nodes['1'] = Process(target = self.DHT_initialization(), args = (self.ip_addr_bootstr, self.bootstr_port)).start()               
#       self.ports[ip_addr_bootr] = self.bootstr_port

   def DHT_initialization(self, ip_addr, port):
       bootstrap = Bootstap(ip_addr, port)
       bootstrap.connection()
#       sys.exit
