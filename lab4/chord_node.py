"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Bobby Brown rbrown3
:Version: 1.0

Lab 4: DHT
Class: chord_node.py

chord_node takes a port number of an existing node (or 0 to indicate it should
start a new network). This program joins a new node into the network using a
system-assigned port number for itself. The node joins and then listens for
incoming connections (other nodes or queriers). You can use blocking TCP for
this and pickle for the marshaling.
"""

import hashlib
import pickle
import socket
import sys
import threading
from datetime import datetime


M = hashlib.sha1().digest_size * 8  # Number of bits
NODES = 2 ** M  # Number of nodes
DEFAULT_HOST = 'localhost'
BUFFER_SIZE = 8192  # socket recv arg
BACKLOG = 100  # socket listen arg
PORT_START = 47500  # Starting port number on localhost
PORT_END = 64999    # Maximum port number

class ModRange(object):
    """
    Range-like object that wraps around 0 at some divisor using modulo
    arithmetic.

    >>> mr = ModRange(1, 4, 100)
    >>> mr
    <mrange [1,4)%100>
    >>> 1 in mr and 2 in mr and 4 not in mr
    True
    >>> [i for i in mr]
    [1, 2, 3]
    >>> mr = ModRange(97, 2, 100)
    >>> 0 in mr and 99 in mr and 2 not in mr and 97 in mr
    True
    >>> [i for i in mr]
    [97, 98, 99, 0, 1]
    >>> [i for i in ModRange(0, 0, 5)]
    [0, 1, 2, 3, 4]

    This code was given as part of Lab 4.
    """

    def __init__(self, start, stop, divisor):
        """
        ModRange constructor

        """
        
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around
        # the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor),)
        else:
            self.intervals = (
                range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)

class ModRangeIter(object):
    """
    Iterator class for ModRange
    
    This code was given as part of Lab 4.
    """

    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]

class FingerEntry(object):
    """
    Row in a finger table.

    >>> fe = FingerEntry(0, 1)
    >>> fe
    
    >>> fe.node = 1
    >>> fe
    
    >>> 1 in fe, 2 in fe
    (True, False)
    >>> FingerEntry(0, 2, 3), FingerEntry(0, 3, 0)
    (, )
    >>> FingerEntry(3, 1, 0), FingerEntry(3, 2, 0), FingerEntry(3, 3, 0)
    (, , )
    >>> fe = FingerEntry(3, 3, 0)
    >>> 7 in fe and 0 in fe and 2 in fe and 3 not in fe
    True

    This code was given as part of Lab 4.
    """

    def __init__(self, n, k, node=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2**(k-1)) % NODES
        self.next_start = (n + 2**k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return '{}| [{:2}, {:<2} | '.format(self.node, self.start, self.next_start)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval

class ChordNode(object):
    """
    ChordNode creates a single node in a Chord Network, based on the pseudocode
    presented in the Chord paper.
    """

    def __init__(self, port, buddy_port=None) -> None:
        """
        The ChordNode constructor initilaizes the node's address, thread for 
        running a server, finger table, and stored keys dictionary.

        Args:
            port:
                The port of the server
            buddy_port:
                The port of a known node on the Chord network
        """
        
        # Define address of the node
        self.address = (DEFAULT_HOST, port)

        # Defined the node as a hash of the address, using Chord class
        self.node = Chord.lookup_node(self.address)

        # Initializes finger table for the node
        self.finger = [None] + [FingerEntry(self.node, k) for k in range(1, M + 1)]
        
        self.predecessor = None # Sets predecessor to None
        self.keys = {}          # Defines initial dictionary

        # Gets the node of an existing node, if passed in
        self.buddy_node = Chord.lookup_node((DEFAULT_HOST, buddy_port)) if buddy_port else None

        # Sets up listening server for this node
        self.listener = self.listening_server()

        self.joined = False # Set flag to if node is part of a Chord network

        # Display message about node
        print('Node ID = {} on {}'.format(self.node, self.address))

        # Start server thread
        threading.Thread(target=self.run_server).start()

    def __repr__(self) -> str:
        """
        Defines the string representation of the ChordNode object
        """

        # Format of finger table contents
        finger_table =  ', '.join([str(self.finger[i].node) for i in range(1, M + 1)])
        
        return '{}: {}[{}]'.format(self.node, self.predecessor, finger_table)

    # Node Properties
    @property
    def successor(self):
        """
        Returns the successor node of the current node.

        Return:
            The successor node
        """

        return self.finger[1].node

    @successor.setter
    def successor(self, id):
        """
        Sets the successor node of the current node.

        Args:
            id:
                The successor node
        """

        self.finger[1].node = id

    # Finger Tables
    def init_finger_table(self):
        """
        Initializes the finger table for the current node, with the help of a
        buddy node.
        """

        # Find the successor and predecessor
        self.successor = self.call_rpc(self.buddy_node, 'find_successor', self.finger[1].start)
        self.predecessor = self.call_rpc(self.successor, 'get_predecessor')
        
        # Set predecessor of the current node
        self.call_rpc(self.successor, 'set_predecessor', self.node)

        # Traverse through nodes in the finger table to copy them into the
        # current node
        for i in range(1, M):
            # Check if the finger entry is in range, else find successor from 
            # the buddy node
            if self.finger[i + 1].start in ModRange(self.node, self.finger[i].node, NODES):
                self.finger[i + 1].node = self.finger[i].node
            else:
                self.finger[i + 1].node = self.call_rpc(self.buddy_node, 'find_sucessor', self.finger[i + 1].start)

    def find_successor(self, id):
        """
        Returns the successor node with the given id.

        Return:
            The successor node
        """

        # Find the precessor node for the give id
        n_prime = self.find_predecessor(id)
        return self.call_rpc(n_prime, 'successor')

    def find_predecessor(self, id) -> int:
        """
        Finds the predecessor of the current node.

        Return:
            The predecessor of the current node
        """

        # Temporary node to hold
        n_prime = self.node

        # Traverse range to find the closest preceding node
        while id not in ModRange(n_prime + 1, self.call_rpc(n_prime, 'successor') + 1, NODES):
            n_prime = self.call_rpc(n_prime, 'closest_preceding_finger', id)
        
        return n_prime

    def closest_preceding_finger(self, id) -> int:
        """
        Returns the node in the current node's finger table that is the closest
        preceding node of the given id.

        Return:
            The closest preceding node
        """

        # Traverse though range of nodes, starting at the bottom
        for i in range(M, 0, -1):
            # Return finger if it is found in the node's table
            if self.finger[i].node in ModRange(self.node + 1, id, NODES):
                return self.finger[i].node
        
        return self.node

    def update_others(self):
        """
        Update other nodes that have the current node in their finger tables.
        """

        # Traverse through range of nodes
        for i in range(1, M + 1):
            # Find the last node p who's i-th finger might be this node
            p = self.find_predecessor((1 + self.node - 2 ** (i - 1) + NODES) % NODES)
            self.call_rpc(p, 'update_finger_table', self.node, i)

    def update_finger_table(self, s, i):
        """
        Updates the finger table for the current node.

        If s is i-th finger of node, update this node's finger table with s 

        """

        if (self.finger[i].start != self.finger[i].node 
                and s in ModRange(self.finger[i].start, self.finger[i].node, NODES)):
            print('update_finger_table({},{}): {}[{}] = {} since {} in [{},{})'.format(
                    s, i, self.node, i, s, s, self.finger[i].start, self.finger[i].node))
            self.finger[i].node = s
            print('#', self)
            p = self.predecessor  # Get first node preceding myself
            self.call_rpc(p, 'update_finger_table', s, i)
            return str(self)
        else:
            return 'did nothing {}'.format(self)

    # Data
    def put_value(self, key, data):
        """
        Puts a value associated with the given key into the system.

        Args:
            key:
                The key of the key/value pair
            data:
                The value of the key/value pair to store

        Return:
            Exits method when data is added
        """

        # Get the id based on the hashed value of the key
        id = Chord.hash(key, M)

        # Check if the id already exists, and store data
        if id in ModRange(self.predecessor + 1, self.node + 1, NODES):
            self.keys[id] = data
            print('Putting {}, {} at: {}'.format(key, data, id))
            return
        else:
            # Key has not been found, need to find the successor recursively
            n_prime = self.find_successor(id)
            return self.call_rpc(n_prime, 'put_value', id, data)

    def get_value(self, key):
        """
        Gets a value associated with the given key.

        Args:
            key:
                The key for the value being retrieved

        Return:
            The value
        """

        # Get the id based on the hashed value of the key
        id = Chord.hash(key, M)

        # Check if the id already exists, and return data
        if id in ModRange(self.predecessor + 1, self.node + 1, NODES):
            print('Data found at: {}'.format(id))
            return self.keys[id] if id in self.keys else None
        else:
            # Key has not been found, need to find the successor recursively
            n_prime = self.find_successor(id)
            return self.call_rpc(n_prime, 'get_value', id)

    # RPCs
    def call_rpc(self, n_prime, method, arg1=None, arg2=None):
        """
        Makes a remote procedure call based on the passed in arguments.

        The call_rpc function performs a remote procedure call to the passed in
        node, for the method supplied.

        Args:
            n_prime:
                The node to contact
            method:
                The remote method to invoke
            arg1:
                The first argument
            arg2:
                The second argument

        Return:
            The value of the RPC method
        """

        # If the RPC call is for the current node, perform local call
        if n_prime == self.node:
            return self.dispatch_rpc(method, arg1, arg2)

        # Return a call to the static Chord RPC function
        return Chord.call_rpc(n_prime, method, arg1, arg2)

    def dispatch_rpc(self, method, arg1=None, arg2=None):
        """
        Dispatches a RPC call locally for the current node.

        The dispatch_rpc function performs a local procedure call for the
        current node, based on the method supplied.

        Args:
            method:
                The remote method to invoke
            arg1:
                The first argument
            arg2:
                The second argument

        Return:
            The value of the method being called
        """

        # Format request for display
        request = 'Node: {}; Method: {}; ({},{})'.format(self.node, method,
                                                        str(arg1), str(arg2))
        print(request)

        # Parse procedure call based on method value
        if method == 'get_value':           # get_value call
            result = self.get_value(arg1)

        elif method == 'put_value':         # put_value call
            result = self.put_value(arg1, arg2)

        elif method == 'successor':         # sucessor call
            result = self.successor

        elif method == 'find_successor':    # find_successor call
            result = self.find_successor(arg1)

        elif method == 'predecessor':       # predecessor call
            if arg1 is not None:
                self.successor = arg1
            
            result = self.successor

        # closest_preceding_finger call
        elif method == 'closest_preceding_finger':
            result = self.closest_preceding_finger(arg1)

        # update_finger_table call
        elif method == 'update_finger_table':
            result = self.update_finger_table(arg1, arg2)

        else:
            # Cannot find method, display error
            result = None
            raise ValueError('Unknown RPC method {}'.format(method))

        # Display message verifying procedure call completion with result
        print('\t{} -> {}'.format(request, result))

        return result

    def handle_rpc(self, client):
        """
        Handles RPC from other nodes.

        The handle_rpc function processes remote calls from the listening server

        Args:
            client:
                The incoming TCP socket from another node

        """

        # Receive message and parse
        rpc = client.recv(BUFFER_SIZE)
        method, arg1, arg2 = pickle.loads(rpc)

        # Display request
        print('RPC request {} from {}'.format(method, client))
        
        # Get result and send back to other node
        result = self.dispatch_rpc(method, arg1, arg2)
        client.sendall(pickle.dumps(result))

    # Servers
    def listening_server(self) -> socket:
        """
        Starts a server for listening using TCP/IP.

        Return:
            A socket for the server
        """

        # Use socket to create a TCP/IP server
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(self.address)
        server.listen(BACKLOG)

        return server

    def run_server(self):
        """
        Runs the server to listen for incoming connections via multithreading.
        """

        # Continue running
        while True:
            # Display message if current node is already part of the system
            if self.joined:
                print('NODE ID: {}'.format(self.node))
            
            # Display message to verify running server
            print('\nPort {}: Waiting for connections...\n'.format(self.address[1]))
            
            # When server accepts a connection, handle the rpc via a thread
            sock, address = self.listener.accept()
            threading.Thread(target=self.handle_rpc, args=(sock,)).start()

    def join(self):
        """
        Joins the current node to the Chord network.
        """

        # Get help from the buddy node to initialize
        if self.buddy_node is not None:
            self.init_finger_table()
            self.update_others()
        
        # Otherwise need to initalize own finger table and predecessor
        else:
            for i in range(1, M + 1):
                self.finger[i].node = self.node
            self.predecessor = self.node
        
        # Set joined flag and display message
        self.joined = True
        print('{} has joined the network'.format(self.node))

class Chord(object):
    """
    Chord object class is responsible for general methods needed for the Chord
    system.
    """

    node_map = {}   # Dictionary to store nodes

    @staticmethod
    def call_rpc(node, method, arg1=None, arg2=None):
        """
        The remote procedure call for the given Chord node.
        """

        # Get the address of the given node
        address = Chord.lookup_address(node)
        print(address)
        # Format request for display
        request = 'Node: {}; Method: {}; ({},{})'.format(node, method,
                                                        str(arg1), str(arg2))
        print(request)

        # Create socket using TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            #try:
                # Connect to address, and get message
            sock.connect(address)
            message = pickle.dumps((method, arg1, arg2))
                
            # Send message back and get result
            sock.sendall(message)
            result = pickle.loads(sock.recv(BUFFER_SIZE))
            print('\tResult: ', result)
            return result
            
            #except Exception as e:
               #print('Connection to node at {} failed. Connection timeout at {}'.format(address, datetime.now().time()))

    @staticmethod
    def put_value(node, key, value):
        """
        Puts a value associated with the given key into give Chord node.

        Return:
            The result of the RPC call
        """

        return Chord.call_rpc(node, 'put_value', key, value)

    @staticmethod
    def get_value(node, key):
        """
        Gets the value from the given Chord node.

        Return:
            The result of the RPC call
        """

        return Chord.call_rpc(node, 'get_value', key)

    @staticmethod
    def lookup_address(node) -> tuple:
        """
        Returns the address of a given node.
        
        Args:
            node:
                The node in the system

        Return:
            The host, port tuple address of the node 
        """

        # Looks at current node map
        if node not in Chord.node_map:
            # Traverse port range to find available port
            for port in range(PORT_START, PORT_END):
                address = (DEFAULT_HOST, port)

                # Create temporary node
                temp_node = Chord.lookup_node(address)

                # If the temp node and node are equal, bind address
                if temp_node == node:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        try:
                            sock.bind(address)
                        except Exception as e:
                            Chord.node_map[node] = address
                            return address
            return None

        return Chord.node_map[node]

    @staticmethod
    def lookup_node(address):
        """
        Converts address to hashed id value

        Args:
            address:
                The host, port tuple adress of the node

        Return:
            The hashed value for the node's id
        """

        return Chord.hash(address) % NODES

    @staticmethod
    def hash(key) -> int:
        """
        Gets the SHA1 hash for the given object
        """

        temp_data = pickle.dumps(key)
        hashed_data = hashlib.sha1(temp_data).digest()
        return int.from_bytes(hashed_data, byteorder='big')


# Main Function
if __name__ == '__main__':
    # Check length of command line arguements
    if len(sys.argv) not in (2,3):
        print('Usage: python3 chord_node.py PORT [BUDDY]')
        print('PORT = 0, if starting new network')
        print('[BUDDY] is the port number of an existing node')
        exit(1)
    
    # Set port based on input from the command line arguments
    port = int(sys.argv[1])

    # If a buddy node is given, set port based on the input, otherwise None
    buddy = int(sys.argv[2]) if len(sys.argv) > 2 else None

    # Create ChordNode object
    lab4 = ChordNode(port, buddy)

    # Join network and run server
    lab4.join()
    lab4.run_server()