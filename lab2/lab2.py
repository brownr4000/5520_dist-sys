"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Bobby Brown rbrown3
:Version: 1.0

Lab 2: Bully 
This program is a node that joins a group run by a Group Coordinator Daemon 
(GCD). This group is fully interconnected (like a round table) and the 
objective is to determine a leader using the bully algorithm. This consensus is
determined by comparing the number of days to a date, and a given ID number 
which represents the identiy for each node.

The node created by this program creates its own server to receive messages
from other nodes in the group, while sending messages to other nodes within
the group based on their indentities. THe node sends messages based on the
message it receives, except in the case of startup where it automatically sends
a request for an election.
"""

from enum import Enum
import datetime
import pickle
import selectors
import socket
import sys
import time

BUFFER_SIZE = 1024      # Constant for buffer size for tcp
TIMEOUT = float(1.500)  # Constant for connection timeout
ERROR_MSG = '[ERROR] Connection refused\n'  # Timeout error message constant
MAX_CONNECTIONS = 5     # Constant for maxiumn number of server connections
CHECK_INTERVAL = 0.1    # Constant for selector check interval time 
HOST = 'localhost'      # Default host name

class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.

    This code was given as part of the assignment.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)

class Lab2(object):
    """
    Lab2 creates a node to connect to a GCD and interact with its members.

    Lab2 defines a node program that connects with a Group Coordinator
    Daemon, and communicates with the group members via pickled messages. This
    node creates its own server to receive messages from the group members, and
    acts as a client to send messages to other nodes
    """
    
    def __init__(self, gcd_host, gcd_port, next_birthday, su_id):
        """
        Lab2 constructor creates an object to interact with the given Group 
        Coordinator Daemon and its members
        """

        # Sets the host and port for the GCD and defines its address
        self.gcd_host = gcd_host
        self.gcd_port = gcd_port
        self.gcd_address = (gcd_host, gcd_port)

        # Determines the amount of days for identification
        days_to_birthday = (next_birthday - datetime.datetime.now()).days
        
        # Defines the personal identification tuple for the object
        self.pid = (days_to_birthday, int(su_id))

        self.members = {}   # Dictionary to store memebers from GCD
        self.state = State.QUIESCENT    # Stores state of node
        
        # Dictionary to store nodes that have been sent messages
        self.waiting = {}

        # Stores the pid of the current leader. None means election is pending.
        self.bully = None

        # Selector for the node
        self.selector = selectors.DefaultSelector()

        # Creates listening server
        self.listener, self.listener_address = self.start_a_server()

    def run(self):
        """
        Runs the node program and functions

        The run function is the method that creates the connection to the GCD
        server, and starts an election for the group.
        """

        self.join_group()       # Call to the join_group function
        self.start_election()   # Call to the start_election function

        # Loop to run while program is active
        while True:
            # Check the selector for events for the given interval
            events = self.selector.select(CHECK_INTERVAL)

            # Loop through all entities in the events list
            for key, mask in events:
                # Checks if the object listener is present
                if key.fileobj == self.listener:
                    # Call accept_connection function to process the connection
                    self.accept_connection()
                
                # Checks if the selector is EVENT_READ
                elif mask & selectors.EVENT_READ:
                    # Call the receive_message function to get a message
                    self.receive_message(key.fileobj)
                else:
                    # Sends a stored message
                    self.send_message(key.fileobj)
            
            self.check_timeouts()   # Process timeouts for servers

    def check_timeouts(self):
        """
        Checks status of waiting connections

        The check_timeouts function determines the status of other nodes waiting
        to communicate with the current node's server
        """

        # Check the state of the current node
        if self.state is State.WAITING_FOR_ANY_MESSAGE:
            # Loop though members in the waiting dictionary
            for key, value in self.waiting:
                sock, node_time = value
                
                # Compare the current time to the waiting node time
                if time.time() > node_time:
                    print(f'Timeout for: {key}')

                    # Determined server timeout and try to close connection
                    try:
                        self.close_peer(sock)
                        del self.waiting[key]
                    except socket.error as err:
                        print(ERROR_MSG, err)
            
            # Sets leader to self if no connections are waiting
            if len(self.waiting) is 0:
                self.set_leader(self.pid)

    def join_group(self):
        """
        Sends the JOIN message to a GCD.

        The join_group function sends a JOIN message to the host and port
        stored as part of the Lab2 object.
        """
        
        # Try to JOIN the GCD
        try:
            # Display JOIN message to user
            print(f'Connecting to GCD ({self.gcd_host}: {self.gcd_port})')

            # Connect to GCD
            gcd_sock = self.server_connect(self.gcd_host, self.gcd_port)

            # Sending pickled message
            gcd_sock.sendall(pickle.dumps('JOIN', 
                (self.pid, self.listener_address)))

            # Modify the selector to update the GCD server and get members of
            # the group
            self.selector.modify(gcd_sock, selectors.EVENT_READ, 
                (self.get_members, None))

        except(socket.timeout, socket.error) as error:
            # Display error message if socket timeout or error
            print(ERROR_MSG, error)

    def meet_members(self, sock):
        """
        Receives list of members from the GCD

        The meet_members function gets the list of members from the GCD and
        stores it in the object's member dictionary
        """

        # The reply message from the server
        reply = pickle.loads(sock.recv(BUFFER_SIZE))

        # Close connection
        self.close_peer(sock)

        # Stores the reply message as the mebers dictionary
        self.members = reply

    def server_connect(self, host, port):
        """
        Performs a connection to a server

        The server_connect function creates and returns a socket to a specified
        server.

        Args:
            host:
                The host to connect to.
            port:
                The port of the host to connect to
        
        Returns:
            A socket connection
        """

        # Use socket to connect to the server with the passed in host and port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Define server timeout
            sock.settimeout(TIMEOUT)

            # Set Blocking to False
            sock.setblocking(False)

            # Connect to host and port
            sock.connect((host, port))

            return sock

    def start_a_server(self):
        """
        Starts a server for listening

        The start_a_server function creates a server for listening for messages
        for the Lab2 object node.

        Returns:
            The server socket and address
        """

        # Use socket to create server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            # Define server host and port
            server.bind((HOST, 0))

            # Define maximum number of connections for listening
            server.listen(MAX_CONNECTIONS)

            # Set Blocking to False
            server.setblocking(False)

            # Regester the created server in the selector
            self.selector.register(server, selectors.EVENT_READ, 
                (self.accept_connection, None))

            # Display server start message
            print(f'Server started at {server.getsockname()}')

            return (server, server.getsockname())

    def accept_connection(self, peer):
        """
        Accepts a connection with a peer

        The accept_connection function accepts a request for a connection to the
        node's server from a member of the group

        Args:
            peer:
                The scoket requesting a connection
        """

        # Store the new socket object and the address of the connection
        connect, address = peer.accept()
        print(f'Connection with {address}')

        # Set Blocking to False
        connect.setblocking(False)

        # Register the connection in the selector and receieve message
        self.selector.register(connect, selectors.EVENT_READ, 
            (self.receive_message, None))

    def receive_message(self, peer):
        """
        Receives a message

        The receive_message function gets a message from the socket, and
        processes it based on the State(Enum) that was sent with the message
        
        Args:
            peer:
                The socket sending a message
        """

        # The unpickled message based on buffer size from the socket
        message = pickle.loads(peer.recv(BUFFER_SIZE))

        # Parse message into command and data
        command, data = message[0], message[1]
        
        # Check if command is None
        if command is None:
            print(f'No message from {peer}')
            self.close_peer(peer)
        
        # Display command
        print(f'Received {command} message')

        # Check if command is COORDINATOR
        if command is State.SEND_VICTORY:
            # Set leader to current socket
            self.set_leader(peer)

            # Close connection with current socket
            self.close_peer(peer)

            # Reset state of self
            self.set_state(State.QUIESCENT)

        # Else if command is ELECTION
        elif command is State.SEND_ELECTION:
            # Process election in progress
            self.election_in_progress(peer, data)

        # Else if command is OK
        elif command is State.SEND_OK:
            # Update state of self to WAITING
            self.set_state(State.WAITING_FOR_ANY_MESSAGE)

    def send_message(self, peer, command):
        """
        Sends a message

        The send_message function sends a specific command and list of members
        to the passed in peer socket connection

        Args:
            peer:
                The socket being sent a message
            command:
                The State(Enum) command message being sent
        """

        # Check if the command is OK, and send member dictionary if not
        if command is State.SEND_OK:
            package = (command, None)
        else:
            package = (command, self.members)

        # Try to send message
        try:
            # Send pickled package
            peer.send(pickle.dumps(package))

        except socket.error as err:
            print(ERROR_MSG, err)

    def set_leader(self, peer):
        """
        Sets the leader or bully for the group
        
        The set_leader function sets the bully of the group based on the passed
        in socket.

        Args:
            peer:
                The socket promoted to leader/bully
        """

        # Set the object bully to the passed in peer
        self.bully = peer
        print(f'The current leader is: {peer}')

        # Check if the leader is self
        if(self.bully == self.pid):

            # Loop through all members to send COORDINATE message
            for key, address in self.members:
                if key is not self.pid:
                    # Establish connection to member
                    sock = self.server_connect(address[0], address[1])
                    
                    # Register event and send COORDINATE message
                    self.selector.register(sock, selectors.EVENT_WRITE, 
                        (self.send_message(sock, State.SEND_VICTORY), key))
                    
                    # Close connection
                    self.close_peer(sock)

            # Set state of self and clear waiting list
            self.set_state(State.SEND_VICTORY)
            self.waiting = {}

    def set_state(self, state):
        """
        Sets the state of the current node object

        Args:
            state:
                The new state of the node
        """

        # Display message and set the state
        print(f'Setting state {self.state} to {state}')
        self.state = state

    def election_in_progress(self, peer):
        """
        Processes an election in progress.

        The election_in_progress function replies to a previously sent ELECTION
        message, and starts a new election for the current node

        Args:
            peer:
                The socket that sent the ELECTION message
        """

        # Send OK message to the socket
        self.send_message(peer, State.SEND_OK)
        
        # Start an election
        self.start_election()

    def close_peer(self, peer):
        """
        Closes a socket connection.

        The close_peer function disconnects from a peer and removes it
        from the selector.

        Args:
            peer:
                The socket to be closed
        """
        
        # Unregister socket from selector and close connection
        self.selector.unregister(peer)
        peer.close()

    def start_election(self):
        """
        Starts an election.

        The start_election function starts an election with the current member
        list and sends ELECTION messages as needed to nodes with larger keys.
        """

        # Display ELECTION message
        print(f'Starting an ELECTION. I am: {self.pid}')

        # Loop though all group members
        for key, address in self.members:
            # Display peer message
            print(f'Peer: {key}, {address}')

            # Check if the key is larger than the current node pid
            if (key > self.pid):
                # Create connection to node
                peer_sock = self.server_connect(address[0], int(address[1]))
                
                # Register the ELECTION for the connection in the selector
                self.selector.register(peer_sock, selectors.EVENT_WRITE, 
                    (self.send_message(peer_sock, State.SEND_ELECTION), key))
                
                # Add socket and timestamp to waiting dictionary
                self.waiting[key] = (peer_sock, time.time() + TIMEOUT)

                # Set the selector to wait for a message from the socket
                self.selector.register(peer_sock, selectors.EVENT_READ, 
                    (self.receive_message, key))

        # Change state to WAITING
        self.set_state(State.WAITING_FOR_ANY_MESSAGE)

# Main Function
if __name__ == '__main__':
    # Check length of command line arguements
    if len(sys.argv) != 4:
        print("Usage: python lab2.py GCDPORT NEXT_BIRTHDAY(YYYY-MM-DD) SU_ID")
        exit(1);
    
    # Set host and port based on the command line arguemnts
    port = int(sys.argv[1])
    birthday = datetime.datetime.strptime(sys.argv[2],'%Y-%m-%d')
    su_id = int(sys.argv[3])

    # Create Lab2 object
    lab2 = Lab2(HOST, port, birthday, su_id)

    # Call run function
    lab2.run()