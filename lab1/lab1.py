"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Bobby Brown rbrown3
:Version: 1.0

Lab 1: Simple Client
This is a simple client program that connects to a Group Coordinator Daemon 
(GCD) that responds with a list of potential group members. It sends a 
message to each of the group members, prints out their response, and then exits.

"""

import pickle
import socket
import sys

BUFFER_SIZE = 1024      # Constant for buffer size for tcp
TIMEOUT = float(1.500)  # Constant for connection timeout
ERROR_MSG = '[ERROR] Connection refused\n'  # Timeout error message constant

class Lab1(object):
    """
    Lab 1 creates a client to connect to a GCD and its members.

    Lab1 defines a simple client program that connects with a Group Coordinator
    Daemon, and communicates with the group members via pickled messages.
    """
    
    def __init__(self, gcd_host, gcd_port):
        """
        Lab1 constructor defines host and port for the object
        """
        self.host = gcd_host
        self.port = gcd_port

    @staticmethod
    def get_message(host, port, send_data):
        """
        Connects to a host, port and sends a message

        The get_message fuction is a static method that creates the connection
        to the server host and port using socket, then it sends a pickled 
        message request and returns the unpickled response.

        Args:
            host:
                The host to connect to.
            port:
                The port of the host to connect to
            send_data:
                The message to send to the server

        Returns:
            An unpickled response from the server
        """

        # Use socket to connect to the server with the passed in host and port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Define server timeout
            sock.settimeout(TIMEOUT)

            # Connect to host and port
            sock.connect((host, port))

            # Sending pickled message
            sock.sendall(pickle.dumps(send_data))

            # Return unpickled message based on buffer size
            return pickle.loads(sock.recv(BUFFER_SIZE))

    def join_group(self):
        """
        Sends the JOIN message to a GCD.

        The join_group function sends a JOIN message to the host and port
        stored as part of the Lab1 object.

        Returns:
            A list of members of the GCD
        """
        
        # Try to JOIN the GCD
        try:
            # Display JOIN message to user
            print(f"JOIN ('{host}': {port})" )

            # Define memberList from GCD and return
            memberList = self.get_message(host, port,'JOIN')
            return memberList

        except(socket.timeout, socket.error) as error:
            # Display error message if socket timeout or error
            print(ERROR_MSG, error)

    def meet_members(self, memberList):
        """
        Sends HELLO messages to the member nodes from the GCD.

        The meet_members fuction sends a HELLO message to each of the members
        of the GCD.

        Args:
            memberList: The list of members from the GCD
        """

        # Check to determine if memberList is empty
        if memberList is not None:
            # Loop through all key:value pairs of the memberList
            for member in memberList:
                # Get host and port from each member
                memberHost, memberPort = member['host'], member['port']

                # Try to connect to each member 
                try:
                    # Display HELLO message for the member
                    print(f'HELLO to {member}')

                    # Display member message to the screen by calling
                    # get_message function using host, port for the member
                    print(self.get_message(memberHost, memberPort, 'HELLO'))

                except(socket.timeout, socket.error) as error:
                    # Display error message if socket timeout or error
                    print(f'failed to connect: {memberHost}: {memberPort}:',
                        error)
        else:
            print(ERROR_MSG) # Display error message if list is empty

# Main Function
if __name__ == '__main__':
    # Check length of command line arguements
    if len(sys.argv) != 3:
        print("Usage: python lab1.py HOST PORT")
        exit(1);
    
    # Set host and port based on the command line arguemnts
    host, port = sys.argv[1], int(sys.argv[2])

    # Create Lab1 object
    lab1 = Lab1(host, port)

    # Create memberList
    memberList = lab1.join_group()

    # Call meet_members function using memberList
    lab1.meet_members(memberList)