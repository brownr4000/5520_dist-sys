"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Bobby Brown rbrown3
:Version: 1.0

Lab 4: DHT
Class: chord_query.py

Performs a query on Chord system using a port number for an existing node, and
a given key.
"""

import sys
from chord_node import Chord

HOST = 'localhost'

class chord_query(object):
    """
    Performs a query on a single node in a Chord system.

    """

    def __init__(self, port, key) -> None:
        """
        chord_query construuctor

        Args:
            port:
                The port of the node
            key:
                The key to be found
            node:
                The node in the system based on the port
        """

        self.address = (HOST, port)
        self.key = key
        self.node = Chord.lookup_node(self.address)

    def query(self):
        """
        Calls a RPC to get a value from the Chord system.

        The query method searches the Chord system for a given key, and
        displays the results if found.
        """
        
        # Display message
        print('Searching for: ', self.key, 'from Node: ', self.node)

        # Get value from Chord
        result = Chord.get_value(self.node, self.key)
        
        # If a value is returned
        if result:
            # Set id and year based on values retured
            id = result[0][1]
            year = result[2][1] if result[2][0] == 'Year' else result[3][1]
            found_key = id + year

            # If key retuned is the same as the key being searched
            if found_key == self.key:
                # Traverse dictionary and display
                for key, value in result:
                    print('{}: {}'.format(key,value))
            else:
                print('Collision: {} and {} do not match').format(self.key, found_key)
        
        else:
            print('Nothing found for {}'.format(self.key))

# Main Function
if __name__ == '__main__':
    # Check length of command line arguements
    if len(sys.argv) != 4:
        print('Usage: python3 chord_query.py PORT NAME YEAR')
        print("KEY = Player Id, Year")
        name = 'tommymaddox/2501842'
        year = 2005
        print('EX: python3 chord_query.py 47500 {} {}'.format(name, year))
        exit(1)

    # Set address based on host and port based from the command line arguemnts
    port = int(sys.argv[1])
    key = (sys.argv[2], int(sys.argv[3]))

    # Create chord_query object
    search = chord_query(port, key)

    # Call query function
    search.query()