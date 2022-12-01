"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Bobby Brown rbrown3
:Version: 1.0

Lab 4: DHT
Class: chord_populate.py

chord_populate takes a port number of an existing node and the filename of the
data file

This populates a Chord system with data from a csv file.
The data set is some data from some National Football League passing
statistics file. Treat the value in the first column (playerid) concatenated
with the fourth column (year) as the key and use SHA-1 to hash it. If there are
any duplicates, write them both to the DHT (so the last row wins). For the
nodes, use the string of its endpoint name, including port number, as its name
and use SHA-1 to hash it (similar to what is suggested in the Stoica, et al. 
paper). The other columns for the row can be put together in a dictionary and
returned when that key is fetched.

Help from https://www.geeksforgeeks.org/working-csv-files-python/

"""

import csv
import sys
import time

from chord_node import Chord

DEFAULT_HOST = 'localhost'


def populate_from_csv(port, filename):
    """
    Populates a dictionary using values from a csv file.

    Args:
        port:
            The port of the node to call Chord.put_value
        filename:
            The filename of the csv file
    """

    # Get the node for the give port
    node = Chord.lookup_address(port)
    count = 0       # Count the number of rows

    # Open a csv file
    with open(filename) as csvfile:
        # Traverse the file and map to a dictionary
        for row in csv.DictReader(csvfile):
            dataset = {}    # Dictionary to store values

            # Traverse through row by column name
            for name in row:
                cell = row[name]    # Set data to store based on contents

                # Parse data based on contents of cell
                if cell == '--':
                    cell = None
                elif cell.isdecimal():
                    cell = float(cell)
                    if cell.is_integer():
                        cell = int(cell)
                
                # Populate dictionary
                dataset[name] = cell
            
            # Define key
            key = (dataset['Player Id'], dataset['Year'])

            # Display message
            print(count, 'Putting ', key, dataset)

            # Put values into Chord node
            Chord.put_value(node, key, dataset)
            count += 1  # Increment count

            # Wait to process next row
            time.sleep(0.5)


# Main Function
if __name__ == '__main__':
    # Check length of command line arguements
    if len(sys.argv) != 3:
        print("Usage: python3 chord_populate.py PORT FILENAME")
        exit(1)
    
    # Set address based on host and port based from the command line arguemnts
    port = int(sys.argv[1])
    name = sys.argv[2]

    # Call populate_from_csv function
    populate_from_csv(port, name)