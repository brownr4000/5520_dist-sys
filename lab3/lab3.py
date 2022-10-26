"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Bobby Brown rbrown3
:Version: 1.0

Lab 3: Pub/Sub
Class: lab3.py
Detecting Arbitrage Opportunities Using Published Quotes

In this lab we will build a process that listens to currency exchange rates
from a price feed and prints out a message whenever there is an arbitrage
opportunity available.

"""

import fxp_bytes_subscriber as subscriber
import math
import socket
import sys
import threading
from bellman_ford import Bellman_Ford
from datetime import datetime, timedelta

BUFFER_SIZE = 1024          # Constant for receiving buffer size
DEFAULT_CURRENCY = 'USD'    # Constant for base currency
MSG_BUFFER = 0.1            # Constant for time between messages
QUOTE_EXPIRY = 1.5          # Constant for duration of quotes
STARTING_AMOUNT = 100       # Starting dollar amount
SUBSCRIPTION_EXPIRY = 600   # Time in seconds for subsription to be valid
TOLERANCE = 1e-12           # Constant for tolerance

class Lab3(object):
    """
    Lab3 creates a subsrciption client that runs the Bellman-Ford algorithm on
    incoming data from a exchange data publisher.
    """

    def __init__(self, address) -> None:
        """
        The Lab3 Constructor initializes the publisher address, creates a graph
        structure, defines the listener address, and sets a starting time.

        Args:
            address:
                The passed in address for the publisher server
        """

        self.publisher = address
        self.graph = {}

        # Defines the listener address as the host the program is running on
        self.listener_address = (socket.gethostbyname(socket.gethostname()), 50000)

        # Define the start time of the object
        self.start_time = datetime.utcnow()

    def run(self):
        """
        Creates threads to handle subscribing to a procider, and receiving
        messages.

        The run function creates the threads for running the listen and 
        subscription functions to handle the functionality of the program.
        """

        # Run subcription function to connect with publisher
        self.subscription()

        # Create threads for the listen functions
        listener_thread = threading.Thread(target = self.listen)

        # Start the threads
        listener_thread.start()


    def check_expiry(self):
        """
        Determines if the time elapsed is greater than the subscription time
        """

        # Returns True if the amount of time passed since object creation is 
        # less than the subscription time
        return SUBSCRIPTION_EXPIRY > (datetime.utcnow() - self.start_time).total_seconds() 

    def listen(self):
        """
        Creates a connection to receive messages, and processes it

        The listen function creates a UDP socket connection to receive messages.
        When a message is received, the byte stream is processed and converted
        into a node on a graph. The graph is then passed into a Bellman-Ford
        algortithm function to determine if a negative cycle is found. When 
        found, the graph is passed to another function to determine the value
        of an arbitrage.
        """

        # Create a listening socket using UDP
        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener.bind(self.listener_address)

        # Initialize time for logging purposes
        log_time = datetime.now() + (datetime.utcnow() - datetime.now())

        # Loop while subscription time is valid
        while self.check_expiry():
            # Recieve message from publisher and unmarshal data
            incoming = listener.recv(BUFFER_SIZE)
            message = subscriber.unmarshal_message(incoming)

            # Traverse message to separate quotes from publisher
            for quote in message:
                # Set timestamp for quote
                timestamp = quote['timestamp']

                # Compare time difference to message buffer to determine
                # sequence of data
                if (log_time - timestamp).total_seconds() < MSG_BUFFER:
                    # Parse currency string from quote
                    money = quote['cross'].split('/')

                    # Display timestamp, currencies, and price
                    print('['+ str(datetime.now()) +'] {} {} {}'.format(money[0], money[1], quote['price']))

                    # Add currencies and price to graph node
                    self.add_node(money, quote)

                    # Reset log_time
                    log_time = quote['timestamp']

                else:
                    # Display message ignoring duplicate messages
                    print('Ignoring out-of-sequence message')

            # Call funciton to check for stale quotes
            stale_data = self.manage_nodes()

            # Display message with number of removed quotes
            if stale_data > 0:
                print('Removed {} stale quotes'.format(stale_data))

            # Call function to perform Bellman-Ford  shortest path anaylsis
            analysis = Bellman_Ford(self.graph)

            # Return predecessor and the negative cycle edge, if any
            distance, pred, neg_cycle = analysis.shortest_paths(DEFAULT_CURRENCY, TOLERANCE)

            # Check if negative cycle exists and pass data to arbitrage function
            if neg_cycle is not None:
                self.arbitrage(pred, DEFAULT_CURRENCY)

            # Display message if subscription time has elapsed
            if self.check_expiry() is False:
                print('Subscription timeout of {} seconds achieved'.format(SUBSCRIPTION_EXPIRY))

        return  # Return to end thread

    def add_node(self, money, quote):
        """
        Adds currency pair and price quote nodes to the graph.

        Args:
            money:
                The string of the currency pair
            quote:
                The value of the exchange
        """

        # Determine the value of the exchange
        exchange = -math.log(quote['price'])

        # Check if first currency is already in graph
        if money[0] not in self.graph:
            # Create new dictionary for the currency
            self.graph[money[0]] = {}

        # Add price value to currency edge
        self.graph[money[0]][money[1]] = {'timestamp': quote['timestamp'], 'price': exchange}

        # Check if second currecny is already in the graph
        if money[1] not in self.graph:
            # Create new dictionary for the currecny
            self.graph[money[1]] = {}

        # Add the inverse price value to the inverse currecny edge
        self.graph[money[1]][money[0]] = {'timestamp': quote['timestamp'], 'price': -exchange}
    
    def manage_nodes(self):
        """
        Removes stale price quotes from graph, based on the time to live for
        a published quotes.

        Return:
            The number of removed nodes from the graph
        """

        # Deterimine cutoff time based on expiry time
        cutoff = datetime.utcnow() - timedelta(seconds = QUOTE_EXPIRY)
        count = 0   # Hold the number of expired quotes

        # Traverse through the list representation of the graph
        for one in list(self.graph):
            for two in list(self.graph[one]):
                # Compare timestamp within graph to cutoff time
                if self.graph[one][two]['timestamp'] <= cutoff:
                    del self.graph[one][two]    # Remove node from graph
                    count += 1                  # Increment count

        return count

    def arbitrage(self, pred, money):
        '''
        Determine the arbirtage amount using the precessor currency and 
        specified currency.

        Args:
            pred:
                The dictionary of predecessor nodes in the graph
            money:
                The initial currency used for the aribtrage
        '''

        # Create starting points to traverse dictionary
        records =  [money]          # List for currencies
        last_record = pred[money]   # The entry for the inital currency

        # Traverse list from end and add last node to the records list
        while not last_record == money:
            records.append(last_record)
            last_record = pred[last_record]

        # Add the intial currecny to the list
        records.append(money)

        # Reverse the list to get starting and ending values
        records.reverse()

        # Display Arbitrage message
        print("ARBITRAGE\n\tstart with 100 {}".format(money))

        # Initalize starting amount and currency
        value = STARTING_AMOUNT
        last = money

        # Traverse records list
        for i in range(1, len(records)):
            # Set current to the list index value
            current = records[i]

            # Convert stored price into exchange rate between the currencies
            price = math.exp(-1 * self.graph[last][current]['price'])
            value *= price  # Update the value of the trade

            # Display exchange information
            print("\texchange {} for {} at {} --> {} {}".format(last, current, price, value, current))
            last = current  # Reset pointer for next iteration
        
        # Display result of arbitrage
        print('\t-> profit of {} {}'.format(value - STARTING_AMOUNT, money))

    def subscription(self):
        '''
        Creates connection with publisher by sending message with serialized
        address.
        '''

        # Display connection to publisher message
        print('Connecting to {}'.format(self.publisher))

        # Create socket using UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
            # Serialize address for sending to publisher
            address = subscriber.serialize_address(self.listener_address)

            # Send serialized message
            connection.sendto(address, self.publisher)

            # Close connection
            connection.close()


# Main Function
if __name__ == '__main__':
    # Check length of command line arguements
    if len(sys.argv) != 3:
        print("Usage: python3 lab3.py PUBLISHER_HOST PUBLISHER_PORT")
        exit(1)
    
    # Set address based on host and port based from the command line arguemnts
    address = (sys.argv[1], int(sys.argv[2]))

    # Create Lab3 object
    lab3 = Lab3(address)

    # Call run function
    lab3.run()