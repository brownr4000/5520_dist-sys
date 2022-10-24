"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Bobby Brown rbrown3
:Version: 1.0

Lab 3: Pub/Sub
Class: fxp_bytes_subscriber.py

The Forex provider Bytes Subscriber is a utility that helps marshal published
messages sent via UDP. This module contains functions that assist in
manipulating the message package contents.

The format of the published messages are a series of 1 to 50 of the following
32-byte records:

<timestamp, currency 1, currency 2, exchange rate>

Bytes[0:8] The timestamp is a 64-bit integer number of microseconds that have
passed since 00:00:00 UTC on 1 January 1970 (excluding leap seconds). Sent in 
big-endian network format.

Bytes[8:14] The currency names are the three-character ISO codes ('USD', 'GBP',
'EUR', etc.) transmitted in 8-bit ASCII from left to right.

Bytes[14:22] The exchange rate is 64-bit floating point number represented in
IEEE 754 binary64 little-endian format. The rate is number of currency2 units
to be exchanged per one unit of currency1. So, for example, if currency1 is USD
and currency2 is JPY, we'd expect the exchange rate to be around 100.

Bytes[22:32] Reserved. These are not currently used (typically all set to
0-bits).

"""

import ipaddress
import struct
from datetime import datetime, timedelta
from typing import Tuple

MAX_QUOTES_PER_MESSAGE = 50     # Maximum number of quotes in a datagram
MICROS_PER_SECOND = 1_000_000   # Constant for microseconds in a second
RECORD_LENGTH = 32              # Length of record in a datagram
ENCODING = 'utf-8'              # Encoding for bytes to string

def deserialize_price(x: bytes) -> float:
    """
    Converts a byte stream into a float from incoming message.

    Args:
        x: 
            The passed in byte array from the message to be converted
    
    Returns:
        The value of the converstion from bytes to decimal
    """

    # Use struct module to convert from bytes
    return struct.unpack('d', x)

def serialize_address(address: Tuple(str, int)) -> bytes:
    """
    Searialize the address tuple of a server into bytes in order to establish
    a connection.

    Args:
        address: 
            The tuple of host name and port number for the server
    
    Returns:
        The byte representation of the host and port pair
    """

    # Set host to the first part of the tuple and pack it as bytes
    host = ipaddress.ip_address(address[0]).packed

    # Set post the the byte represntation of the integer
    port = address[1].to_bytes(2, byteorder="big")

    return host + port


def deserialize_utcdatetime(time: bytes) -> datetime:
    """
    Converts a byte stream into a UTC datetime.

    Args:
        time: 
            The byte stream representation of a timestamp
    
    Returns:
        The datetime represenation of the timestamp
    """

    epoch = datetime(1970, 1, 1)    # UNIX time epoch
    
    # Convert byte stream into integer using big-endian
    duration = int.from_bytes(time, 'big')

    # Convert from microseconds to seconds
    seconds = duration / MICROS_PER_SECOND

    # Return the difference between epoch time and the number of seconds
    return epoch + timedelta(seconds)

def unmarshal_message(msg: bytes) -> list:
    """
    Constructs messages from a byte stream into a string list.

    Takes a byte stream with a format of <timestamp, currency 1, currency 2,
    exchange rate> and a record size of 32 bytes, and unmarshals the message
    into a string list to be used by a client program.

    Args:
        msg:
            The byte stream representation of the message
    Returns:
        quotes:
            The list of quotes in a dictionary string format
    """

    # Determine the number of records within the message
    num_records = int(len(msg) / RECORD_LENGTH)
    quotes = []     # A list to hold the each record
    
    # Traverse through each record
    for record in range(0, num_records):
        info = {}   # Dictionary to store information from the record
        
        # Set the start and end of the current message in the byte stream
        msg_bytes = msg[(record * RECORD_LENGTH):(record * RECORD_LENGTH + RECORD_LENGTH)]
        
        # Assign parts of the byte stream to entries in the dictionary
        info['timestamp'] = deserialize_utcdatetime(msg_bytes[0:8])
        info['cross'] = msg_bytes[8:11].decode(ENCODING) + '/' + msg_bytes[11:14].deco(ENCODING)
        info['price'] = deserialize_price(msg_bytes[14:22])

        quotes.append(info)     # Add info dictionary to quotes list

    return quotes