"""
..  default-domain:: py
..  codeauthor:: Charles Blais <charles.blais@canada.ca>

Quick sample command-line script that takes a miniseed file
from input, initiates the threaded communication, and sends the
miniseed to the queue.
"""
import sys
import argparse
import logging
import signal
import io

# Third-party library
from obspy import read

# User contributed libraries
from pyringserver.ringserver import RingServerStore


def main():
    """Main routine"""
    # Get the command line arguments
    parser = argparse.ArgumentParser(
        description='MiniSEED from stdin forward to connected ringserver')
    parser.add_argument(
        '-H', '--host',
        default='localhost',
        help='Ringserver host (default: localhost)')
    parser.add_argument(
        '-p', '--port',
        default=18000,
        type=int,
        help='Ringserver port (default: 18000)')
    parser.add_argument(
        '--loop',
        action='store_true',
        help='Read constantly data from stdin')
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose mode')
    args = parser.parse_args()

    # Create the logging utility
    logging.basicConfig(
        level=logging.INFO if getattr(args, 'verbose') else logging.WARNING,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Creating the ringserverstore that will hold all messages
    store = RingServerStore(
        host=args.host,
        port=args.port
    )

    # Start signal handler SIGTERM in case we stop the infinite loop
    signal.signal(signal.SIGTERM, store.exit_gracefully)
    signal.signal(signal.SIGINT, store.exit_gracefully)
    while not store.shutdown or not args.loop:
        # Add to the store
        logging.info('Reading/waiting for content in stdin')
        data = io.BytesIO(sys.stdin.buffer.read())
        store.add(read(data))
    logging.info('Exiting program')
    return 0
