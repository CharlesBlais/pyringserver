"""
..  default-domain:: py
..  codeauthor:: Charles Blais <charles.blais@canada.ca>

:mod:`pyringserver.datalink` -- DataLink library
================================================

Library to communicate to a RingServer using DataLink protocol
"""
import threading
import queue

import io
import socket
import logging
import struct
import time

from typing import Union

# Third-party library
from obspy import Stream, Trace


class DataLinkConnectionError(Exception):
    """Custom exception"""


class DataLinkNotImplemented(Exception):
    """Custom exception"""


class DataLinkBadMessage(Exception):
    """Custom exception"""


def get_preheader(header: bytes) -> bytes:
    """
    DataLink preheader is DL followed by the header lenght as a unsigned 1byte

    :param bytes header: header message
    :rtype: bytes
    :returns: DataLink preheader
    """
    return b'DL' + struct.pack('B', len(header))


class DataLinkConnection(threading.Thread):
    """
    Threaded process that tries to maintain a DataLink connection
    to the server by using a queue as storage.

    :param queue: Queue object where messages are stored
    :param str host: host address
    :param int port: port
    :param int timeout: connection timeout in seconds
    :param int reconnect_attempts: if the connection is lost
        attempt to reconnect this amount of times (negative == infinite)
    :param int reconnect_sleep: time in seconds between reconnect attempts
    """
    def __init__(
        self,
        queue: queue.Queue,
        host: str = '0.0.0.0',
        port: int = 18000,
        timeout: int = 60,
        reconnect_attempts: int = -1,
        reconnect_sleep: int = 1,
    ):
        threading.Thread.__init__(self)
        self._thread_running = True

        self.queue = queue
        self._queue_read_timeout = 5  # seconds

        self.host = host
        self.port = port
        self.reconnect_attempts = reconnect_attempts
        self.reconnect_sleep = reconnect_sleep

        # Initialize connections parameters
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.connect()

    def connect(self) -> None:
        """
        Connect to host

        :raises DataLinkConnectionError: could not connect to server
        """
        try:
            logging.info(f'Connecting to host {self.host}:{self.port}')
            self.sock.connect((self.host, self.port))
        except socket.error as err:
            raise DataLinkConnectionError(err)

    def reconnect(self) -> None:
        """
        Attempt to reconnect to host

        :raises DataLinkConnectionError: Could not reconnect to host
        """
        attempt_count = 0
        connected = False
        while attempt_count != self.reconnect_attempts:
            attempt_count += 1
            logging.info(f'Connection attempt {attempt_count}')
            try:
                self.connect()
                connected = True
                break
            except DataLinkConnectionError:
                time.sleep(self.reconnect_sleep)
        if not connected:
            raise DataLinkConnectionError("Could not reconnect to host")

    def run(self) -> None:
        """
        Read DataLink messages in queue and write them to DataLink
        """
        while self._thread_running:
            try:
                logging.debug('Waiting for message in queue')
                msg = self.queue.get(True, self._queue_read_timeout)
                self._send_message(msg)
            except queue.Empty:
                logging.debug('No trace in queue')
            except DataLinkConnectionError as err:
                logging.error(err)
                # server did not response so we add the message back to the
                # queue
                self.queue.put(msg)
                self.reconnect()

    def _send_message(self, message: bytes) -> None:
        """
        Send message over DataLink

        :param str message: message to send over DataLink
        :raises DataLinkConnectionError: Error with the ringserver
        :raises DataLinkBadMessage: Server returned bad message
        """
        try:
            sent = self.sock.send(message)
            if not sent:
                raise DataLinkConnectionError('Could not send WRITE')
            msg = self.sock.recv(1028).decode('utf-8')
            if not msg:
                raise DataLinkConnectionError('No response from server')
            if 'OK' not in msg:
                raise DataLinkBadMessage(f'Bad response from server: {msg}')
        except socket.error as err:
            raise DataLinkConnectionError(err)

    def join(self, **kwargs):
        """Close thread"""
        self._thread_running = False
        return super(DataLinkConnection, self).join(**kwargs)


class DataLink(object):
    """
    DataLink protocol handler

    For more information, visit:
    https://seiscode.iris.washington.edu/svn/orb2ringserver/tags/release-1.0/libdali/doc/DataLink.protocol

    At the moment, we only support sending data

    It it important the DataLink spawns a thread therefore it is highly
    recommended to close() the connection before stopping.

    Most arguments are described under DataLinkConnection object

    NOTE a queue is used to accumulate message if the DataLink connection
    is lost. If the queue is full, older messages are dropped are newer
    messages are replaced.

    :param int queue_size:
        amount of message the DataLink will store in queue (default: 100)
    """
    def __init__(
        self,
        queue_size: int = 100,
        **kwargs
    ):
        self._queue: queue.Queue[bytes] = queue.Queue(queue_size)
        self._conn = DataLinkConnection(self._queue, **kwargs)
        # Start the thread
        self._conn.start()

    def send(
        self,
        stream: Union[Stream, Trace],
        flags: str = 'A',
        encoding: str = 'FLOAT32',
    ):
        """
        Unpack the stream to traces and send via DataLink

        :type stream: :class:`obspy.Stream`
        :param stream: stream object
        :param str flags: "N" = no acknowledgement requested
            "A" = acknowledgement requested
        :param str encoding: encoding must one of those supported by obspy
            to miniseed routine
        :raises DataLinkConnectionError: Error with the ringserver
        """
        if flags not in ['A', 'N']:
            raise ValueError('Flags can only be A or N')

        if isinstance(stream, Stream):
            for trace in stream:
                self._send_trace(trace, flags, encoding)
        else:
            self._send_trace(stream, flags, encoding)

    def _send_trace(
        self,
        trace: Trace,
        flags: str,
        encoding: str,
    ):
        """
        Write the trace to DataLink.  Format of the message is:

        ..  note:: see :func:`send` for more information
        """
        # Before adding any element to the queue
        # we check if the thread is running
        if not self._conn.isAlive():
            raise DataLinkConnectionError('DataLink connection has stopped')

        output = io.BytesIO()
        trace.write(output, encoding=encoding, format='MSEED', reclen=512)
        if len(output.getvalue()) != 512:
            raise DataLinkNotImplemented(
                f'MSEED wrong length (512) = {len(output.getvalue())}')
        header = b'WRITE %s %d %d %s %d' % (
            (trace.id.replace(".", "_") + "/MSEED").encode(),
            trace.meta.starttime.timestamp*1000000,
            trace.meta.endtime.timestamp*1000000,
            flags.encode(),
            512
        )
        # It is important that the message is a bytes string
        msg = get_preheader(header) + header + output.getvalue()
        logging.debug(f'Adding message to queue: {header!r}')

        try:
            self._queue.put(msg, False)
        except queue.Full:
            # Extract the oldest packet and add the new packet
            logging.warning(f'Queue full, dropping the message {msg!r}')
            self._queue.get(True, 1)
            self._queue.put(msg, False)

    def close(self):
        '''Destructor, close the thread'''
        self._conn.join()
