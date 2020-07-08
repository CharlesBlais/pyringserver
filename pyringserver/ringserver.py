"""
..  default-domain:: py
..  codeauthor:: Charles Blais <charles.blais@canada.ca>

:mod:`pyringserver.ringserver` -- Ringserver Library
====================================================

Library for storing information for sending to ringserver.
"""
# user for logging
import logging
# used for multi-threading
import queue
import threading
# for thread sleeping
import time

from typing import Dict

# Third-party libraries
from obspy import Stream

# User-contributed libraries
from pyringserver.datalink import DataLink, DataLinkNotImplemented


class InvalidTrace(Exception):
    """Invalid message set to stream"""


class RingServerStore(object):
    """
    Message thread store for async publishing of message decoded
    by the system.

    shutdown attribute can be used to monitor if the async store can be safely
    closed

    Handle all threads and their associated queues.

    :param str thread_prefix: prefix given to threads for easier identifying
        (default: '')
    :param str host: host, port of ringserver
    :param int write_interval: write interval to datalink
    """
    def __init__(
        self,
        host: str = '0.0.0.0',
        port: int = 18000,
        thread_prefix: str = '',
        write_interval: int = 0,
    ):
        self.queues: Dict[str, queue.Queue] = {}
        self.threads: Dict[str, RingServerThread] = {}
        self.shutdown = False
        self.host = host
        self.port = port
        self.thread_prefix = thread_prefix
        self.write_interval = write_interval

    def add(self, stream):
        """
        Add the stream to the queue.  The stream can not be a masked array.

        ..  note::
            At the moment we let the store handle a single thread for all
            sampling rate however we leave the oppurtinity to create multiple
            threads (one per sampling rate). To do so, simply convert the
            queue and threads to lists.

        ..  note::
            We do not use a mutex on the queue since only one thread reads the
            queue and only one writes.  There can never be a case where two
            threads read the queue at the same time in our logic.
        """
        # If the sampling rate has not been executed yet, initiate it
        if not self.threads:
            thread_name = self.thread_prefix
            self.queues[thread_name] = queue.Queue()
            # At the moment, we write all message to the same queue
            self.threads[thread_name] = RingServerThread(
                thread_name, self.queues[thread_name],
                self.host, self.write_interval
            )
            self.threads[thread_name].start()

        for thread_name in self.threads:
            logging.info(f'Queing traces {str(stream)}')
            self.queues[thread_name].put(stream)

    def exit_gracefully(self, signum, frame):
        """
        Using sigterm, close threads gracefully
        """
        # Send a signal to all thread to shutdown gracefully
        for thread_name in self.threads:
            logging.info("Closing thread %s", thread_name)
            self.threads[thread_name].shutdown()

        # Wait to complete
        for thread_name in self.threads:
            self.threads[thread_name].join()
            logging.info("Thread %s closed", thread_name)

        self.shutdown = True
        del self  # [TODO] does not work


class RingServerThread(threading.Thread):
    """
    Canmos message thread

    :param str identifier: unique identifier for the thread
    :type queue: :class:`queue.Queue`
    :param queue: queue associated to the thread
    :param str host: host of ringserver
    :param int port: port of ringserver
    :param int write_interval: float write interval of thread \
        to miniseed (default: 0)

    """
    def __init__(
        self,
        identifier: str,
        queue: queue.Queue,
        host: str = '0.0.0.0',
        port: int = 18000,
        write_interval: int = 0,
    ):
        super(RingServerThread, self).__init__()
        self.identifier = identifier
        self.queue = queue
        self.write_interval = write_interval
        self.datalink = DataLink(host=host, port=port)

        self._shutdown = False
        self._queue_read_timeout = 1  # seconds

    def shutdown(self):
        """Shutdown thread"""
        self._shutdown = True

    def run(self):
        """Execute thread (infinite)"""
        while not self._shutdown:
            self._run_rt() if self.write_interval == 0 else \
                self._run_time(self.write_interval)
        logging.info(f'[thread {self.identifier}] shutdown')

    def _run_rt(self):
        """
        Run the thread in "real-time" mode.  It doesn't wait for the an
        interval time.  It sends a packet of information as soon as
        its received.
        """
        try:
            stream = self.queue.get(True, self._queue_read_timeout)
        except queue.Empty:
            logging.info(f'[thread {self.identifier}] no message to publish')
            return  # timeout reached and queue is empty, continue
        if stream:
            self._write(stream)

    def _run_time(self, interval):
        """
        Run the thread in with an interval time.  This lets the queue build-up
        and then once the time completes, it sends the content of the queue.

        :param int interval: interval time in seconds
        """
        # If there are no elements in the queue, there is nothing to do
        stream = Stream()
        while not self.queue.empty():
            stream.extend(self.queue.get(False))
        if stream:
            stream.merge(method=-1)
            self._write(stream)
        time.sleep(interval)

    def _write(self, stream):
        """
        Write stream to DataLink connection

        :type stream: :class:`obspy.Stream`
        :param stream: stream to write
        """
        try:
            self.datalink.send(stream)
        except DataLinkNotImplemented as err:
            logging.error(err)
