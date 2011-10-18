# ***** BEGIN LICENSE BLOCK *****
#
# For copyright and licensing please refer to COPYING.
#
# ***** END LICENSE BLOCK *****
from pika.log import info,warning
from random import random


class ReconnectionStrategy(object):
    """Interface defining the entry points available for ReconnectionStrategy subclasses"""
    can_reconnect = False
    _active = True

    @property
    def is_active(self):
        return ReconnectionStrategy._active

    def set_active(self, value):
        ReconnectionStrategy._active = value

    def on_connect_attempt(self, conn):
        """Called each time the connection attempts to connect to the server"""

    def on_connect_attempt_failure(self, conn, err):
        """Called by the connection on failure to establish initial connection"""

    def on_transport_connected(self, conn):
        """Called when the connection's low-level transport is connected"""

    def on_transport_disconnected(self, conn):
        """Called when the connection's low-level transport is disconnected"""

    def on_connection_open(self, conn):
        """Called when a connection has been opened, negotiated, and is ready-to-run"""

    def on_connection_closed(self, conn):
        """Called when a connection has been closed"""

class NullReconnectionStrategy(ReconnectionStrategy):
    pass


class SimpleReconnectionStrategy(ReconnectionStrategy):
    """Strategy for keeping up a persistent connection 
    
    On connection failure or closure, will delay by:
    
        current_delay = current_delay * (1 + (0.5*random.random()))
        (to a maximum of self.max_delay)
    
    then issue a reconnect (via a timeout).
    """
    can_reconnect = True

    def __init__(self, initial_retry_delay=1.0, multiplier=2.0,
                 max_delay=30.0, jitter=0.5):

        self.initial_retry_delay = initial_retry_delay
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.jitter = jitter
        self._reset()

    def _reset(self):
        self.current_delay = self.initial_retry_delay
        self.attempts_since_last_success = 0

    def on_connect_attempt(self, conn):
        """Increments our attempt counter"""
        self.attempts_since_last_success += 1
    def on_connect_attempt_failure(self, conn, err):
        """Called by the connection on failure to establish initial connection"""
        warning( "Connection failure (attempt %s): %s", self.attempts_since_last_success, err )

    def on_connection_open(self, conn):
        """Reset our internal counters"""
        self._reset()
    
    def new_delay( self ):
        t = self.current_delay * ((random() * self.jitter) + 1) * self.multiplier
        self.current_delay = min(self.max_delay,t)
        return self.current_delay
    def on_connection_closed(self, conn):
        """Calculate our reconnection delay, create a timeout to issue a _reconnect"""
        if not self.is_active:
            return
        t = self.new_delay()
        info("%s retrying %r in %r seconds (%r attempts)",
                 self.__class__.__name__, conn.parameters, t,
                 self.attempts_since_last_success)
        conn.add_timeout(t, conn._reconnect)
