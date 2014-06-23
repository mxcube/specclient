"""SpecWaitObject module

This module defines the classes for helper objects
designed for waiting specific events from Spec

Classes:
SpecWaitObject -- base class for Wait objects

Functions:
waitChannel -- wait for a channel update
waitReply -- wait for a reply
waitConnection -- wait for a connection
"""

__author__ = 'Matias Guijarro'
__version__ = '1.0'

import weakref
import time
import types
import gevent
from gevent.util import wrap_errors

import SpecEventsDispatcher
from .SpecClientError import SpecClientError, SpecClientTimeoutError
import SpecConnectionsManager


def spawn_greenlet(func, *args, **kwargs):
    t = gevent.spawn(wrap_errors(Exception, func), *args)

    t._get = t.get
    def new_get(self, *args, **kwargs):
       ret = self._get(*args, **kwargs)
       if isinstance(ret, Exception):
           raise ret
       else:
           return ret
    setattr(t, "get", types.MethodType(new_get, t))

    return t


class SpecWaitObject:
    """Helper class for waiting specific events from Spec"""
    def __init__(self, connection):
        """Constructor

        Arguments:
        connection -- a SpecConnection object
        """
        self.connection = weakref.ref(connection)
        self.isdisconnected = True
        self.channelWasUnregistered = False
        self.value = None
        self.spec_reply_arrived_event = gevent.event.Event()
        self.channel_updated_event = gevent.event.Event()

        SpecEventsDispatcher.connect(connection, 'connected', self.connected)
        SpecEventsDispatcher.connect(connection, 'disconnected', self.disconnected)

        if connection.isSpecConnected():
            self.connected()


    def connected(self):
        """Callback triggered by a 'connected' event."""
        self.isdisconnected = False


    def disconnected(self):
        """Callback triggered by a 'disconnected' event."""
        self.isdisconnected = True


    def waitReply(self, command, argsTuple, timeout = None):
        """Wait for a reply from Spec

        Arguments:
        command -- method returning a replyID to be executed on the connection object
        argsTuple -- tuple of arguments to be passed to the command
        timeout -- optional timeout (defaults to None)
        """
        with gevent.Timeout(timeout, SpecClientTimeoutError):
            self.waitConnection()
            connection = self.connection()

            if connection is not None:
                try:
                    func = getattr(connection, command)
                except:
                    return
                else:
                    self.spec_reply_arrived_event.clear()

                    if callable(func):
                        func(*argsTuple)
                    
                    self.spec_reply_arrived_event.wait(timeout)
                


    def waitChannelUpdate(self, chanName, waitValue = None, timeout = None):
        """Wait for a channel update

        Arguments:
        chanName -- channel name
        waitValue -- particular value to wait (defaults to None, meaning any value)
        timeout -- optional timeout (defaults to None)
        """
        with gevent.Timeout(timeout, SpecClientTimeoutError):
            self.waitConnection()
            connection = self.connection()

            if connection is not None:
                self.channelWasUnregistered = False
                channel = connection.getChannel(chanName)
                self.channel_updated_event.clear()

                if not channel.registered:
                    self.channelWasUnregistered = True
                    connection.registerChannel(chanName, self.channelUpdated) #channel.register()
                else:
                    SpecEventsDispatcher.connect(channel, 'valueChanged', self.channelUpdated)

                if waitValue is None:
                  try:
                    self.channel_updated_event.wait(timeout)
                  except:
                    raise SpecClientTimeoutError
                else:
                  while waitValue != self.value:
                      self.channel_updated_event.wait(timeout)

                if self.channelWasUnregistered:
                    connection.unregisterChannel(chanName) #channel.unregister()


    def waitConnection(self, timeout = None):
        """Wait for the connection to Spec being established

        Arguments:
        timeout -- optional timeout (defaults to None)

        Exceptions:
        timeout -- raise a timeout exception on timeout
        """
        connection = self.connection()
        
        with gevent.Timeout(timeout, SpecClientTimeoutError):
          connection.connected_event.wait(timeout)
        

    def replyArrived(self, reply):
        """Callback triggered by a reply from Spec."""
        self.spec_reply_arrived_event.set()
        value = reply.getValue()

        if reply.error:
            raise SpecClientError('Server request did not complete: %s' % value, reply.error_code)

        self.value = value
        

    def channelUpdated(self, channelValue):
        """Callback triggered by a channel update

        If channel was unregistered, we skip the first update,
        else we update our internal value
        """
        if self.channelWasUnregistered == True:
            #
            # if we were unregistered, skip first update
            #
            self.channelWasUnregistered = 2
        else:
            self.value = channelValue
            self.channel_updated_event.set()


def waitConnection(connection, timeout = None):
    """Wait for a connection to Spec to be established

    Arguments:
    connection -- a 'host:port' string
    timeout -- optional timeout (defaults to None)
    """
    w = SpecWaitObject(connection)

    wait_greenlet = spawn_greenlet(w.waitConnection, timeout=timeout)
    wait_greenlet.get()


def waitChannelUpdate(chanName, connection, waitValue = None, timeout = None):
    """Wait for a channel to be updated

    Arguments:
    chanName -- channel name (e.g 'var/toto')
    connection -- a 'host:port' string
    waitValue -- value to wait (defaults to None)
    timeout -- optional timeout (defaults to None)
    """
    w = SpecWaitObject(connection)

    wait_greenlet = spawn_greenlet(w.waitChannelUpdate, chanName, waitValue = waitValue, timeout=timeout)
    wait_greenlet.get()

    return w.value


def waitReply(connection, command, argsTuple, timeout = None):
    """Wait for a reply from a remote Spec server

    Arguments:
    connection -- a 'host:port' string
    command -- command to execute
    argsTuple -- tuple of arguments for the command
    timeout -- optional timeout (defaults to None)
    """
    w = SpecWaitObject(connection)
    
    w.waitReply(command, argsTuple, timeout=timeout)

    return w.value















