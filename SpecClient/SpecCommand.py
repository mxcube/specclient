"""SpecCommand module

This module defines the classes Spec command
objects

Classes:
BaseSpecCommand
SpecCommand
SpecCommandA
"""

__author__ = 'Matias Guijarro'
__version__ = '1.0'

import types
import logging
import gevent
from gevent.event import Event
from .SpecConnection import SpecClientNotConnectedError
from .SpecReply import SpecReply
import SpecConnectionsManager
import SpecEventsDispatcher
import SpecWaitObject
from .SpecClientError import SpecClientTimeoutError, SpecClientError

class abort_spec_on_exception:
  def __init__(self, cmd):
    self.cmd = cmd

  def __enter__(self):
    pass

  def __exit__(self, type, value, traceback):
    if type is not None:
      if not isinstance(type, SpecClientError):
        # abort from Python => have to abort in spec
        logging.getLogger("SpecClient").info("Aborting spec command %s", self.cmd)
        self.cmd.abort()

def wait_end_of_spec_cmd(cmd_obj):
  with abort_spec_on_exception(cmd_obj):
     cmd_obj._reply_arrived_event.wait()

     if cmd_obj._last_reply.error:
        raise SpecClientError(cmd_obj._last_reply.error)
     else:
        return cmd_obj._last_reply.data


class BaseSpecCommand:
    """Base class for SpecCommand objects"""
    def __init__(self, command = None, connection = None, callbacks = None):
        self.command = None
        self.connection = None
        self.specVersion = None
        self.isConnected = self.isSpecConnected #alias

        if connection is not None:
            if type(connection) in (types.UnicodeType, types.StringType):
                #
                # connection is given in the 'host:port' form
                #
                self.connectToSpec(str(connection))
            else:
                self.connection = connection

        if command is not None:
            self.setCommand(command)


    def connectToSpec(self, specVersion):
        pass


    def isSpecConnected(self):
        return self.connection is not None and self.connection.isSpecConnected()


    def isSpecReady(self):
        if self.isSpecConnected():
            try:
                status_channel = self.connection.getChannel("status/ready")
                status = status_channel.read()
            except:
                pass
            else:
                return status

        return False


    def setCommand(self, command):
        self.command = command


    def __repr__(self):
        return '<SpecCommand object, command=%s>' % self.command or ''


    def __call__(self, *args, **kwargs):
        if self.command is None:
            return

        if self.connection is None or not self.connection.isSpecConnected():
            return

        if self.connection.serverVersion < 3:
            func = False

            if 'function' in kwargs:
                func = kwargs['function']

            #convert args list to string args list
            #it is much more convenient using .call('psvo', 12) than .call('psvo', '12')
            #a possible problem will be seen in Spec
            args = map(repr, args)

            if func:
                # macro function
                command = self.command + '(' + ','.join(args) + ')'
            else:
                # macro
                command = self.command + ' ' + ' '.join(args)
        else:
            # Spec knows
            command = [self.command] + list(args)

        return self.executeCommand(command, kwargs.get("wait", False))


    def executeCommand(self, command, wait=False):
        pass



class SpecCommand(BaseSpecCommand):
    """SpecCommand objects execute macros and wait for results to get back"""
    def __init__(self, command, connection, timeout = None):
        self.__timeout = timeout
        BaseSpecCommand.__init__(self, command, connection)


    def connectToSpec(self, specVersion):
        self.connection = SpecConnectionsManager.SpecConnectionsManager().getConnection(specVersion)
        self.specVersion = specVersion

        SpecWaitObject.waitConnection(self.connection, self.__timeout)


    def executeCommand(self, command, wait=None):
        if self.connection.serverVersion < 3:
            connectionCommand = 'send_msg_cmd_with_return'
        else:
            if type(command) == types.StringType:
                connectionCommand = 'send_msg_cmd_with_return'
            else:
                connectionCommand = 'send_msg_func_with_return'

        return SpecWaitObject.waitReply(self.connection, connectionCommand, (command, ), self.__timeout)



class SpecCommandA(BaseSpecCommand):
    """SpecCommandA is the asynchronous version of SpecCommand.
    It allows custom waiting by subclassing."""
    def __init__(self, *args, **kwargs):
        self._reply_arrived_event = Event()
        self._last_reply = None
        self.__callback = None
        self.__error_callback = None
        self.__callbacks = {
          'connected': None,
          'disconnected': None,
          'statusChanged': None,
        }
        callbacks = kwargs.get("callbacks", {})
        for cb_name in self.__callbacks.iterkeys():
          if callable(callbacks.get(cb_name)):
            self.__callbacks[cb_name] = SpecEventsDispatcher.callableObjectRef(callbacks[cb_name])

        BaseSpecCommand.__init__(self, *args, **kwargs)


    def connectToSpec(self, specVersion, timeout=0.2):
        if self.connection is not None:
            SpecEventsDispatcher.disconnect(self.connection, 'connected', self._connected)
            SpecEventsDispatcher.disconnect(self.connection, 'disconnected', self._disconnected)

        self.connection = SpecConnectionsManager.SpecConnectionsManager().getConnection(specVersion)
        self.specVersion = specVersion

        SpecEventsDispatcher.connect(self.connection, 'connected', self._connected)
        SpecEventsDispatcher.connect(self.connection, 'disconnected', self._disconnected)

        if self.connection.isSpecConnected():
            self._connected()
        else:
            with gevent.Timeout(timeout, SpecClientTimeoutError):
              SpecWaitObject.waitConnection(self.connection, timeout)
            SpecEventsDispatcher.dispatch()

    def connected(self):
        pass

    def _connected(self):
        self.connection.registerChannel("status/ready", self._statusChanged)
 
        self.connection.send_msg_hello()        

        try:
            cb_ref = self.__callbacks.get("connected")
            if cb_ref is not None:
                cb = cb_ref()
                if cb is not None:
                    cb()
        finally:
            self.connected()

    def _disconnected(self):
        try:
            cb_ref = self.__callbacks.get("disconnected")
            if cb_ref is not None:
                cb = cb_ref()
                if cb is not None:
                    cb()
        finally:
           self.disconnected()


    def disconnected(self):
        pass

 
    def _statusChanged(self, ready):
        try:
            cb_ref = self.__callbacks.get("statusChanged")
            if cb_ref is not None:
                cb = cb_ref()
                if cb is not None:
                   cb(ready)
        finally:
            self.statusChanged(ready)
    

    def statusChanged(self, ready):
        pass


    def executeCommand(self, command, wait=False, timeout=None):
        self._reply_arrived_event.clear()
        self.beginWait()

        if self.connection.serverVersion < 3:
            id = self.connection.send_msg_cmd_with_return(command)
        else:
            if type(command) == types.StringType:
                id = self.connection.send_msg_cmd_with_return(command)
            else:
                id = self.connection.send_msg_func_with_return(command)

        task = gevent.spawn(wait_end_of_spec_cmd, self)
        if wait:
            return task.get(timeout=timeout)
        else:
            return task


    def __call__(self, *args, **kwargs):
        self.__callback = kwargs.get("callback", None)
        self.__error_callback = kwargs.get("error_callback", None)

        return BaseSpecCommand.__call__(self, *args, **kwargs)


    def replyArrived(self, reply):
        self._last_reply = reply

        if reply.error:
            if callable(self.__error_callback):
                try:
                    self.__error_callback(reply.error)
                except:
                    logging.getLogger("SpecClient").exception("Error while calling error callback (command=%s,spec version=%s)", self.command, self.specVersion)
                self.__error_callback = None
        else:
            if callable(self.__callback):
                try:
                    self.__callback(reply.data)
                except:
                    logging.getLogger("SpecClient").exception("Error while calling reply callback (command=%s,spec version=%s)", self.command, self.specVersion)
                self.__callback = None

        self._reply_arrived_event.set()


    def beginWait(self):
        pass


    def abort(self):
        if self.connection is None or not self.connection.isSpecConnected():
            return

        self.connection.abort()













