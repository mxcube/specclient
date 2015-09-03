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

import sys
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


class wrap_errors(object):
    def __init__(self, func):
        """Make a new function from `func', such that it catches all exceptions
        and return it as a SpecClientError object
        """
        self.func = func

    def __call__(self, *args, **kwargs):
        func = self.func
        try:
            return func(*args, **kwargs)
        except Exception, e:
            return SpecClientError(e)

    def __str__(self):
        return str(self.func)

    def __repr__(self):
        return repr(self.func)

    def __getattr__(self, item):
        return getattr(self.func, item)


def wait_end_of_spec_cmd(cmd_obj):
   cmd_obj._reply_arrived_event.wait()

   if cmd_obj._last_reply.error:
      raise SpecClientError("command %r aborted from spec" % cmd_obj.command)
   else:
      return cmd_obj._last_reply.data


class BaseSpecCommand:
    """Base class for SpecCommand objects"""
    def __init__(self, command = None, connection = None, callbacks = None, timeout=None):
        self.command = None
        self.connection = None
        self.specVersion = None
        if command is not None:
            self.setCommand(command)
            
        if connection is not None:
            if type(connection) in (types.UnicodeType, types.StringType):
                #
                # connection is given in the 'host:port' form
                #
                self.connectToSpec(str(connection), timeout)
            else:
                self.connection = connection


    def connectToSpec(self, specVersion, timeout=None):
        pass


    def isConnected(self):
        return self.isSpecConnected()

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

        if self.connection is None:
            raise SpecClientNotConnectedError
        
        self.connection.connected_event.wait()

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

        return self.executeCommand(command, kwargs.get("wait", False), kwargs.get("timeout"))


    def executeCommand(self, command, wait=False, timeout=None):
        pass


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


    def connectToSpec(self, specVersion, timeout=None):
        if self.connection is not None:
            SpecEventsDispatcher.disconnect(self.connection, 'connected', self._connected)
            SpecEventsDispatcher.disconnect(self.connection, 'disconnected', self._disconnected)

        self.connection = SpecConnectionsManager.SpecConnectionsManager().getConnection(specVersion)
        self.specVersion = specVersion

        SpecEventsDispatcher.connect(self.connection, 'connected', self._connected)
        SpecEventsDispatcher.connect(self.connection, 'disconnected', self._disconnected)

        if self.connection.isSpecConnected():
            self._connected()
        

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

        with gevent.Timeout(timeout, SpecClientTimeoutError):
            waiter = SpecWaitObject.SpecWaitObject(self.connection)
            waiter.waitConnection()
 
            if self.connection.serverVersion < 3:
                id = self.connection.send_msg_cmd_with_return(command)
            else:
                if type(command) == types.StringType:
                    id = self.connection.send_msg_cmd_with_return(command)
                else:
                    id = self.connection.send_msg_func_with_return(command)

            t = gevent.spawn(wrap_errors(wait_end_of_spec_cmd), self)

            if wait:
                ret = t.get()
                if isinstance(ret, SpecClientError):
                  raise ret
                elif isinstance(ret, Exception):
                  self.abort() #abort spec
                  raise
                else:
                  return ret
            else:
                t._get = t.get
                def special_get(self, *args, **kwargs):
                  ret = self._get(*args, **kwargs)
                  if isinstance(ret, SpecClientError):
                    raise ret
                  elif isinstance(ret, Exception):
                    self.abort() #abort spec
                    raise
                  else:
                    return ret
                setattr(t, "get", types.MethodType(special_get, t))

                return t


    def _set_callbacks(self, callback, error_callback):
        if callable(callback):
            self.__callback = SpecEventsDispatcher.callableObjectRef(callback)
        else:
            self.__callback = None
        if callable(error_callback):
            self.__error_callback = SpecEventsDispatcher.callableObjectRef(error_callback)
        else:
            self.__error_callback = None


    def __call__(self, *args, **kwargs):
        callback = kwargs.get("callback", None)
        error_callback = kwargs.get("error_callback", None)
        self._set_callbacks(callback, error_callback)
        return BaseSpecCommand.__call__(self, *args, **kwargs)


    def replyArrived(self, reply):
        self._last_reply = reply

        if reply.error:
            if callable(self.__error_callback):
                error_callback = self.__error_callback()
                try:
                    error_callback(reply.error)
                except:
                    logging.getLogger("SpecClient").exception("Error while calling error callback (command=%s,spec version=%s)", self.command, self.specVersion)
                self.__error_callback = None
        else:
            if callable(self.__callback):
                callback = self.__callback()
                try:
                    callback(reply.data)
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



class SpecCommand(SpecCommandA):
    def __init__(self, *args, **kwargs):
        SpecCommandA.__init__(self, *args, **kwargs)

    def connectToSpec(self, specVersion, timeout):
        SpecCommandA.connectToSpec(self, specVersion, timeout)

        if not self.connection.isSpecConnected():
            with gevent.Timeout(timeout, SpecClientTimeoutError):
                SpecWaitObject.waitConnection(self.connection, timeout)
            self._connected()
        
    def abort(self):
        if self.connection is None or not self.connection.isSpecConnected():
            return

        self.connection.abort(wait=True)

    def __call__(self, *args, **kwargs):
        self._set_callbacks(kwargs.get("callback", None), kwargs.get("error_callback", None))

        wait = kwargs.get("wait", True)
        timeout = kwargs.get("timeout", None)
        return SpecCommandA.__call__(self, *args, wait=wait, timeout=timeout)

    def executeCommand(self, command, wait=True, timeout=None):
        return SpecCommandA.executeCommand(self, command, wait, timeout)
         





