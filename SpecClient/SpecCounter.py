"""SpecCounter module

This module defines the classes for counter objects

Classes:
SpecCounter -- class representing a counter in Spec
SpecCounterA -- class representing a counter in Spec, to be used with a GUI
"""

__author__ = 'Matias Guijarro'
__version__ = '1.1'

import SpecConnectionsManager
import SpecEventsDispatcher
import SpecWaitObject
import SpecCommand
import math

NOTINITIALIZED, NOTCOUNTING, COUNTING = range(3)
UNKNOWN, SCALER, TIMER, MONITOR = 0, 1, 2, 3

ALL_COUNT = "scaler/.all./count"


class SpecCounterA:
    """SpecCounter class"""
    def __init__(self, specName = None, specVersion = None, callbacks = None, timeout = None):
         """Constructor

         Keyword arguments:
         specName -- the name of the counter in Spec (defaults to None)
         specVersion -- 'host:port' string representing a Spec server to connect to (defaults to None)
         callbacks -- dict of callbacks. key is callback name; value is a python callable
                      allowed keys: connected, disconnected, counterStateChanged, counterValueChanged
         timeout -- optional timeout for connection (defaults to None)
         """
         self.counterState = NOTINITIALIZED
         self.chanNamePrefix = ""
         self.connection = None
         self.type = UNKNOWN
         self.__old_value = None
         self.__callbacks = {
            "counterStateChanged": None,
            "counterValueChanged": None,
            "connected": None,
            "disconnected": None,
         }
         if callbacks is None:
            callbacks = {}
         for cb_name in self.__callbacks.iterkeys():
            if callable(callbacks.get(cb_name)):
                self.__callbacks[cb_name] = SpecEventsDispatcher.callableObjectRef(callbacks[cb_name])

         if specName is not None and specVersion is not None:
            self.connectToSpec(specName, specVersion, timeout)
         else:
            self.specName = None
            self.specVersion = None


    @property
    def _allCountChannel(self):
        return self.connection.getChannel(ALL_COUNT)


    @property
    def _valueChannel(self):
        return self.connection.getChannel(self.chanNamePrefix % 'value')


    def connectToSpec(self, specName, specVersion, timeout = None):
        """Connect to a remote Spec

        Connect to Spec

        Arguments:
        specName -- name of the counter in Spec
        specVersion -- 'host:port' string representing a Spec server to connect to
        timeout -- optional timeout for connection (defaults to None)
        """
        self.specName = specName
        self.specVersion = specVersion
        self.chanNamePrefix = 'scaler/%s/%%s' % specName

        self.connection = SpecConnectionsManager.SpecConnectionsManager().getConnection(specVersion)
        SpecEventsDispatcher.connect(self.connection, 'connected', self._connected)
        SpecEventsDispatcher.connect(self.connection, 'disconnected', self._disconnected)

        if self.connection.isSpecConnected():
            self._connected()


    def getType(self):
        c = self.connection.getChannel('var/%s' % self.specName)
        index = c.read()
        typ = SCALER
        if index == 0:
            typ = TIMER
        elif index == 1:
            typ = MONITOR
        return typ


    def _connected(self):
        self.type = self.getType()
        self.connection.registerChannel(self.chanNamePrefix % 'value',
                                        self.__counterValueChanged)
                                        #dispatchMode=SpecEventsDispatcher.FIREEVENT)
        self.connection.registerChannel(ALL_COUNT,
                                        self.__counterStateChanged)
        try:
          if self.__callbacks.get("connected"):
            cb = self.__callbacks["connected"]()
            if cb is not None:
              cb()
        finally:
          self.connected()


    def connected(self):
        """Callback triggered by a 'connected' event from Spec

        To be extended by derivated classes.
        """
        pass


    def _disconnected(self):
        """Protected callback triggered by a 'disconnected' event from Spec

        """
        try:
          if self.__callbacks.get("disconnected"):
            cb = self.__callbacks["disconnected"]()
            if cb is not None:
              cb()
        finally:
          self.disconnected()


    def disconnected(self):
        """Callback triggered by a 'disconnected' event from Spec

        To be extended by derivated classes.
        """
        pass


    def __counterValueChanged(self, value):
        if self.__old_value is None:
            self.__old_value = value
        else:
            if math.fabs(value - self.__old_value) > 1E-6:
                self.__old_value = value
            else:
                return
        try:
            if self.__callbacks.get("counterValueChanged"):
                cb = self.__callbacks["counterValueChanged"]()
                if cb is not None:
                    cb(value)
        finally:
            self.counterValueChanged(value)


    def counterValueChanged(self, value):
        """Callback triggered by a value channel update

        To be extended by derivated classes.

        Arguments:
        value -- counter value
        """
        pass


    def __counterStateChanged(self, value):
        self._updateCounterState(value)


    def _updateCounterState(self, state):
        if state == 0:
            state = NOTCOUNTING
        else:
            state = COUNTING

        self.__counterState = state
        try:
            if self.__callbacks.get("counterStateChanged"):
                cb = self.__callbacks["counterStateChanged"]()
                if cb is not None:
                    cb(state)
        finally:
            self.counterStateChanged(state)


    def counterStateChanged(self, state):
        """Callback to take into account a counter state update

        To be extended by derivated classes

        Arguments:
        state -- the counter state
        """
        pass


    def count(self, time, wait=False, timeout=None):
        """Count up to a certain time or monitor count

        Arguments:
        time -- count time
        """
        if self.type == MONITOR:
            time = -time

        self._allCountChannel.write(time)

        if wait:
            self.waitCount(timeout)
            return self.getValue()


    def waitCount(self, timeout=None):
        w = SpecWaitObject.SpecWaitObject(self.connection)
        w.waitChannelUpdate(ALL_COUNT, waitValue = 0)
        return self.getValue()


    def stop(self):
        self._allCountChannel.write(0)


    def getState(self):
        """Return the current motor state."""
        return self.counterState


    def getValue(self):
        """Return current counter value."""
        return self._valueChannel.read()


    def setEnabled(self, enabled=True):
        if enabled:
            disable = "0"
        else:
            disable = "1"
        cmd = 'counter_par({0}, "disable", {1})'.format(self.specName, disable)
        return SpecCommand.SpecCommand(cmd, self.connection)()


    def isEnabled(self):
        cmd = 'counter_par({0}, "disable")'.format(self.specName)
        result = SpecCommand.SpecCommand(cmd, self.connection)()
        return result == 0


class SpecCounter(SpecCounterA):
    """SpecCounter class"""

    def connectToSpec(self, specName, specVersion, timeout = None):
        """Connect to a remote Spec

        Connect to Spec

        Arguments:
        specName -- name of the counter in Spec
        specVersion -- 'host:port' string representing a Spec server to connect to
        timeout -- optional timeout for connection (defaults to None)
        """
        SpecCounterA.connectToSpec(self, specName, specVersion)

        if not self.connection.isSpecConnected():
            w = SpecWaitObject.SpecWaitObject(self.connection)
            w.waitConnection(timeout)
            self._connected()


    def getState(self):
        """Return the current motor state."""
        # TODO
        state = self._allCountChannel.read(force_read=True)
        self._updateCounterState(state)
        return SpecCounterA.getState(self)


    def getValue(self):
        """Return current counter value."""
        return self._valueChannel.read(force_read=True)


