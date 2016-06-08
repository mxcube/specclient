"""SpecMotor module

This module defines the classes for motor objects

Classes:
SpecMotor -- class representing a motor in Spec
SpecMotorA -- class representing a motor in Spec, to be used with a GUI
"""

__author__ = 'Matias Guijarro'
__version__ = '1.0'

import gevent
from gevent.event import Event
from .SpecClientError import SpecClientTimeoutError
import SpecConnectionsManager
import SpecEventsDispatcher
import SpecWaitObject
import SpecCommand
import logging
import types
import math

(NOTINITIALIZED, UNUSABLE, READY, MOVESTARTED, MOVING, ONLIMIT) = (0,1,2,3,4,5)
(NOLIMIT, LOWLIMIT, HIGHLIMIT) = (0,2,4)

class SpecMotorA:
    """SpecMotorA class"""
    def __init__(self, specName = None, specVersion = None, callbacks={}, timeout=None):
        """Constructor

        Keyword arguments:
        specName -- name of the motor in Spec (defaults to None)
        specVersion -- 'host:port' string representing a Spec server to connect to (defaults to None)
        """
        self._ready_state_event = Event()
        self.motorState = NOTINITIALIZED
        self.limit = NOLIMIT
        self.limits = (None, None)
        self.chanNamePrefix = ''
        self.connection = None
        self.timeout = timeout
        self.__old_position = None
        # the callbacks listed below can be set directly using the 'callbacks' keyword argument ;
        # when the event occurs, the corresponding callback will be called automatically
	self.__callbacks = {
          'connected': None,
          'disconnected': None,
          'motorLimitsChanged': None,
          'motorPositionChanged': None,
          'motorStateChanged': None
        }
        for cb_name in self.__callbacks.iterkeys():
          if callable(callbacks.get(cb_name)):
            self.__callbacks[cb_name] = SpecEventsDispatcher.callableObjectRef(callbacks[cb_name])

        if specName is not None and specVersion is not None:
            self.connectToSpec(specName, specVersion)
        else:
            self.specName = None
            self.specVersion = None


    def _read_channel(self, channel_name, timeout=None, force_read=False):
        channel = self.connection.getChannel(self.chanNamePrefix % channel_name)
        timeout = self.timeout if timeout is None else timeout
        if timeout is None:
            return channel.read(force_read=force_read)
        else:
            return channel.read(timeout=timeout, force_read=force_read)


    def connectToSpec(self, specName, specVersion, timeout=None):
        """Connect to a remote Spec

        Connect to Spec and register channels of interest for the specified motor

        Arguments:
        specName -- name of the motor in Spec
        specVersion -- 'host:port' string representing a Spec server to connect to
        """
        timeout = self.timeout if timeout is None else timeout
        self.specName = specName
        self.specVersion = specVersion
        self.chanNamePrefix = 'motor/%s/%%s' % specName

        self.connection = SpecConnectionsManager.SpecConnectionsManager().getConnection(specVersion)
        SpecEventsDispatcher.connect(self.connection, 'connected', self._connected)
        SpecEventsDispatcher.connect(self.connection, 'disconnected', self._disconnected)

        if self.connection.isSpecConnected():
            self._connected()


    def _connected(self):
        """Protected callback triggered by a 'connected' event from Spec."""
        #
        # register channels
        #
        self.connection.registerChannel(self.chanNamePrefix % 'low_limit', self._motorLimitsChanged)
        self.connection.registerChannel(self.chanNamePrefix % 'high_limit', self._motorLimitsChanged)
        self.connection.registerChannel(self.chanNamePrefix % 'position', self.__motorPositionChanged, dispatchMode=SpecEventsDispatcher.FIREEVENT)
        self.connection.registerChannel(self.chanNamePrefix % 'move_done', self.motorMoveDone, dispatchMode = SpecEventsDispatcher.FIREEVENT)
        self.connection.registerChannel(self.chanNamePrefix % 'high_lim_hit', self.__motorLimitHit)
        self.connection.registerChannel(self.chanNamePrefix % 'low_lim_hit', self.__motorLimitHit)
        self.connection.registerChannel(self.chanNamePrefix % 'sync_check', self.__syncQuestion)
        self.connection.registerChannel(self.chanNamePrefix % 'unusable', self.__motorUnusable)
        self.connection.registerChannel(self.chanNamePrefix % 'offset', self.motorOffsetChanged)
        self.connection.registerChannel(self.chanNamePrefix % 'sign', self.signChanged)
        #self.connection.registerChannel(self.chanNamePrefix % 'dial_position', self.dialPositionChanged)

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

        Put the motor in NOTINITIALIZED state.
        """
        self.__changeMotorState(NOTINITIALIZED)

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


    #def dialPositionChanged(self, dial_position):
    #    pass


    def signChanged(self, sign):
        self._motorLimitsChanged()


    def motorOffsetChanged(self, offset):
        self._motorLimitsChanged()


    def _motorLimitsChanged(self):
        try:
          if self.__callbacks.get("motorLimitsChanged"):
            cb = self.__callbacks["motorLimitsChanged"]()
            if cb is not None:
              gevent.spawn(cb)
        finally:
          gevent.spawn(self.motorLimitsChanged)


    def motorLimitsChanged(self):
        """Callback triggered by a 'low_limit' or a 'high_limit' channel update,
        or when the sign or offset for motor changes

        To be extended by derivated classes.
        """
        pass


    def motorMoveDone(self, channelValue):
        """Callback triggered when motor starts or stops moving

        Change the motor state accordingly.

        Arguments:
        channelValue -- value of the channel
        """
        if channelValue:
            self.__changeMotorState(MOVING)
        elif self.motorState == MOVING or self.motorState == MOVESTARTED or self.motorState == NOTINITIALIZED:
            self.__changeMotorState(READY)


    def __motorLimitHit(self, channelValue, channelName):
        """Private callback triggered by a 'low_lim_hit' or a 'high_lim_hit' channel update

        Update the motor state accordingly.

        Arguments:
        channelValue -- value of the channel
        channelName -- name of the channel (either 'low_lim_hit' or 'high_lim_hit')
        """
        if channelValue:
            if channelName.endswith('low_lim_hit'):
                self.limit = self.limit | LOWLIMIT
                self.__changeMotorState(ONLIMIT)
            else:
                self.limit = self.limit | HIGHLIMIT
                self.__changeMotorState(ONLIMIT)


    def __motorPositionChanged(self, absolutePosition):
        if self.__old_position is None:
           self.__old_position = absolutePosition
        else:
           if math.fabs(absolutePosition - self.__old_position) > 1E-6:
              self.__old_position = absolutePosition
           else:
              return
        try:
          if self.__callbacks.get("motorPositionChanged"):
            cb = self.__callbacks["motorPositionChanged"]()
            if cb is not None:
              cb(absolutePosition)
        finally:
          self.motorPositionChanged(absolutePosition)


    def motorPositionChanged(self, absolutePosition):
        """Callback triggered by a position channel update

        To be extended by derivated classes.

        Arguments:
        absolutePosition -- motor absolute position
        """
        pass


    def setOffset(self, offset):
        """Set the motor offset value"""
        c = self.connection.getChannel(self.chanNamePrefix % 'offset')

        c.write(offset)


    def getOffset(self, timeout=None):
        return self._read_channel('offset', timeout=timeout)


    def getSign(self, timeout=None):
        return self._read_channel('sign', timeout=timeout)


    def __syncQuestion(self, channelValue):
        """Callback triggered by a 'sync_check' channel update

        Call the self.syncQuestionAnswer method and reply to the sync. question.

        Arguments:
        channelValue -- value of the channel
        """
        if type(channelValue) == types.StringType:
            steps = channelValue.split()
            specSteps = steps[0]
            controllerSteps = steps[1]

            a = self.syncQuestionAnswer(specSteps, controllerSteps)

            if a is not None:
                c = self.connection.getChannel(self.chanNamePrefix % 'sync_check')
                c.write(a)


    def syncQuestionAnswer(self, specSteps, controllerSteps):
        """Answer to the sync. question

        Return either '1' (YES) or '0' (NO)

        Arguments:
        specSteps -- steps measured by Spec
        controllerSteps -- steps indicated by the controller
        """
        pass


    def __motorUnusable(self, unusable):
        """Private callback triggered by a 'unusable' channel update

        Update the motor state accordingly

        Arguments:
        unusable -- value of the channel
        """
        if unusable:
            self.__changeMotorState(UNUSABLE)
        else:
            self.__changeMotorState(READY)


    def __changeMotorState(self, state):
        """Private method for changing the SpecMotor object's internal state

        Arguments:
        state -- the motor state
        """
        self.motorState = state
        if self.motorState in (UNUSABLE, READY, ONLIMIT):
          self._ready_state_event.set()
        else:
          self._ready_state_event.clear()

        try:
          if self.__callbacks.get("motorStateChanged"):
            cb = self.__callbacks["motorStateChanged"]()
            if cb is not None:
              cb(state)
        finally:
          self.motorStateChanged(state)


    def motorStateChanged(self, state):
        """Callback to take into account a motor state update

        To be extended by derivated classes

        Arguments:
        state -- the motor state
        """
        pass


    def move(self, absolutePosition, wait=False, timeout=None):
        """Move the motor to the required position

        Arguments:
        absolutePosition -- position to move to
        """
        if type(absolutePosition) != types.FloatType and type(absolutePosition) != types.IntType:
            logging.getLogger("SpecClient").error("Cannot move %s: position '%s' is not a number", self.specName, absolutePosition)

        self.__changeMotorState(MOVESTARTED)
        
        c = self.connection.getChannel(self.chanNamePrefix % 'start_one')
        
        c.write(absolutePosition)
        self._ready_state_event.clear()

        if wait:
            self.waitMove(timeout)
            

    def waitMove(self, timeout=None):
        if not self._ready_state_event.wait(timeout):
            raise SpecClientTimeoutError  
    

    def moveRelative(self, relativePosition, wait=False, timeout=None):
        self.move(self.getPosition() + relativePosition, wait=wait, timeout=timeout)


    def moveToLimit(self, limit):
        cmdObject = SpecCommand.SpecCommandA("_mvc", self.connection)

        if cmdObject.isSpecReady():
            if limit:
                cmdObject(1)
            else:
                cmdObject(-1)


    def stop(self):
        """Stop the current motor

        Send an 'abort' message to the remote Spec
        """
        self.connection.abort()


    def stopMoveToLimit(self):
        c = self.connection.getChannel("var/_MVC_CONTINUE_MOVING")
        c.write(0)


    def getParameter(self, param, timeout=None):
        return self._read_channel(param, timeout=timeout)


    def setParameter(self, param, value):
        c = self.connection.getChannel(self.chanNamePrefix % param)
        c.write(value)


    def getPosition(self, timeout=None):
        """Return the current position of the motor."""
        return self._read_channel('position', timeout=timeout)


    def getState(self, timeout=None):
        """Return the current motor state."""
        return self.motorState


    def getLimits(self, timeout=None):
        """Return a (low limit, high limit) tuple in user units."""
        lims = [ x * self.getSign(timeout=timeout) + self.getOffset(timeout=timeout)
                 for x in (self._read_channel('low_limit', timeout=timeout), \
                           self._read_channel('high_limit', timeout=timeout))]

        return (min(lims), max(lims))


    def getDialPosition(self, timeout=None):
        """Return the motor dial position."""
        return self._read_channel('dial_position', timeout=timeout)


class SpecMotor(SpecMotorA):
    """SpecMotor class"""
    def __init__(self, *args, **kwargs): #specName = None, specVersion = None, callbacks={}):
        SpecMotorA.__init__(self, *args, **kwargs)

    def _read_channel(self, channel_name, timeout=None, force_read=True):
        return SpecMotorA._read_channel(self, channel_name, timeout=timeout,
                                        force_read=force_read)

    def connectToSpec(self, specName, specVersion, timeout=None):
        SpecMotorA.connectToSpec(self, specName, specVersion)

        if not self.connection.isSpecConnected():
            w = SpecWaitObject.SpecWaitObject(self.connection)
            w.waitConnection(timeout)
            self._connected()

    def setOffset(self, offset):
        """Set the motor offset value"""
        c = self.connection.getChannel(self.chanNamePrefix % 'offset')

        c.write(offset, wait=True)

    def __syncQuestion(self, channelValue):
        """Callback triggered by a 'sync_check' channel update

        Call the self.syncQuestionAnswer method and reply to the sync. question.

        Arguments:
        channelValue -- value of the channel
        """
        if type(channelValue) == types.StringType:
            steps = channelValue.split()
            specSteps = steps[0]
            controllerSteps = steps[1]

            a = self.syncQuestionAnswer(specSteps, controllerSteps)

            if a is not None:
                c = self.connection.getChannel(self.chanNamePrefix % 'sync_check')
                c.write(a, wait=True)


    def move(self, absolutePosition, wait=True, timeout=None):
        return SpecMotorA.move(self, absolutePosition, wait, timeout)
    

    def moveRelative(self, relativePosition, wait=True, timeout=None):
        return self.move(self.getPosition() + relativePosition, wait=wait, timeout=timeout)


    def waitMove(self, timeout=None):
        return SpecMotorA.waitMove(self, timeout)
    
        
    def moveToLimit(self, limit):
        raise NotImplementedError
       

    def stop(self):
        """Stop the current motor

        Send an 'abort' message to the remote Spec
        """
        self.connection.abort(wait=True)


    def stopMoveToLimit(self):
        raise NotImplementedError


    def setParameter(self, param, value):
        c = self.connection.getChannel(self.chanNamePrefix % param)
        c.write(value, wait=True)


    def getState(self, timeout=None):
        """Return the current motor state."""
        value = self._read_channel('move_done', timeout=timeout)
        self.motorMoveDone(value)
        return self.motorState
