#$Id: SpecConnection.py,v 1.11 2005/12/09 10:32:24 guijarro Exp $
"""SpecConnection module

Low-level module for communicating with a
remote Spec server

Classes :
SpecClientNotConnectedError -- exception class
SpecConnection
SpecConnectionDispatcher
"""

__author__ = 'Matias Guijarro'
__version__ = '1.0'

import gevent
import gevent.socket
import gevent.queue
import socket
import weakref
import string
import logging
import time
from .SpecClientError import SpecClientNotConnectedError
import SpecEventsDispatcher
import SpecChannel
import SpecMessage
import SpecReply
import traceback
import sys

(DISCONNECTED, PORTSCANNING, WAITINGFORHELLO, CONNECTED) = (1,2,3,4)
(MIN_PORT, MAX_PORT) = (6510, 6530)

def makeConnection(connection_ref):
   """Establish a connection to Spec

   If we are in port scanning mode, try to connect using
   a port defined in the range from MIN_PORT to MAX_PORT
   """
   while True:
     conn = connection_ref()
     if conn is None:
       break
      
     if conn.scanport:
       if conn.port is None or conn.port > MAX_PORT:
         conn.port = MIN_PORT
       else:
         conn.port += 1

     try:
       s = gevent.socket.create_connection((conn.host, conn.port), timeout=0.2)
     except socket.error:
       del conn
       time.sleep(0.1)
     else:
       connection_greenlet = gevent.spawn(connectionHandler, connection_ref, s)
       if conn.scanport:
         conn.port -= 1
       del conn
       connection_greenlet.join() 

def connectionHandler(connection_ref, socket_to_spec):
   receivedStrings = []
   message = None
   serverVersion = None
   socket_to_spec.settimeout(None)

   conn = connection_ref()
   if conn is not None:
      conn.connected = True
      conn.state = WAITINGFORHELLO
      conn.socket = socket_to_spec
      conn.send_msg_hello()
      del conn

   while True: 
      try:
        receivedStrings.append(socket_to_spec.recv(4096)) 
      except:
        receivedStrings.append("")

      if receivedStrings[-1] == "":
         conn = connection_ref()
         if conn is None:
            break
         conn.handle_close()
         del conn
         break
                
      s = ''.join(receivedStrings)
      consumedBytes = 0
      offset = 0

      while offset < len(s):
         if message is None:
            message = SpecMessage.message(version = serverVersion)
         
         consumedBytes = message.readFromStream(s[offset:])

         if consumedBytes == 0:
            break

         offset += consumedBytes

         if message.isComplete():
            conn = connection_ref()
            if conn is None:
               break
            
            try:
               try:
                  # dispatch incoming message
                  if message.cmd == SpecMessage.REPLY:
                     replyID = message.sn
                     if replyID > 0:
                        try:
                           reply = conn.registeredReplies[replyID]
                        except:
                           logging.getLogger("SpecClient").exception("Unexpected error while receiving a message from server")
                        else:
                           del conn.registeredReplies[replyID]
                           #replies_queue.put((reply, message.data, message.type==SpecMessage.ERROR, message.err))
                           gevent.spawn(reply.update, message.data, message.type==SpecMessage.ERROR, message.err)
                           time.sleep(1E-6)
                  elif message.cmd == SpecMessage.EVENT:
                     try:
                        channel = conn.registeredChannels[message.name]
                     except KeyError:
                        pass
                     else:
                        #channels_queue.put((channel, message.data, message.flags == SpecMessage.DELETED))
                        gevent.spawn(channel.update, message.data, message.flags == SpecMessage.DELETED)
                        time.sleep(1E-6)
                  elif message.cmd == SpecMessage.HELLO_REPLY:
                     if conn.checkourversion(message.name):
                        serverVersion = message.vers #header version
                        conn.serverVersion = serverVersion
                        gevent.spawn(conn.specConnected)
                        time.sleep(1E-6)
                     else:
                        serverVersion = None
                        conn.serverVersion = None
                        conn.connected = False
                        conn.disconnect()
                        conn.state = DISCONNECTED
                        if conn.scanport:
                            conn.port += 1
               except:
                  receivedStrings = [ s[offset:] ]
                  raise
            finally:
               del conn
               message = None

         receivedStrings = [ s[offset:] ]

   #process_replies_greenlet.kill()
   #process_channels_greenlet.kill()
   

class SpecConnection:
    """SpecConnection class

    Signals:
    connected() -- emitted when the required Spec version gets connected
    disconnected() -- emitted when the required Spec version gets disconnected
    replyFromSpec(reply id, SpecReply object) -- emitted when a reply comes from the remote Spec
    error(error code) -- emitted when an error event is received from the remote Spec
    """
    def __init__(self, specVersion):
        """Constructor

        Arguments:
        specVersion -- a 'host:port' string
        """
        self.state = DISCONNECTED
        self.connected = False
        self.scanport = False
        self.scanname = ''
        self.registeredChannels = {}
        self.registeredReplies = {}
        self.simulationMode = False
        self.connected_event = gevent.event.Event()
        self._completed_writing_event = gevent.event.Event()
        self.outgoing_queue = []
        self.socket_write_event = None

        tmp = str(specVersion).split(':')
        self.host = tmp[0]

        if len(tmp) > 1:
            self.port = tmp[1]
        else:
            self.port = 6789

        try:
            self.port = int(self.port)
        except:
            self.scanname = self.port
            self.port = None
            self.scanport = True

        #
        # register 'service' channels
        #
        self.registerChannel('error', self.error, dispatchMode = SpecEventsDispatcher.FIREEVENT)
        self.registerChannel('status/simulate', self.simulationStatusChanged)

    def __str__(self):
        return '<connection to Spec, host=%s, port=%s>' % (self.host, self.port or self.scanname)

    def __del__(self):
        self.disconnect()

    def __getattr__(self, attr):
        if attr == 'macro':
          return getattr(self, "send_msg_cmd_with_return")
        elif attr == 'macro_noret': 
          return getattr(self, "send_msg_cmd")
        elif attr == 'abort':
          return getattr(self, "send_msg_abort")
        else:
          raise AttributeError("Attribute '%s' unexpected" % attr)

    def registerChannel(self, chanName, receiverSlot, registrationFlag = SpecChannel.DOREG, dispatchMode = SpecEventsDispatcher.UPDATEVALUE):
        """Register a channel

        Tell the remote Spec we are interested in receiving channel update events.
        If the channel is not already registered, create a new SpecChannel object,
        and connect the channel 'valueChanged' signal to the receiver slot. If the
        channel is already registered, simply add a connection to the receiver
        slot.

        Arguments:
        chanName -- a string representing the channel name, i.e. 'var/toto'
        receiverSlot -- any callable object in Python

        Keywords arguments:
        registrationFlag -- internal flag
        dispatchMode -- can be SpecEventsDispatcher.UPDATEVALUE (default) or SpecEventsDispatcher.FIREEVENT,
        depending on how the receiver slot will be called. UPDATEVALUE means we don't mind skipping some
        channel update events as long as we got the last one (for example, a motor position). FIREEVENT means
        we want to call the receiver slot for every event.
        """
        if dispatchMode is None:
            return

        chanName = str(chanName)

        try:
          if not chanName in self.registeredChannels:
            channel = SpecChannel.SpecChannel(self, chanName, registrationFlag)
            self.registeredChannels[chanName] = channel
            if channel.spec_chan_name != chanName:
                self.registerChannel(channel.spec_chan_name, channel.update)
            channel.registered = True
          else:
            channel = self.registeredChannels[chanName]

          SpecEventsDispatcher.connect(channel, 'valueChanged', receiverSlot, dispatchMode)

          channelValue = self.registeredChannels[channel.spec_chan_name].value #channel.spec_chan_name].value
          if channelValue is not None:
            # we received a value, so emit an update signal
            channel.update(channelValue, force=True)
        except:
          logging.getLogger("SpecClient").exception("Uncaught exception in SpecConnection.registerChannel")


    def unregisterChannel(self, chanName):
        """Unregister a channel

        Arguments:
        chanName -- a string representing the channel to unregister, i.e. 'var/toto'
        """
        chanName = str(chanName)

        if chanName in self.registeredChannels:
            self.registeredChannels[chanName].unregister()
            del self.registeredChannels[chanName]


    def getChannel(self, chanName):
        """Return a channel object

        If the required channel is already registered, return it.
        Otherwise, return a new 'temporary' unregistered SpecChannel object ;
        reference should be kept in the caller or the object will get dereferenced.

        Arguments:
        chanName -- a string representing the channel name, i.e. 'var/toto'
        """
        if not chanName in self.registeredChannels:
            # return a newly created temporary SpecChannel object, without registering
            return SpecChannel.SpecChannel(self, chanName, SpecChannel.DONTREG)

        return self.registeredChannels[chanName]


    def error(self, error):
        """Emit the 'error' signal when the remote Spec version signals an error."""
        logging.getLogger('SpecClient').error('Error from Spec: %s', error)

        SpecEventsDispatcher.emit(self, 'error', (error, ))


    def simulationStatusChanged(self, simulationMode):
        self.simulationMode = simulationMode


    def isSpecConnected(self):
        """Return True if the remote Spec version is connected."""
        return self.state == CONNECTED


    def specConnected(self):
        """Emit the 'connected' signal when the remote Spec version is connected."""
        old_state = self.state
        self.state = CONNECTED
        if old_state != CONNECTED:
            logging.getLogger('SpecClient').info('Connected to %s:%s', self.host, (self.scanport and self.scanname) or self.port)

            self.connected_event.set()
            
            SpecEventsDispatcher.emit(self, 'connected', ())


    def specDisconnected(self):
        """Emit the 'disconnected' signal when the remote Spec version is disconnected."""
        old_state = self.state
        self.state = DISCONNECTED
        if old_state == CONNECTED:
            logging.getLogger('SpecClient').info('Disconnected from %s:%s', self.host, (self.scanport and self.scanname) or self.port)

            SpecEventsDispatcher.emit(self, 'disconnected', ())
 
            self.connected_event.clear()

    def handle_close(self):
        """Handle 'close' event on socket."""
        self.connected = False
        self.serverVersion = None
        if self.socket:
            self.socket.close()
        self.registeredChannels = {}
        self.specDisconnected()

    def disconnect(self):
        """Disconnect from the remote Spec version."""
        self.handle_close()

    def checkourversion(self, name):
        """Check remote Spec version

        If we are in port scanning mode, check if the name from
        Spec corresponds to our required Spec version.
        """
        if self.scanport:
            if name == self.scanname:
                return True
            else:
                #connected version does not match
                return False
        else:
            return True


    def send_msg_cmd_with_return(self, cmd):
        """Send a command message to the remote Spec server, and return the reply id.

        Arguments:
        cmd -- command string, i.e. '1+1'
        """
        if self.isSpecConnected():
            try:
                caller = sys._getframe(1).f_locals['self']
            except KeyError:
                caller = None

            return self.__send_msg_with_reply(replyReceiverObject = caller, *SpecMessage.msg_cmd_with_return(cmd, version = self.serverVersion))
        else:
            raise SpecClientNotConnectedError


    def send_msg_func_with_return(self, cmd):
        """Send a command message to the remote Spec server using the new 'func' feature, and return the reply id.

        Arguments:
        cmd -- command string
        """
        if self.serverVersion < 3:
            logging.getLogger('SpecClient').error('Cannot execute command in Spec : feature is available since Spec server v3 only')
        else:
            if self.isSpecConnected():
                try:
                    caller = sys._getframe(1).f_locals['self']
                except KeyError:
                    caller = None

                message = SpecMessage.msg_func_with_return(cmd, version = self.serverVersion)
                return self.__send_msg_with_reply(replyReceiverObject = caller, *message)
            else:
                raise SpecClientNotConnectedError


    def send_msg_cmd(self, cmd):
        """Send a command message to the remote Spec server.

        Arguments:
        cmd -- command string, i.e. 'mv psvo 1.2'
        """
        if self.isSpecConnected():
            self.__send_msg_no_reply(SpecMessage.msg_cmd(cmd, version = self.serverVersion))
        else:
            raise SpecClientNotConnectedError


    def send_msg_func(self, cmd):
        """Send a command message to the remote Spec server using the new 'func' feature

        Arguments:
        cmd -- command string
        """
        if self.serverVersion < 3:
            logging.getLogger('SpecClient').error('Cannot execute command in Spec : feature is available since Spec server v3 only')
        else:
            if self.isSpecConnected():
                self.__send_msg_no_reply(SpecMessage.msg_func(cmd, version = self.serverVersion))
            else:
                raise SpecClientNotConnectedError


    def send_msg_chan_read(self, chanName):
        """Send a channel read message, and return the reply id.

        Arguments:
        chanName -- a string representing the channel name, i.e. 'var/toto'
        """
        if self.isSpecConnected():
            try:
                caller = sys._getframe(1).f_locals['self']
            except KeyError:
                caller = None

            return self.__send_msg_with_reply(replyReceiverObject = caller, *SpecMessage.msg_chan_read(chanName, version = self.serverVersion))
        else:
            raise SpecClientNotConnectedError


    def send_msg_chan_send(self, chanName, value, wait=False):
        """Send a channel write message.

        Arguments:
        chanName -- a string representing the channel name, i.e. 'var/toto'
        value -- channel value
        """
        if self.isSpecConnected():
            self.__send_msg_no_reply(SpecMessage.msg_chan_send(chanName, value, version = self.serverVersion), wait)
        else:
            raise SpecClientNotConnectedError


    def send_msg_register(self, chanName):
        """Send a channel register message.

        Arguments:
        chanName -- a string representing the channel name, i.e. 'var/toto'
        """
        if self.isSpecConnected():
            self.__send_msg_no_reply(SpecMessage.msg_register(chanName, version = self.serverVersion))
        else:
            raise SpecClientNotConnectedError


    def send_msg_unregister(self, chanName):
        """Send a channel unregister message.

        Arguments:
        chanName -- a string representing the channel name, i.e. 'var/toto'
        """
        if self.isSpecConnected():
            self.__send_msg_no_reply(SpecMessage.msg_unregister(chanName, version = self.serverVersion))
        else:
            raise SpecClientNotConnectedError


    def send_msg_close(self):
        """Send a close message."""
        if self.isSpecConnected():
            self.__send_msg_no_reply(SpecMessage.msg_close(version = self.serverVersion))
        else:
            raise SpecClientNotConnectedError


    def send_msg_abort(self, wait=False):
        """Send an abort message."""
        if self.isSpecConnected():
            self.__send_msg_no_reply(SpecMessage.msg_abort(version = self.serverVersion), wait)
        else:
            raise SpecClientNotConnectedError


    def send_msg_hello(self):
        """Send a hello message."""
        self.__send_msg_no_reply(SpecMessage.msg_hello())


    def __send_msg_with_reply(self, reply, message, replyReceiverObject = None):
        """Send a message to the remote Spec, and return the reply id.

        The reply object is added to the registeredReplies dictionary,
        with its reply id as the key. The reply id permits then to
        register for the reply using the 'registerReply' method.

        Arguments:
        reply -- SpecReply object which will receive the reply
        message -- SpecMessage object defining the message to send
        """
        replyID = reply.id
        self.registeredReplies[replyID] = reply
   
        if hasattr(replyReceiverObject, 'replyArrived'):
            reply.callback = replyReceiverObject.replyArrived

        self.__send_msg_no_reply(message)

        return reply #print "REPLY ID", replyID


    def __do_send_data(self):
        buffer = "".join(self.outgoing_queue)
        if not buffer:
           self.socket_write_event.stop()
           self.socket_write_event = None
           self._completed_writing_event.set()
           return
        sent_bytes = self.socket.send(buffer)
        self.outgoing_queue = [buffer[sent_bytes:]]


    def __send_msg_no_reply(self, message, wait=False):
        """Send a message to the remote Spec.

        If a reply is sent depends only on the message, and not on the
        method to send the message. Using this method, any reply is
        lost.
        """
        self.outgoing_queue.append(message.sendingString()) 
        if self.socket_write_event is None:
           if wait:
              self._completed_writing_event.clear()
           self.socket_write_event = gevent.get_hub().loop.io(self.socket.fileno(), 2)
           self.socket_write_event.start(self.__do_send_data)
           if wait:
              self._completed_writing_event.wait()
          

