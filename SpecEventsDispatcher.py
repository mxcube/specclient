import weakref
import exceptions
import Queue
import time
import saferef

(UPDATEVALUE, FIREEVENT) = (1, 2)

class SpecClientDispatcherError(exceptions.Exception):
    def __init__(self, args=None):
        self.args = args


def robustApply(slot, arguments = ()):
    """Call slot with appropriate number of arguments"""
    if hasattr(slot, '__call__'):
        # Slot is a class instance ?
        if hasattr( slot.__call__, 'im_func'): # or hasattr( slot.__call__, 'im_code'): WARNING:im_code does not seem to exist?
            # Reassign slot to the actual method that will be called
            slot = slot.__call__

    if hasattr(slot, 'im_func'):
        # an instance method
        n_default_args = slot.im_func.func_defaults and len(slot.im_func.func_defaults) or 0
        n_args = slot.im_func.func_code.co_argcount - n_default_args - 1
    else:
        try:
            n_default_args = slot.func_defaults and len(slot.func_defaults) or 0
            n_args = slot.func_code.co_argcount - n_default_args
        except:
            raise SpecClientDispatcherError, 'Unknown slot type %s %s' % (repr(slot), type(slot))

    if len(arguments) < n_args:
        raise SpecClientDispatcherError, 'Not enough arguments for calling slot %s (need: %d, given: %d)' % (repr(slot), n_args, len(arguments))
    else:
        return slot(*arguments[0:n_args])


class Receiver:
    def __init__(self, weakReceiver, dispatchMode):
        self.weakReceiver = weakReceiver
        self.dispatchMode = dispatchMode


    def __call__(self, arguments):
        slot = self.weakReceiver() #get the strong reference

        if slot is not None:
            return robustApply(slot, arguments)


class Event:
    def __init__(self, sender, signal, arguments):
        self.receivers = []
        senderId = id(sender)
        signal = str(signal)
        self.args = arguments

        try:
            self.receivers = connections[senderId][signal]
        except:
            pass


class EventsQueue(Queue.Queue):
    def __init__(self):
        Queue.Queue.__init__(self, 0)


    def get(self):
        """Remove and return an item from the queue."""
        try:
            return Queue.Queue.get(self, False)
        except Queue.Empty:
            raise IndexError


    def put(self, event):
        """Put an event into the queue."""
        receiversList = event.receivers

        self.mutex.acquire()
        try:
            was_empty = not self._qsize()

            for r in receiversList:
                if not was_empty:
                    if r.dispatchMode == UPDATEVALUE:
                        for i in range(len(self.queue)):
                            _r, args = self.queue[i]
                            if r == _r:
                                del self.queue[i]
                                break

                self._put( (r, event.args) )
        finally:
            self.mutex.release()


eventsToDispatch = EventsQueue()
connections = {} # { senderId0: { signal0: [receiver0, ..., receiverN], signal1: [...], ... }, senderId1: ... }
senders = {} # { senderId: sender, ... }


def callableObjectRef(object):
    """Return a safe weak reference to a callable object"""
    return saferef.safe_ref(object, _removeReceiver)

def connect(sender, signal, slot, dispatchMode = UPDATEVALUE):
    if sender is None or signal is None:
        return

    if not callable(slot):
        return

    senderId = id(sender)
    signal = str(signal)
    signals = {}

    if senderId in connections:
        signals = connections[senderId]
    else:
        connections[senderId] = signals

    def remove(object, senderId=senderId):
        _removeSender(senderId)

    try:
        weakSender = weakref.ref(sender, remove)
        senders[senderId] = weakSender
    except:
        pass

    receivers = []

    if signal in signals:
        receivers = signals[signal]
    else:
        signals[signal] = receivers

    weakReceiver = callableObjectRef(slot)

    for r in receivers:
        if r.weakReceiver == weakReceiver:
            r.dispatchMode = dispatchMode
            return

    receivers.append(Receiver(weakReceiver, dispatchMode))


def disconnect(sender, signal, slot):
    if sender is None or signal is None:
        return

    if not callable(slot):
        return

    senderId = id(sender)
    signal = str(signal)

    try:
        signals = connections[senderId]
    except KeyError:
        return
    else:
        try:
            receivers = signals[signal]
        except KeyError:
            return
        else:
            weakReceiver = callableObjectRef(slot)

            toDel = None
            for r in receivers:
                if r.weakReceiver == weakReceiver:
                    toDel = r
                    break
            if toDel is not None:
                receivers.remove(toDel)

                _cleanupConnections(senderId, signal)


def emit(sender, signal, arguments = ()):
    senderId = id(sender)
    signal = str(signal)

    try:
      receivers = connections[senderId][signal]
    except:
      return
    else:
      for receiver in receivers:
        receiver(arguments)  
 
def dispatch(max_time_in_s=1):
    return
    t0 = time.time()
    while 1:
        try:
            receiver, args = eventsToDispatch.get()
        except IndexError:
            break
        else:
            receiver(args)
            if max_time_in_s < 0:
              continue
            elif (time.time()-t0) >= max_time_in_s:
              break


def _removeSender(senderId):
    try:
        del connections[senderId]
        del senders[senderId]
    except KeyError:
         pass


def _removeReceiver(weakReceiver):
    """Remove receiver from connections"""
    for senderId in connections.keys():
        for signal in connections[senderId].keys():
            receivers = connections[senderId][signal]

            for r in receivers:
                if r.weakReceiver == weakReceiver:
                    receivers.remove(r)
                    break

            _cleanupConnections(senderId, signal)


def _cleanupConnections(senderId, signal):
    """Delete any empty signals for sender. Delete sender if empty."""
    receivers = connections[senderId][signal]

    if len(receivers) == 0:
        # no more receivers
        signals = connections[senderId]
        del signals[signal]

        if len(signals) == 0:
            # no more signals
            _removeSender(senderId)

















