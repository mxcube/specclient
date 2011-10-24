#$Id: SpecConnectionsManager.py,v 1.13 2005/09/27 13:54:58 guijarro Exp $
"""Module for managing connections to Spec

The SpecConnectionsManager module provides facilities to get
a connection to Spec. It can run a thread for 'asynchronous'
polling of socket events. It prevents from having more than
one connection to the same Spec server at the same time, and
automatically reconnects lost connections.


Classes :
  _ThreadedSpecConnectionsManager
  _SpecConnectionsManager
"""

__author__ = 'Matias Guijarro'
__version__ = '1.1'

import gevent
import time
import weakref
import sys
import gc

import SpecConnection
import SpecEventsDispatcher

_SpecConnectionsManagerInstance = None

def SpecConnectionsManager():
    """Return the Singleton Spec connections manager instance"""
    global _SpecConnectionsManagerInstance

    if _SpecConnectionsManagerInstance is None:
            _SpecConnectionsManagerInstance = _SpecConnectionsManager()

    return _SpecConnectionsManagerInstance


class _SpecConnectionsManager:
    """Class for managing connections to Spec
    """
    def __init__(self):
        """Constructor"""
        self.connections = weakref.WeakValueDictionary()

    def getConnection(self, specVersion):
        """Return a SpecConnection object

        Arguments:
        specVersion -- a string in the 'host:port' form
        """
        con = self.connections.get(specVersion)
        if con is None:
            con = SpecConnection.SpecConnection(specVersion)
            gevent.spawn(SpecConnection.makeConnection, weakref.ref(con))

            self.connections[specVersion] = con
        
        return con


    def closeConnection(self, specVersion):
        try:
            del self.connections[specVersion]
        except:
            pass


    def closeConnections(self):
        for connectionName in self.connections.keys():
            self.closeConnection(connectionName)


















