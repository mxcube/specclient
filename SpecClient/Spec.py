"""Spec module

This module define the Spec class for emulating a kind of Spec interpreter in
a Python object
"""

__author__ = 'Matias Guijarro'
__version__ = '1.1'

import SpecConnectionsManager
import SpecEventsDispatcher
import SpecCommand
import SpecWaitObject

class Spec:
    """Spec objects provide remote Spec facilities to the connected client."""

    @property
    def specVersion(self):
        return self.__specVersion

    def __init__(self, specVersion = None, timeout = None):
        """Constructor

        Keyword arguments:
        connection -- either a 'host:port' string pointing to a Spec version (defaults to None)
        timeout -- optional connection timeout (defaults to None)
        """
        self.connection = None

        if specVersion is not None:
            self.connectToSpec(specVersion, timeout = timeout)
        else:
            self.__specVersion = None


    def connectToSpec(self, specVersion, timeout = None):
        """Connect to a remote Spec

        Mainly used for two-steps object creation.
        To be extended by derivated classes.

        Arguments:
        specVersion -- 'host:port' string representing the Spec version to connect to
        timeout -- optional connection timeout (defaults to None)
        """
        self.__specVersion = specVersion

        self.connection = SpecConnectionsManager.SpecConnectionsManager().getConnection(specVersion)

        w = SpecWaitObject.SpecWaitObject(self.connection)
        w.waitConnection(timeout)


    def __getattr__(self, attr):
        if attr.startswith('__'):
            raise AttributeError

        return SpecCommand.SpecCommand(attr, self.connection)

    def _getMotorsMneNames(self):
        """Return motors mnemonics and names list."""
        if self.connection is not None and self.connection.isSpecConnected():
            get_motor_mnemonics = SpecCommand.SpecCommand('local md[]; for (i=0; i<MOTORS; i++) { md[i][motor_mne(i)]=motor_name(i) }; return md', self.connection)

            motorMne = get_motor_mnemonics()
            motorList = [None]*len(motorMne)
            for motor_index, motor_dict in motorMne.iteritems():
                mne, name = motor_dict.items()[0]
                motorList[int(motor_index)]={"mne": mne, "name": name }
            return motorList
        else:
            return []

    def getMotorsMne(self):
       """Return motor mnemonics list."""
       motorMneList = []
       for motor_dict in self._getMotorsMneNames():
           motorMneList.append(motor_dict["mne"])
       return motorMneList

    def getMotorsNames(self):
       """Return motors names list."""
       motorNamesList = []
       for motor_dict in self._getMotorsMneNames():
           motorNamesList.append(motor_dict["name"])
       return motorNamesList

    def _getCountersMneNames(self):
        """Return counters mnemonics and names list."""
        if self.connection is not None and self.connection.isSpecConnected():
            get_counter_mnemonics = SpecCommand.SpecCommand('local ca[]; for (i=0; i<COUNTERS; i++) { ca[i][cnt_mne(i)]=cnt_name(i) }; return ca', self.connection)

            counterMne = get_counter_mnemonics()
            counterList = [None]*len(counterMne)
            for counter_index, counter_dict in counterMne.iteritems():
                mne, name = counter_dict.items()[0]
                counterList[int(counter_index)]={"mne": mne, "name": name }
            return counterList
        else:
            return []

    def getCountersMne(self):
       """Return counter mnemonics list."""
       counterMneList = []
       for counter_dict in self._getCountersMneNames():
           counterMneList.append(counter_dict["mne"])
       return counterMneList

    def getCountersNames(self):
       """Return counters names list."""
       counterNamesList = []
       for counter_dict in self._getCountersMneNames():
           counterNamesList.append(counter_dict["name"])
       return counterNamesList

    def getVersion(self):
        if self.connection is not None:
            versionChannel = self.connection.getChannel('var/VERSION')

            return versionChannel.read()


    def getName(self):
        if self.connection is not None:
            nameChannel = self.connection.getChannel('var/SPEC')

            return nameChannel.read()
