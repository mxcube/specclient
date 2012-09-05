__all__ = ['SpecMotor', 'SpecCounter', 'SpecCommand', 'Spec', 'SpecVariable']

import gevent.monkey; gevent.monkey.patch_all(thread=False, subprocess=False)

from . import SpecVariable
from . import SpecCommand
from . import SpecMotor
from . import SpecCounter
from . import Spec

#
# create the SpecClient logger
#
import logging

_logger = logging.getLogger('SpecClient')
_logger.setLevel(logging.DEBUG)
_oldLevel = logging.DEBUG
_formatter = logging.Formatter('* [%(name)s] %(levelname)s %(asctime)s %(message)s')

if len(logging.root.handlers) == 0:
    #
    # log to stdout
    #
    import sys

    _hdlr = logging.StreamHandler(sys.stdout)
    _hdlr.setFormatter(_formatter)
    _logger.addHandler(_hdlr)


def removeLoggingHandlers():
    for handler in _logger.handlers:
        _logger.removeHandler(handler)


def setLoggingOff():
    global _oldLevel
    _oldLevel = _logger.getEffectiveLevel()
    _logger.setLevel(1000) #disable all logging events less severe than 1000 (CRITICAL is 50...)


def setLoggingOn():
    _logger.setLevel(_oldLevel)


def addLoggingHandler(handler):
    _logger.addHandler(handler)


def setLoggingHandler(handler):
    global _hdlr

    removeLoggingHandlers() #_logger.removeHandler(_hdlr)

    _hdlr = handler
    addLoggingHandler(_hdlr)


def setLogFile(filename):
    #
    # log to rotating files
    #
    from logging.handlers import RotatingFileHandler

    hdlr = RotatingFileHandler(filename, 'a', 1048576, 5) #1 MB by file, 5 files max.
    hdlr.setFormatter(_formatter)

    setLoggingHandler(hdlr)
