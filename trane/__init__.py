from .libinfo import __version__

from .core import *
from .utils import *
from . import ops
import logging
logname = 'trane.log'
logging.basicConfig(filename=logname,
                    filemode='w',
                    level=logging.DEBUG)
