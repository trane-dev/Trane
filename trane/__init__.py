from .core import *  # noqa
from .utils import *  # noqa
from version import *

import logging
logname = 'trane.log'
logging.basicConfig(filename=logname,
                    filemode='w',
                    level=logging.INFO)
