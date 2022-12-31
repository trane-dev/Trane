from trane.core import *  # noqa
from trane.utils import *  # noqa
from trane.version import __version__

import logging

logname = "trane.log"
logging.basicConfig(filename=logname, filemode="w", level=logging.INFO)
