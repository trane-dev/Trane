import logging

from .core import *  # noqa
from .utils import *  # noqa

logname = 'trane.log'
logging.basicConfig(
    filename=logname,
    filemode='w',
    level=logging.INFO,
)
