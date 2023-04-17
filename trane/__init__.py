from trane.core import *  # noqa
from trane.datasets import (
    load_covid,
    load_covid_tablemeta,
)
from trane.utils import *  # noqa
from trane.version import __version__

import logging

logname = "trane.log"
logging.basicConfig(filename=logname, filemode="w", level=logging.INFO)
