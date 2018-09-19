import numpy as np
import pandas as pd


class CutoffStrategy:
    """
    Class that holds a CutoffStrategy. This is a measure to prevent leakage

    Parameters
    ----------
    generate_fn: a function that generates a cutoff time for a given entity.
        input: entity rows
        output: a training cutoff in np.datetime64 format

    Returns
    -------
    CutoffStrategy Instance
    """

    def __init__(self, generate_fn, description='undescribed cutoff strategy'):
        self.generate_fn = generate_fn
        self.description = description        
