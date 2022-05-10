import numpy as np 
import pandas as pd 
import sys
sys.path.insert(0, 'daily_factor_path')
from AlphaFactor import *


class demo_2(AlphaFactor):
    '''
    {'def_args': ['close', 'adjfactor'], 'prelength': 4, 'min_prelen': 0, 'type': 'daily'}
    '''
    def definition(self, close, adjfactor):
        factor = dfunc(close, adjfactor)
        return factor


def dfunc(close, adjfactor):
    ret = (close * adjfactor).pct_change()
    factor = ret.rolling(5, 3).max()
    factor[np.isinf(factor)] = np.nan
    return factor

