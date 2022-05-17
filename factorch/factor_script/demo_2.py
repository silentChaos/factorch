import numpy as np 
import pandas as pd 
import sys
from pathlib import Path
lib_path = Path(__file__).absolute().parents[1]
sys.path.append(str(lib_path))
from scripter.compute import AlphaFactor


class Factor(AlphaFactor):

    @staticmethod
    def set_params():
        return {
            'file': __file__, 
            'prelength': 5, 
            'ilength': 1, 
            'idaily_data': None,  
        }

    def definition(self):
        daily_data = self.load_daily(fields=['close', 'adjfactor'])
        return dfunc(**daily_data)
        

def dfunc(close, adjfactor):
    ret = (close * adjfactor).pct_change()
    factor = ret.rolling(5, 3).max()
    factor[np.isinf(factor)] = np.nan
    return factor
