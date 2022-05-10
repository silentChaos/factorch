import numpy as np 
import pandas as pd 
import sys
sys.path.insert(0, 'daily_factor_path')
from AlphaFactor import *


class demo_2(AlphaFactor):
    '''
    {'def_args': ['mkt_cap_ard', 'm_volume', 'float_a_shares'], 'prelength': 0, 'min_prelen': 1, 'type': 'daily'}
    '''
    def definition(self, mkt_cap_ard, m_volume, float_a_shares):
        factor = self.minute_help(ifunc, 'minute', m_volume, float_a_shares)
        factor = dfunc(factor, mkt_cap_ard)
        return factor


def dfunc(factor, mkt_cap_ard):
    # 所有日间处理
    factor = factor * mkt_cap_ard
    factor[np.isinf(factor)] = np.nan
    return factor # 返回为 pd.DataFrame

def ifunc(m_volume, float_a_shares):
    # 所有日内处理
    tr = m_volume / float_a_shares.reindex(m_volume.index.date).values
    return tr.mean() # 返回为 pd.Series

