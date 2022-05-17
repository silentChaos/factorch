import numpy as np 
import pandas as pd 
import sys
from pathlib import Path
lib_path = Path(__file__).absolute().parents[1]
sys.path.append(str(lib_path))
from scripter.compute import AlphaFactor


class Factor(AlphaFactor): # 必须命名为 Factor

    @staticmethod
    def set_params():
        return {
            'file': __file__, # 保持不动
            'prelength': 1, # = 原 prelength + 1
            'ilength': 1, # = 原 min_prelen + 1
            'idaily_data': ['float_a_shares'], # 日内计算时所需要用的日频数据（以免反复加载）
        }

    def definition(self):
        daily_data = self.load_daily(fields=['mkt_cap_ard']) # 日间计算所需日频数据
        factor = self.ihelper(func=self.minute)
        # dfunc如需添加已计算出的日内数据，必须命名为 factor，以便移植
        return dfunc(factor, **daily_data) 

    def minute(self, date):
        minute_data = self.load_minute(date, fields=['m_volume']) # 分钟数据以`m_`开头
        trade_data = self.load_trade(date, fields=['B_Volume'])
        idaily_data = self.read_idaily_data(date)
        return ifunc(**minute_data, **idaily_data, **trade_data)


def dfunc(factor, mkt_cap_ard):
    # 所有日间处理
    factor = factor * mkt_cap_ard
    factor[np.isinf(factor)] = np.nan
    return factor # 返回为 pd.DataFrame

def ifunc(m_volume, float_a_shares, B_Volume):
    # 所有日内处理
    s_tr = (m_volume.sum() - B_Volume.sum()) / float_a_shares.squeeze()
    return s_tr # 返回为 pd.Series
