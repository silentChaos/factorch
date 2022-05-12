import numpy as np 
import pandas as pd
from pathlib import Path
from operator import and_
from functools import reduce
from .base import BaseClass


def check_prediction(factor, factor_value, length, d, group_num=20):
    if factor_value is None:
        try:
            factor_value = pd.read_pickle(BaseClass.finfo_path / factor / 'factor_value.pkl')
        except FileNotFoundError:
            factor_value = pd.read_pickle(BaseClass.fval_path / factor / 'factor_value.pickle')

    fval = factor_value.dropna(how='all', axis=0).rank(ascending=False, method='first', axis=1).apply(
        lambda x: pd.qcut(x, q=group_num, labels=range(1, group_num+1)), 
        axis=1).drop(pd.Timestamp('20200203'), errors='ignore')
    del factor_value

    def load_daily(field, valid_field=None):
        df = BaseClass.read_data(
            BaseClass.daily_path / field
        ).loc['20180101':].drop(pd.Timestamp('20200203'), errors='ignore')
        if valid_field is not None:
            df = df[reduce(and_, [load_daily(field=vf).astype(bool) for vf in valid_field])]
        return df

    # 第一次卖出日期与因子值日期相对应
    ret_v2v_1d = (load_daily(field='vwap', valid_field=('is_valid_raw',)) 
                * load_daily(field='adjfactor')).pct_change().shift(-2).sort_index(axis=0).reindex(columns=fval.columns)

    is_buyable = load_daily('vwap') < load_daily('limit_up_price')

    nan_stocks = fval.isna().stack()
    nan_stocks = nan_stocks[nan_stocks]

    excess_return = {}
    for i in range(len(ret_v2v_1d)+1-length):
        x = ret_v2v_1d.iloc[i:i+length].copy()
        factor_date, buying_date = x.index[0], x.index[1]
        try:
            # 因子值为 nan 的股票
            daily_nan_stocks = nan_stocks.loc[factor_date].index
            x.loc[:, daily_nan_stocks] = np.nan
        except KeyError:
            pass

        x.loc[:, ~is_buyable.loc[buying_date]] = np.nan
        x = x.sub(x.mean(axis=1), axis=0)  # excess return

        # Exponential weights, e.g. np.array([[1], [0.9], [0.81], ...])
        w = np.tile(np.array([d**i for i in range(len(x))])[..., np.newaxis], x.shape[1])
        excess_return[factor_date] = (x * w).sum() / (x.notna() * w).sum().replace(0, np.nan)  # Ignore nans

    excess_return = pd.DataFrame(excess_return).T.reindex(fval.index) * 100
    return pd.DataFrame({i: excess_return[fval == i].mean(axis=1) for i in range(1, group_num+1)})


