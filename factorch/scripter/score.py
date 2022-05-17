import numpy as np 
import pandas as pd
from pathlib import Path
from operator import and_
from functools import reduce
from .base import BaseClass


def load_daily(field, valid_field=None):
    df = BaseClass.read_data(
        BaseClass.daily_path / field
    ).loc['20180101':].drop(pd.Timestamp('20200203'), errors='ignore')
    if valid_field is not None:
        df = df[reduce(and_, [load_daily(field=vf).astype(bool) for vf in valid_field])]
    return df


def check_prediction(factor, factor_value, length, d, group_num=20):
    if factor_value is None:
        try:
            factor_value = pd.read_pickle(BaseClass.finfo_path / factor / 'factor_value.pkl')
        except FileNotFoundError:
            factor_value = pd.read_pickle(BaseClass.fval_path / factor / 'factor_value.pickle')

    fval = factor_value.dropna(how='all', axis=0).drop(pd.Timestamp('20200203'), errors='ignore').stack()
    fval_dates = fval.index.get_level_values(0)
    del factor_value

    # 第一次卖出日期与因子值日期相对应
    ret_v2v_1d = (
        load_daily(field='vwap', valid_field=('is_valid_raw',)) * load_daily(field='adjfactor')
    ).pct_change().shift(-2).sort_index(axis=0) * 100
    # ret_v2v_1d = ret_v2v_1d[ret_v2v_1d.index.isin(fval_dates)]
    is_buyable = load_daily('vwap') < load_daily('limit_up_price')

    # fval = fval.loc[:is_buyable.index[-length-1]].stack()

    excrets = {}
    w = np.array([d**i for i in range(length)])  # Exponential weights, e.g. np.array([[1], [0.9], [0.81], ...])
    for i in range(len(ret_v2v_1d)+1-length):
        x = ret_v2v_1d.iloc[i:i+length].copy()
        factor_date, buying_date = x.index[0], x.index[1]
        if factor_date in fval_dates:  # Do no useless computation
            x.loc[:, ~x.columns.isin(fval.loc[factor_date].index)] = np.nan  # 因子值为 nan 的股票
            x.loc[:, ~is_buyable.loc[buying_date]] = np.nan

            x = x.sub(x.mean(axis=1), axis=0).T  # Excess return
            x.columns = [f'r{i}' for i in range(1, length+1)]
            x['#r'] = (x * w).sum(axis=1) / (x.notna() * w).sum(axis=1)
            excrets[factor_date] = x

    excrets = pd.concat(excrets).dropna(how='all', axis=0)
    fval = fval.reindex(excrets.index).groupby(level=0).apply(
        lambda x: pd.qcut(x.rank(ascending=False, method='first'), q=group_num, labels=range(1, group_num+1))
    )  # Ranking and binning factor value
    excess_return = excrets.groupby([fval.index.get_level_values(0), fval]).mean()
    return excess_return
    