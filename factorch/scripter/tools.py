import numpy as np 
import pandas as pd 
from pandas import DataFrame


def get_minute_turnover_rate(m_volume: DataFrame, float_a_shares: DataFrame) -> DataFrame:
    return m_volume / float_a_shares.reindex(m_volume.index.date).values


def get_vwap(amt: DataFrame, vol: DataFrame) -> DataFrame:
    vwap = amt / vol
    vwap[np.isinf(vwap)] = np.nan
    return vwap.fillna(method='ffill')


def standardize(df: DataFrame) -> DataFrame:
    return df.sub(df.mean(axis=1), axis=0).div(df.std(axis=1), axis=0)


def rolling_ewm(df: DataFrame, d: int) -> DataFrame:
    weight = np.array([(1 - (2. / (d+1))) ** (d-i) for i in range(1, d+1)])
    return df.rolling(window=d).apply(lambda x: np.sum(x * weight) / np.sum(weight))


def ewm(df: DataFrame, span: int) -> DataFrame:
    return pd.concat([df.iloc[idx-span:idx].ewm(span=span).mean().iloc[-1] 
                        for idx in range(span, len(df)+1)], axis=1).T.sort_index(0).sort_index(1)


