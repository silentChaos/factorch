import numpy as np 
import pandas as pd 
from datetime import datetime 
from pandas import DataFrame 
from typing import Optional, Union, Iterable, Sequence 
from .base import BaseClass, EPS 


class DataLoader(BaseClass):

    def __init__(self, begin_date=None, end_date=None, *, time_range=('0930', '1501'), stock_pool=None):
        self.begin_date = begin_date if begin_date else '20170101'
        self.end_date = end_date
        self.time_range = time_range

        self.stock_pool = None
        if stock_pool is None:
            stock_pool = self.load_daily('adjfactor').columns
        elif isinstance(stock_pool, str):
            stock_pool = np.load(self.stock_pool_path / f'{stock_pool}.npy', allow_pickle=True)
        elif isinstance(stock_pool, Sequence):
            pass
        else:
            raise ValueError(':param:`stock_pool` wrong type.')
        self.stock_pool = sorted(stock_pool)

    def load_daily(
        self,
        field: str,
        begin_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> DataFrame:
        begin_date = begin_date if begin_date else self.begin_date
        end_date = end_date if end_date else self.end_date
        return self.read_data(
            self.daily_path / field, cols=self.stock_pool
        ).loc[begin_date:end_date].drop(index=self.date_blacklist, errors='ignore')

    def load_minute(
        self,
        field: str,
        date: Union[str, datetime],
        time_range: Optional[Iterable[str]] = None, 
    ) -> DataFrame:
        time_range = time_range if time_range else self.time_range
        return self.read_data(
            self.minute_path / field.capitalize() / self.dt2str(date), cols=self.stock_pool
        ).between_time(*time_range, inclusive='left')

    def load_trade(
        self,
        field: str,
        date: Union[str, datetime],
        time_range: Optional[Iterable[str]] = None, 
    ) -> DataFrame:
        time_range = time_range if time_range else self.time_range
        return self.read_data(
            self.trade_path / field / self.dt2str(date), cols=self.stock_pool
        ).between_time(*time_range, inclusive='left')

    def load_tools(
            self,
            tool_lst: Iterable[str],
            begin_date: Optional[Union[str, datetime]] = None,
            end_date: Optional[Union[str, datetime]] = None,
    ) -> DataFrame:
        df = pd.concat([self.load_daily(
            field=tool,
            begin_date=begin_date,
            end_date=end_date,
        ).stack(dropna=False).rename(tool) for tool in tool_lst], axis=1)
        df.index.set_names(['time_id', 'stock_id'], inplace=True)
        return df

    def get_is_valid(
            self,
            valid_field: Optional[Iterable[str]],
            begin_date: Optional[Union[str, datetime]] = None,
            end_date: Optional[Union[str, datetime]] = None,
    ) -> DataFrame:
        if valid_field:
            return self.load_tools(tool_lst=valid_field, begin_date=begin_date, end_date=end_date) > EPS
        else:
            idx = self.load_daily(field='vwap', begin_date=begin_date, end_date=end_date).stack(dropna=False).index
            return DataFrame(True, index=idx, columns=['is_valid_all'])


