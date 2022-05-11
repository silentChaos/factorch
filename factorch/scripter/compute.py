import numpy as np
import pandas as pd
import multiprocessing as mp 
import os
import time
import logging
from datetime import datetime
from pandas import DataFrame
from typing import Optional, Union, Iterable, Mapping, Callable
from pathlib import Path
from abc import abstractmethod
from .base import BaseClass
from .data_help import DataLoader
from .utils import stream_logger_handler, chunks


class AlphaFactor(BaseClass):

    def __init__(
        self, 
        begin_date: Optional[Union[str, datetime]] = '20190101', 
        end_date: Optional[Union[str, datetime]] = None, 
        trans_time: str = '1501', 
        stock_pool: Optional[Iterable[str]] = None, 
        logger = None,
        n_jobs: int = 10, 
    ):
        assert not end_date or begin_date <= end_date, ':params:`begin_date` should be'\
                                                    ' less than or equal to :params:`end_date`.'

        if logger is None:
            self.logger = logging.getLogger(__name__)
            self.logger.handlers.clear()
            self.logger.addHandler(stream_logger_handler())
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger = logger

        self.begin_date = begin_date
        self.end_date = end_date
        self.trans_time = trans_time
        self.n_jobs = n_jobs

        self.ds = 1 if self.trans_time < '1457' else 0 # day-shifted of daily data

        self.params = self.set_params()
        self.prelength = self.params.get('prelength', 1)
        self.ilength = self.params.get('ilength', 1) # intra-day length (a.k.a. min_prelen before), default to be 1
        assert self.prelength > 0 and self.ilength > 0, ':params:`self.prelength` and :params:`self.ilength` '
        'should be greater than 0.'
        self.name = Path(self.params.pop('file')).stem
        
        self.minute_status = (
            self.read_data(self.minute_path / 'Status' / 'Minute_Status') == 0
        ).loc['20170601':].rolling(self.ilength).mean() > .5
        
        self.real_begin = self.minute_status.loc[:begin_date].iloc[-self.prelength:].index[0]
        self.minute_status = self.minute_status.loc[self.real_begin:]
        self.date_list = self.minute_status.index.sort_values()

        self.dataloader = DataLoader( 
            begin_date=self.real_begin, 
            end_date=end_date, 
            time_range=('0930', '1500'), 
            stock_pool=stock_pool, 
        )
        
        self.data_loaded = mp.Manager().list()
        self.idaily_data = self.load_daily(fields=self.params.get('idaily_data', None))

    @abstractmethod
    def definition(self):
        pass

    @abstractmethod
    def set_params():
        pass # set [`prelength`, `ilength`, `idaily_data`]

    def get_params(self) -> Mapping:
        return {
            'def_args': list(self.data_loaded), 
            'prelength': self.prelength - 1, 
            'min_prelen': self.ilength - 1, 
            'type': 'hf' if self.ds else 'daily', 
        }

    def _add_fields(self, fields: Iterable[str]) -> None:
        for field in fields:
            if field not in self.data_loaded:
                self.data_loaded.append(field)

    def load_trade(self):
        raise NotImplementedError # to add

    def load_order(self):
        raise NotImplementedError # to add

    def load_minute(self, date: Union[str, datetime], fields: Iterable[str]) -> Mapping:
        self._add_fields(fields=[f'Minute{field[2:].capitalize()}' for field in fields])
        date_list = self.date_list[self.date_list <= date][-self.ilength:]
        end_dt = f'{self.dt2str(date_list[-1])}{self.trans_time}'
        return {
            field: pd.concat([self.dataloader.load_minute(field=field[2:], date=d).loc[:end_dt]
                    for d in date_list]) for field in fields
        }

    def load_daily(self, fields: Iterable[str]) -> Mapping:
        if fields is None:
            return 
        self._add_fields(fields=fields)
        return mp.Manager().dict({field: self.dataloader.load_daily(field=field) for field in fields})

    def read_idaily_data(self, date: Union[str, datetime]) -> Mapping:
        if self.ds:
            ds_map = {'adjfactor': None, 'open': None}
            return {k: v.loc[:date].iloc[:ds_map.get(k, -1)].iloc[-self.ilength:].copy() 
                        for k, v in self.idaily_data.items()}
        else:
            return {k: v.loc[:date].iloc[-self.ilength:].copy() for k, v in self.idaily_data.items()}

    def _mp_wrapper(self, func, date_list) -> DataFrame:
        return [func(date).rename(date) for date in date_list]

    def ihelper(self, func: Callable[[DataFrame], DataFrame]) -> DataFrame:
        with mp.Pool(processes=self.n_jobs) as pool:
            r = [job.get() for job in [pool.apply_async(func=self._mp_wrapper, args=(func, date_list)) 
                for date_list in chunks(self.date_list, 50)]]
        r = pd.concat([s for l in r for s in l], axis=1).T.sort_index(axis=0)
        return r[self.minute_status.reindex(r.index)]

    def calculate(self) -> DataFrame:
        self.logger.info(f'*** Computing {self.name} - {self.begin_date} to {self.end_date} ***')
        t0 = time.perf_counter()
        factor_value = self.definition().sort_index(axis=0).sort_index(axis=1).loc[:self.end_date]
        if self.ds and pd.Timestamp('20200204') in factor_value.index:
            factor_value.loc['20200204'] = np.nan
        self.logger.info(f'Factor Computing Time: {time.perf_counter()-t0:.2f}s.')
        to_path = self.finfo_path / self.name
        os.makedirs(to_path, exist_ok=True)
        factor_value.to_pickle(to_path / 'factor_value.pkl')
        return factor_value


