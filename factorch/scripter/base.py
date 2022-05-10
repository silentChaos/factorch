import os 
import pandas as pd
from pathlib import Path
from datetime import datetime
from pandas import DataFrame 
from typing import Union, Optional, Iterable
from configparser import ConfigParser

lib_path = Path(__file__).absolute().parents[2]

EPS = 1e-7


class BaseClass(object):

    tm_fmt = r'%Y%m%d'

    config = ConfigParser()
    config.read(lib_path / 'config.ini', encoding='utf-8')

    date_blacklist = [pd.Timestamp(d) for d in ['20191023', '20200203']]

    for pname in ['daily_path', 'minute_path', 'trade_path', 'order_path', 'stock_pool_path']:
        vars()[pname] = Path(config.get('Path', pname))

    factor_root_path = Path(config.get('Path', 'factor_root_path'))

    finfo_path = factor_root_path / 'factor_info'
    os.makedirs(finfo_path, exist_ok=True)

    data_path = factor_root_path / 'data'

    for subdir in ['price', 'price_table']:
        p = data_path / subdir
        os.makedirs(p, exist_ok=True)
        vars()[f'{subdir}_path'] = p

    file_type = config.get('FileType', 'file_type').lower()

    @classmethod
    def dt2str(cls, date: Union[str, datetime]) -> str:
        return pd.Timestamp(date).strftime(cls.tm_fmt)
    
    @classmethod
    def read_data(cls, p: Path, cols: Optional[Iterable[str]] = None) -> DataFrame:
        file = p.with_suffix(f'.{cls.file_type}')
        if cls.file_type == 'csv':
            df = pd.read_csv(file, index_col=0, parse_dates=True).reindex(columns=cols)
        elif cls.file_type == 'pkl' or cls.file_type == 'pickle':
            df = pd.read_pickle(file).reindex(columns=cols)
        elif cls.file_type == 'parquet':
            df = pd.read_parquet(file, columns=cols)
        return df


