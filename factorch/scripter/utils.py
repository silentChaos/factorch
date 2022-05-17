import logging
from pandas import Series, DataFrame
from typing import Union, Optional, Iterable, Generator


def chunks(lst: Iterable, n: int) -> Generator:
    '''Yield successive n-sized chunks from lst.'''
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def stream_logger_handler(
    fmt: str = '%(asctime)s - %(levelname)s - %(filename)s - lineno:%(lineno)d - %(message)s', 
    level: Optional[Union[int, str]] = None
) -> logging.Handler:
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(fmt))
    if level is not None:
        stream_handler.setLevel(level=level)
    return stream_handler


def get_intersect_idx(left: Union[Series, DataFrame], right: Union[Series, DataFrame]) -> Iterable[str]:
    return left.index.intersection(right.index).sort_values() 
