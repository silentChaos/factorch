import numpy as np 
import importlib 
import os 
import logging
import time
import multiprocessing as mp 
from pathlib import Path 
from termcolor import colored 
from scripter.base import BaseClass
from scripter.score import check_prediction
from scripter.utils import stream_logger_handler

here = Path(__file__).absolute().parent


# factors = sorted([f.stem+'\n' for f in (here / 'factor_script').glob('*.py')])
# with open(here / 'factor_list.txt', 'w') as fp:
#     fp.writelines(factors)


OP_DICT = { # 添加对日频因子值的操作
    'raw': '{factor}', 
    'ma2': '{factor}.rolling(2, 1).mean()', 
    'ma5': '{factor}.rolling(5, 3).mean()', 
}


def main(args):
    begin_date = args.begin_date
    factor = args.factor
    factor_file = args.factor_file
    op_flag = args.op_flag

    assert (factor or factor_file) and not (factor and factor_file), 'One and only one of :args:`factor` '
    'and :args:`factor_file` has to be provided.'
    assert op_flag in ['single', 'all']

    logger = logging.getLogger(__name__)
    logger.handlers.clear()
    logger.addHandler(stream_logger_handler(fmt='%(asctime)s - %(message)s'))
    logger.setLevel(logging.DEBUG)
    
    if factor:
        factor_list = [factor]
    if factor_file:
        with open(here / factor_file, 'r') as fp:
            factor_list = [f.strip() for f in fp.readlines()]

    for factor in factor_list:
        to_path = BaseClass.finfo_path / factor
        os.makedirs(to_path, exist_ok=True)

        ok_path = to_path / '_ok'
        fail_path = to_path / '_fail'
        if ok_path.exists():
            continue # NOTE
        
        try:
            mod = importlib.import_module(f'factor_script.{factor}')
            try:
                obj = getattr(mod, 'Factor')
            except AttributeError:
                obj = getattr(mod, factor)

            factor_value = obj(
                begin_date=begin_date, 
                end_date=args.end_date, 
                trans_time=args.trans_time, 
                stock_pool=args.stock_pool, 
                logger=logger, 
                n_jobs=args.n_jobs
            ).calculate()

        except Exception as e:
            if e is KeyboardInterrupt:
                raise e
            else:
                logger.warning(f'{factor} factor value computing failed. Error message: {e}')
                fail_path.touch()
                continue

        factor_value[np.isinf(factor_value)] = np.nan
        factor_value.loc[begin_date:].to_pickle(to_path / f'factor_value.pkl') 

        logger.info('Checking predictions.')
        t0 = time.perf_counter()

        if op_flag == 'single':
            _ = _mp_wrapper(factor, factor_value, begin_date, op_name='raw', op_func=OP_DICT['raw'], to_path=to_path)
        elif op_flag == 'all':
            with mp.Pool(processes=args.n_jobs) as pool:
                _ = [job.get() for job in [pool.apply_async(func=_mp_wrapper, 
                                        args=(factor, factor_value, begin_date, op_name, op_func, to_path))
                        for op_name, op_func in OP_DICT.items()]]

        logger.info(f'Check completed. Time elapsed: {time.perf_counter()-t0:.2f}s')
        ok_path.touch()
        fail_path.unlink(missing_ok=True)


def _mp_wrapper(factor, factor_value, begin_date, op_name, op_func, to_path):
    fname = f'{factor}_{op_name}'
    fval = eval(op_func.format(factor='factor_value')).loc[begin_date:].replace([-np.inf, np.inf], np.nan)

    excess_return = check_prediction(
        factor=fname, 
        factor_value=fval, 
        length=4, 
        d=.9, 
        group_num=20, 
    )

    excret_mean = excess_return.mean()
    top_group = excret_mean.idxmax()
    top_group_excret_mean = excret_mean[top_group]
    top_group_sharpe = top_group_excret_mean / excess_return.std()[top_group]

    color = None
    if top_group_excret_mean > .05 or top_group_sharpe > .25:
        fval.to_pickle(to_path / f'factor_value_{op_name}.pkl') 
        excess_return.to_pickle(to_path / f'excess_return_{op_name}.pkl') 
        color = 'yellow'
    
    msg = f'{fname: <25}TopGroup: {top_group}\t\tExcRet: {top_group_excret_mean:.4f}\t\tSharpe: {top_group_sharpe:.4f}'
    print(colored(msg, color))


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description='Check factor(s).')
    parser.add_argument(
        '-b', dest='begin_date', type=str, default='20210101', 
        help='begin_date (default: 20210101)', 
    )
    parser.add_argument(
        '-e', dest='end_date', type=str, default='20210110', 
        help='end_date (default: 20210110)', 
    )
    parser.add_argument(
        '-t', dest='trans_time', type=str, default='1501', 
        help='trans_time (default: 1501)', 
    )
    parser.add_argument(
        '-f', dest='factor', type=str, default=None, 
        help='factor (default: None)', 
    )
    parser.add_argument(
        '--ff', dest='factor_file', type=str, default=None, 
        help='factor_file (default: None)', 
    )
    parser.add_argument(
        '--sp', dest='stock_pool', type=str, default=None, 
        help='stock_pool (default: None)'
    )
    parser.add_argument(
        '--op', dest='op_flag', type=str, default='all', 
        help='op_flag (default: all)'
    )
    parser.add_argument(
        '-p', dest='n_jobs', type=int, default=10, 
        help='n_jobs (default: 10)', 
    )
    args = parser.parse_args()

    main(args)


