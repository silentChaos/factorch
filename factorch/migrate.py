import os 
import re 
import inspect
import importlib.util
import json
from pathlib import Path 
from scripter.base import BaseClass

here = Path(__file__).absolute().parent


template = """import sys
sys.path.insert(0, '{path_insert}')
from {base_class} import *


class {factor_name}({base_class}):
    '''
    {json_params}
    '''
    def definition(self, {dfunc_args}, {ifunc_args}):
        {definition_body}

"""


def main(args):
    global template 
    factor = args.factor
    factor_file = args.factor_file
    from_path = args.from_path
    to_path = args.to_path

    if factor:
        factor_list = [factor]
    if factor_file:
        with open(here / factor_file, 'r') as fp:
            factor_list = [f.strip() for f in fp.readlines()]

    if from_path is None:
        from_path = here / 'factor_script'
    if to_path is None:
        to_path = here / 'migrate_script'

    for factor_name in factor_list:
        spec = importlib.util.spec_from_file_location('Factor', from_path / f'{factor_name}.py')
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        begin_date = BaseClass.read_data(BaseClass.daily_path / 'adjfactor').index[-1].strftime(BaseClass.tm_fmt)
        json_params = mod.Factor(begin_date=begin_date).get_params()
        def_code = ''.join(inspect.getsourcelines(mod.Factor.definition)[0][1:])
        funcs = inspect.getmembers(mod, inspect.isfunction)

        d = {}
        for flag, func_type in [('ihelper', 'ifunc'), ('dfunc', 'dfunc')]:
            if flag in def_code:
                func = getattr(mod, func_type)
                d[f'{func_type}_args'] = ', '.join([arg for arg in inspect.getfullargspec(func).args])

        definition_body = ['return factor']

        dfunc_args, ifunc_args = '', ''
        if 'dfunc_args' in d:
            dfunc_args = d['dfunc_args']
            definition_body.append("factor = dfunc({dfunc_args})".format(dfunc_args=dfunc_args))
            dfunc_args = re.sub(pattern=r'\bfactor\b', repl='', string=dfunc_args)
        if 'ifunc_args' in d:
            ifunc_args = d['ifunc_args']
            definition_body.append(
                "factor = self.minute_help(ifunc, 'minute', {ifunc_args})".format(ifunc_args=ifunc_args)
            )

        definition_body = (' ' * 8).join([c+'\n' for c in definition_body[::-1]])

        json_params['def_args'] = f'{dfunc_args}, {ifunc_args}'.strip(', ').split(', ')

        code = template
        for _, func in funcs:
            func_code = inspect.getsource(func)
            code += func_code + '\n'

        if json_params['type'] == 'daily':
            base_class = 'AlphaFactor' 
            path_insert = Path('daily_factor_path') # NOTE
        else:
            base_class = 'HFactor'
            path_insert = Path('hf_factor_path') # NOTE

        code = code.format(
            base_class=base_class, 
            path_insert=path_insert, 
            factor_name=factor_name, 
            json_params=str(json_params), 
            dfunc_args=dfunc_args, 
            ifunc_args=ifunc_args, 
            definition_body=definition_body, 
        )
        for pat in [', )', ', ,', "'', '"]: # clean code
            code = re.sub(re.escape(pat), pat[-1], code)

        mod_imported = []
        for line in inspect.getsourcelines(mod)[0]:
            if 'import ' in line:
                for mod_ignored in ['sys', 'pathlib', 'scripter']:
                    if mod_ignored in line:
                        break
                else:
                    mod_imported.append(line)
        mod_imported = ''.join(mod_imported)
        code = mod_imported + code

        for subdir in ['compute_script', 'parameter_json']:
            os.makedirs(to_path / subdir, exist_ok=True)

        with open(to_path / 'compute_script' / f'{factor_name}.py', 'w') as fp:
            fp.write(code)

        with open(to_path / 'parameter_json' / f'{factor_name}.json', 'w') as fp:
            json.dump(json_params, fp)

    
if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description='Migrate factor(s).')
    parser.add_argument(
        '-f', dest='factor', type=str, default=None, 
        help='factor (default: None)', 
    )
    parser.add_argument(
        '--ff', dest='factor_file', type=str, default=None, 
        help='factor_file (default: None)', 
    )
    parser.add_argument(
        '--fp', dest='from_path', type=str, default=None, 
        help='from_path (default: None)', 
    )
    parser.add_argument(
        '--tp', dest='to_path', type=str, default=None, 
        help='to_path (default: None)', 
    )
    args = parser.parse_args()

    main(args)
