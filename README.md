# factorch

## 计算并检验因子值 
```bash
$ python check_factor.py -b 20210101 -e 20210110 -f demo_1 --op all
$ python check_factor.py -b 20210101 -e 20210110 --ff factor_list.txt --op single
```
更多参数参见`.py`文件

## 将本框架下的因子移植至原框架 
```bash
$ python migrate.py -f demo_1
$ python migrate.py --ff factor_list.txt
```
更多参数参见`.py`文件

## 注意事项
- 所有`i`开头的 attributes / methods，均为日内所需，与原框架`minute`的含义类似
