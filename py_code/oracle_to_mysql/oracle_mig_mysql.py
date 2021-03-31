# -*- coding: utf-8 -*-
# oracle_mig_mysql.py
# Oracle database migration to MySQL
# V1.6.0 2021-03-31
import argparse
import textwrap
import cx_Oracle
import os
import time
import csv
import datetime
import sys
import traceback
import decimal
import re
import logging
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import concurrent  # 异步任务包
import configDB  # 引用配置文件以及产生连接池


# from multiprocessing.dummy import Pool as ThreadPool
# import threading

# from threading import Thread # 多线程
# from multiprocessing import Process  # 下面用了动态变量执行多进程，所以这里是灰色


# 记录执行日志
class Logger(object):
    def __init__(self, filename='/tmp/mig.log', stream=sys.stdout):
        self.terminal = stream
        self.log = open(filename, 'w')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass


parser = argparse.ArgumentParser(prog='oracle_mig_mysql',
                                 formatter_class=argparse.RawDescriptionHelpFormatter,
                                 description=textwrap.dedent('''\
VERSION:
V1.6.0

EXAMPLE:
    EG(1):RUN MIGRATION FETCH AND INSERT 10000 ROWS DATA INTO TABLE:\n ./oracle_to_mysql -b 10000\n
    EG(2):RUN IMPORT CUSTOM TABLE MIGRATION TO MySQL INCLUDE METADATA AND DATA ROWS:\n ./oracle_to_mysql -c true\n
    EG(3):NEED TO CREATE TARGET TABLE BEFORE RUN,THIS ONLY MIG TABLE DATA ROWS,INDEX,TRIGGER:\n ./oracle_to_mysql -c true -d true
    
    '''))
parser.add_argument('--batch_size', '-b', help='FETCH AND INSERT ROW SIZE,DEFAULT 10000', type=int)
parser.add_argument('--custom_table', '-c', help='MIG CUSTOM TABLES INTO MySQL,DEFAULT FALSE',
                    choices=['true', 'false'], default='false')  # 默认是全表迁移
parser.add_argument('--data_only', '-d', help='MIG ONLY DATA ROW DO NOT CREATE TABLE', choices=['true', 'false'],
                    default='false')
parser.add_argument('--parallel_degree', '-p', help='parallel degree default 16', type=int)
args = parser.parse_args()

# -c命令与-d命令不能同时使用的判断
if args.custom_table.upper() == 'TRUE' and args.data_only.upper() == 'TRUE':
    print('ERROR: -c AND -d OPTION CAN NOT BE USED TOGETHER!\nEXIT')
    sys.exit(0)

# 判断命令行参数-b是否指定
if args.batch_size:
    row_batch_size = args.batch_size
else:
    row_batch_size = 20000

# 判断命令行参数-c是否指定
if args.custom_table.upper() == 'TRUE':
    custom_table = 'true'
    with open('custom_table.txt', 'r', encoding='utf-8') as fr, open('/tmp/table.txt', 'w', encoding='utf-8') as fd:
        row_count = len(fr.readlines())
    if row_count < 1:
        print('!!!请检查当前目录custom_table.txt是否有表名!!!\n\n\n')
        time.sleep(2)
    #  在当前目录下编辑custom_table.txt，然后对该文件做去掉空行处理，输出到tmp目录
    with open('custom_table.txt', 'r', encoding='utf-8') as fr, open('/tmp/table.txt', 'w', encoding='utf-8') as fd:
        for text in fr.readlines():
            if text.split():
                fd.write(text)
else:
    custom_table = 'false'

# 并行度,可指定并行度，默认为16
list_parallel = []
if args.parallel_degree:
    degree = args.parallel_degree
else:
    degree = 16
for i in range(degree):
    list_parallel.append(i)
v_max_workers = len(list_parallel)

# oracle、mysql连接池以及游标对象
oracle_cursor = configDB.OraclePool()  # Oracle连接池
mysql_cursor = configDB.MySQLPOOL.connection().cursor()  # MySQL连接池
mysql_cursor.arraysize = row_batch_size

# 非连接池连接方式以及游标
ora_conn = configDB.ora_conn  # 读取config.ini文件中ora_conn变量的值
source_db = cx_Oracle.connect(ora_conn)  # 源库Oracle的数据库连接
# source_db = cx_Oracle.connect("datatest", "oracle", "orcl")  # tns连接方式（username,password,tns_name）
cur_oracle_result = source_db.cursor()  # 查询Oracle源表的游标结果集
cur_oracle_result.prefetchrows = row_batch_size
cur_oracle_result.arraysize = row_batch_size  # Oracle数据库游标对象结果集返回的行数即每次获取多少行
fetch_many_count = row_batch_size

# 计数变量
all_table_count = 0  # oracle要迁移的表总数
list_table_name = []  # 查完user_tables即当前用户所有表存入list
list_success_table = []  # 创建成功的表存入到list
new_list = []  # 用于存储1分为2的表，将原表分成2个list
ddl_failed_table_result = []  # 用于记录ddl创建失败的表名称
all_view_count = 0  # oracle要创建的视图总数
all_view_success_count = 0  # MySQL中创建成功视图的总数
all_view_failed_count = 0  # MySQL中创建失败视图的总数
view_failed_result = []  # 用于记录ddl创建失败的视图名称
oracle_autocol_total = []  # 用于统计Oracle中自增列的总数
all_inc_col_success_count = 0  # mysql中自增列成功的总数
all_inc_col_failed_count = 0  # mysql中自增列失败的总数
normal_trigger_count = 0  # 用于统计Oracle中触发器（排除序列相关触发器）的总数
trigger_success_count = 0  # mysql中触发器创建成功的总数
trigger_failed_count = 0  # mysql中触发器创建失败的总数
all_constraints_count = 0  # 约束以及索引总数（排除掉非normal index）
all_constraints_success_count = 0  # mysql中创建约束以及索引成功的总数
constraint_failed_count = []  # 用于统计主键以及索引创建失败的总数
function_based_index_count = 0  # 非normal索引总数
all_fk_count = 0  # 外键总数
all_fk_success_count = 0  # mysql中外键创建成功的总数
foreignkey_failed_count = []  # 用于统计外键创建失败的总数
comment_failed_count = []  # 用于统计注释添加失败的总数

# 环境有关变量
sys.stdout = Logger(stream=sys.stdout)
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  # 设置字符集为UTF8，防止中文乱码


# clob、blob、nclob要在读取源表前加载outputtypehandler属性,即将Oracle大字段转为string类型
# 处理Oracle的number类型浮点数据与Python decimal类型的转换
# Python遇到超过3位小数的浮点类型，小数部分只能保留3位，其余会被截断，会造成数据不准确，需要用此handler做转换，可指定数据库连接或者游标对象
def dataconvert(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_NCLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_NUMBER:  # NumberToDecimal
        return cursor.var(decimal.Decimal, arraysize=cursor.arraysize)


cur_oracle_result.outputtypehandler = dataconvert  # 查询Oracle表数据结果集的游标


# 对list平均分，可以一分为二
def bisector_list(tabulation: list, num: int):
    """
    将列表平均分成几份
    :param tabulation: 列表
    :param num: 份数
    :return: 返回一个新的列表
    """
    new_list = []

    '''列表长度大于等于份数'''
    if len(tabulation) >= num:
        '''remainder:列表长度除以份数，取余'''
        remainder = len(tabulation) % num
        if remainder == 0:
            '''merchant:列表长度除以分数'''
            merchant = int(len(tabulation) / num)
            '''将列表平均拆分'''
            for i in range(1, num + 1):
                if i == 1:
                    new_list.append(tabulation[:merchant])
                else:
                    new_list.append(tabulation[(i - 1) * merchant:i * merchant])
            return new_list
        else:
            '''merchant：列表长度除以分数 取商'''
            merchant = int(len(tabulation) // num)
            '''remainder:列表长度除以份数，取余'''
            remainder = int(len(tabulation) % num)
            '''将列表平均拆分'''
            for i in range(1, num + 1):
                if i == 1:
                    new_list.append(tabulation[:merchant])
                else:
                    new_list.append(tabulation[(i - 1) * merchant:i * merchant])
                    '''将剩余数据的添加前面列表中'''
                    if int(len(tabulation) - i * merchant) <= merchant:
                        for j in tabulation[-remainder:]:
                            new_list[tabulation[-remainder:].index(j)].append(j)
            return new_list
    else:
        '''如果列表长度小于份数'''
        for i in range(1, len(tabulation) + 1):
            tabulation_subset = []
            tabulation_subset.append(tabulation[i - 1])
            new_list.append(tabulation_subset)
        return new_list


def split_success_list():  # 将创建表成功的list结果分为2个小list,平均分
    # n = round(len(list_success_table) / 2)
    n = v_max_workers  # 将所有数据表平均分成n份list
    # print('切片的大小:', n)
    # print('原始list：', list_success_table, '\n')
    new_list.append(bisector_list(list_success_table, n))
    # print('一分为二：', new_list)
    # print('新的list长度：', len(new_list[0]))  # 分了几个片，如果一分为二就是2，如过分片不足就是1


# 打印连接信息
def print_source_info():
    oracle_info = oracle_cursor._OraclePool__pool._kwargs
    mysql_info = mysql_cursor._con._kwargs
    print('-' * 50 + 'Oracle->MySQL' + '-' * 50)
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    print('源数据库连接信息: ' + '模式名: ' + str(oracle_info['user']) + ' 连接字符串: ' + str(oracle_info['dsn']))
    print('\n要迁移的表如下:')
    if custom_table.upper() == 'TRUE' or args.data_only.upper() == 'TRUE':
        with open("/tmp/table.txt", "r") as f:  # 打开文件
            for line in f:
                print(line.upper())
    else:
        source_table_count = oracle_cursor.fetch_one("""select count(*) from user_tables""")[0]
        source_view_count = oracle_cursor.fetch_one("""select count(*) from user_views""")[0]
        source_trigger_count = \
            oracle_cursor.fetch_one("""select count(*) from user_triggers where TRIGGER_NAME not like 'BIN$%'""")[0]
        source_procedure_count = oracle_cursor.fetch_one(
            """select count(*) from USER_PROCEDURES where OBJECT_TYPE='PROCEDURE' and OBJECT_NAME  not like 'BIN$%'""")[
            0]
        source_function_count = oracle_cursor.fetch_one(
            """select count(*) from USER_PROCEDURES where OBJECT_TYPE='FUNCTION' and OBJECT_NAME  not like 'BIN$%'""")[
            0]
        source_package_count = oracle_cursor.fetch_one(
            """select count(*) from USER_PROCEDURES where OBJECT_TYPE='PACKAGE' and OBJECT_NAME  not like 'BIN$%'""")[0]
        print('源表总计: ' + str(source_table_count))
        print('源视图总计: ' + str(source_view_count))
        print('源触发器总计: ' + str(source_trigger_count))
        print('源存储过程总计: ' + str(source_procedure_count))
        print('源数据库函数总计: ' + str(source_function_count))
        print('源数据库包总计: ' + str(source_package_count))
    print('\n目标数据库连接信息: ' + 'ip:' + str(mysql_info['host']) + ':' + str(mysql_info['port']) + ' 数据库名称: ' + str(
        mysql_info['database']))
    is_continue = input('\n是否准备迁移数据：Y|N\n')
    if is_continue == 'Y' or is_continue == 'y':
        print('开始迁移数据')  # continue
    else:
        sys.exit()


# 获取Oracle的主键字段
def table_primary(table_name):
    cur_table_primary = source_db.cursor()
    cur_table_primary.execute("""SELECT cols.column_name FROM user_constraints cons, user_cons_columns cols
                    WHERE cols.table_name = '%s' AND cons.constraint_type = 'P'
                    AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
                    ORDER BY cols.table_name""" % table_name)
    result = []
    for d in cur_table_primary:
        result.append(d[0])
    # print(result)
    cur_table_primary.close()
    return result  # 这里需要返回值


# 获取Oracle的列字段类型以及字段长度以及映射数据类型到MySQL的规则
def tbl_columns(table_name):
    sql = """SELECT A.COLUMN_NAME, A.DATA_TYPE, A.DATA_LENGTH, case when A.DATA_PRECISION is null then -1 else  A.DATA_PRECISION end DATA_PRECISION, case when A.DATA_SCALE is null then -1 else  A.DATA_SCALE end DATA_SCALE,  case when A.NULLABLE ='Y' THEN 'True' ELSE 'False' END as isnull, B.COMMENTS,A.DATA_DEFAULT,case when a.AVG_COL_LEN is null then -1 else a.AVG_COL_LEN end AVG_COL_LEN
            FROM USER_TAB_COLUMNS A LEFT JOIN USER_COL_COMMENTS B 
            ON A.TABLE_NAME=B.TABLE_NAME AND A.COLUMN_NAME=B.COLUMN_NAME 
            WHERE A.TABLE_NAME='%s' ORDER BY COLUMN_ID ASC""" % table_name
    output_table_col = oracle_cursor.fetch_all(sql)
    result = []
    # primary_key = table_primary(table_name)
    for column in output_table_col:  # 按照游标行遍历字段
        '''
        result.append({'column_name': column[0],
                       'type': column[1],
                       'primary': column[0] ,
                       'length': column[2],
                       'precision': column[3],
                       'scale': column[4],
                       'nullable': column[5],
                       'comment': column[6]})
        '''
        # 对游标cur_tbl_columns中每行的column[0-8]各字段进行层级判断
        # 字符类型映射规则，字符串类型映射为MySQL类型varchar(n)
        if column[1] == 'VARCHAR2' or column[1] == 'NVARCHAR2':
            #  由于MySQL创建表的时候除了大字段，所有列长度不能大于64k，为了转换方便，如果Oracle字符串长度大于等于1000映射为MySQL的tinytext
            #  由于MySQL大字段不能有默认值，所以这里的默认值都统一为null
            if column[2] >= 10000:  # 此处设定了一个大值，目的是对此条件不生效，即这条规则当前弃用
                result.append({'fieldname': column[0],  # 如下为字段的属性值
                               'type': 'TINYTEXT',  # 列字段类型以及长度范围
                               'primary': column[0],  # 如果有主键字段返回true，否则false
                               'default': 'null',  # 字段默认值
                               'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                               'comment': column[6]
                               }
                              )

            #  Oracle字符串小于1000的映射为MySQL的varchar，然后下面再对字符串的默认值做判断
            elif column[7] is None:  # 对Oracle字符串类型默认值为null的判断
                result.append({'fieldname': column[0],  # 如下为字段的属性值
                               'type': 'VARCHAR' + '(' + str(column[2]) + ')',  # 列字段类型以及长度范围
                               'primary': column[0],  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                               'comment': column[6]
                               }
                              )
            elif column[7].upper() == '(USER)' or column[
                7].upper() == '( \'USER\' )':  # Oracle有些字符类型默认值带有括号，这里在MySQL中去掉括号
                result.append({'fieldname': column[0],  # 如下为字段的属性值
                               'type': 'VARCHAR' + '(' + str(column[2]) + ')',  # 列字段类型以及长度范围
                               'primary': column[0],  # 如果有主键字段返回true，否则false
                               'default': '\'USER\'',  # 字段默认值
                               'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                               'comment': column[6]
                               }
                              )
            else:  # 其余情况的默认值，MySQL保持默认不变
                result.append({'fieldname': column[0],  # 如下为字段的属性值
                               'type': 'VARCHAR' + '(' + str(column[2]) + ')',  # 列字段类型以及长度范围
                               'primary': column[0],  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                               'comment': column[6]
                               }
                              )
        # 字符类型映射规则，字符串类型映射为MySQL类型char(n)
        elif column[1] == 'CHAR' or column[1] == 'NCHAR':
            result.append({'fieldname': column[0],  # 如下为字段的属性值
                           'type': 'CHAR' + '(' + str(column[2]) + ')',  # 列字段类型以及长度范围
                           'primary': column[0],  # 如果有主键字段返回true，否则false
                           'default': column[7],  # 字段默认值
                           'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                           'comment': column[6]
                           }
                          )

        # 时间日期类型映射规则，Oracle date类型映射为MySQL类型datetime
        elif column[1] == 'DATE' or column[1] == 'TIMESTAMP(6)' or column[1] == 'TIMESTAMP(0)':
            # Oracle 默认值sysdate映射到MySQL默认值current_timestamp
            if column[7] == 'sysdate' or column[7] == '( (SYSDATE) )':
                result.append({'fieldname': column[0],  # 如下为字段的属性值
                               'type': 'DATETIME',  # 列字段类型以及长度范围
                               'primary': column[0],  # 如果有主键字段返回true，否则false
                               'default': 'current_timestamp()',  # 字段默认值
                               'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                               'comment': column[6]
                               }
                              )
            # 其他时间日期默认值保持不变(原模原样对应)
            else:
                result.append({'fieldname': column[0],  # 如下为字段的属性值
                               'type': 'DATETIME',  # 列字段类型以及长度范围
                               'primary': column[0],  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                               'comment': column[6]
                               }
                              )

        # 数值类型映射规则，判断Oracle number类型是否是浮点，是否是整数，转为MySQL的int或者decimal。下面分了3种情况区分整数与浮点
        # column[n] == -1,即DATA_PRECISION，DATA_SCALE，AVG_COL_LEN为null，仅在如下if条件判断是否为空
        elif column[1] == 'NUMBER':
            # 场景1：浮点类型判断，如number(5,2)映射为MySQL的DECIMAL(5,2)
            # Oracle number(m,n) -> MySQL decimal(m,n)
            if column[3] > 0 and column[4] > 0:
                result.append({'fieldname': column[0],
                               'type': 'DECIMAL' + '(' + str(column[3]) + ',' + str(column[4]) + ')',  # 列字段类型以及长度范围
                               'primary': column[0],  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                               'comment': column[6]
                               }
                              )
            # 场景2：整数类型以及平均字段长度判断，如number(20,0)，如果AVG_COL_LEN比较大，映射为MySQL的bigint
            # column[8] >= 6 ,Oracle number(m,0) -> MySQL bigint
            elif column[3] > 0 and column[4] == 0 and column[8] >= 6:
                # number类型的默认值有3种情况，一种是null，一种是字符串值为null，剩余其他类型只提取默认值数字部分
                if column[7] is None:  # 对Oracle number字段类型默认值为null的判断
                    result.append({'fieldname': column[0],
                                   'type': 'BIGINT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                elif column[7].upper().startswith('NULL'):  # 对默认值的字符串值等于'null'的做判断
                    result.append({'fieldname': column[0],
                                   'type': 'BIGINT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                else:  # 其余情况通过正则只提取数字部分，即去掉原Oracle中有括号的默认值
                    result.append({'fieldname': column[0],
                                   'type': 'BIGINT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': '' if column[7].upper() == """''""" else
                                   re.findall(r'\b\d+\b', column[7])[0],
                                   # 字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )

            # 场景3：整数类型以及平均字段长度判断，如number(10,0)，如果AVG_COL_LEN比较小，映射为MySQL的int
            # column[8] < 6 ,Oracle number(m,0) -> MySQL bigint
            elif column[3] > 0 and column[4] == 0 and column[8] < 6:
                if column[7] is None:  # 对Oracle number字段类型默认值为null的判断
                    result.append({'fieldname': column[0],
                                   'type': 'INT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                elif column[7].upper().startswith('NULL'):  # 对默认值的字符串值等于'null'的做判断
                    result.append({'fieldname': column[0],
                                   'type': 'INT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                elif column[7].upper() == '':  # 对默认值的字符串值等于''的做判断
                    result.append({'fieldname': column[0],
                                   'type': 'INT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                else:  # 其余情况通过正则只提取数字部分，即去掉原Oracle中有括号的默认值
                    result.append({'fieldname': column[0],
                                   'type': 'INT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': '' if column[7].upper() == """''""" else
                                   re.findall(r'\b\d+\b', column[7])[0],
                                   # 字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )

            # 场景4：无括号包围的number整数类型以及长度判断，如id number,若AVG_COL_LEN比较大，映射为MySQL的bigint
            # column[8] >= 6 ,Oracle number -> MySQL bigint
            elif column[3] == -1 and column[4] == -1 and column[8] >= 6:
                if column[7] is None:  # 对Oracle number字段类型默认值为null的判断
                    result.append({'fieldname': column[0],
                                   'type': 'BIGINT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                elif column[7].upper().startswith('NULL'):  # 对默认值的字符串值等于'null'的做判断
                    result.append({'fieldname': column[0],
                                   'type': 'BIGINT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                elif column[7].upper() == '':  # 对默认值的字符串值等于''的做判断
                    result.append({'fieldname': column[0],
                                   'type': 'BIGINT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                else:  # 其余情况通过正则只提取数字部分，即去掉原Oracle中有括号的默认值
                    result.append({'fieldname': column[0],
                                   'type': 'BIGINT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': '' if column[7].upper() == """''""" else
                                   re.findall(r'\b\d+\b', column[7])[0],
                                   # 字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )

            # 场景5：无括号包围的number整数类型判断，如id number,若AVG_COL_LEN比较小，映射为MySQL的int
            # column[8] < 6 ,Oracle number -> MySQL int
            elif column[3] == -1 and column[4] == -1 and column[8] < 6:
                if column[7] is None:  # 对默认值是否为null的判断
                    result.append({'fieldname': column[0],
                                   'type': 'INT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                elif column[7].upper().startswith('NULL'):  # 对数据库中默认值字符串为'null'的判断
                    result.append({'fieldname': column[0],
                                   'type': 'INT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                else:  # 其余情况number字段类型正则提取默认值数字部分
                    result.append({'fieldname': column[0],
                                   'type': 'INT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': '' if column[7].upper() == """''""" else
                                   re.findall(r'\b\d+\b', column[7])[0],
                                   # 字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )

            # 场景6：int整数类型判断，如id int,(oracle的int会自动转为number),若AVG_COL_LEN比较大，映射为MySQL的bigint
            elif column[3] == -1 and column[4] == 0 and column[8] >= 6:
                if column[7] is None:  # 对默认值是否为null的判断
                    result.append({'fieldname': column[0],
                                   'type': 'BIGINT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                elif column[7].upper().startswith('NULL'):  # 数据库中字段类型默认值为字符串'null'的判断
                    result.append({'fieldname': column[0],
                                   'type': 'BIGINT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                else:  # 其余情况number字段类型正则提取默认值数字部分
                    result.append({'fieldname': column[0],
                                   'type': 'BIGINT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': '' if column[7].upper() == """''""" else
                                   re.findall(r'\b\d+\b', column[7])[0],
                                   # 字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )

            # 场景7：int整数类型判断，如id int,(oracle的int会自动转为number)若AVG_COL_LEN比较小，映射为MySQL的int
            elif column[3] == -1 and column[4] == 0 and column[8] < 6:
                if column[7] is None:  # 对默认值是否为null的判断
                    result.append({'fieldname': column[0],
                                   'type': 'INT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                elif column[7].upper().startswith('NULL'):  # 数据库中字段类型默认值为字符串'null'的判断
                    result.append({'fieldname': column[0],
                                   'type': 'INT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': column[7],  # 字段默认值,设为原值null
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
                else:  # 其余情况number字段类型正则提取默认值数字部分
                    result.append({'fieldname': column[0],
                                   'type': 'INT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': '' if column[7].upper() == """''""" else
                                   re.findall(r'\b\d+\b', column[7])[0],
                                   # 字段默认值如果是''包围则将MySQL默认值调整为null，其余单引号包围去掉括号，仅提取数字
                                   'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                                   'comment': column[6]
                                   }
                                  )
        # 大字段映射规则，文本类型大字段映射为MySQL类型longtext
        elif column[1] == 'CLOB' or column[1] == 'NCLOB' or column[1] == 'LONG':
            result.append({'fieldname': column[0],  # 如下为字段的属性值
                           'type': 'LONGTEXT',  # 列字段类型以及长度范围
                           'primary': column[0],  # 如果有主键字段返回true，否则false
                           'default': column[7],  # 字段默认值
                           'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                           'comment': column[6]
                           }
                          )
        # 大字段映射规则，16进制类型大字段映射为MySQL类型longblob
        elif column[1] == 'BLOB' or column[1] == 'RAW' or column[1] == 'LONG RAW':
            result.append({'fieldname': column[0],  # 如下为字段的属性值
                           'type': 'LONGBLOB',  # 列字段类型以及长度范围
                           'primary': column[0],  # 如果有主键字段返回true，否则false
                           'default': column[7],  # 字段默认值
                           'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                           'comment': column[6]
                           }
                          )
        else:
            result.append({'fieldname': column[0],  # 如果是非大字段类型，通过括号加上字段类型长度范围
                           'type': column[1] + '(' + str(column[2]) + ')',  # 列字段类型以及长度范围
                           'primary': column[0],  # 如果有主键字段返回true，否则false
                           'default': column[7],  # 字段默认值
                           'isnull': column[5],  # 字段是否允许为空，true为允许，否则为false
                           'comment': column[6]
                           }

                          )
    # print('列属性：\n')
    # print(result)
    # cur_tbl_columns.close()
    return result


# 批量创建外键
def create_meta_foreignkey():
    if args.data_only.upper() == 'TRUE':  # 如果指定-d选项不创建外键
        return 1
    global all_fk_count
    global all_fk_success_count
    fk_err_count = 0
    begin_time = datetime.datetime.now()
    fk_table = []  # 存储要创建外键的表
    print('#' * 50 + '开始创建' + '外键约束 ' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    if custom_table.upper() == 'TRUE':  # 如果命令行参数有-c选项，仅创建部分外键
        with open("/tmp/table.txt", "r") as f:
            for line in f:  # 将自定义表存到list
                fk_table.append(list(line.strip('\n').upper().split(',')))
    else:  # 创建全部外键
        table_foreign_key = 'select table_name from USER_CONSTRAINTS where CONSTRAINT_TYPE= \'R\''
        fk_table = oracle_cursor.fetch_all(table_foreign_key)
    if len(fk_table) > 0:
        print('开始创建外键')
        for v_result_table in fk_table:  # 获得一张表创建外键的拼接语句，按照每张表顺序来创建外键
            table_name = v_result_table[0]
            all_foreign_key = oracle_cursor.fetch_all("""SELECT 'ALTER TABLE ' || B.TABLE_NAME || ' ADD CONSTRAINT ' ||
                            B.CONSTRAINT_NAME || ' FOREIGN KEY (' ||
                            (SELECT TO_CHAR(WMSYS.WM_CONCAT(A.COLUMN_NAME))
                               FROM USER_CONS_COLUMNS A
                              WHERE A.CONSTRAINT_NAME = B.CONSTRAINT_NAME) || ') REFERENCES ' ||
                            (SELECT B1.table_name FROM USER_CONSTRAINTS B1
                              WHERE B1.CONSTRAINT_NAME = B.R_CONSTRAINT_NAME) || '(' ||
                            (SELECT TO_CHAR(WMSYS.WM_CONCAT(A.COLUMN_NAME))
                               FROM USER_CONS_COLUMNS A
                              WHERE A.CONSTRAINT_NAME = B.R_CONSTRAINT_NAME) || ');'
                       FROM USER_CONSTRAINTS B
                      WHERE B.CONSTRAINT_TYPE = 'R' and TABLE_NAME='%s'""" % table_name)
            for e in all_foreign_key:  # 根据上面的查询结果集，创建外键
                create_foreign_key_sql = e[0]
                print(create_foreign_key_sql)
                all_fk_count += 1  # 外键总数
                try:
                    mysql_cursor.execute(create_foreign_key_sql)
                    print('外键创建完毕\n')
                    all_fk_success_count += 1
                except Exception as e:
                    fk_err_count += 1
                    foreignkey_failed_count.append('1')  # 外键创建失败就往list对象存1
                    print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                    print('外键创建失败请检查ddl语句!\n')
                    # print(traceback.format_exc())
                    filename = '/tmp/ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('\n-- ' + ' FOREIGNKEY CREATE ERROR ' + str(fk_err_count) + '\n')
                    f.write(create_foreign_key_sql + ';\n')
                    f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                    f.close()
    else:
        print('没有外键约束')
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    end_time = datetime.datetime.now()
    print('创建外键耗时： ' + str((end_time - begin_time).seconds))
    print('#' * 50 + '外键创建完成' + '#' * 50 + '\n\n\n')


# 批量创建主键以及索引
def create_meta_constraint():
    if args.data_only.upper() == 'TRUE':  # -d选项不创建约束
        return 1
    global all_constraints_count  # 约束以及索引总数
    global all_constraints_success_count  # mysql中约束以及索引创建成功的计数
    global function_based_index_count  # function_based_index总数
    function_based_index = []
    user_name = oracle_cursor.fetch_one("""select user from dual""")
    user_name = user_name[0]
    err_count = 0
    output_table_name = []  # 迁移部分表
    create_index = ''
    all_index = []  # 存储执行创建约束的结果集
    start_time = datetime.datetime.now()
    print('#' * 50 + '开始创建' + '约束以及索引 ' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    # 以下是创建 NORMAL的主键以及普通索引
    if custom_table.upper() == 'TRUE':  # 如果命令行参数有-c选项，仅创建部分约束
        with open("/tmp/table.txt", "r") as f:  # 读取自定义表
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))  # 将自定义表全部保存到list
        for v_custom_table in output_table_name:  # 读取第N个表查询生成拼接sql
            custom_index = oracle_cursor.fetch_all("""SELECT
                       (CASE
                         WHEN C.CONSTRAINT_TYPE = 'P' OR C.CONSTRAINT_TYPE = 'R' THEN
                          'ALTER TABLE ' || T.TABLE_NAME || ' ADD CONSTRAINT ' ||
                          T.INDEX_NAME || (CASE
                            WHEN C.CONSTRAINT_TYPE = 'P' THEN
                             ' PRIMARY KEY ('
                            ELSE
                             ' FOREIGN KEY ('
                          END) || WM_CONCAT(T.COLUMN_NAME) || ');'
                         ELSE
                          'CREATE ' || (CASE
                            WHEN I.UNIQUENESS = 'UNIQUE' THEN
                             I.UNIQUENESS || ' '
                            ELSE
                             CASE
                               WHEN I.INDEX_TYPE = 'NORMAL' THEN
                                ''
                               ELSE
                                I.INDEX_TYPE || ' '
                             END
                          END) || 'INDEX ' || T.INDEX_NAME || ' ON ' || T.TABLE_NAME || '(' ||
                          WM_CONCAT(COLUMN_NAME) || ');'
                       END) SQL_CMD
                  FROM USER_IND_COLUMNS T, USER_INDEXES I, USER_CONSTRAINTS C
                 WHERE T.INDEX_NAME = I.INDEX_NAME
                   AND T.INDEX_NAME = C.CONSTRAINT_NAME(+)
                   AND T.TABLE_NAME = '%s'
                   and i.index_type != 'FUNCTION-BASED NORMAL'
                 GROUP BY T.TABLE_NAME,
                          T.INDEX_NAME,
                          I.UNIQUENESS,
                          I.INDEX_TYPE,
                          C.CONSTRAINT_TYPE""" % v_custom_table[0])
            for v_out in custom_index:  # 每次将上面单表全部结果集全部存到all_index的list里面
                all_index.append(v_out)
    else:  # 命令行参数没有-c选项，创建所有约束
        all_index = oracle_cursor.fetch_all("""SELECT
                   (CASE
                     WHEN C.CONSTRAINT_TYPE = 'P' OR C.CONSTRAINT_TYPE = 'R' THEN
                      'ALTER TABLE ' || T.TABLE_NAME || ' ADD CONSTRAINT ' ||
                      T.INDEX_NAME || (CASE
                        WHEN C.CONSTRAINT_TYPE = 'P' THEN
                         ' PRIMARY KEY ('
                        ELSE
                         ' FOREIGN KEY ('
                      END) || WM_CONCAT(T.COLUMN_NAME) || ');'
                     ELSE
                      'CREATE ' || (CASE
                        WHEN I.UNIQUENESS = 'UNIQUE' THEN
                         I.UNIQUENESS || ' '
                        ELSE
                         CASE
                           WHEN I.INDEX_TYPE = 'NORMAL' THEN
                            ''
                           ELSE
                            I.INDEX_TYPE || ' '
                         END
                      END) || 'INDEX ' || T.INDEX_NAME || ' ON ' || T.TABLE_NAME || '(' ||
                      WM_CONCAT(COLUMN_NAME) || ');'
                   END) SQL_CMD
              FROM USER_IND_COLUMNS T, USER_INDEXES I, USER_CONSTRAINTS C
             WHERE T.INDEX_NAME = I.INDEX_NAME
               AND T.INDEX_NAME = C.CONSTRAINT_NAME(+)
               and i.index_type != 'FUNCTION-BASED NORMAL'
             GROUP BY T.TABLE_NAME,
                      T.INDEX_NAME,
                      I.UNIQUENESS,
                      I.INDEX_TYPE,
                      C.CONSTRAINT_TYPE""")  # 如果要每张表查使用T.TABLE_NAME = '%s',%s传进去是没有单引号，所以需要用单引号号包围
    all_constraints_count = len(all_index)
    if all_constraints_count > 0:
        print('开始创建normal index：\n')
        for d in all_index:
            create_index_sql = d[0].read()  # 用read读取大对象，否则会报错
            print(create_index_sql)
            try:
                mysql_cursor.execute(create_index_sql)
                print('约束以及索引创建完毕\n')
                all_constraints_success_count += 1
            except Exception as e:
                err_count += 1
                constraint_failed_count.append('1')  # 用来统计主键或者索引创建失败的计数，只要创建失败就往list存1
                print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                print('约束或者索引创建失败请检查ddl语句!\n')
                filename = '/tmp/ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('\n-- ' + ' CONSTRAINTS CREATE ERROR' + str(err_count) + ' -- \n')
                f.write(create_index_sql + '\n\n\n')
                f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                f.close()
    else:
        print('没有normal index需要创建')
    # 以下是创建非normal索引
    if custom_table.upper() == 'TRUE':  # 如果命令行参数有-c选项，仅创建部分约束
        for v_custom_table in output_table_name:  # 读取第N个表
            function_index = oracle_cursor.fetch_all(
                """Select index_name from user_indexes where index_type='FUNCTION-BASED NORMAL' and table_name ='%s'""" %
                v_custom_table[0])  # 根据第N个表，获取所有所有名称
            for v_out0 in function_index:  # 将上述获取的若干索引名称一一存入list
                function_based_index.append(v_out0)
    else:  # 查询所有表的索引名称
        function_based_index = oracle_cursor.fetch_all(
            """Select index_name from user_indexes where index_type='FUNCTION-BASED NORMAL'""")
    function_based_index_count = len(function_based_index)  # 如果有非normal索引
    if function_based_index_count > 0:
        print('开始创建非normal index：\n')
    for v_function_based_index in function_based_index:
        fun_index_name = v_function_based_index[0]
        try:  # 下面是生成非normal索引的拼接sql，来源于dbms_metadata.get_ddl
            create_index = oracle_cursor.fetch_one(
                """select trim(replace(regexp_replace(regexp_replace(SUBSTR(upper(to_char(dbms_metadata.get_ddl('INDEX','%s','%s'))), 1, INSTR(upper(to_char(dbms_metadata.get_ddl('INDEX','%s','%s'))), ' PCTFREE')-1),'"','',1,0,'i'),'%s'||'.','',1,0,'i'),chr(10),'')) from dual""" % (
                    fun_index_name, user_name, fun_index_name, user_name, user_name))
            create_index = create_index[0]
            print(create_index)
            mysql_cursor.execute(create_index)
            print('success\n')
            all_constraints_success_count += 1
        except Exception as e:
            err_count += 1
            constraint_failed_count.append('1')
            print('\n' + '/* ' + str(e.args) + ' */' + '\n')
            print('非NORMAL索引创建失败请检查ddl语句!\n')
            filename = '/tmp/ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('\n-- ' + 'NON NORMAL INDEX CREATE ERROR ' + str(err_count) + '\n')
            f.write(create_index + ';\n')
            f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
            f.close()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    end_time = datetime.datetime.now()
    print('创建约束以及索引耗时： ' + str((end_time - start_time).seconds))
    print('#' * 50 + '主键约束、索引创建完成' + '#' * 50 + '\n\n\n')


# 查找具有自增特性的表以及字段名称
def create_trigger_col():
    if args.data_only.upper() == 'TRUE':  # -d选项不会创建自增列
        return 1
    global all_inc_col_success_count
    global all_inc_col_failed_count
    global normal_trigger_count  # 用于统计oracle触发器（排除掉序列相关触发器）的总数
    global trigger_success_count  # mysql中触发器创建成功的总数
    global trigger_failed_count  # mysql中触发器创建失败的总数
    normal_trigger = []  # 用于存自定义表读取之后的触发器名称
    create_trigger = ''  # 触发器创建的sql
    count_1 = 0  # 自增列索引创建失败的计数
    start_time = datetime.datetime.now()
    user_name = oracle_cursor.fetch_one("""select user from dual""")
    user_name = user_name[0]
    # Oracle中无法对long类型数据截取，创建用于存储触发器字段信息的临时表TRIGGER_NAME
    count_num_tri = oracle_cursor.fetch_one("""select count(*) from user_tables where table_name='TRIGGER_NAME'""")[
        0]
    if count_num_tri == 1:  # 判断表trigger_name是否存在
        try:
            oracle_cursor.execute_sql("""truncate table trigger_name""")
        except Exception:
            print(traceback.format_exc())
            print('truncate table trigger_name失败')
    else:
        try:
            oracle_cursor.execute_sql(
                """create table trigger_name (table_name varchar2(200),trigger_type varchar2(100),trigger_body clob)""")
        except Exception:
            print(traceback.format_exc())
            print('无法在Oracle创建用于触发器的表')
    print('#' * 50 + '开始增加自增列' + '#' * 50)
    if custom_table.upper() == 'TRUE':  # 如果命令行参数有-c选项，仅创建部分自增列
        with open("/tmp/table.txt", "r") as f:  # 读取自定义表
            for table_name in f.readlines():  # 按顺序读取每一个表
                table_name = table_name.strip('\n').upper()  # 去掉列表中每一个元素的换行符
                # Oracle中无法对long类型数据截取，创建用于存储触发器字段信息的临时表TRIGGER_NAME
                try:  # 按照每张表，将单张表结果集插入到trigger_name
                    oracle_cursor.execute_sql(
                        """insert into trigger_name select table_name ,trigger_type,to_lob(trigger_body) from user_triggers where table_name= '%s'  """ % table_name)
                except Exception:
                    print(traceback.format_exc())
                    print('无法在Oracle插入存放触发器的数据')
    else:  # 创建所有自增列索引
        try:
            oracle_cursor.execute_sql("""truncate table trigger_name""")
            oracle_cursor.execute_sql(
                """insert into trigger_name select table_name ,trigger_type,to_lob(trigger_body) from user_triggers""")
        except Exception:
            print(traceback.format_exc())
            print('无法在Oracle插入存放触发器的数据')
    all_create_index = oracle_cursor.fetch_all(
        """select 'create  index ids_'||substr(table_name,1,26)||' on '||table_name||'('||upper(substr(substr(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.')), 1, instr(upper(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.'))), ' FROM DUAL;') - 1), 5)) ||');' as sql_create from trigger_name where trigger_type='BEFORE EACH ROW' and instr(upper(trigger_body), 'NEXTVAL')>0""")  # 在Oracle拼接sql生成用于在MySQL中自增列的索引
    auto_inc_count = len(all_create_index)
    if auto_inc_count > 0:
        print('创建用于自增列的索引:\n ')
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        for v_increa_index in all_create_index:
            create_autoincrea_index = v_increa_index[0].read()  # 用read读取大字段，否则无法执行
            print(create_autoincrea_index)
            try:
                mysql_cursor.execute(create_autoincrea_index)
            except Exception as e:
                count_1 += 1
                print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                print('用于自增列的索引创建失败，请检查源触发器！\n')
                # print(traceback.format_exc())
                filename = '/tmp/ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-- ' + str(count_1) + ' AUTO_INCREAMENT COL INDEX CREATE ERROR' + ' -- ' + '\n')
                f.write(create_autoincrea_index + '\n\n\n')
                f.close()
                ddl_incindex_error = '\n' + '/* ' + str(e.args) + ' */' + '\n'
                logging.error(ddl_incindex_error)  # 自增用索引创建失败的sql语句输出到文件/tmp/ddl_failed_table.log
        print('自增列索引创建完成 ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        print('\n开始修改自增列属性：')
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        all_alter_sql = oracle_cursor.fetch_all("""
            select 'alter table '||table_name||' modify '||upper(substr(substr(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.')), 1, instr(upper(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.'))), ' FROM DUAL;') - 1), 5)) ||' int auto_increment;' from trigger_name where trigger_type='BEFORE EACH ROW' and instr(upper(trigger_body), 'NEXTVAL')>0
            """)
        for v_increa_col in all_alter_sql:
            alter_increa_col = v_increa_col[0].read()  # 用read读取大字段，否则无法执行
            print('\n执行sql alter table：\n')
            print(alter_increa_col)
            try:  # 注意下try要在for里面
                mysql_cursor.execute(alter_increa_col)
                all_inc_col_success_count += 1
            except Exception as e:  # 如果有异常打印异常信息，并跳过继续下个自增列修改
                all_inc_col_failed_count += 1
                print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                print('修改自增列失败，请检查源触发器！\n')
                # print(traceback.format_exc())
                filename = '/tmp/ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('\n-- ' + ' MODIFY AUTO_COL ERROR ' + str(all_inc_col_failed_count) + ' -- \n')
                f.write(alter_increa_col + ';\n')
                f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                f.close()
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        end_time = datetime.datetime.now()
        print('修改自增列耗时： ' + str((end_time - start_time).seconds))
        print('#' * 50 + '自增列修改完成' + '#' * 50 + '\n\n\n')
        oracle_autocol_total.append(
            oracle_cursor.fetch_one(
                """select count(*) from trigger_name  where trigger_type='BEFORE EACH ROW' and instr(upper(trigger_body), 'NEXTVAL')>0""")[
                0])  # 将自增列的总数存入list
    else:
        print('没有相关自增列需要创建！')
    oracle_cursor.execute_sql("""drop table trigger_name purge""")  # 删除之前在oracle创建的临时表

    # 以下是创建常规触发器
    if custom_table.upper() == 'TRUE':  # 如果命令行参数有-c选项，仅创建部分自增列
        with open("/tmp/table.txt", "r") as f:  # 读取自定义表
            for table_name in f.readlines():  # 按顺序读取每一个表
                table_name = table_name.strip('\n').upper()  # 去掉列表中每一个元素的换行符
                try:
                    trigger_name = oracle_cursor.fetch_all(
                        """select trigger_name from user_triggers where trigger_type !='BEFORE EACH ROW' and table_name='%s'""" % table_name)
                    for v_trt_name in trigger_name:
                        normal_trigger.append(v_trt_name)
                except Exception as e:
                    print(e)
    else:
        try:
            normal_trigger = oracle_cursor.fetch_all(
                """select trigger_name from user_triggers where trigger_type !='BEFORE EACH ROW'""")
        except Exception as e:
            print(e)
    normal_trigger_count = len(normal_trigger)
    if normal_trigger_count > 0:
        print('开始创建NORMAL TRIGGER：\n')
        for v_normal_trigger in normal_trigger:
            trigger_name = v_normal_trigger[0]
            try:
                create_trigger = oracle_cursor.fetch_one(
                    """select regexp_replace(regexp_replace(trim(replace(regexp_replace(regexp_replace(SUBSTR(upper(to_char(dbms_metadata.get_ddl('TRIGGER','%s','%s'))), 1, INSTR(upper(to_char(dbms_metadata.get_ddl('TRIGGER','%s','%s'))), 'ALTER TRIGGER')-1),'"','',1,0,'i'),'%s.','',1,0,'i'),chr(10),'')),'OR REPLACE','',1,0,'i'),':','',1,0,'i') from dual""" % (
                        trigger_name, user_name, trigger_name, user_name, user_name))
                create_trigger = create_trigger[0]
                print(create_trigger)
                print('触发器创建完成\n')
                mysql_cursor.execute(create_trigger)
                trigger_success_count += 1
            except Exception as e:
                trigger_failed_count += 1
                print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                print('常规触发器创建失败请检查ddl语句!\n')
                filename = '/tmp/ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('\n-- ' + ' NORMAL TRIGGER CREATE ERROR' + str(trigger_failed_count) + ' -- \n')
                f.write(create_trigger + ';\n')
                f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                f.close()
    else:
        print('没有常规触发器\n')


# 获取视图定义以及创建
def create_view():
    if args.data_only.upper() == 'TRUE' or args.custom_table.upper() == 'TRUE':  # 如果指定-d或者-c选项不创建视图
        return 1
    global all_view_count
    global all_view_success_count
    global all_view_failed_count
    begin_time = datetime.datetime.now()
    if custom_table.upper() == 'TRUE':  # 如果命令行-c开启就不创建视图
        print('\n\n' + '#' * 50 + '无视图创建' + '#' * 50 + '\n')
    else:  # 创建全部视图
        print('#' * 50 + '开始创建视图' + '#' * 50)
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        # Oracle中无法对long类型数据截取，创建用于存储视图信息的临时表content_view
        count_num_view = \
            oracle_cursor.fetch_one("""select count(*) from user_tables where table_name='CONTENT_VIEW'""")[0]
        if count_num_view == 1:
            oracle_cursor.execute_sql("""drop table CONTENT_VIEW purge""")
            oracle_cursor.execute_sql("""create table content_view (view_name varchar2(200),text clob)""")
            oracle_cursor.execute_sql(
                """insert into content_view(view_name,text) select view_name,to_lob(text) from USER_VIEWS""")
        else:
            oracle_cursor.execute_sql("""create table content_view (view_name varchar2(200),text clob)""")
            oracle_cursor.execute_sql(
                """insert into content_view(view_name,text) select view_name,to_lob(text) from USER_VIEWS""")
        all_view_create = oracle_cursor.fetch_all("""
            select  view_name,'create view '||view_name||' as '||replace(text, '"'  , '') as view_sql from CONTENT_VIEW
            """)
        all_view_count = len(all_view_create)
        if all_view_count > 0:
            for e in all_view_create:
                view_name = e[0]
                create_view_sql = e[1].read()  # 用read读取大字段，否则无法执行
                print(create_view_sql)
                try:
                    # cur_target_constraint.execute("""drop view  if exists %s""" % view_name)
                    # cur_target_constraint.execute(create_view_sql)
                    mysql_cursor.execute("""drop view  if exists %s""" % view_name)
                    mysql_cursor.execute(create_view_sql)
                    print('视图创建完毕\n')
                    all_view_success_count += 1
                except Exception as e:
                    all_view_failed_count += 1
                    view_failed_result.append(view_name)
                    print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                    print('视图创建失败请检查ddl语句!\n')
                    # print(traceback.format_exc())
                    filename = '/tmp/ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('\n-- ' + ' CREATE VIEW ' + view_name + ' ERROR ' + str(all_view_failed_count) + ' -- \n')
                    f.write(create_view_sql + ';\n')
                    f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                    f.close()
        else:
            print('没有视图要创建')
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        end_time = datetime.datetime.now()
        print('创建视图耗时: ' + str((end_time - begin_time).seconds))
        print('\033[31m*' * 50 + '视图创建完成' + '*' * 50 + '\033[0m\n\n\n')
        oracle_cursor.execute_sql("""drop table content_view purge""")


# 数据库对象的comment注释,这里仅包含表的注释，列的注释在上面创建表结构的时候已经包括
def create_comment():
    if args.data_only.upper() == 'TRUE':  # 如果指定-d选项，不创建注释
        return 1
    err_count = 0
    all_comment_sql = []
    output_table_name = []
    begin_time = datetime.datetime.now()
    print('#' * 50 + '开始添加comment注释' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    if custom_table.upper() == 'TRUE':  # 命令行选项-c指定后，仅创建部分注释
        with open("/tmp/table.txt", "r") as f:  # 读取自定义表
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))  # 读取txt中的自定义表到list
        for v_custom_table in output_table_name:  # 根据第N个表查询生成拼接sql
            custom_comment = oracle_cursor.fetch_all("""
                        select 'alter table '||TABLE_NAME||' comment '||''''||COMMENTS||'''' as create_comment
                     from USER_TAB_COMMENTS where COMMENTS is not null and table_name = '%s' 
                     """ % v_custom_table[0])
            for v_out in custom_comment:  # 每次将上面单表全部结果集全部存到all_comment_sql的list里面
                all_comment_sql.append(v_out)
    else:  # 创建全部注释
        all_comment_sql = oracle_cursor.fetch_all("""
            select 'alter table '||TABLE_NAME||' comment '||''''||COMMENTS||'''' as create_comment
         from USER_TAB_COMMENTS where COMMENTS is not null
            """)
    if len(all_comment_sql) > 0:
        for e in all_comment_sql:  # 一次性创建注释
            # table_name = e[0]
            create_comment_sql = e[0]
            try:
                print('正在添加注释:')
                print(create_comment_sql)
                # cur_target_constraint.execute(create_comment_sql)
                mysql_cursor.execute(create_comment_sql)
                print('comment注释添加完毕\n')
            except Exception as e:
                err_count += 1
                comment_failed_count.append('1')  # comment添加失败就往list对象存1
                print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                print('comment添加失败请检查ddl语句!\n')
                # print(traceback.format_exc())
                filename = '/tmp/ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('\n-- ' + ' CREATE COMMENT ERROR ' + str(err_count) + '\n')
                f.write(create_comment_sql + ';\n')
                f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                f.close()
    else:
        print('没有注释要创建')
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    end_time = datetime.datetime.now()
    print('创建comment耗时：' + str((end_time - begin_time).seconds))
    print('#' * 50 + 'comment注释添加完成' + '#' * 50 + '\033[0m\n\n\n')


# 仅输出Oracle当前用户的表，即user_tables的table_name
def print_table():
    tableoutput_sql = 'select table_name from user_tables  order by table_name  desc'  # 查询需要导出的表
    all_table = oracle_cursor.fetch_all(tableoutput_sql)
    for v_table in all_table:
        list_table_name.append(v_table[0])


# 将ddl创建成功的表记录到csv文件
def print_ddl_success_table(table_name):
    filename = '/tmp/ddl_success_table.log'
    f = open(filename, 'a', encoding='utf-8')
    f.write(table_name + '\n')
    f.close()


# 打印输出插入失败的表名称
def print_insert_failed_table(table_name):
    filename = '/tmp/insert_failed_table.csv'
    f = open(filename, 'a', encoding='utf-8')
    f.write(table_name + '\n')
    f.close()


# 批量将Oracle数据插入到MySQL的方法,之前是调用该函数串行迁移表，现在是异步，async_work来去取代
def mig_table():
    mysql_con = configDB.MySQLPOOL.connection()
    mysql_cur = mysql_con.cursor()  # MySQL连接池
    mysql_cur.arraysize = row_batch_size
    err_count = 0
    col_len = 0
    if args.data_only.upper() == 'FALSE':  # 只有指定了-d选项才会执行此单步迁移
        return 1
    with open("/tmp/table.txt", "r") as f:  # 读取自定义表
        for table_name in f.readlines():  # 按顺序读取每一个表
            table_name = table_name.strip('\n').upper()  # 去掉列表中每一个元素的换行符
            target_table = source_table = table_name
            try:
                get_table_count = oracle_cursor.fetch_one("""select count(*) from %s""" % source_table)[0]
                get_column_length = 'select count(*) from user_tab_columns where table_name= ' + "'" + source_table.upper() + "'"  # 拼接获取源表有多少个列的SQL
                col_len = oracle_cursor.fetch_one(get_column_length)  # 获取源表有多少个列 oracle连接池
                col_len = col_len[0]  # 将游标结果数组的值赋值，该值为表列字段总数
            except Exception as e:
                print('获取源表总数以及列总数失败，请检查是否存在该表或者表名小写！' + table_name)
                err_count += 1
                sql_insert_error = traceback.format_exc()
                filename = '/tmp/insert_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-' * 50 + str(err_count) + ' ' + table_name + ' INSERT ERROR' + '-' * 50 + '\n')
                f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n\n')
                f.write(sql_insert_error + '\n\n')
                f.close()
                logging.error(sql_insert_error)  # 插入失败的sql语句输出到文件/tmp/ddl_failed_table.log
                # mysql_cur.execute("""insert into my_mig_task_info(table_name, task_start_time,thread,is_success) values ('%s',
                #                     current_timestamp,%s,'CHECK TABLE')""" % (table_name, 100))  # %s占位符的值需要引号包围
            val_str = ''  # 用于生成批量插入的列字段变量
            for i in range(1, col_len):
                val_str = val_str + '%s' + ','
            val_str = val_str + '%s'  # MySQL批量插入语法是 insert into tb_name values(%s,%s,%s,%s)
            insert_sql = 'insert into ' + target_table + ' values(' + val_str + ')'  # 拼接insert into 目标表 values  #目标表插入语句
            select_sql = 'select * from ' + source_table  # 源查询SQL，如果有where过滤条件，在这里拼接
            try:
                cur_oracle_result.execute(select_sql)  # 执行
                # temp_cur = oracle_cursor.execute_sql(select_sql, 1000)
            except Exception:
                print('查询Oracle源表数据失败，请检查是否存在该表或者表名小写！\n\n' + table_name)
            print("正在执行插入表:", table_name)
            print(datetime.datetime.now())
            source_effectrow = 0
            target_effectrow = 0
            mysql_insert_count = 0
            sql_insert_error = ''
            # mig_table = "'" + table_name + "'"
            # 往MySQL表my_mig_task_info记录开始插入的时间
            try:
                mysql_cur.execute("""insert into my_mig_task_info(table_name, task_start_time,thread) values ('%s',
                        current_timestamp,%s)""" % (table_name, 100))  # %s占位符的值需要引号包围
                mysql_con.commit()
            except Exception:
                print(traceback.format_exc())
            while True:
                rows = list(
                    cur_oracle_result.fetchmany(
                        fetch_many_count))
                # 例如每次获取2000行，cur_oracle_result.arraysize值决定
                # MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
                # print(cur_oracle_result.description)  # 打印Oracle查询结果集字段列表以及类型
                try:
                    mysql_cur.executemany(insert_sql, rows)  # 批量插入每次5000行，需要注意的是 rows 必须是 list [] 数据类型
                    mysql_insert_count = mysql_insert_count + mysql_cur.rowcount  # 每次插入的行数
                    mysql_con.commit()  # 如果连接池没有配置自动提交，否则这里需要显式提交
                except Exception as e:
                    err_count += 1
                    print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                    # print(traceback.format_exc())  # 遇到异常记录到log，会继续迁移下张表
                    sql_insert_error = '\n' + '/* ' + str(e.args) + ' */' + '\n'
                    filename = '/tmp/insert_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('-' * 50 + str(err_count) + ' ' + table_name + ' INSERT ERROR' + '-' * 50 + '\n')
                    f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n\n')
                    f.write(insert_sql + '\n\n\n')
                    f.write(str(rows[0]) + '\n\n')
                    f.write(sql_insert_error + '\n\n')
                    f.close()
                    logging.error(sql_insert_error)  # 插入失败的sql语句输出到文件/tmp/ddl_failed_table.log
                if get_table_count == 0:
                    print('\n无数据插入')
                    # print('\r', str(mysql_insert_count), '/', str(get_table_count), ' ',str(round((mysql_insert_count /
                    # get_table_count), 2) * 100) + '%', end='',flush=True)  # 实时刷新插入行数
                if not rows:
                    break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表
                source_effectrow = cur_oracle_result.rowcount  # 计数源表插入的行数
                target_effectrow = target_effectrow + mysql_cur.rowcount  # 计数目标表插入的行数
            print('source_effectrow:' + str(source_effectrow))
            print('target_effectrow:' + str(target_effectrow))
            if source_effectrow == target_effectrow:
                is_success = 'Y'
            else:
                is_success = 'N'
            if sql_insert_error:
                print('\n' + table_name + '插入失败')
            else:
                print('\n' + table_name + '插入完成')
            try:
                mysql_cur.execute("""update my_mig_task_info set task_end_time=current_timestamp, 
                            source_table_rows=%s,
                            target_table_rows=%s,
                            is_success='%s' where table_name='%s'""" % (
                    source_effectrow, target_effectrow, is_success, table_name))  # 占位符需要引号包围
                mysql_con.commit()
            except Exception:
                print(traceback.format_exc())
            print('\n\n')


# 在MySQL创建表结构以及添加主键
def create_meta_table():
    output_table_name = []  # 用于存储要迁移的部分表
    if args.data_only.upper() == 'TRUE':
        return 1
    if custom_table.upper() == 'TRUE':
        with open("/tmp/table.txt", "r") as f:  # 打开文件
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))
    else:
        tableoutput_sql = """select table_name from user_tables  order by table_name  desc"""  # 查询需要导出的表
        output_table_name = oracle_cursor.fetch_all(tableoutput_sql)
    global all_table_count  # 将oracle源表总数存入全局变量
    all_table_count = len(output_table_name)  # 无论是自定义表还是全库，都可以存入全局变量
    starttime = datetime.datetime.now()
    table_index = 0
    for row in output_table_name:
        table_name = row[0]
        print('#' * 50 + '开始创建表' + table_name + '#' * 50)
        #  将创建失败的sql记录到log文件
        logging.basicConfig(filename='/tmp/ddl_failed_table.log')
        # 在MySQL创建表前先删除存在的表
        drop_target_table = 'drop table if exists ' + table_name
        mysql_cursor.execute(drop_target_table)
        # cur_drop_table.execute(drop_target_table)
        fieldinfos = []
        structs = tbl_columns(table_name)  # 获取源表的表字段信息
        # v_pri_key = table_primary(table_name)  # 获取源表的主键字段，因为已经有创建约束的sql，这里可以不用执行
        # 以下字段已映射为MySQL字段类型
        for struct in structs:
            defaultvalue = struct.get('default')
            commentvalue = struct.get('comment')
            if defaultvalue:  # 对默认值以及注释数据类型的判断，如果不是str类型，转为str类型
                defaultvalue = "'{0}'".format(defaultvalue) if type(defaultvalue) == 'str' else str(defaultvalue)
            if commentvalue:
                commentvalue = "'{0}'".format(commentvalue) if type(commentvalue) == 'str' else str(commentvalue)
            fieldinfos.append('{0} {1} {2} {3} {4}'.format(struct['fieldname'],
                                                           struct['type'],
                                                           # 'primary key' if struct.get('primary') else '',主键在创建表的时候定义
                                                           # ('default ' + '\'' + defaultvalue + '\'') if defaultvalue else '',
                                                           ('default ' + defaultvalue) if defaultvalue else '',
                                                           '' if struct.get('isnull') else 'not null',
                                                           (
                                                                   'comment ' + '\'' + commentvalue + '\'') if commentvalue else ''
                                                           ),

                              )
        create_table_sql = 'create table {0} ({1})'.format(table_name, ','.join(fieldinfos))  # 生成创建目标表的sql
        # add_pri_key_sql = 'alter table {0} add primary key ({1})'.format(table_name, ','.join(v_pri_key))  #
        # 创建目标表之后增加主键
        print('创建表:' + table_name + '\n')
        print(create_table_sql)
        try:
            # cur_createtbl.execute(create_table_sql)
            mysql_cursor.execute(create_table_sql)
            #  if v_pri_key: 因为已经有创建约束的sql，这里可以不用执行
            #    cur_createtbl.execute(add_pri_key_sql) 因为已经有创建约束的sql，这里可以不用执行
            print(table_name + '表创建完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '\n')
            print_ddl_success_table(table_name)  # MySQL ddl创建成功的表，记录下表名到/tmp/ddl_success_table.csv
            list_success_table.append(table_name)  # MySQL ddl创建成功的表也存到list中

        except Exception as e:
            table_index = table_index + 1
            print('\n' + '/* ' + str(e.args) + ' */' + '\n')
            # print(traceback.format_exc())  # 如果某张表创建失败，遇到异常记录到log，会继续创建下张表
            # ddl创建失败的表名记录到文件/tmp/ddl_failed_table.log
            filename = '/tmp/ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('-- ' + 'CREATE TABLE ' + table_name + ' ERROR ' + str(table_index) + ' -- \n')
            f.write('/* ' + table_name + ' */' + '\n')
            f.write(create_table_sql + ';\n')
            f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
            f.close()
            ddl_failed_table_result.append(table_name)  # 将当前ddl创建失败的表名记录到ddl_failed_table_result的list中
            print('表' + table_name + '创建失败请检查ddl语句!\n')
    endtime = datetime.datetime.now()
    print("表创建耗时\n" + "开始时间:" + str(starttime) + '\n' + "结束时间:" + str(endtime) + '\n' + "消耗时间:" + str(
        (endtime - starttime).seconds) + "秒\n")
    print('#' * 50 + '表创建完成' + '#' * 50 + '\n\n\n')


def mig_table_task(list_index):
    err_count = 0
    err_log = ''
    if args.data_only.upper() == 'TRUE':
        return 1
    list_number = list_parallel  # 并行度
    if list_index in list_number:
        source_db0 = cx_Oracle.connect(ora_conn)
        cur_oracle_result = source_db0.cursor()  # 查询Oracle源表的游标结果集
        cur_oracle_result.outputtypehandler = dataconvert
        cur_oracle_result.prefetchrows = row_batch_size
        cur_oracle_result.arraysize = row_batch_size  # Oracle数据库游标对象结果集返回的行数即每次获取多少行
        mysql_con0 = configDB.MySQLPOOL.connection()
        mysql_cursor = mysql_con0.cursor()  # MySQL连接池
        mysql_cursor.arraysize = row_batch_size
    # print('游标arraysize:' + str(mysql_cursor.arraysize))
    task_starttime = datetime.datetime.now()
    for v_table_name in new_list[0][int(list_index)]:
        table_name = v_table_name
        target_table = source_table = table_name
        try:
            # get_table_count = oracle_cursor.fetch_one("""select count(*) from %s""" % source_table)[0]
            get_column_length = 'select count(*) from user_tab_columns where table_name= ' + "'" + source_table.upper() + "'"  # 拼接获取源表有多少个列的SQL
            col_len = oracle_cursor.fetch_one(get_column_length)  # 获取源表有多少个列 oracle连接池
            col_len = col_len[0]  # 将游标结果数组的值赋值，该值为表列字段总数
        except Exception as e:
            print(traceback.format_exc() + '获取源表总数以及列总数失败，请检查是否存在该表或者表名小写！' + table_name)
            err_count += 1
            sql_insert_error = traceback.format_exc()
            filename = '/tmp/insert_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('-' * 50 + str(err_count) + ' ' + table_name + ' INSERT ERROR' + '-' * 50 + '\n')
            f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n\n')
            f.write(sql_insert_error + '\n\n')
            f.close()
            logging.error(sql_insert_error)  # 插入失败的sql语句输出到文件/tmp/ddl_failed_table.log
            mysql_cursor.execute("""insert into my_mig_task_info(table_name, task_start_time,thread,is_success) values ('%s',
                    current_timestamp,%s,'CHECK TABLE')""" % (table_name, list_index))  # %s占位符的值需要引号包围
            continue  # 这里需要显式指定continue，否则表不存在或者其他问题，会直接跳出for循环
        val_str = ''  # 用于生成批量插入的列字段变量
        for i in range(1, col_len):
            val_str = val_str + '%s' + ','
        val_str = val_str + '%s'  # MySQL批量插入语法是 insert into tb_name values(%s,%s,%s,%s)
        insert_sql = 'insert  into ' + target_table + ' values(' + val_str + ')'  # 拼接insert into 目标表 values  #目标表插入语句
        select_sql = 'select * from ' + source_table  # 源查询SQL，如果有where过滤条件，在这里拼接
        try:
            cur_oracle_result.execute(select_sql)  # 执行
            # temp_cur = oracle_cursor.execute_sql(select_sql, 1000)
        except Exception:
            print(traceback.format_exc() + '查询Oracle源表数据失败，请检查是否存在该表或者表名小写！\n\n' + table_name)
            continue  # 这里需要显式指定continue，否则某张表不存在就会跳出此函数
        source_effectrow = 0
        target_effectrow = 0
        mysql_insert_count = 0
        sql_insert_error = ''
        # mig_table = "'" + table_name + "'"
        # 往MySQL表my_mig_task_info记录开始插入的时间
        try:
            mysql_cursor.execute("""insert into my_mig_task_info(table_name, task_start_time,thread) values ('%s',
        current_timestamp,%s)""" % (table_name, list_index))  # %s占位符的值需要引号包围
        except Exception:
            print(traceback.format_exc())
        while True:
            rows = list(
                cur_oracle_result.fetchmany(
                    fetch_many_count))
            # 例如每次获取2000行，cur_oracle_result.arraysize值决定
            # MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
            # print(cur_oracle_result.description)  # 打印Oracle查询结果集字段列表以及类型
            try:
                mysql_cursor.executemany(insert_sql, rows)  # 批量插入每次5000行，需要注意的是 rows 必须是 list [] 数据类型
                mysql_insert_count = mysql_insert_count + mysql_cursor.rowcount  # 每次插入的行数
                mysql_con0.commit()  # 如果连接池没有配置自动提交，否则这里需要显式提交
            except Exception as e:
                err_count += 1
                # print('\n' + '/* ' + str(e.args) + ' */' + '\n')
                # print(traceback.format_exc())  # 遇到异常记录到log，会继续迁移下张表
                sql_insert_error = '\n' + '/* ' + str(e.args) + ' */' + '\n'
                err_log = str(e.args)
                filename = '/tmp/insert_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('\n-- ' + str(err_count) + ' ' + table_name + ' INSERT ERROR' + '\n')
                f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n\n')
                f.write(insert_sql + '\n\n\n')
                f.write(str(rows[0]) + '\n\n')
                f.write(sql_insert_error + '\n\n')
                f.close()
            # if get_table_count == 0:
            #     print('\n无数据插入')
            # print('\r', str(mysql_insert_count), '/', str(get_table_count), ' ',str(round((mysql_insert_count /
            # get_table_count), 2) * 100) + '%', end='',flush=True)  # 实时刷新插入行数
            if not rows:
                break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表
            source_effectrow = cur_oracle_result.rowcount  # 计数源表插入的行数
            target_effectrow = target_effectrow + mysql_cursor.rowcount  # 计数目标表插入的行数
        if source_effectrow == target_effectrow:
            is_success = 'Y'
        else:
            is_success = 'N'
        if sql_insert_error:
            print(f'[{table_name}] 插入失败  {err_log}\n', end='')
        else:
            print(
                f'[{table_name}] 插入完成 源表行数：{source_effectrow} 目标行数：{target_effectrow}  THREAD {list_index} {str(datetime.datetime.now())}\n',
                end='')
        try:
            mysql_cursor.execute("""update my_mig_task_info set task_end_time=current_timestamp, 
            source_table_rows=%s,
            target_table_rows=%s,
            is_success='%s' where table_name='%s'""" % (
                source_effectrow, target_effectrow, is_success, table_name))  # 占位符需要引号包围
        except Exception:
            print(traceback.format_exc())


def async_work():  # 异步不阻塞方式同时插入表
    if args.data_only.upper() == 'TRUE':
        return 1
    print('#' * 50 + '开始数据迁移' + '#' * 50 + '\n')
    begin_time = datetime.datetime.now()
    index = list_parallel  # 任务序列
    csv_file = open("/tmp/insert_table.csv", 'w', newline='')
    writer = csv.writer(csv_file)
    writer.writerow(('TABLE_NAME', 'TASK_START_TIME', 'TASK_END_TIME', 'THREAD', 'RUN_TIME', 'TOTAL_ROWS', 'INSERT_ROWS'
                     , 'IS_SUCCESS'))
    csv_file.close()
    # 创建迁移任务表，用来统计表插入以及完成的时间
    mysql_cursor.execute("""drop table if exists my_mig_task_info""")
    mysql_cursor.execute("""create table my_mig_task_info(table_name varchar(100),task_start_time datetime,
        task_end_time datetime ,thread int,run_time int,source_table_rows int,target_table_rows int,
        is_success varchar(100))""")
    # 生成异步任务并开启
    with concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix='aa') as executor:
        task = {executor.submit(mig_table_task, v_index): v_index for v_index in index}
        for future in concurrent.futures.as_completed(task):
            task_name = task[future]
            try:
                data = future.done()
            except Exception as exc:
                print('%r generated an exception: %s' % (task_name, exc))
            else:
                # print(str(task_name))
                print(str(datetime.datetime.now()) + ' TASK JOB ' + str(task_name) + ' Is Done:' + str(data))
    end_time = datetime.datetime.now()
    print('表数据迁移耗时：' + str((end_time - begin_time).seconds) + '\n')
    print('#' * 50 + '表数据插入完成' + '#' * 50 + '\n')
    #  计算每张表插入时间
    try:
        mysql_cursor.execute("""update my_mig_task_info set run_time=(UNIX_TIMESTAMP(task_end_time) - UNIX_TIMESTAMP(
    task_start_time))""")
    except Exception:
        print('compute my_mig_task_info error')
    #  表迁移详细记录输出到csv文件
    csv_file = open("/tmp/insert_table.csv", 'a', newline='')
    writer = csv.writer(csv_file)
    mysql_cursor.execute("""select table_name, concat('''',cast(task_start_time as char(20)),''''),
     concat('''',cast(task_end_time as char(20)),''''), cast(thread as char(20)), run_time,
    cast(source_table_rows as char(20)), cast(target_table_rows as char(20)),
    cast(is_success as char(20)) from my_mig_task_info""")
    mig_task_run_detail = mysql_cursor.fetchall()
    for res in mig_task_run_detail:
        writer.writerow(res)
    writer.writerow([str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))])  # 表迁移完成之后插入时间
    csv_file.close()


def mig_table_main_process():  # long long ago used just for test
    p_starttime = datetime.datetime.now()
    p_pro = []
    # 批量调
    for i in range(2):  # 下面先生成多进程执行的动态变量以及把进程加到list，加到list后统一调用进程开始run
        exec('p{} = Process(target=mig_table_task{}, args=({},))'.format(i, i + 1, i))
        exec('p_pro.append(p{})'.format(i))
        # exec('p{}.start()'.format(i))
    for p_t in p_pro:  # 这里对list中存在的进程统一开始调用
        p_t.start()
    for p_t in p_pro:
        p_t.join()
    # 批量调
    time.sleep(0.1)
    p_endtime = datetime.datetime.now()
    print('多进程开始时间：', p_starttime, '多进程结束时间：', p_endtime, '执行时间：', str((p_endtime - p_starttime).seconds), 'seconds')
    time.sleep(0.1)


# 输出函数以及存储过程定义
def ddl_function_procedure():
    sql_path = '/tmp/ddl_function_procedure.sql'  # 用来记录表迁移记录
    if os.path.exists(sql_path):
        os.remove(sql_path)
    index = 0
    filename = '/tmp/ddl_function_procedure.sql'
    f = open(filename, 'a', encoding='utf-8')
    f.write('/*EXPORT FROM ORACLE DATABASE FUNCTION AND PROCEDURE ' + '*/\n')
    f.close()
    try:
        ddl_out = oracle_cursor.fetch_all(
            """SELECT DBMS_METADATA.GET_DDL(U.OBJECT_TYPE, u.object_name) ddl_sql,u.object_name,u.object_type,u.status,(select user from dual) FROM USER_OBJECTS u where U.OBJECT_TYPE IN ('FUNCTION','PROCEDURE','PACKAGE') order by OBJECT_TYPE""")
        for v_out_ddl in ddl_out:
            index += 1
            ddl_sql = str(v_out_ddl[0])
            object_name = v_out_ddl[1]
            object_type = v_out_ddl[2]
            status = v_out_ddl[3]
            current_user = v_out_ddl[4]
            # print("""/* """, '[' + str(index) + ']', object_type, object_name.upper(), '[' + status + ']', """*/""")
            # print((ddl_sql.replace('"' + current_user + '".', '')).replace('"', ''))  # 去掉模式名以及双引号包围
            f = open(filename, 'a', encoding='utf-8')
            f.write('\n/*' + '[' + str(
                index) + '] ' + object_type + ' ' + object_name.upper() + ' [' + status + ']' + '*/\n')
            f.write((ddl_sql.replace('"' + current_user + '".', '')).replace('"', ''))   # 去掉模式名以及双引号包围
            f.close()
    except Exception as e:
        print('查询存储过程以及函数失败！' + str(e))


# 迁移摘要
def mig_summary():
    # Oracle源表信息
    # oracle_schema = oracle_cursor.fetch_one("""select user from dual""")[0]
    oracle_tab_count = all_table_count  # oracle要迁移的表总数
    oracle_view_count = all_view_count  # oracle要创建的视图总数
    oracle_constraint_count = all_constraints_count + function_based_index_count  # oracle的约束以及索引总数
    oracle_fk_count = all_fk_count  # oracle外键总数
    if oracle_autocol_total:
        oracle_autocol_count = oracle_autocol_total[0]  # oracle自增列总数
    else:
        oracle_autocol_count = 0
    # Oracle源表信息

    # MySQL迁移计数
    mysql_cursor.execute("""select database()""")
    mysql_database_name = mysql_cursor.fetchone()[0]
    mysql_success_table_count = str(len(list_success_table))  # mysql创建成功的表总数
    table_failed_count = len(ddl_failed_table_result)  # mysql创建失败的表总数
    mysql_success_view_count = str(all_view_success_count)  # mysql视图创建成功的总数
    view_error_count = all_view_failed_count  # mysql创建视图失败的总数
    mysql_success_incol_count = str(all_inc_col_success_count)  # mysql自增列成功的总数
    autocol_error_count = all_inc_col_failed_count  # mysql自增列失败的总数
    mysql_success_constraint = str(all_constraints_success_count)  # mysql中索引以及约束创建成功的总数
    index_failed_count = len(constraint_failed_count)  # mysql中索引以及约束创建失败的总数
    mysql_success_fk = str(all_fk_success_count)  # mysql中外键创建成功的总数
    fk_failed_count = len(foreignkey_failed_count)  # mysql中外键创建失败的总数
    comment_error_count = len(comment_failed_count)

    # MySQL迁移计数

    print('\033[31m*' * 50 + '数据迁移摘要' + '*' * 50 + '\033[0m\n\n\n')
    print("Oracle迁移数据到MySQL完毕\n" + "开始时间:" + str(mig_start_time) + '\n' + "结束时间:" + str(
        mig_end_time) + '\n' + "耗时:" + str(
        (mig_end_time - mig_start_time).seconds) + "秒\n")
    print('\n\n\n')
    print('目标数据库: ' + mysql_database_name)
    # print('目标表成功创建计数: ' + str(mysql_table_count))
    print('1、表数量总计: ' + str(oracle_tab_count) + ' 目标表创建成功计数: ' + mysql_success_table_count + ' 目标表创建失败计数: ' + str(
        table_failed_count))
    print('2、视图数量总计: ' + str(oracle_view_count) + ' 目标视图创建成功计数: ' + mysql_success_view_count + ' 目标视图创建失败计数: ' + str(
        view_error_count))
    print('3、自增列数量总计: ' + str(
        oracle_autocol_count) + ' 目标自增列创建成功计数: ' + mysql_success_incol_count + ' 目标自增列修改失败计数: ' + str(
        autocol_error_count))
    print('4、触发器数量总计: ' + str(
        normal_trigger_count) + ' 触发器创建成功计数: ' + str(trigger_success_count) + ' 触发器创建失败计数: ' + str(
        trigger_failed_count))
    print('5、索引以及约束总计: ' + str(
        oracle_constraint_count) + ' 目标索引以及约束创建成功计数: ' + mysql_success_constraint + ' 目标索引以及约束创建失败计数: ' + str(
        index_failed_count))
    print('6、外键总计: ' + str(oracle_fk_count) + ' 目标外键创建成功计数: ' + mysql_success_fk + ' 目标外键创建失败计数: ' +
          str(fk_failed_count))
    csv_file = open("/tmp/insert_table.csv", 'a', newline='')
    # 将MySQL创建成功的表总数记录保存到csv文件
    try:
        writer = csv.writer(csv_file)
        writer.writerow(('TOTAL:', mysql_success_table_count))
    except Exception:
        print(traceback.format_exc())
    finally:
        csv_file.close()
    if ddl_failed_table_result:  # 输出失败的对象
        print("\n\n创建失败的表如下：")
        for output_ddl_failed_table_result in ddl_failed_table_result:
            print(output_ddl_failed_table_result)
        print('\n\n\n')
    if view_failed_result:
        print("创建失败的视图如下: ")
        for output_fail_view in view_failed_result:
            print(output_fail_view)
        print('\n\n\n')
    print('\n请检查创建失败的表DDL以及约束。有关更多详细信息，请参阅迁移输出信息')
    print(
        '迁移日志已保存到/tmp/mig.log\n表迁移记录请查看/tmp/insert_table.csv或者查询表my_mig_task_info\n有关迁移错误请查看/tmp/ddl_failed_table.log以及/tmp'
        '/insert_failed_table.log')
    print('源数据库存储过程以及函数ddl定义已保存至/tmp/ddl_function_procedure.sql')


# 清理日志
def clean_log():
    path_file = '/tmp/ddl_success_table.log'  # 用来记录DDL创建成功的表
    if os.path.exists(path_file):
        os.remove(path_file)
    path = '/tmp/ddl_failed_table.log'  # 创建失败的ddl日志文件
    if os.path.exists(path):
        os.remove(path)
    if os.path.exists('/tmp/insert_failed_table.log'):  # 插入失败的表以及sql
        os.remove('/tmp/insert_failed_table.log')
    csv_path = '/tmp/insert_table.csv'  # 用来记录表迁移记录
    if os.path.exists(csv_path):
        os.remove(csv_path)


if __name__ == '__main__':
    mig_start_time = datetime.datetime.now()
    print_source_info()
    clean_log()
    create_meta_table()
    split_success_list()
    async_work()
    mig_table()  # -d option single to mig
    create_meta_constraint()
    create_meta_foreignkey()
    create_trigger_col()
    create_view()
    create_comment()
    ddl_function_procedure()
    mig_end_time = datetime.datetime.now()
    mig_summary()
source_db.close()
