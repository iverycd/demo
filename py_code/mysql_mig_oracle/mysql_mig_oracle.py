# -*- coding: utf-8 -*-
# mysql_mig_oracle.py v1.1 2021.03.23
# MySQL database migration to Oracle

import argparse
import logging
import traceback
import textwrap

import cx_Oracle
import pymysql
import os
import time
import csv
import datetime
import sys
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import concurrent  # 异步任务包

from dbutils.pooled_db import PooledDB

import readConfig

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  # 设置字符集为UTF8，防止中文乱码
config = readConfig.ReadConfig()  # 实例化

"""
Config是一些数据库的配置文件,通过调用我们写的readConfig来获取配置文件中对应值
"""
# MySQL read config
mysql_host = config.get_mysql('host')
mysql_port = int(config.get_mysql('port'))
mysql_user = config.get_mysql('user')
mysql_passwd = config.get_mysql('passwd')
mysql_database = config.get_mysql('database')
mysql_dbchar = config.get_mysql('dbchar')

# oracle read config
oracle_host = config.get_oracle('host')
oracle_port = config.get_oracle('port')
oracle_user = config.get_oracle('user')
oracle_passwd = config.get_oracle('passwd')
service_name = config.get_oracle('service_name')

oracle_conn = cx_Oracle.connect(
    oracle_user + '/' + oracle_passwd + '@' + oracle_host + ':' + oracle_port + '/' + service_name)
oracle_cursor = oracle_conn.cursor()
MySQLPOOL = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=10,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=3,
    # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=[],  # 开始会话前执行的命令列表。使用连接池执行dml，这里需要显式指定提交，已测试通过
    ping=0,
    # ping MySQL服务端，检查是否服务可用。
    host=mysql_host,
    port=mysql_port,
    user=mysql_user,
    password=mysql_passwd,
    database=mysql_database,
    charset=mysql_dbchar
)

mysql_conn = MySQLPOOL.connection()
mysql_cursor = mysql_conn.cursor()

all_table_count = 0
list_success_table = []
ddl_failed_table_result = []
new_list = []  # 用于存储1分为2的表，将原表分成2个list
all_constraints_count = 0  # 约束以及索引总数
all_constraints_success_count = 0  # mysql中创建约束以及索引成功的总数
constraint_failed_count = 0  # 用于统计主键以及索引创建失败的总数
# 视图
all_view_count = 0
all_view_success_count = 0
all_view_failed_count = 0
list_fail_view = []

# 自增列
all_auto_count = 0
all_auto_success_count = 0
all_auto_fail_count = 0

# 外键
fk_failed_count = 0
all_fk_count = 0  # mysql中外键总数
all_fk_success_count = 0  # 外键创建成功计数

# 触发器
all_trigger_count = 0  # 源数据库中触发器总数
all_trigger_success_count = 0  # 目标数据库触发器创建成功的总数
trigger_failed_count = 0  # 目标触发器创建失败的总数



parser = argparse.ArgumentParser(prog='mysql_mig_oracle',
                                 formatter_class=argparse.RawDescriptionHelpFormatter,
                                 description=textwrap.dedent('''\
EXAMPLE:
    eg(1):perform migration fetch and insert 10000 rows data into table:\n ./mysql_mig_oracle -b 10000\n
    eg(2):perform custom table migration to oracle:\n ./mysql_mig_oracle -c true'''))
parser.add_argument('--batch_size', '-b', help='fetch and insert row size,default 10000', type=int)
parser.add_argument('--custom_table', '-c', help='create and mig some tables not all tables into oracle,default false',
                    choices=['true', 'false'], default='false')  # 默认是全表迁移
parser.add_argument('--data_only', '-d', help='mig only data row,truncate table before mig data', choices=['true', 'false'], default='false')
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

mysql_cursor.arraysize = row_batch_size
oracle_cursor.arraysize = row_batch_size

# 判断命令行参数-c是否指定
if args.custom_table.upper() == 'TRUE' or args.data_only.upper() == 'TRUE':
    custom_table = 'true'
    path_file = '/tmp/table.txt'  # 用来记录DDL创建成功的表
    if os.path.exists(path_file):
        os.remove(path_file)
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
# list_parallel = ['0', '1', '2', '3', '4', '5', '6', '7']


# 打印连接信息
def print_source_info():
    print('-' * 50 + 'MySQL->Oracle' + '-' * 50)
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    print('源数据库连接信息: ' + '数据库名: ' + str(mysql_database) + ' IP: ' + str(mysql_host))
    print('目标数据库连接信息: ' + '模式名: ' + str(oracle_user) + ' IP: ' + str(oracle_host))
    print('\n要迁移的表如下:')
    if custom_table.upper() == 'TRUE':
        with open("/tmp/table.txt", "r") as f:  # 打开文件
            for line in f:
                print(line.upper())
    is_continue = input('\n是否准备迁移数据：Y|N\n')
    if is_continue == 'Y' or is_continue == 'y':
        print('开始迁移数据')  # continue
    else:
        sys.exit()


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


sys.stdout = Logger(stream=sys.stdout)


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
    n = v_max_workers  # 平均分成2份list
    # print('切片的大小:', n)
    # print('原始list：', list_success_table, '\n')
    new_list.append(bisector_list(list_success_table, n))
    # print('一分为二：', new_list)
    # print('新的list长度：', len(new_list[0]))  # 分了几个片，如果一分为二就是2，如过分片不足就是1


# 获取MySQL表结构
def tbl_columns(table_name):
    list_varchar = ['VARCHAR', 'CHAR']
    list_text = ['LONGTEXT', 'MEDIUMTEXT', 'TEXT', 'TINYTEXT']
    list_int = ['INT', 'MEDIUMINT', 'BIGINT', 'TINYINT']
    list_non_int = ['DECIMAL', 'DOUBLE', 'FLOAT']
    list_time = ['DATETIME', 'TIMESTAMP']
    list_lob = ['TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB']
    list_bit = ['BIT']
    # sql = """SHOW FULL COLUMNS FROM %s""" % table_name
    sql = """select column_name,data_type,character_maximum_length,is_nullable,column_default,case when numeric_precision >38 then 38 else  numeric_precision end as numeric_precision,numeric_scale,datetime_precision,column_key,column_comment from information_schema.COLUMNS where table_schema in (select database()) and table_name='%s'""" % table_name
    mysql_cursor.execute(sql)
    output_table_col = mysql_cursor.fetchall()
    result = []
    for column in output_table_col:  # 按照游标行遍历字段
        # mysql column description
        # result.append({'column_name': column[0],  # 如下为字段的名称
        #                'data_type': column[1],  # 列字段类型
        #                'character_maximum_length': column[2], # 列字段长度范围
        #                'is_nullable': column[3],  # 是否为空
        #                'column_default': column[4],  # 字段默认值
        #                'numeric_precision': column[5],
        #                'numeric_scale': column[6],
        #                'datetime_precision': column[7],
        #                'column_key': column[8],
        #                'column_comment': column[9]
        #                }
        #               )
        if column[1].upper() in list_varchar:  # mysql中普通字符串类型均映射为oracle的普通类型字符串
            if column[2] >= 1300:
                result.append({'column_name': column[0],  # 如下为字段的名称
                               'data_type': 'varchar2' + '(' + '4000' + ')',  # 列字段类型与长度的拼接，这里显式指定为字符存储
                               'character_maximum_length': column[2],
                               'is_nullable': column[3],  # 是否为空
                               'column_default': column[4],  # 字段默认值
                               'numeric_precision': column[5],
                               'numeric_scale': column[6],
                               'datetime_precision': column[7],
                               'column_key': column[8],
                               'column_comment': column[9]
                               }
                              )
            else:
                result.append({'column_name': column[0],  # 如下为字段的名称
                               'data_type': 'varchar2' + '(' + str(column[2] * 3) + ')',  # 列字段类型与长度的拼接，这里显式指定为字符存储
                               'character_maximum_length': column[2],
                               'is_nullable': column[3],  # 是否为空
                               'column_default': column[4],  # 字段默认值
                               'numeric_precision': column[5],
                               'numeric_scale': column[6],
                               'datetime_precision': column[7],
                               'column_key': column[8],
                               'column_comment': column[9]
                               }
                              )
        elif column[1].upper() in list_text:  # mysql中所有text类型均映射为oracle的clob
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': 'CLOB',  # 列字段类型与长度的拼接，这里显式指定为字符存储
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )
        #  mysql整数类型映射
        elif column[1].upper() in list_int:  # mysql中MEDIUMINT类型均映射为oracle的number
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': 'NUMBER',  # 列字段类型与长度的拼接，这里显式指定为字符存储
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )
        #  mysql非整数类型数字映射
        #  mysql的decimal,double,float均映射为oracle的number
        elif column[1].upper() in list_non_int:  # mysql中MEDIUMINT类型均映射为oracle的number
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': 'NUMBER' if str(column[6]).upper() == 'NONE'
                           else 'NUMBER' + '(' + str(column[5]) + ',' + str(column[6]) + ')',
                           # 如果有精度指定精度，否则如果numeric_scale为null，直接映射为oracle的number
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )
        # mysql时间类型映射
        elif column[1].upper() in list_time:  # mysql中datetime,timestamp映射为oracle的date
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': 'date',
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],
                           # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )
        # mysql大字段类型映射
        elif column[1].upper() in list_lob:  # mysql中二进制大字段映射为oracle的blob
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': 'BLOB',
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )
        # mysql的bit类型映射
        elif column[1].upper() in list_bit:  # mysql中bit字段映射为oracle的number(1)
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': 'number',
                           'character_maximum_length': 1,
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )
        # 若不在以上数据类型，就原模原样使用原始ddl建表
        else:
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': column[1],  # 列字段类型与长度的拼接，这里显式指定为字符存储
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )

    return result


def create_meta_table():
    output_table_name = []  # 用于存储要迁移的部分表
    src_tbl_count = 0
    if args.data_only.upper() == 'TRUE':
        return 1
    if custom_table.upper() == 'TRUE':
        with open("/tmp/table.txt", "r") as f:  # 打开文件
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))
    else:
        tableoutput_sql = """select table_name from information_schema.tables where table_schema in (select database())  and TABLE_TYPE='BASE TABLE' """  # 查询需要导出的表
        mysql_cursor.execute(tableoutput_sql)
        output_table_name = mysql_cursor.fetchall()
    global all_table_count  # 将源表总数存入全局变量
    all_table_count = len(output_table_name)  # 无论是自定义表还是全库，都可以存入全局变量
    task_starttime = datetime.datetime.now()
    table_index = 0
    for row in output_table_name:
        table_name = row[0]
        print('#' * 50 + '开始创建表' + table_name + '#' * 50)
        #  将创建失败的sql记录到log文件
        logging.basicConfig(filename='/tmp/ddl_failed_table.log')
        # 在创建表前先删除存在的表
        try:
            oracle_cursor.execute("""select count(*) from user_tables where TABLE_NAME=upper('%s') """ % table_name)
            src_tbl_count = oracle_cursor.fetchone()[0]
        except Exception:
            print('error count(*)')
            print(traceback.format_exc())
        if src_tbl_count > 0:
            try:
                oracle_cursor.execute("""drop table %s""" % table_name)
                # target_db.commit()
            except Exception:
                print('drop table ' + table_name + ' error')
                print(traceback.format_exc())
        fieldinfos = []
        structs = tbl_columns(table_name)  # 获取源表的表字段信息
        # print(structs)  # mysql中各列字段属性
        # MySQL字段类型拼接为oracle字段类型
        for struct in structs:
            default_value = struct.get('column_default')
            is_nullable = struct.get('is_nullable')
            comment_value = struct.get('column_comment')
            if is_nullable == 'YES':  # 字段是否为空的判断
                is_nullable = ''  # 可为null值
            else:
                is_nullable = 'not null'  # 不可为null值
            if default_value:  # 对默认值以及注释数据类型的判断，如果不是str类型，转为str类型
                if default_value.upper() == 'CURRENT_TIMESTAMP':
                    default_value = 'sysdate'
                else:
                    default_value = """'""" + default_value + """'"""
            fieldinfos.append('{0} {1} {2} {3}'.format(
                struct['column_name'],
                struct['data_type'],
                ('default ' + default_value) if default_value else '',
                is_nullable  # 如果is_nullable
            ),
            )
        create_table_sql = 'create table {0} ({1})'.format(table_name, ','.join(fieldinfos))  # 生成创建目标表的sql
        print('\n创建表:' + table_name + '\n')
        print(create_table_sql)
        try:
            oracle_cursor.execute(create_table_sql)
            print(table_name + '表创建完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '\n')
            filename = '/tmp/ddl_success_table.log'  # ddl创建成功的表，记录表名到/tmp/ddl_success_table.csv
            f = open(filename, 'a', encoding='utf-8')
            f.write(table_name + '\n')
            f.close()
            list_success_table.append(table_name)  # MySQL ddl创建成功的表也存到list中
        except Exception as e:
            table_index = table_index + 1
            print('\n' + '/* ' + str(e.args) + ' */' + '\n')  # ddl错误输出异常
            print(traceback.format_exc())  # 如果某张表创建失败，遇到异常记录到log，会继续创建下张表
            # ddl创建失败的表名记录到文件/tmp/ddl_failed_table.log
            filename = '/tmp/ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('-' * 50 + 'CREATE TABLE ERROR ' + str(table_index) + '-' * 50 + '\n')
            f.write('/* ' + table_name + ' */' + '\n')
            f.write(create_table_sql + '\n\n\n')
            f.write('\n' + '/* ' + str(traceback.format_exc()) + ' */' + '\n')
            f.close()
            ddl_failed_table_result.append(table_name)  # 将当前ddl创建失败的表名记录到ddl_failed_table_result的list中
            print('表' + table_name + '创建失败请检查ddl语句!\n')
    task_endtime = datetime.datetime.now()
    print("表创建耗时\n" + "开始时间:" + str(task_starttime) + '\n' + "结束时间:" + str(task_endtime) + '\n' + "消耗时间:" + str(
        (task_endtime - task_starttime).seconds) + "秒\n")
    print('#' * 50 + '表创建完成' + '#' * 50 + '\n\n\n')


def mig_table():  # 只有指定了-d选项才会执行此单步迁移
    task_starttime = datetime.datetime.now()
    start_time = datetime.datetime.now()
    err_count = 0
    get_table_count = 0
    if args.data_only.upper() == 'FALSE':
        return 1
    with open("/tmp/table.txt", "r") as f:  # 读取自定义表
        for table_name in f.readlines():  # 按顺序读取每一个表
            table_name = table_name.strip('\n').upper()  # 去掉列表中每一个元素的换行符
            try:
                mysql_cursor.execute("""select count(*) from %s""" % table_name)
                get_table_count = mysql_cursor.fetchone()[0]
                mysql_cursor.execute(
                    """SELECT count(*) FROM information_schema.COLUMNS WHERE table_schema IN (SELECT DATABASE()) AND table_name = '%s' """ % table_name)
                col_len = mysql_cursor.fetchone()[0]  # 将游标结果数组的值赋值，该值为表列字段总数
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
            val_str = ''  # 用于生成批量插入的列字段变量
            for i in range(1, col_len):
                val_str = val_str + ':' + str(i) + ','
            val_str = val_str + ':' + str(col_len)  # Oracle批量插入语法是 insert into tb_name values(:1,:2,:3)
            insert_sql = 'insert into ' + table_name + ' values(' + val_str + ')'  # 拼接insert into 目标表 values  #目标表插入语句
            select_sql = 'select * from ' + table_name  # 源查询SQL，如果有where过滤条件，在这里拼接
            try:
                mysql_cursor.execute(select_sql)  # 执行
                oracle_cursor.execute("""truncate table %s """ % table_name)
            except Exception:
                print(traceback.format_exc() + '查询MySQL源表数据失败，请检查是否存在该表或者表名小写！\n\n' + table_name)
            source_effectrow = 0
            target_effectrow = 0
            oracle_insert_count = 0
            sql_insert_error = ''
            # mig_table = "'" + table_name + "'"
            # 往MySQL表my_mig_task_info记录开始插入的时间
            try:
                oracle_cursor.execute("""insert into my_mig_task_info(table_name, task_start_time) values ('%s',
                                    sysdate)""" % table_name)  # %s占位符的值需要引号包围
                oracle_conn.commit()
            except Exception:
                print(traceback.format_exc())
            while True:
                rows = list(mysql_cursor.fetchmany(row_batch_size))
                # 例如每次获取2000行，cur_oracle_result.arraysize值决定
                # MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
                # print(cur_oracle_result.description)  # 打印查询结果集字段列表以及类型
                try:
                    oracle_cursor.executemany(insert_sql, rows)  # 批量插入每次5000行，需要注意的是 rows 必须是 list [] 数据类型
                    oracle_insert_count = oracle_insert_count + oracle_cursor.rowcount  # 每次插入的行数
                    # target_db.commit()  # 如果连接池没有配置自动提交，否则这里需要显式提交
                except Exception as e:
                    err_count += 1
                    print('\n' + '/* ' + str(e) + ' */' + '\n')
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
                # if get_table_count == 0:
                #     print('\n无数据插入')
                # print('\r', str(oracle_insert_count), '/', str(get_table_count), ' ',str(round((oracle_insert_count /
                # get_table_count), 2) * 100) + '%', end='',flush=True)  # 实时刷新插入行数
                if not rows:
                    break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表
                source_effectrow = mysql_cursor.rowcount  # 计数源表插入的行数
                target_effectrow = target_effectrow + oracle_cursor.rowcount  # 计数目标表插入的行数
            task_endtime = datetime.datetime.now()
            task_exec_time = str((task_endtime - task_starttime).seconds)
            if source_effectrow == target_effectrow:
                is_success = 'Y'
            else:
                is_success = 'N'
            if sql_insert_error:
                print('\n' + table_name + '插入失败')
            else:
                print(
                    f'[{table_name}] 插入完成 源表行数：{source_effectrow} 目标行数：{target_effectrow}  耗时：{task_exec_time}\n',
                    end='')
            try:
                oracle_cursor.execute("""update my_mig_task_info set task_end_time=sysdate,
                                        source_table_rows=%s,
                                        target_table_rows=%s,
                                        is_success='%s' where table_name='%s'""" % (
                    source_effectrow, target_effectrow, is_success, table_name))  # 占位符需要引号包围
                oracle_conn.commit()
            except Exception:
                print(traceback.format_exc())
    end_time = datetime.datetime.now()
    print("\n\n数据迁移耗时:" + str((end_time - start_time).seconds) + "秒\n")


def mig_table_task(list_index):
    err_count = 0
    list_number = list_parallel
    if args.data_only.upper() == 'TRUE':
        return 1
    if list_index in list_number:
        mysql_con = MySQLPOOL.connection()
        mysql_cursor = mysql_con.cursor()  # MySQL连接池
        mysql_cursor.arraysize = row_batch_size
        oracle_conn = cx_Oracle.connect(
            oracle_user + '/' + oracle_passwd + '@' + oracle_host + ':' + oracle_port + '/' + service_name)
        oracle_cursor = oracle_conn.cursor()
        oracle_cursor.arraysize = row_batch_size  # oracle数据库游标对象结果集返回的行数即每次获取多少行
    # print('游标arraysize:' + str(mysql_cursor.arraysize))
    task_starttime = datetime.datetime.now()
    for v_table_name in new_list[0][int(list_index)]:
        table_name = v_table_name
        # print('\n正在迁移表' + table_name + ' thread ' + str(list_index) + ' ' + str(datetime.datetime.now()))
        # print('THREAD ' + str(list_index) + ' ' + str(datetime.datetime.now()) + ' \n')
        try:
            mysql_cursor.execute("""select count(*) from %s""" % table_name)
            get_table_count = mysql_cursor.fetchone()[0]
            mysql_cursor.execute(
                """SELECT count(*) FROM information_schema.COLUMNS WHERE table_schema IN (SELECT DATABASE()) AND table_name = '%s' """ % table_name)
            col_len = mysql_cursor.fetchone()[0]  # 将游标结果数组的值赋值，该值为表列字段总数
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
            continue  # 这里需要显式指定continue，否则表不存在或者其他问题，会直接跳出for循环
        val_str = ''  # 用于生成批量插入的列字段变量
        for i in range(1, col_len):
            val_str = val_str + ':' + str(i) + ','
        val_str = val_str + ':' + str(col_len)  # Oracle批量插入语法是 insert into tb_name values(:1,:2,:3)
        insert_sql = 'insert into ' + table_name + ' values(' + val_str + ')'  # 拼接insert into 目标表 values  #目标表插入语句
        select_sql = 'select * from ' + table_name  # 源查询SQL，如果有where过滤条件，在这里拼接
        try:
            mysql_cursor.execute(select_sql)  # 执行
        except Exception:
            print(traceback.format_exc() + '查询mysql源表数据失败，请检查是否存在该表或者表名小写！\n\n' + table_name)
            continue  # 这里需要显式指定continue，否则某张表不存在就会跳出此函数
        # print("正在执行插入表:", table_name)
        # print(datetime.datetime.now())
        source_effectrow = 0
        target_effectrow = 0
        oracle_insert_count = 0
        sql_insert_error = ''
        # mig_table = "'" + table_name + "'"
        # 往MySQL表my_mig_task_info记录开始插入的时间
        try:
            oracle_cursor.execute("""insert into my_mig_task_info(table_name, task_start_time,thread) values ('%s',
        sysdate,%s)""" % (table_name, list_index))  # %s占位符的值需要引号包围
            oracle_conn.commit()
        except Exception:
            print(traceback.format_exc())
        while True:
            rows = list(mysql_cursor.fetchmany(row_batch_size))
            # 例如每次获取2000行，cur_oracle_result.arraysize值决定
            # MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
            # print(cur_oracle_result.description)  # 打印oracle查询结果集字段列表以及类型
            try:
                oracle_cursor.executemany(insert_sql, rows)  # 批量插入每次5000行，需要注意的是 rows 必须是 list [] 数据类型
                oracle_insert_count = oracle_insert_count + oracle_cursor.rowcount  # 每次插入的行数
                oracle_conn.commit()
            except Exception as e:
                oracle_conn.rollback()
                err_count += 1
                print(traceback.format_exc())
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
            # if get_table_count == 0:
            #     print('\n无数据插入')
            # print('\r', str(mysql_insert_count), '/', str(get_table_count), ' ',str(round((mysql_insert_count /
            # get_table_count), 2) * 100) + '%', end='',flush=True)  # 实时刷新插入行数
            if not rows:
                break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表
            source_effectrow = mysql_cursor.rowcount  # 计数源表插入的行数
            target_effectrow = target_effectrow + oracle_cursor.rowcount  # 计数目标表插入的行数

        if source_effectrow == target_effectrow:
            is_success = 'Y'
        else:
            is_success = 'N'
        if sql_insert_error:
            print('\n' + table_name + '插入失败')
        else:
            print(
                f'[{table_name}] 插入完成 源表行数：{source_effectrow} 目标行数：{target_effectrow}  THREAD {list_index} {str(datetime.datetime.now())}\n',
                end='')
        try:
            oracle_cursor.execute("""update my_mig_task_info set task_end_time=sysdate,source_table_rows=%s,
            target_table_rows=%s,
            is_success='%s' where table_name='%s'""" % (
                source_effectrow, target_effectrow, is_success, table_name))  # 占位符需要引号包围
            oracle_conn.commit()
        except Exception:
            print(traceback.format_exc())
    task_endtime = datetime.datetime.now()
    task_exec_time = str((task_endtime - task_starttime).seconds)
    # print("第" + str(list_index) + "部分的表\n" + "开始时间:" + str(task_starttime) + '\n' + "结束时间:" + str(
    #     task_endtime) + '\n' + "消耗时间:" + task_exec_time + "秒\n")
    # print('第' + str(list_index) + '部分表迁移完成\n\n\n')


def async_work():  # 异步不阻塞方式同时插入表
    src_tbl_count = 0
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
    try:
        oracle_cursor.execute("""select count(*) from user_tables where TABLE_NAME=upper('my_mig_task_info') """)
        src_tbl_count = oracle_cursor.fetchone()[0]
    except Exception:
        print('error count(*)')
        print(traceback.format_exc())
    if src_tbl_count > 0:
        try:
            oracle_cursor.execute("""drop table my_mig_task_info""")
            oracle_conn.commit()
        except Exception:
            print('drop table ' + ' error')
            print(traceback.format_exc())
    try:
        # 创建迁移任务表，用来统计表插入以及完成的时间
        oracle_cursor.execute("""create table my_mig_task_info(table_name varchar2(100),task_start_time date ,
                task_end_time date ,thread number,run_time number,source_table_rows number,target_table_rows number,
                is_success varchar2(100))""")
    except Exception as e:
        print(e)

    # 生成异步任务并开启
    with concurrent.futures.ThreadPoolExecutor(max_workers=v_max_workers) as executor:
        task = {executor.submit(mig_table_task, v_index): v_index for v_index in index}
        for future in concurrent.futures.as_completed(task):
            task_name = task[future]
            try:
                data = future.done()
            except Exception as exc:
                print('%r generated an exception: %s' % (task_name, exc))
            # print('begin task' + str(task_name))
    end_time = datetime.datetime.now()
    print('表数据迁移耗时：' + str((end_time - begin_time).seconds) + '\n')
    print('#' * 50 + '表数据插入完成' + '#' * 50 + '\n')
    #  计算每张表插入时间
    try:
        oracle_cursor.execute(
            """update my_mig_task_info set run_time=(ROUND(TO_NUMBER(TASK_END_TIME - TASK_START_TIME) * 24 * 60 * 60))""")
        oracle_conn.commit()
    except Exception:
        print('compute my_mig_task_info error')
    # 表迁移详细记录输出到csv文件
    csv_file = open("/tmp/insert_table.csv", 'a', newline='')
    writer = csv.writer(csv_file)
    oracle_cursor.execute("""select upper(table_name), ''''||to_char(task_start_time,'yyyy-mm-dd hh24:mi:ss')||'''',
     ''''||to_char(task_end_time,'yyyy-mm-dd hh24:mi:ss')||'''', to_char(thread), run_time,
    to_char(source_table_rows), to_char(target_table_rows),
    to_char(is_success) from my_mig_task_info order by table_name""")
    mig_task_run_detail = oracle_cursor.fetchall()
    for res in mig_task_run_detail:
        writer.writerow(res)
    writer.writerow([str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))])  # 表迁移完成之后插入时间
    csv_file.close()


# 批量创建主键以及索引
def create_meta_constraint():
    global all_constraints_count  # 源数据库约束以及索引总数
    global all_constraints_success_count  # 目标中约束以及索引创建成功的计数
    global constraint_failed_count  # 目标数据库索引以及约束创建失败的计数
    err_count = 0
    output_table_name = []  # 迁移部分表
    all_index = []  # 存储执行创建约束的结果集
    start_time = datetime.datetime.now()
    if args.data_only.upper() == 'TRUE':
        return 1
    print('#' * 50 + '开始创建' + '约束以及索引 ' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    if custom_table.upper() == 'TRUE':  # 如果命令行参数有-c选项，仅创建部分约束
        with open("/tmp/table.txt", "r") as f:  # 读取自定义表
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))  # 将自定义表全部保存到list
        for v_custom_table in output_table_name:  # 读取第N个表查询生成拼接sql
            mysql_cursor.execute("""SELECT 
IF(
INDEX_NAME='PRIMARY',CONCAT('ALTER TABLE ',TABLE_NAME,' ', 'ADD ', -- 主键的判断，下面是主键的拼接sql
 IF(NON_UNIQUE = 1,
 CASE UPPER(INDEX_TYPE)
 WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX'
 WHEN 'SPATIAL' THEN 'SPATIAL INDEX'
 ELSE CONCAT('INDEX ',
  INDEX_NAME,
  '  '
 )
END,
IF(UPPER(INDEX_NAME) = 'PRIMARY',
 CONCAT('PRIMARY KEY '
 ),
CONCAT('UNIQUE INDEX ',
 INDEX_NAME
)
)
),'(', GROUP_CONCAT(DISTINCT CONCAT('', COLUMN_NAME, '') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');')
,
if(
UPPER(INDEX_NAME) != 'PRIMARY' and non_unique=0, -- 判断是否是唯一索引
CONCAT('CREATE UNIQUE INDEX ',substr(index_name,1,19),'_',substr(MD5(RAND()),1,3),' ON ',table_name,'(', GROUP_CONCAT(DISTINCT CONCAT('', COLUMN_NAME, '') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');'), -- 唯一索引的拼接sql
CONCAT('CREATE INDEX ',substr(index_name,1,19),'_',substr(MD5(RAND()),1,3), ' ON ',-- 非唯一索引，普通索引的拼接sql
 IF(NON_UNIQUE = 1,
 CASE UPPER(INDEX_TYPE)
 WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX'
 WHEN 'SPATIAL' THEN 'SPATIAL INDEX'
 ELSE CONCAT(' ',
  table_name,
  ''
 )
END,
IF(UPPER(INDEX_NAME) = 'PRIMARY',
 CONCAT('PRIMARY KEY '
 ),
CONCAT(table_name,' xxx'
)
)
),'(', GROUP_CONCAT(DISTINCT CONCAT('', COLUMN_NAME, '') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');')
)

) sql_text
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA in (select database())  and table_name='%s'
GROUP BY TABLE_NAME, INDEX_NAME
ORDER BY TABLE_NAME ASC, INDEX_NAME ASC""" % v_custom_table[0])
            custom_index = mysql_cursor.fetchall()
            for v_out in custom_index:  # 每次将上面单表全部结果集全部存到all_index的list里面
                all_index.append(v_out)
    else:  # 命令行参数没有-c选项，创建所有约束
        mysql_cursor.execute("""SELECT 
IF(
INDEX_NAME='PRIMARY',CONCAT('ALTER TABLE ',TABLE_NAME,' ', 'ADD ', -- 主键的判断，下面是主键的拼接sql
 IF(NON_UNIQUE = 1,
 CASE UPPER(INDEX_TYPE)
 WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX'
 WHEN 'SPATIAL' THEN 'SPATIAL INDEX'
 ELSE CONCAT('INDEX ',
  INDEX_NAME,
  '  '
 )
END,
IF(UPPER(INDEX_NAME) = 'PRIMARY',
 CONCAT('PRIMARY KEY '
 ),
CONCAT('UNIQUE INDEX ',
 INDEX_NAME
)
)
),'(', GROUP_CONCAT(DISTINCT CONCAT('', COLUMN_NAME, '') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');')
,
if(
UPPER(INDEX_NAME) != 'PRIMARY' and non_unique=0, -- 判断是否是唯一索引
CONCAT('CREATE UNIQUE INDEX ',substr(index_name,1,19),'_',substr(MD5(RAND()),1,3),' ON ',table_name,'(', GROUP_CONCAT(DISTINCT CONCAT('', COLUMN_NAME, '') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');'), -- 唯一索引的拼接sql
CONCAT('CREATE INDEX ',substr(index_name,1,19),'_',substr(MD5(RAND()),1,3), ' ON ',-- 非唯一索引，普通索引的拼接sql
 IF(NON_UNIQUE = 1,
 CASE UPPER(INDEX_TYPE)
 WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX'
 WHEN 'SPATIAL' THEN 'SPATIAL INDEX'
 ELSE CONCAT(' ',
  table_name,
  ''
 )
END,
IF(UPPER(INDEX_NAME) = 'PRIMARY',
 CONCAT('PRIMARY KEY '
 ),
CONCAT(table_name,' xxx'
)
)
),'(', GROUP_CONCAT(DISTINCT CONCAT('', COLUMN_NAME, '') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');')
)

) sql_text
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA in (select database())
GROUP BY TABLE_NAME, INDEX_NAME
ORDER BY TABLE_NAME ASC, INDEX_NAME ASC""")
        all_index = mysql_cursor.fetchall()  # 如果要每张表查使用T.TABLE_NAME = '%s',%s传进去是没有单引号，所以需要用单引号号包围
    all_constraints_count = len(all_index)
    for d in all_index:
        create_index_sql = d[0]
        create_index_sql = create_index_sql.replace(';', '')
        print(create_index_sql)
        try:
            oracle_cursor.execute(create_index_sql)
            # oracle_conn.commit()
            print('约束以及索引创建完毕\n')
            all_constraints_success_count += 1
        except Exception as e:
            # oracle_conn.rollback()
            print(traceback.format_exc())
            err_count += 1
            constraint_failed_count += 1  # 用来统计主键或者索引创建失败的计数，只要创建失败就往list存1
            print('\n' + '/* ' + str(e.args) + ' */' + '\n')
            print('约束或者索引创建失败请检查ddl语句!\n')
            # print(traceback.format_exc())
            filename = '/tmp/ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('-' * 50 + str(err_count) + ' CONSTRAINTS CREATE ERROR' + '-' * 50 + '\n')
            f.write(create_index_sql + '\n\n\n')
            f.write('\n' + '/* ' + str(traceback.format_exc()) + ' */' + '\n')
            f.close()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    end_time = datetime.datetime.now()
    print('创建约束以及索引耗时： ' + str((end_time - start_time).seconds))
    print('#' * 50 + '主键约束、索引创建完成' + '#' * 50 + '\n\n\n')


def create_view():
    global all_view_count, all_view_success_count, all_view_failed_count
    if args.data_only.upper() == 'TRUE' or args.custom_table.upper() == 'TRUE':  # 如果指定-d选项就退出函数
        return 1
    mysql_cursor.execute(
        """select table_name from information_schema.views where table_schema in (select database())""")
    view_name = mysql_cursor.fetchall()
    all_view_count = len(view_name)
    index = 0
    print('#' * 50 + '开始创建' + '视图 ' + '#' * 50)
    print('create view sql:')
    for v_view_name in view_name:
        index += 1
        mysql_cursor.execute("""SHOW CREATE VIEW %s""" % v_view_name)  # 获取单个视图的定义
        view_info = mysql_cursor.fetchall()
        for v_out in view_info:  # 对单个视图的定义做文本处理，替换文本
            view_name = v_out[0]
            view_define = v_out[1]
            # print(view_name.upper())
            # print('original view sql: ' + view_define)
            format_sql1 = view_define[view_define.rfind('VIEW'):]
            # print('format_sql1: ' + format_sql1)
            format_sql2 = format_sql1.replace('`', '')
            # print('format_sql2: ' + format_sql2)
            create_view_sql = 'create or replace force ' + format_sql2  # 创建视图的原始sql
            create_view_sql_out = create_view_sql.upper()  # 对创建视图的文本全部大写,这里结尾无需加分号;
            create_view_sql_out = create_view_sql_out.replace('CONVERT(', '')  # 去掉MySQL视图的CONVERT
            create_view_sql_out = create_view_sql_out.replace('USING UTF8MB4)', '')  # 去掉MySQL视图的UTF8MB4
            if view_name.upper() == 'VIEW_FRAME_OU':  # 对以下特定视图做个别处理
                create_view_sql_out = create_view_sql_out.replace('FROM (FRAME_OU JOIN FRAME_OU_EXTENDINFO) WHERE',
                                                                  'FROM FRAME_OU JOIN FRAME_OU_EXTENDINFO on')
            if view_name.upper() == 'VIEW_FRAME_USER':
                create_view_sql_out = create_view_sql_out.replace('FROM (FRAME_USER JOIN FRAME_USER_EXTENDINFO) WHERE',
                                                                  'FROM FRAME_USER JOIN FRAME_USER_EXTENDINFO on')
            if view_name.upper() == 'VIEW_PERSONAL_ELEMENT':
                create_view_sql_out = create_view_sql_out.replace(
                    'FROM ((PERSONAL_PORTAL_ELEMENT A JOIN APP_ELEMENT B) JOIN APP_PORTAL_ELEMENT C ON(((C.ELEMENTGUID = B.ROWGUID ) AND (A.PTROWGUID = C.ROWGUID))))',
                    'FROM (PERSONAL_PORTAL_ELEMENT A JOIN APP_PORTAL_ELEMENT C ON A.PTROWGUID = C.ROWGUID) JOIN APP_ELEMENT B ON C.ELEMENTGUID = B.ROWGUID')
            print('[' + str(index) + '] ' + create_view_sql_out + '\n')  # 剩余的执行原始视图
            try:
                oracle_cursor.execute(create_view_sql_out)
                oracle_conn.commit()
                all_view_success_count += 1
            except Exception as e:
                oracle_conn.rollback()
                all_view_failed_count += 1
                print('视图创建失败 ' + str(e))
                filename = '/tmp/ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-' * 50 + 'CREATE VIEW ' + view_name.upper() + ' ERROR ' + str(
                    all_view_failed_count) + '-' * 50 + '\n')
                f.write(create_view_sql_out)
                f.write('\n' + str(e.args) + '\n')
                f.close()
                list_fail_view.append(create_view_sql_out)
                if view_name.upper() == 'VIEW_PERSONAL_ELEMENT':
                    print(
                        'use this fix it: FROM (PERSONAL_PORTAL_ELEMENT A JOIN APP_PORTAL_ELEMENT C ON A.PTROWGUID = C.ROWGUID) JOIN APP_ELEMENT B ON C.ELEMENTGUID = B.ROWGUID')
    print('\n视图创建完毕!\n')
    if all_view_failed_count:
        print('创建视图失败的sql如下:\n')
    for v_fail_sql in list_fail_view:
        print(v_fail_sql)


def create_auto_column():
    global all_auto_count, all_auto_success_count, all_auto_fail_count
    output_table_name = []  # 用于存储要迁移的部分表
    oracle_seq_sql = ''
    oracle_add_auto = ''
    print('#' * 50 + '开始修改自增列' + '#' * 50)
    if args.data_only.upper() == 'TRUE':
        return 1
    if custom_table.upper() == 'TRUE':  # 根据自定义表创建自增列
        with open("/tmp/table.txt", "r") as f:  # 打开文件
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))
        for v_custom_table in output_table_name:  # 读取第N个表查询生成拼接sql
            sequence_sql = """select table_name,COLUMN_NAME,upper(concat(TABLE_NAME,'_',COLUMN_NAME,'_seq')) sequence_name from information_schema. COLUMNS where
TABLE_SCHEMA in (select database()) and table_name ='%s' and EXTRA='auto_increment'""" % v_custom_table[0]
            mysql_cursor.execute(sequence_sql)
            output_name = mysql_cursor.fetchall()  # 获取所有的自增列
            all_auto_count = len(output_name)
            for v_output_name in output_name:
                table_name = v_output_name[0]  # 获取表名
                column_name = v_output_name[1]  # 获取列名
                sequence_name = v_output_name[2]  # 生成序列名称
                try:
                    mysql_cursor.execute("""SELECT Auto_increment FROM information_schema.TABLES WHERE Table_Schema in (select database())
                            AND table_name ='%s'""" % table_name)
                    auto_column_startval = mysql_cursor.fetchone()[0]  # 获取自增列起始值
                    oracle_seq_sql = """CREATE SEQUENCE %s INCREMENT BY 1 START %s """ % (
                        sequence_name, auto_column_startval)
                    print(oracle_seq_sql + ';')
                    oracle_cursor.execute("""DROP SEQUENCE IF EXISTS %s """ % sequence_name)
                    oracle_cursor.execute(oracle_seq_sql)
                    oracle_conn.commit()
                except Exception as e:
                    oracle_conn.rollback()
                    print(oracle_seq_sql + ';创建序列失败！' + str(e.args))
                    filename = '/tmp/ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('-' * 50 + 'CREATE SEQUENCE ERROR ' + '-' * 50 + '\n')
                    f.write(oracle_seq_sql + ';')
                    f.write(str(e.args) + '\n')
                    f.close()
                try:
                    oracle_add_auto = """ALTER TABLE %s ALTER COLUMN %s SET DEFAULT NEXTVAL('%s')""" % (
                        table_name, column_name, sequence_name)
                    print(oracle_add_auto + ';')
                    oracle_cursor.execute(oracle_add_auto)
                    oracle_conn.commit()
                    print('自增列修改完毕\n')
                    all_auto_success_count += 1
                except Exception as e:
                    oracle_conn.rollback()
                    print(oracle_add_auto + ';' + '修改表' + table_name + '列 ' + column_name + ' 自增列失败 ' + str(e.args))
                    filename = '/tmp/ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('-' * 50 + 'ALTER AUTO COL ERROR ' + '-' * 50 + '\n')
                    f.write(oracle_add_auto + ';')
                    f.write(str(e.args) + '\n')
                    f.close()
                    all_auto_fail_count += 1
    else:  # 创建全部的自增列
        sequence_sql = """select table_name,COLUMN_NAME,substr(upper(concat(TABLE_NAME,'_',COLUMN_NAME,'_seq')),1,29) sequence_name from information_schema. COLUMNS where
TABLE_SCHEMA in (select database()) and table_name in(select t.TABLE_NAME
from information_schema. TABLES t where TABLE_SCHEMA in (select database()) and AUTO_INCREMENT is not null)  and EXTRA='auto_increment' """  # 获取自增列的表名以及列名,用于生成序列名称
        mysql_cursor.execute(sequence_sql)
        output_name = mysql_cursor.fetchall()  # 获取所有的自增列
        all_auto_count = len(output_name)
        for v_output_name in output_name:
            table_name = v_output_name[0]  # 获取表名
            column_name = v_output_name[1]  # 获取列名
            sequence_name = v_output_name[2]  # 生成序列名称
            try:  # 创建序列
                mysql_cursor.execute("""SELECT Auto_increment FROM information_schema.TABLES WHERE Table_Schema in (select database())
                AND table_name ='%s'""" % table_name)
                auto_column_startval = mysql_cursor.fetchone()[0]  # 获取自增列起始值
                oracle_seq_sql = """CREATE SEQUENCE %s INCREMENT BY 1 START with %s """ % (
                    sequence_name, auto_column_startval)
                print(oracle_seq_sql)
                # 判断下序列是否存在
                oracle_cursor.execute(
                    """select count(*) from user_sequences where sequence_name=upper('%s')""" % sequence_name)
                sequence_exist = oracle_cursor.fetchone()[0]
                if sequence_exist == 1:
                    oracle_cursor.execute("""DROP SEQUENCE  %s """ % sequence_name)
                # 判读下MySQL中自增列的表在oracle中是否存在
                oracle_cursor.execute("""select count(*) from user_tables where TABLE_NAME=upper('%s')""" % table_name)
                table_exist = oracle_cursor.fetchone()[0]
                if table_exist == 1:
                    oracle_cursor.execute(oracle_seq_sql)
                    oracle_conn.commit()
                else:
                    print('创建序列异常，表', table_name, '不存在')
            except Exception as e:
                oracle_conn.rollback()
                print(oracle_seq_sql + ';' + '创建序列失败！' + str(e))
                filename = '/tmp/ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-' * 50 + 'CREATE SEQUENCE ERROR ' + '-' * 50 + '\n')
                f.write(oracle_seq_sql)
                f.write(str(e) + '\n')
                f.close()
            try:  # 创建序列对应的触发器
                oracle_add_auto = """create or replace trigger tri_%s before insert on %s for each row begin  select %s.nextval into :new.%s from dual;  end;""" % (
                    str(table_name)[0:24], table_name, sequence_name, column_name)
                print(oracle_add_auto)
                # 判读下MySQL中自增列的表在oracle中是否存在
                oracle_cursor.execute("""select count(*) from user_tables where TABLE_NAME=upper('%s')""" % table_name)
                table_exist = oracle_cursor.fetchone()[0]
                if table_exist == 1:
                    oracle_cursor.execute(oracle_add_auto)
                    oracle_conn.commit()
                else:
                    print('创建触发器异常，表', table_name, '不存在')
                print('自增列修改完毕\n')
                all_auto_success_count += 1
            except Exception as e:
                print('修改表' + table_name + '列 ' + column_name + ' 自增列失败 ' + oracle_add_auto + ';' + str(e))
                filename = '/tmp/ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-' * 50 + 'ALTER AUTO COL ERROR ' + '-' * 50 + '\n')
                f.write(oracle_add_auto + ';')
                f.write(str(e) + '\n')
                f.close()
                all_auto_fail_count += 1
    print('自增列创建完毕!')


# 创建外键
def create_foreign_key():
    global fk_failed_count
    if args.data_only.upper() == 'TRUE':  # 如果指定-d选项就退出函数
        return 1
    global all_fk_count  # mysql中外键总数
    global all_fk_success_count  # 外键创建成功计数
    err_count = 0
    output_table_name = []  # 迁移部分表
    all_fk = []  # 存储执行创建外键约束的结果集
    start_time = datetime.datetime.now()
    print('#' * 50 + '开始创建' + '外键约束 ' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    if custom_table.upper() == 'TRUE':  # 如果命令行参数有-c选项，仅创建部分约束
        with open("/tmp/table.txt", "r") as f:  # 读取自定义表
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))  # 将自定义表全部保存到list
        for v_custom_table in output_table_name:  # 读取第N个表查询生成拼接sql
            mysql_cursor.execute(
                """select  table_name from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS where CONSTRAINT_SCHEMA in (select database()) and table_name ='%s' """ %
                v_custom_table[0])
            custom_index = mysql_cursor.fetchall()
            for v_out in custom_index:  # 每次将上面单表全部结果集全部存到all_fk的list里面
                all_fk.append(v_out)
    else:  # 命令行参数没有-c选项，创建所有约束
        mysql_cursor.execute(
            """select  table_name from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS where CONSTRAINT_SCHEMA in (select database()) """)
        all_fk = mysql_cursor.fetchall()  # 如果要每张表查使用T.TABLE_NAME = '%s',%s传进去是没有单引号，所以需要用单引号号包围
    all_fk_count = len(all_fk)
    if all_fk_count == 0:
        print('无约束需要创建')
    for d in all_fk:
        fk_table = d[0]
        try:
            mysql_cursor.execute("""SELECT concat('ALTER TABLE ',K.TABLE_NAME,' ADD CONSTRAINT ',K.CONSTRAINT_NAME,' FOREIGN KEY(',GROUP_CONCAT(COLUMN_NAME),')',' REFERENCES '
,K.REFERENCED_TABLE_NAME,'(',GROUP_CONCAT(REFERENCED_COLUMN_NAME),')')
FROM
	INFORMATION_SCHEMA.KEY_COLUMN_USAGE k INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS r
	on k.CONSTRAINT_NAME = r.CONSTRAINT_NAME
	where k.CONSTRAINT_SCHEMA in (select database()) AND r.CONSTRAINT_SCHEMA in (select database())  and k.REFERENCED_TABLE_NAME is not null
	and k.TABLE_NAME = '%s'
order by k.ORDINAL_POSITION""" % fk_table)  # 根据子表去获取创建外键的sql
            all_fk_sql = mysql_cursor.fetchall()  # 以上拼接sql结果集
            for v_all_fk_sql in all_fk_sql:  # 循环在目标数据库创建外键
                try:
                    oracle_cursor.execute(v_all_fk_sql[0])  # 目标库执行创建外键sql
                    print(v_all_fk_sql[0])  # 打印创建外键的sql
                    print('外键约束创建完毕\n')
                    all_fk_success_count += 1
                except Exception as e:
                    oracle_conn.rollback()
                    err_count += 1
                    fk_failed_count += 1  # 用来统计主键或者索引创建失败的计数，只要创建失败就往list存1
                    print(v_all_fk_sql[0] + '外键创建失败请检查ddl语句!\n' + str(e))
                    filename = '/tmp/ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('-' * 50 + str(err_count) + ' FK CREATE ERROR' + '-' * 50 + '\n')
                    f.write(v_all_fk_sql[0] + '\n')
                    f.write('\n' + '/* ' + str(e) + ' */' + '\n')
                    f.close()
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                    end_time = datetime.datetime.now()
                    print('创建外键约束耗时： ' + str((end_time - start_time).seconds))
                    print('#' * 50 + '外键约束创建完成' + '#' * 50 + '\n\n\n')
        except Exception as e:
            print('查找源库外键失败,请检查源库外键定义')


# 创建触发器
def create_trigger():
    global all_trigger_count  # 源数据库触发器总数
    global all_trigger_success_count  # 目标触发器创建成功数
    global trigger_failed_count  # 目标触发器创建失败数
    if args.data_only.upper() == 'TRUE':  # 如果指定-d选项就退出函数
        return 1
    err_count = 0
    output_table_name = []  # 迁移部分表
    all_trigger = []  # 存储执行创建触发器的结果集
    start_time = datetime.datetime.now()
    print('#' * 50 + '开始创建' + '触发器 ' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    if custom_table.upper() == 'TRUE':  # 如果命令行参数有-c选项，仅创建部分约束
        with open("/tmp/table.txt", "r") as f:  # 读取自定义表
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))  # 将自定义表全部保存到list
        for v_custom_table in output_table_name:  # 读取第N个表查询生成拼接sql
            mysql_cursor.execute(
                """SELECT replace(upper(concat('create or replace trigger ',trigger_name,' ',action_timing,' ',event_manipulation,' on ',event_object_table,' for each row as ',action_statement)),'#','-- ') FROM information_schema.triggers WHERE trigger_schema in (select database()) and event_object_table='%s'""" %
                v_custom_table[0])
            custom_trigger = mysql_cursor.fetchall()
            for v_out in custom_trigger:  # 每次将上面单表全部结果集全部存到all_trigger的list里面
                all_trigger.append(v_out)
    else:  # 命令行参数没有-c选项，创建所有触发器
        mysql_cursor.execute(
            """SELECT replace(upper(concat('create or replace trigger ',trigger_name,' ',action_timing,' ',event_manipulation,' on ',event_object_table,' for each row as ',action_statement)),'#','-- ') FROM information_schema.triggers WHERE trigger_schema in (select database()) """)
        all_trigger = mysql_cursor.fetchall()  # 如果要每张表查使用T.TABLE_NAME = '%s',%s传进去是没有单引号，所以需要用单引号号包围
    all_trigger_count = len(all_trigger)
    if all_trigger_count == 0:
        print('无触发器要创建')
    for d in all_trigger:
        create_tri_sql = d[0]
        print(create_tri_sql)
        try:
            oracle_cursor.execute(create_tri_sql)
            print('触发器创建完毕\n')
            all_trigger_success_count += 1
        except Exception as e:
            oracle_conn.rollback()
            err_count += 1
            trigger_failed_count += 1
            print('触发器创建失败请检查ddl语句!\n' + str(e))
            filename = '/tmp/ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('-' * 50 + str(err_count) + ' TRIGGER CREATE ERROR' + '-' * 50 + '\n')
            f.write(create_tri_sql + '\n')
            f.write('\n' + '/* ' + str(e) + ' */' + '\n')
            f.close()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    end_time = datetime.datetime.now()
    print('创建触发器耗时： ' + str((end_time - start_time).seconds))
    print('#' * 50 + '触发器创建完成' + '#' * 50 + '\n\n\n')


# 数据库对象的comment注释
def create_comment():
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
            mysql_cursor.execute("""SELECT
	concat( 'comment on column ', table_name, '.', column_name, ' is ', '''', column_comment, '''' ) 
FROM
	information_schema.COLUMNS 
WHERE
	table_schema IN (
	SELECT DATABASE
	()) 
	AND table_name = '%s' 
	AND column_comment IS NOT NULL 
	AND column_comment != ''""" % v_custom_table[0])
            custom_comment = mysql_cursor.fetchall()
            for v_out in custom_comment:  # 每次将上面单表全部结果集全部存到all_comment_sql的list里面
                all_comment_sql.append(v_out)
    else:  # 创建全部注释
        mysql_cursor.execute("""
            	SELECT
	concat( 'comment on column ', table_name, '.', column_name, ' is ', '''', column_comment, '''' ) 
FROM
	information_schema.COLUMNS 
WHERE
	table_schema IN (	SELECT DATABASE	()) 
	AND column_comment IS NOT NULL 
	AND column_comment != ''
            """)
        all_comment_sql = mysql_cursor.fetchall()
    if len(all_comment_sql) > 0:
        for e in all_comment_sql:  # 一次性创建注释
            # table_name = e[0]
            create_comment_sql = e[0]
            try:
                print('正在添加注释:')
                print(create_comment_sql)
                # cur_target_constraint.execute(create_comment_sql)
                oracle_cursor.execute(create_comment_sql)
                print('comment注释添加完毕\n')
            except Exception as e:
                err_count += 1
                print('\n' + '/* ' + str(e) + ' */' + '\n')
                print('comment添加失败请检查ddl语句!\n')
                # print(traceback.format_exc())
                filename = '/tmp/ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('\n-- ' + ' CREATE COMMENT ERROR ' + str(err_count) + '\n')
                f.write(create_comment_sql + ';\n')
                f.write('\n' + '/* ' + str(e) + ' */' + '\n')
                f.close()
    else:
        print('没有注释要创建')
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    end_time = datetime.datetime.now()
    print('创建comment耗时：' + str((end_time - begin_time).seconds))
    print('#' * 50 + 'comment注释添加完成' + '#' * 50 + '\033[0m\n\n\n')


# 迁移摘要
def mig_summary():
    # mysql源表信息
    mysql_tab_count = all_table_count  # 源表要迁移的表总数
    mysql_view_count = all_view_count  # 源视图总数
    mysql_auto_count = all_auto_count  # 源数据库自增列总数
    mysql_trigger_count = all_trigger_count  # 源数据库触发器总数
    mysql_index_count = all_constraints_count  # 源数据库约束以及索引总数
    mysql_fk_count = all_fk_count  # 源数据库外键总数
    # mysql源表信息

    # oracle迁移计数
    oracle_success_table_count = str(len(list_success_table))  # 目标数据库创建成功的表总数
    table_failed_count = len(ddl_failed_table_result)  # 目标数据库创建失败的表总数
    oracle_success_view_count = str(all_view_success_count)  # 目标数据库视图创建成功的总数
    view_error_count = all_view_failed_count  # 目标数据库创建视图失败的总数
    oracle_success_auto_count = str(all_auto_success_count)  # 目标数据库自增列成功的总数
    oracle_fail_auto_count = str(all_auto_fail_count)  # 目标数据库自增列失败的总数
    oracle_success_tri_count = str(all_trigger_success_count)  # 目标数据库创建触发器成功的总数
    oracle_fail_tri_count = str(trigger_failed_count)  # 目标数据库创建触发器失败的总数
    oracle_success_idx_count = str(all_constraints_success_count)  # 目标数据库创建约束以及索引成功总数
    oracle_fail_idx_count = str(constraint_failed_count)  # 目标数据库创建失败的约束以及索引总数
    oracle_success_fk_count = str(all_fk_success_count)  # 目标数据库外键创建成功的总数
    oracle_fail_fk_count = str(fk_failed_count)  # 目标数据库外键创建失败的总数
    # oracle迁移计数

    print('\033[31m*' * 50 + '数据迁移摘要' + '*' * 50 + '\033[0m\n\n\n')
    print('\n\n\n')
    print('目标数据库: ' + oracle_user)
    print('1、表数量总计: ' + str(mysql_tab_count) + ' 目标表创建成功计数: ' + oracle_success_table_count + ' 目标表创建失败计数: ' + str(
        table_failed_count))
    print('2、视图数量总计: ' + str(mysql_view_count) + ' 目标视图创建成功计数: ' + oracle_success_view_count + ' 目标视图创建失败计数: ' + str(
        view_error_count))
    print(
        '3、自增列数量总计: ' + str(mysql_auto_count) + ' 目标自增列创建成功计数: ' + oracle_success_auto_count + ' 目标自增列修改失败计数: ' + str(
            oracle_fail_auto_count))
    print('4、触发器数量总计: ' + str(
        mysql_trigger_count) + ' 触发器创建成功计数: ' + str(oracle_success_tri_count) + ' 触发器创建失败计数: ' + str(
        oracle_fail_tri_count))
    print('5、索引以及约束总计: ' + str(
        mysql_index_count) + ' 目标索引以及约束创建成功计数: ' + oracle_success_idx_count + ' 目标索引以及约束创建失败计数: ' + str(
        oracle_fail_idx_count))
    print('6、外键总计: ' + str(mysql_fk_count) + ' 目标外键创建成功计数: ' + oracle_success_fk_count + ' 目标外键创建失败计数: ' +
          str(oracle_fail_fk_count))
    csv_file = open("/tmp/insert_table.csv", 'a', newline='')
    # 将MySQL创建成功的表总数记录保存到csv文件
    try:
        writer = csv.writer(csv_file)
        writer.writerow(('TOTAL:', oracle_success_table_count))
    except Exception:
        print(traceback.format_exc())
    finally:
        csv_file.close()
    if ddl_failed_table_result:  # 输出失败的对象
        print("\n\n创建失败的表如下：")
        for output_ddl_failed_table_result in ddl_failed_table_result:
            print(output_ddl_failed_table_result)
        print('\n\n\n')
    print('\n请检查创建失败的表DDL以及约束。有关更多详细信息，请参阅迁移输出信息')
    print(
        '迁移日志已保存到/tmp/mig.log\n表迁移记录请查看/tmp/insert_table.csv或者在目标数据库查询表my_mig_task_info\n有关迁移错误请查看/tmp/ddl_failed_table.log以及/tmp'
        '/insert_failed_table.log')


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
    starttime = datetime.datetime.now()
    print_source_info()
    clean_log()
    create_meta_table()
    split_success_list()
    async_work()  # 默认是使用多线程迁移数据
    mig_table()  # 如果指定-d，调用单线程函数
    create_meta_constraint()
    create_view()
    create_auto_column()
    create_trigger()
    create_foreign_key()
    create_comment()
    endtime = datetime.datetime.now()
    mig_summary()
    print("MySQL迁移数据到Oracle完毕,一共耗时" + str((endtime - starttime).seconds) + "秒")
oracle_cursor.close()
mysql_cursor.close()
oracle_conn.close()
mysql_cursor.close()
