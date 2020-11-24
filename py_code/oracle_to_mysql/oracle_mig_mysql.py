# -*- coding: utf-8 -*-
# oracle_mig_mysql.py
# Oracle database migration to MySQL
# CURRENT VERSION
# V1.4.2

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
# from multiprocessing.dummy import Pool as ThreadPool
# import threading
from threading import Thread
from multiprocessing import Process  # 下面用了动态变量执行多进程，所以这里是灰色
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import concurrent
from db_config import configDB  # 引用配置文件以及产生连接池


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


# oracle、mysql连接池以及游标对象
oracle_cursor = configDB.OraclePool()  # Oracle连接池
mysql_cursor = configDB.MySQLPOOL.connection().cursor()  # MySQL连接池
mysql_cursor.arraysize = 20000

# 非连接池连接方式以及游标
ora_conn = configDB.ora_conn
source_db = cx_Oracle.connect(ora_conn)  # 源库Oracle的数据库连接
cur_oracle_result = source_db.cursor()  # 查询Oracle源表的游标结果集
cur_oracle_result.prefetchrows = 20000
cur_oracle_result.arraysize = 20000  # Oracle数据库游标对象结果集返回的行数即每次获取多少行
fetch_many_count = 20000

# 计数变量
list_table_name = []  # 查完user_tables即当前用户所有表存入list
list_success_table = []  # 创建成功的表存入到list
new_list = []  # 用于存储1分为2的表，将原表分成2个list
ddl_failed_table_result = []  # 用于记录ddl创建失败的表名
constraint_failed_count = []  # 用于统计主键以及索引创建失败的计数
foreignkey_failed_count = []  # 用于统计外键创建失败的计数
view_failed_count = []  # 用于统计视图创建失败的计数
comment_failed_count = []  # 用于统计注释添加失败的计数
oracle_autocol_total = []  # 用于统计Oracle中自增列的计数
autocol_failed_count = []  # 用于统计MySQL中自增列创建失败的计数

# 环境有关变量
sys.stdout = Logger(stream=sys.stdout)
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  # 设置字符集为UTF8，防止中文乱码

source_db_type = 'Oracle'  # 大小写无关，后面会被转为大写
target_db_type = 'MySQL'  # 大小写无关，后面会被转为大写


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


# cur_oracle_result2.outputtypehandler = dataconvert  # 查询Oracle表数据结果集的游标
# cur_source_constraint.outputtypehandler = dataconvert  # 查询Oracle主键以及索引、外键的游标


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
    n = 2  # 平均分成2份list
    print('切片的大小:', n)
    print('原始list：', list_success_table, '\n')
    new_list.append(bisector_list(list_success_table, n))
    print('一分为二：', new_list)
    print('新的list长度：', len(new_list[0]))  # 分了几个片，如果一分为二就是2，如过分片不足就是1


# 打印连接信息
def print_source_info():
    print('-' * 50 + 'Oracle->MySQL' + '-' * 50)
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    # print('源Oracle数据库连接信息: ' + cur_oracle_result.connection.tnsentry + ' 用户名:' + cur_oracle_result.connection.username + ' 版本:' + cur_oracle_result.connection.version + ' 编码:' + cur_oracle_result.connection.encoding)
    print('源Oracle数据库连接信息: ' + str(oracle_cursor._OraclePool__pool._kwargs))
    source_table_count = oracle_cursor.fetch_one("""select count(*) from user_tables""")[0]
    source_view_count = oracle_cursor.fetch_one("""select count(*) from user_views""")[0]
    source_trigger_count = \
        oracle_cursor.fetch_one("""select count(*) from user_triggers where TRIGGER_NAME not like 'BIN$%'""")[0]
    source_procedure_count = oracle_cursor.fetch_one(
        """select count(*) from USER_PROCEDURES where OBJECT_TYPE='PROCEDURE' and OBJECT_NAME  not like 'BIN$%'""")[0]
    source_function_count = oracle_cursor.fetch_one(
        """select count(*) from USER_PROCEDURES where OBJECT_TYPE='FUNCTION' and OBJECT_NAME  not like 'BIN$%'""")[0]
    source_package_count = oracle_cursor.fetch_one(
        """select count(*) from USER_PROCEDURES where OBJECT_TYPE='PACKAGE' and OBJECT_NAME  not like 'BIN$%'""")[0]
    print('源表总计: ' + str(source_table_count))
    print('源视图总计: ' + str(source_view_count))
    print('源触发器总计: ' + str(source_trigger_count))
    print('源存储过程总计: ' + str(source_procedure_count))
    print('源数据库函数总计: ' + str(source_function_count))
    print('源数据库包总计: ' + str(source_package_count))
    print('目标MySQL数据库连接信息: ' + str(mysql_cursor._con._kwargs))


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
    # source_db = cx_Oracle.connect(ora_conn)  # ljd 这行必须要加才不会hang
    # cur_tbl_columns = source_db.cursor()
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
        if column[1] == 'VARCHAR2' or column[1] == 'CHAR' or column[1] == 'NCHAR' or column[1] == 'NVARCHAR2':
            #  由于MySQL创建表的时候除了大字段，所有列长度不能大于64k，为了转换方便，如果Oracle字符串长度大于等于1000映射为MySQL的tinytext
            #  由于MySQL大字段不能有默认值，所以这里的默认值都统一为null
            if column[2] >= 10000:
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

        # 时间日期类型映射规则，Oracle date类型映射为MySQL类型datetime
        elif column[1] == 'DATE' or column[1] == 'TIMESTAMP(6)':
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
                                   'default': re.findall(r'\b\d+\b', column[7])[0],  # 字段默认值,正则方式仅提取数字
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
                else:  # 其余情况通过正则只提取数字部分，即去掉原Oracle中有括号的默认值
                    result.append({'fieldname': column[0],
                                   'type': 'INT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': re.findall(r'\b\d+\b', column[7])[0],  # 字段默认值去掉括号，仅提取数字
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
                else:  # 其余情况通过正则只提取数字部分，即去掉原Oracle中有括号的默认值
                    result.append({'fieldname': column[0],
                                   'type': 'BIGINT',  # 列字段类型以及长度范围
                                   'primary': column[0],  # 如果有主键字段返回true，否则false
                                   'default': re.findall(r'\b\d+\b', column[7])[0],  # 字段默认值去掉括号，仅提取数字
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
                                   'default': re.findall(r'\b\d+\b', column[7])[0],  # 字段默认值去掉括号，仅提取数字
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
                                   'default': re.findall(r'\b\d+\b', column[7])[0],  # 字段默认值仅提取数字部分
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
                                   'default': re.findall(r'\b\d+\b', column[7])[0],  # 字段默认值，仅提取数字部分
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
    table_foreign_key = 'select table_name from USER_CONSTRAINTS where CONSTRAINT_TYPE= \'R\''
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    fk_table = oracle_cursor.fetch_all(table_foreign_key)
    for v_result_table in fk_table:
        table_name = v_result_table[0]
        print('#' * 50 + '开始创建' + table_name + '外键 ' + '#' * 50)
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
        for e in all_foreign_key:
            create_foreign_key_sql = e[0]
            print(create_foreign_key_sql)
            try:
                # cur_target_constraint.execute(create_foreign_key_sql)
                mysql_cursor.execute(create_foreign_key_sql)
                print('外键创建完毕\n')
            except Exception:
                foreignkey_failed_count.append('1')  # 外键创建失败就往list对象存1
                print('外键创建失败请检查ddl语句!\n')
                print(traceback.format_exc())
                filename = '/tmp/ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('/' + '*' * 50 + 'FOREIGNKEY CREATE ERROR' + '*' * 50 + '/\n')
                f.write(create_foreign_key_sql + '\n\n\n')
                f.close()
                ddl_foreignkey_error = traceback.format_exc()
                logging.error(ddl_foreignkey_error)  # 外键创建失败的sql语句输出到文件/tmp/ddl_failed_table.log
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('\033[31m*' * 50 + '外键创建完成' + '*' * 50 + '\033[0m\n\n\n')


# 批量创建主键以及索引
def create_meta_constraint():
    print('#' * 50 + '开始创建' + '约束以及索引 ' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
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
     GROUP BY T.TABLE_NAME,
              T.INDEX_NAME,
              I.UNIQUENESS,
              I.INDEX_TYPE,
              C.CONSTRAINT_TYPE""")  # 如果要每张表查使用T.TABLE_NAME = '%s',%s传进去是没有单引号，所以需要用单引号号包围
    for d in all_index:
        create_index_sql = d[0].read()  # 用read读取大对象，否则会报错
        print(create_index_sql)
        try:
            # cur_target_constraint.execute(create_index_sql)
            mysql_cursor.execute(create_index_sql)
            print('约束以及索引创建完毕\n')
        except Exception:
            constraint_failed_count.append('1')  # 用来统计主键或者索引创建失败的计数，只要创建失败就往list存1
            print('约束或者索引创建失败请检查ddl语句!\n')
            print(traceback.format_exc())
            filename = '/tmp/ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('/' + '*' * 50 + 'PK AND INDEX CREATE ERROR' + '*' * 50 + '/\n')
            f.write(create_index_sql + '\n\n\n')
            f.close()
            constraint_error_table = traceback.format_exc()  # 这里记下索引创建失败的sql在 ddl_failed_table.log
            logging.error(constraint_error_table)  # ddl创建失败的sql语句输出到文件/tmp/constraint_error_table.log
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('\033[31m*' * 50 + '主键约束、索引创建完成' + '*' * 50 + '\033[0m\n\n\n')
    #  将创建失败的sql记录到log文件
    # logging.basicConfig(filename='/tmp/constraint_failed_table.log')
    """
    之前是调用user_constraint函数按每张表批量创建主键以及索引，现在改为了一次性创建约束以及索引
     filename = '/tmp/ddl_success_table.log'  # 读取DDL创建成功的表名
    with open(filename) as f:
        reader = csv.reader(f)
        for row in reader:
            tbl_name = row[0]
            print('#' * 50 + '开始创建' + tbl_name + '约束以及索引 ' + '#' * 50)
            user_constraint()  # 之前是调用user_constraint函数按每张表批量创建主键以及索引，现在改为了一次性创建约束以及索引
        print('\033[31m*' * 50 + '主键约束、索引创建完成' + '*' * 50 + '\033[0m\n\n\n')
    f.close()
    """


# 查找具有自增特性的表以及字段名称
def auto_increament_col():
    print('#' * 50 + '开始增加自增列' + '#' * 50)
    # Oracle中无法对long类型数据截取，创建用于存储触发器字段信息的临时表TRIGGER_NAME
    count_num_tri = oracle_cursor.fetch_one("""select count(*) from user_tables where table_name='TRIGGER_NAME'""")[0]
    if count_num_tri == 1:
        try:
            oracle_cursor.execute_sql("""truncate table trigger_name""")
            oracle_cursor.execute_sql(
                """insert into trigger_name select table_name ,to_lob(trigger_body) from user_triggers""")
        except Exception:
            print(traceback.format_exc())
            print('无法在Oracle插入存放触发器的数据')
    else:
        try:
            oracle_cursor.execute_sql("""create table trigger_name (table_name varchar2(200),trigger_body clob)""")
            oracle_cursor.execute_sql(
                """insert into trigger_name select table_name ,to_lob(trigger_body) from user_triggers""")
        except Exception:
            print(traceback.format_exc())
            print('无法在Oracle创建用于触发器的表')
    all_create_index = oracle_cursor.fetch_all(
        """select 'create  index ids_'||substr(table_name,1,26)||' on '||table_name||'('||upper(substr(substr(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.')), 1, instr(upper(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.'))), ' FROM DUAL;') - 1), 5)) ||');' as sql_create from trigger_name where instr(upper(trigger_body), 'NEXTVAL')>0""")  # 在Oracle拼接sql生成用于在MySQL中自增列的索引
    print('创建用于自增列的索引:\n ')
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    for v_increa_index in all_create_index:
        create_autoincrea_index = v_increa_index[0].read()  # 用read读取大字段，否则无法执行
        print(create_autoincrea_index)
        try:
            mysql_cursor.execute(create_autoincrea_index)
        except Exception:
            print('用于自增列的索引创建失败，请检查源触发器！\n')
            print(traceback.format_exc())
            filename = '/tmp/ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('/' + '*' * 50 + 'FOR AUTOCREAMENT INDEX CREATE ERROR' + '*' * 50 + '/\n')
            f.write(create_autoincrea_index + '\n\n\n')
            f.close()
            ddl_incindex_error = traceback.format_exc()
            logging.error(ddl_incindex_error)  # 自增用索引创建失败的sql语句输出到文件/tmp/ddl_failed_table.log
    print('自增列索引创建完成')
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    print('开始修改自增列属性：')
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    all_alter_sql = oracle_cursor.fetch_all("""
    select 'alter table '||table_name||' modify '||upper(substr(substr(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.')), 1, instr(upper(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.'))), ' FROM DUAL;') - 1), 5)) ||' int auto_increment;' from trigger_name where instr(upper(trigger_body), 'NEXTVAL')>0
    """)
    for v_increa_col in all_alter_sql:
        alter_increa_col = v_increa_col[0].read()  # 用read读取大字段，否则无法执行
        print('\n执行sql alter table：\n')
        print(alter_increa_col)
        try:  # 注意下try要在for里面
            mysql_cursor.execute(alter_increa_col)
        except Exception:  # 如果有异常打印异常信息，并跳过继续下个自增列修改
            autocol_failed_count.append('1')
            print('修改自增列失败，请检查源触发器！\n')
            print(traceback.format_exc())
            filename = '/tmp/ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('/' + '*' * 50 + 'MODIFY AUTOCREAMENT COL ERROR' + '*' * 50 + '/\n')
            f.write(alter_increa_col + '\n\n\n')
            f.close()
            ddl_increa_col_error = traceback.format_exc()
            logging.error(ddl_increa_col_error)  # 自增用索引创建失败的sql语句输出到文件/tmp/ddl_failed_table.log
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('\033[31m*' * 50 + '自增列修改完成' + '*' * 50 + '\033[0m\n\n\n')
    oracle_autocol_total.append(
        oracle_cursor.fetch_one("""select count(*) from trigger_name  where instr(upper(trigger_body), 'NEXTVAL')>0""")[
            0])  # 将自增列的总数存入list
    oracle_cursor.execute_sql("""drop table trigger_name purge""")


# 获取视图定义以及创建
def create_view():
    print('#' * 50 + '开始创建视图' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    # Oracle中无法对long类型数据截取，创建用于存储视图信息的临时表content_view
    count_num_view = oracle_cursor.fetch_one("""select count(*) from user_tables where table_name='CONTENT_VIEW'""")[0]
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
        except Exception:
            view_failed_count.append('1')  # 视图创建失败就往list对象存1
            print('视图创建失败请检查ddl语句!\n')
            print(traceback.format_exc())
            filename = '/tmp/ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('/' + '*' * 50 + 'VIEW CREATE ERROR' + '*' * 50 + '/\n')
            f.write(create_view_sql + '\n\n\n')
            f.close()
            ddl_view_error = traceback.format_exc()
            logging.error(ddl_view_error)  # 视图创建失败的sql语句输出到文件/tmp/ddl_failed_table.log
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('\033[31m*' * 50 + '视图创建完成' + '*' * 50 + '\033[0m\n\n\n')
    oracle_cursor.execute_sql("""drop table content_view purge""")


# 数据库对象的comment注释
def create_comment():
    print('#' * 50 + '开始添加comment注释' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    all_comment_sql = oracle_cursor.fetch_all("""
    select TABLE_NAME,'alter table '||TABLE_NAME||' comment '||''''||COMMENTS||'''' as create_comment
 from USER_TAB_COMMENTS where COMMENTS is not null
    """)
    for e in all_comment_sql:
        table_name = e[0]
        create_comment_sql = e[1]
        print(create_comment_sql)
        try:
            print('正在添加' + table_name + '的注释:')
            # cur_target_constraint.execute(create_comment_sql)
            mysql_cursor.execute(create_comment_sql)
            print('comment注释添加完毕\n')
        except Exception:
            comment_failed_count.append('1')  # comment添加失败就往list对象存1
            print('comment添加失败请检查ddl语句!\n')
            print(traceback.format_exc())
            filename = '/tmp/ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('/' + '*' * 50 + 'COMMENT CREATE ERROR' + '*' * 50 + '/\n')
            f.write(create_comment_sql + '\n\n\n')
            f.close()
            ddl_comment_error = traceback.format_exc()
            logging.error(ddl_comment_error)  # comment注释创建失败的sql语句输出到文件/tmp/ddl_failed_table.log
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('\033[31m*' * 50 + 'comment注释添加完成' + '*' * 50 + '\033[0m\n\n\n')


# 仅输出Oracle当前用户的表，即user_tables的table_name
def print_table():
    tableoutput_sql = 'select table_name from user_tables  order by table_name  desc'  # 查询需要导出的表
    all_table = oracle_cursor.fetch_all(tableoutput_sql)
    for v_table in all_table:
        list_table_name.append(v_table[0])
    '''    
    cur_tblprt = source_db.cursor()  # 生成用于输出表名的游标对象
    #   where table_name in (\'TEST4\',\'TEST3\',\'TEST2\')
    tableoutput_sql = 'select table_name from user_tables  order by table_name  desc'  # 查询需要导出的表
    cur_tblprt.execute(tableoutput_sql)  # 执行
    filename = '/tmp/table_name.csv'
    f = open(filename, 'w')
    for row_table in cur_tblprt:
        table_name = row_table[0]
        f.write(table_name + '\n')
    f.close()
    '''


# 打印输出DDL创建失败的sql语句
def print_ddl_failed_table(table_name, p1):
    filename = '/tmp/ddl_failed_table.log'
    f = open(filename, 'a', encoding='utf-8')
    f.write('/' + '*' * 50 + 'CREATE TABLE ERROR: ' + table_name + '*' * 50 + '/\n')
    f.write(p1 + '\n\n\n')
    f.close()


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
def mig_table(tablename):
    # source_db = cx_Oracle.connect(ora_conn)
    # cur_oracle_result = source_db.cursor
    # mysql_conn = MySQLPOOL.connection()  # MySQL连接池
    # mysql_cursor = mysql_conn.cursor()
    target_table = source_table = tablename
    if source_db_type.upper() == 'ORACLE':
        get_column_length = 'select count(*) from user_tab_columns where table_name= ' + "'" + source_table.upper() + "'"  # 拼接获取源表有多少个列的SQL
    # lock.acquire()
    cur_select.execute(get_column_length)  # 执行
    col_len = cur_select.fetchone()
    # col_len = oracle_cursor.fetch_one(get_column_length)  # 获取源表有多少个列
    col_len = col_len[0]  # 将游标结果数组的值赋值，该值为表列字段总数
    val_str = ''  # 用于生成批量插入的列字段变量
    if target_db_type.upper() == 'MYSQL':
        for i in range(1, col_len):
            val_str = val_str + '%s' + ','
        val_str = val_str + '%s'  # MySQL批量插入语法是 insert into tb_name values(%s,%s,%s,%s)
    elif target_db_type.upper() == 'ORACLE':
        for i in range(1, col_len):
            val_str = val_str + ':' + str(i) + ','
        val_str = val_str + ':' + str(col_len)  # Oracle批量插入语法是 insert into tb_name values(:1,:2,:3)
    insert_sql = 'insert into ' + target_table + ' values(' + val_str + ')'  # 拼接insert into 目标表 values  #目标表插入语句
    select_sql = 'select * from ' + source_table  # 源查询SQL，如果有where过滤条件，在这里拼接
    cur_oracle_result.execute(select_sql)  # 执行
    print("\033[31m正在执行插入表:\033[0m", tablename)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    source_effectrow = 0
    target_effectrow = 0
    while True:
        rows = list(cur_oracle_result.fetchmany(
            fetch_many_count))  # 每次获取2000行，cur_oracle_result.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
        #  print(cur_oracle_result.description)  # 打印Oracle查询结果集字段列表以及类型
        try:
            mysql_cursor.executemany(insert_sql, rows)  # 批量插入每次5000行，需要注意的是 rows 必须是 list [] 数据类型
            # mysql_cursor.executemany(insert_sql, rows)
            # mysql_conn.commit() 对于连接池，这里需要显式提交
            # target_db.commit()  # 提交
        except Exception as e:
            print(traceback.format_exc())  # 遇到异常记录到log，会继续迁移下张表
            #  print(tablename, '表记录', rows, '插入失败') 插入失败时输出insert语句
            print_insert_failed_table(tablename)
            filename = '/tmp/ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('/' + '*' * 50 + 'INSERT ERROR' + '*' * 50 + '/\n')
            f.write(insert_sql + '\n\n\n')
            f.close()
            sql_insert_error = traceback.format_exc()
            logging.error(sql_insert_error)  # 插入失败的sql语句输出到文件/tmp/ddl_failed_table.log

        if not rows:
            break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表
        source_effectrow = cur_oracle_result.rowcount  # 计数源表插入的行数
        target_effectrow = target_effectrow + mysql_cursor.rowcount  # 计数目标表插入的行数
    print('源表查询总数:', source_effectrow)
    print('目标插入总数:', target_effectrow)
    print('插入完成')
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('\n\n')
    # lock.release()


# 在MySQL创建表结构以及添加主键
def create_meta_table():  # 调用create_table函数来创建表的
    tableoutput_sql = 'select table_name from user_tables  order by table_name  desc'  # 查询需要导出的表
    output_table_name = oracle_cursor.fetch_all(tableoutput_sql)
    starttime = datetime.datetime.now()
    for row in output_table_name:
        table_name = row[0]
        print('#' * 50 + '开始创建表' + table_name + '#' * 50)
        print(table_name + '\n')
        #  将创建失败的sql记录到log文件
        logging.basicConfig(filename='/tmp/ddl_failed_table.log')
        # 在MySQL创建表前先删除存在的表
        drop_target_table = 'drop table if exists ' + table_name
        # lock.acquire()  # 并行执行加锁
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
        # add_pri_key_sql = 'alter table {0} add primary key ({1})'.format(table_name, ','.join(v_pri_key))  # 创建目标表之后增加主键
        print('\n创建表:' + table_name + '\n')
        print(create_table_sql)
        try:
            # cur_createtbl.execute(create_table_sql)
            mysql_cursor.execute(create_table_sql)
            #  if v_pri_key: 因为已经有创建约束的sql，这里可以不用执行
            #    cur_createtbl.execute(add_pri_key_sql) 因为已经有创建约束的sql，这里可以不用执行
            print(table_name + '表创建完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '\n')
            print_ddl_success_table(table_name)  # MySQL ddl创建成功的表，记录下表名到/tmp/ddl_success_table.csv
            list_success_table.append(table_name)  # MySQL ddl创建成功的表也存到list中

        except Exception:
            '''
            print(traceback.format_exc())  # 如果某张表创建失败，遇到异常记录到log，会继续创建下张表
            if write_fail == 1:
                print_ddl_failed_table(table_name)
                ddl_failed_table_result.append(table_name)
                ddl_create_error_table = traceback.format_exc()
                logging.error(ddl_create_error_table)
            else:
                pass
            '''
            print(traceback.format_exc())  # 如果某张表创建失败，遇到异常记录到log，会继续创建下张表
            print_ddl_failed_table(table_name, create_table_sql)  # ddl创建失败的表名记录到文件/tmp/ddl_failed_table.log
            ddl_failed_table_result.append(table_name)  # 将当前ddl创建失败的表名记录到ddl_failed_table_result的list中
            ddl_create_error_table = traceback.format_exc()
            logging.error(ddl_create_error_table)  # ddl创建失败的sql语句输出到文件/tmp/ddl_failed_table.log
            print('表' + table_name + '创建失败请检查ddl语句!\n')
        # lock.release()
    print('\033[31m*' * 50 + '表创建完成' + '*' * 50 + '\033[0m\n\n\n')
    endtime = datetime.datetime.now()
    print("表创建耗时\n" + "开始时间:" + str(starttime) + '\n' + "结束时间:" + str(endtime) + '\n' + "消耗时间:" + str(
        (endtime - starttime).seconds) + "秒\n")
    '''
    # 之前将表存在了csv文件，现在不用了
    filename = '/tmp/table_name.csv'  # 从user_tables输出的表
    with open(filename) as f:
        reader = csv.reader(f)
        for row in reader:
            tbl_name = row[0]
            print('#' * 50 + '开始创建表' + tbl_name + '#' * 50)
            # print("\033[31m开始创建表：\033[0m\n")
            print(tbl_name + '\n')
            create_table(tbl_name)  # 调用Oracle映射到MySQL规则的函数
        print('\033[31m*' * 50 + '表创建完成' + '*' * 50 + '\033[0m\n\n\n')
    f.close()
    '''


def create_table_main_process():
    p_starttime = datetime.datetime.now()
    p_pro = []
    # 批量调
    for i in range(2):  # 下面先生成多进程执行的动态变量以及把进程加到list，加到list后统一调用进程开始run
        exec('p{} = Process(target=create_table_task{}, args=({},))'.format(i, i + 1, i))
        exec('p_pro.append(p{})'.format(i))
        # exec('p{}.start()'.format(i))
    for p_t in p_pro:  # 这里对list中存在的进程统一开始调用
        p_t.start()
    for p_t in p_pro:
        p_t.join()
    # 批量调
    '''
    # 直接调
    p1 = Process(target=task1, args=('0',))
    p1.start()
    p2 = Process(target=task2, args=('1',))
    p2.start()
    # 直接调
    '''
    time.sleep(0.1)
    p_endtime = datetime.datetime.now()
    print('多进程开始时间：', p_starttime, '多进程结束时间：', p_endtime, '执行时间：', str((p_endtime - p_starttime).seconds), 'seconds')
    time.sleep(0.1)
    '''
    t1 = Thread(target=task1)
    t2 = Thread(target=task2)
    t1.start()
    t2.start()
    '''


def mig_table_task(list_index):
    time.sleep(0.1)  # 错开屏幕打印信息
    task1_starttime = datetime.datetime.now()
    for v_table_name in new_list[0][int(list_index)]:
        table_name = v_table_name
        print('#' * 50 + '开始迁移表' + table_name + '#' * 50)
        print('task' + str(list_index) + ' \n')
        target_table = source_table = table_name
        get_column_length = 'select count(*) from user_tab_columns where table_name= ' + "'" + source_table.upper() + "'"  # 拼接获取源表有多少个列的SQL
        col_len = oracle_cursor.fetch_one(get_column_length)  # 获取源表有多少个列 oracle连接池
        col_len = col_len[0]  # 将游标结果数组的值赋值，该值为表列字段总数
        val_str = ''  # 用于生成批量插入的列字段变量
        for i in range(1, col_len):
            val_str = val_str + '%s' + ','
        val_str = val_str + '%s'  # MySQL批量插入语法是 insert into tb_name values(%s,%s,%s,%s)
        '''
        if target_db_type.upper() == 'MYSQL':
            for i in range(1, col_len):
                val_str = val_str + '%s' + ','
            val_str = val_str + '%s'  # MySQL批量插入语法是 insert into tb_name values(%s,%s,%s,%s)
        elif target_db_type.upper() == 'ORACLE':
            for i in range(1, col_len):
                val_str = val_str + ':' + str(i) + ','
            val_str = val_str + ':' + str(col_len)  # Oracle批量插入语法是 insert into tb_name values(:1,:2,:3)
        '''
        insert_sql = 'insert into ' + target_table + ' values(' + val_str + ')'  # 拼接insert into 目标表 values  #目标表插入语句
        select_sql = 'select * from ' + source_table  # 源查询SQL，如果有where过滤条件，在这里拼接
        try:
            cur_oracle_result.execute(select_sql)  # 执行
            # temp_cur = oracle_cursor.execute_sql(select_sql, 1000)
        except Exception:
            print('查询Oracle源表失败，请检查连接！\n\n')
            print(traceback.format_exc())
            break
        print("\033[31m正在执行插入表:\033[0m", table_name)
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        source_effectrow = 0
        target_effectrow = 0
        while True:
            rows = list(
                cur_oracle_result.fetchmany(
                    fetch_many_count))  # 例如每次获取2000行，cur_oracle_result.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
            #  print(cur_oracle_result.description)  # 打印Oracle查询结果集字段列表以及类型
            try:
                mysql_cursor.executemany(insert_sql, rows)  # 批量插入每次5000行，需要注意的是 rows 必须是 list [] 数据类型
                # mysql_cursor.executemany(insert_sql, rows)
                # mysql_conn.commit() 对于连接池，这里需要显式提交
                # target_db1.commit()  # 提交
            except Exception as e:
                time.sleep(0.1)
                print(traceback.format_exc())  # 遇到异常记录到log，会继续迁移下张表
                sql_insert_error1 = traceback.format_exc()
                #  print(tablename, '表记录', rows, '插入失败') 插入失败时输出insert语句
                # print_insert_failed_table(table_name)
                filename = '/tmp/insert_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('/' + '*' * 50 + table_name + ' INSERT ERROR' + '*' * 50 + '/\n')
                f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n\n')
                f.write(insert_sql + '\n\n\n')
                f.write(sql_insert_error1 + '\n\n')
                f.close()
                logging.error(sql_insert_error1)  # 插入失败的sql语句输出到文件/tmp/ddl_failed_table.log

            if not rows:
                break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表
            source_effectrow = cur_oracle_result.rowcount  # 计数源表插入的行数
            target_effectrow = target_effectrow + mysql_cursor.rowcount  # 计数目标表插入的行数
        print('源表查询总数:', source_effectrow)
        print('目标插入总数:', target_effectrow)
        print('插入完成')
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('\n\n')
    print('\033[31m*' * 50 + '第' + str(list_index) + '部分表迁移完成' + '*' * 50 + '\033[0m\n\n\n')
    task1_endtime = datetime.datetime.now()
    task1_exec_time = str((task1_endtime - task1_starttime).seconds)
    print("第" + str(list_index) + "部分的表\n" + "开始时间:" + str(task1_starttime) + '\n' + "结束时间:" + str(
        task1_endtime) + '\n' + "消耗时间:" + task1_exec_time + "秒\n")


'''
def mig_table_task1(list_index):
    time.sleep(0.1)
    # source_db = cx_Oracle.connect(ora_conn)
    # cur_oracle_result = source_db.cursor
    # cur_select = source_db.cursor
    # target_db1 = pymysql.connect(mysql_conn, "root", "Gepoint", mysql_target_db)
    # cur_insert_mysql1 = target_db1.cursor
    # print(list_index)
    task1_starttime = datetime.datetime.now()
    for v_table_name in new_list[0][int(list_index)]:
        table_name = v_table_name
        print('#' * 50 + '开始迁移表' + table_name + '#' * 50)
        print('task1 ' + table_name + '\n')

        # mysql_conn = MySQLPOOL.connection()  # MySQL连接池
        # mysql_cursor = mysql_conn.cursor()
        target_table = source_table = table_name
        if source_db_type.upper() == 'ORACLE':
            get_column_length = 'select count(*) from user_tab_columns where table_name= ' + "'" + source_table.upper() + "'"  # 拼接获取源表有多少个列的SQL
        # lock.acquire()
        cur_select.execute(get_column_length)  # 执行
        col_len = cur_select.fetchone()
        # col_len = oracle_cursor.fetch_one(get_column_length)  # 获取源表有多少个列
        col_len = col_len[0]  # 将游标结果数组的值赋值，该值为表列字段总数
        val_str = ''  # 用于生成批量插入的列字段变量
        if target_db_type.upper() == 'MYSQL':
            for i in range(1, col_len):
                val_str = val_str + '%s' + ','
            val_str = val_str + '%s'  # MySQL批量插入语法是 insert into tb_name values(%s,%s,%s,%s)
        elif target_db_type.upper() == 'ORACLE':
            for i in range(1, col_len):
                val_str = val_str + ':' + str(i) + ','
            val_str = val_str + ':' + str(col_len)  # Oracle批量插入语法是 insert into tb_name values(:1,:2,:3)
        insert_sql = 'insert into ' + target_table + ' values(' + val_str + ')'  # 拼接insert into 目标表 values  #目标表插入语句
        select_sql = 'select * from ' + source_table  # 源查询SQL，如果有where过滤条件，在这里拼接
        try:
            cur_oracle_result.execute(select_sql)  # 执行
        except Exception:
            print('查询Oracle源表失败，请检查连接！\n\n')
            break
        print("\033[31m正在执行插入表:\033[0m", table_name)
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        source_effectrow = 0
        target_effectrow = 0
        while True:
            rows = list(cur_oracle_result.fetchmany(
                20000))  # 每次获取2000行，cur_oracle_result.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
            #  print(cur_oracle_result.description)  # 打印Oracle查询结果集字段列表以及类型
            try:
                cur_insert_mysql1.executemany(insert_sql, rows)  # 批量插入每次5000行，需要注意的是 rows 必须是 list [] 数据类型
                # mysql_cursor.executemany(insert_sql, rows)
                # mysql_conn.commit() 对于连接池，这里需要显式提交
                target_db1.commit()  # 提交
            except Exception as e:
                time.sleep(0.1)
                print(traceback.format_exc())  # 遇到异常记录到log，会继续迁移下张表
                sql_insert_error1 = traceback.format_exc()
                #  print(tablename, '表记录', rows, '插入失败') 插入失败时输出insert语句
                # print_insert_failed_table(table_name)
                filename = '/tmp/insert_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('/' + '*' * 50 + table_name + ' INSERT ERROR' + '*' * 50 + '/\n')
                f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n\n')
                f.write(insert_sql + '\n\n\n')
                f.write(sql_insert_error1 + '\n\n')
                f.close()
                logging.error(sql_insert_error1)  # 插入失败的sql语句输出到文件/tmp/ddl_failed_table.log

            if not rows:
                break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表
            source_effectrow = cur_oracle_result.rowcount  # 计数源表插入的行数
            target_effectrow = target_effectrow + cur_insert_mysql1.rowcount  # 计数目标表插入的行数
        print('源表查询总数:', source_effectrow)
        print('目标插入总数:', target_effectrow)
        print('插入完成')
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('\n\n')
    print('\033[31m*' * 50 + '第一部分表迁移完成' + '*' * 50 + '\033[0m\n\n\n')
    task1_endtime = datetime.datetime.now()
    task1_exec_time = str((task1_endtime - task1_starttime).seconds)
    print("第一部分的表\n" + "开始时间:" + str(task1_starttime) + '\n' + "结束时间:" + str(
        task1_endtime) + '\n' + "消耗时间:" + task1_exec_time + "秒\n")


def mig_table_task2(list_index):
    time.sleep(0.2)
    # print(list_index)
    task2_starttime = datetime.datetime.now()
    for v_table_name in new_list[0][int(list_index)]:
        table_name = v_table_name
        print('#' * 50 + '开始迁移表' + table_name + '#' * 50)
        print('task2 ' + table_name + '\n')
        target_table = source_table = table_name
        if source_db_type.upper() == 'ORACLE':
            get_column_length = 'select count(*) from user_tab_columns where table_name= ' + "'" + source_table.upper() + "'"  # 拼接获取源表有多少个列的SQL
        # lock.acquire()
        cur_select2.execute(get_column_length)  # 执行
        col_len = cur_select2.fetchone()
        # col_len = oracle_cursor.fetch_one(get_column_length)  # 获取源表有多少个列
        col_len = col_len[0]  # 将游标结果数组的值赋值，该值为表列字段总数
        val_str = ''  # 用于生成批量插入的列字段变量
        if target_db_type.upper() == 'MYSQL':
            for i in range(1, col_len):
                val_str = val_str + '%s' + ','
            val_str = val_str + '%s'  # MySQL批量插入语法是 insert into tb_name values(%s,%s,%s,%s)
        elif target_db_type.upper() == 'ORACLE':
            for i in range(1, col_len):
                val_str = val_str + ':' + str(i) + ','
            val_str = val_str + ':' + str(col_len)  # Oracle批量插入语法是 insert into tb_name values(:1,:2,:3)
        insert_sql = 'insert into ' + target_table + ' values(' + val_str + ')'  # 拼接insert into 目标表 values  #目标表插入语句
        select_sql = 'select * from ' + source_table  # 源查询SQL，如果有where过滤条件，在这里拼接
        try:
            cur_oracle_result2.execute(select_sql)  # 执行
        except Exception:
            print('查询Oracle源表失败，请检查连接！\n\n')
            break
        print("\033[31m正在执行插入表:\033[0m", table_name)
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        source_effectrow = 0
        target_effectrow = 0
        while True:
            rows = list(cur_oracle_result2.fetchmany(
                20000))  # 每次获取2000行，cur_oracle_result.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
            #  print(cur_oracle_result.description)  # 打印Oracle查询结果集字段列表以及类型
            try:
                cur_insert_mysql2.executemany(insert_sql, rows)  # 批量插入每次5000行，需要注意的是 rows 必须是 list [] 数据类型
                # mysql_cursor.executemany(insert_sql, rows)
                # mysql_conn.commit() 对于连接池，这里需要显式提交
                target_db2.commit()  # 提交
            except Exception as e:
                time.sleep(0.2)
                print(traceback.format_exc())  # 遇到异常记录到log，会继续迁移下张表
                sql_insert_error2 = traceback.format_exc()
                #  print(tablename, '表记录', rows, '插入失败') 插入失败时输出insert语句
                # print_insert_failed_table(table_name)
                filename = '/tmp/insert_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('/' + '*' * 50 + table_name + ' INSERT ERROR' + '*' * 50 + '/\n')
                f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n\n')
                f.write(insert_sql + '\n\n\n')
                f.write(sql_insert_error2 + '\n\n')
                f.close()
                logging.error(sql_insert_error2)  # 插入失败的sql语句输出到文件/tmp/ddl_failed_table.log

            if not rows:
                break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表
            source_effectrow = cur_oracle_result2.rowcount  # 计数源表插入的行数
            target_effectrow = target_effectrow + cur_insert_mysql2.rowcount  # 计数目标表插入的行数
        print('源表查询总数:', source_effectrow)
        print('目标插入总数:', target_effectrow)
        print('插入完成')
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('\n\n')
    print('\033[31m*' * 50 + '第二部分表迁移完成' + '*' * 50 + '\033[0m\n\n\n')
    task2_endtime = datetime.datetime.now()
    task2_exec_time = str((task2_endtime - task2_starttime).seconds)
    # time.sleep(0.5)
    print("第二部分的表\n" + "开始时间:" + str(task2_starttime) + '\n' + "结束时间:" + str(
        task2_endtime) + '\n' + "消耗时间:" + task2_exec_time + "秒\n")
'''


def async_work():  # 异步不阻塞方式同时插入表
    index = ['0']  # 第一个任务序列
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        task = {executor.submit(mig_table_task, v_index): v_index for v_index in index}
        for future in concurrent.futures.as_completed(task):
            task_name = task[future]
            try:
                data = future.done()
            except Exception as exc:
                print('%r generated an exception: %s' % (task_name, exc))
            else:
                print('async job done:' + str(data))
                print('async work completed!\n\n\n')
    if len(new_list[0]) == 2:  # 值为2说明表已经一分为二，如果不为2，说明只有1张表，就不用再去执行第二个任务
        mig_table_task('1')  # 第二个任务序列
    else:
        print('source table:1\n\n')


def mig_table_main_process():
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


def mig_table_main_thread():
    task2_starttime = datetime.datetime.now()
    for v_table_name in list_success_table:
        table_name = v_table_name
        print('#' * 50 + '开始迁移表' + table_name + '#' * 50)
        print('task2 ' + table_name + '\n')
        t = Thread(target=mig_table, args=(table_name,))
        t.start()
    print('\033[31m*' * 50 + '表迁移完成' + '*' * 50 + '\033[0m\n\n\n')
    task2_endtime = datetime.datetime.now()
    task2_exec_time = str((task2_endtime - task2_starttime).seconds)
    # time.sleep(0.5)
    print("第二部分的表\n" + "开始时间:" + str(task2_starttime) + '\n' + "结束时间:" + str(
        task2_endtime) + '\n' + "消耗时间:" + task2_exec_time + "秒\n")
    '''
    p_starttime = datetime.datetime.now()
    p_t = []
    # 批量调
    for i in range(2):  # 下面先生成多进程执行的动态变量以及把进程加到list，加到list后统一调用进程开始run
        exec('t{} = Thread(target=mig_table_task{}, args=({},))'.format(i, i + 1, i))
        exec('p_t.append(t{})'.format(i))
        # exec('p{}.start()'.format(i))
    for t_t in p_t:  # 这里对list中存在的进程统一开始调用
        t_t.start()
    for t_t in p_t:
        t_t.join()
    # 批量调
    time.sleep(0.1)
    p_endtime = datetime.datetime.now()
    print('多进程开始时间：', p_starttime, '多进程结束时间：', p_endtime, '执行时间：', str((p_endtime - p_starttime).seconds), 'seconds')
    time.sleep(0.1)
     '''


# 仅用于迁移数据插入失败的表
def mig_failed_table():
    filename = '/tmp/insert_failed_table.csv'
    with open(filename) as f:
        reader = csv.reader(f)
        for row in reader:
            tbl_name = row[0]
            print("\033[31m开始迁移：\033[0m" + tbl_name)
            mig_table(tbl_name)
            is_continue = input('是否结束：Y|N\n')
            print(tbl_name + '插入完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            '''
            if is_continue == 'Y' or is_continue == 'y':
                print() #  continue
            else:
                print(tbl_name + '插入完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                sys.exit()
            '''
    f.close()


# 迁移摘要
def mig_summary():
    # Oracle源表信息
    oracle_schema = oracle_cursor.fetch_one("""select user from dual""")[0]
    oracle_tab_count = oracle_cursor.fetch_one("""select count(*) from user_tables""")[0]
    oracle_constraint_count = oracle_cursor.fetch_one("""select sum(row_count) from (
                               SELECT 1 row_count
                               FROM USER_IND_COLUMNS T,
                                    USER_INDEXES I,
                                    USER_CONSTRAINTS C
                               WHERE T.INDEX_NAME = I.INDEX_NAME
                                 AND T.INDEX_NAME = C.CONSTRAINT_NAME(+)
                               GROUP BY T.TABLE_NAME,
                                        T.INDEX_NAME,
                                        I.UNIQUENESS,
                                        I.INDEX_TYPE,
                                        C.CONSTRAINT_TYPE
                           )""")[0]
    oracle_fk_count = oracle_cursor.fetch_one("""select count(*) from USER_CONSTRAINTS where CONSTRAINT_TYPE='R'""")[0]
    oracle_view_count = oracle_cursor.fetch_one("""select count(*) from USER_VIEWS""")[0]
    oracle_autocol_count = oracle_autocol_total[0]
    # Oracle源表信息

    # MySQL迁移计数
    mysql_cursor.execute("""select database()""")
    mysql_database_name = mysql_cursor.fetchone()[0]
    mysql_table_count = 0
    filepath = '/tmp/ddl_success_table.log'
    if os.path.exists(filepath):
        for index, line in enumerate(open(filepath, 'r')):
            mysql_table_count += 1

    table_failed_count = len(ddl_failed_table_result)
    index_failed_count = len(constraint_failed_count)
    view_error_count = len(view_failed_count)
    fk_failed_count = len(foreignkey_failed_count)
    comment_error_count = len(comment_failed_count)
    autocol_error_count = len(autocol_failed_count)
    mysql_cursor.execute(
        """select count(*) from information_schema.TABLES where TABLE_SCHEMA in (select database()) and TABLE_TYPE='BASE TABLE'""")
    mysql_success_table_count = str(mysql_cursor.fetchone()[0])
    # MySQL迁移计数

    print('\033[31m*' * 50 + '数据迁移摘要' + '*' * 50 + '\033[0m\n\n\n')
    print("Oracle迁移数据到MySQL完毕\n" + "开始时间:" + str(starttime) + '\n' + "结束时间:" + str(endtime) + '\n' + "消耗时间:" + str(
        (endtime - starttime).seconds) + "秒\n")
    print('源数据库模式名: ' + oracle_schema)
    print('1、源表数量计数: ' + str(oracle_tab_count))
    print('2、源视图数量计数: ' + str(oracle_view_count))
    print('3、源触发器自增列计数：' + str(oracle_autocol_count))
    print('4、源表约束以及索引数量计数: ' + str(oracle_constraint_count))
    print('5、源表外键数量计数: ' + str(oracle_fk_count))
    print('\n\n\n')
    print('目标数据库: ' + mysql_database_name)
    # print('目标表成功创建计数: ' + str(mysql_table_count))
    print('1、' + '目标表创建成功计数: ' + mysql_success_table_count + ' 目标表创建失败计数: ' + str(table_failed_count))
    print('2、目标视图创建失败计数: ' + str(view_error_count))
    print('3、目标自增列修改失败计数: ' + str(autocol_error_count))
    print('4、目标表索引以及约束创建失败计数: ' + str(index_failed_count))
    print('5、目标外键创建失败计数: ' + str(fk_failed_count))
    if ddl_failed_table_result:  # if os.path.exists(path) 判断下/tmp/ddl_failed_table.log是否存在，没有就是没有迁移错误
        print("\n\nDDL创建失败的表如下：")
        for output_ddl_failed_table_result in ddl_failed_table_result:
            print(output_ddl_failed_table_result)
        print('\n\n\n')
    print('\n请检查创建失败的表DDL以及约束。有关更多详细信息，请参阅迁移输出信息')
    print('迁移日志已保存到/tmp/mig.log\n有关迁移错误请查看/tmp/ddl_failed_table.log')


#  print('目标注释添加失败计数: ' + str(comment_error_count))


if __name__ == '__main__':
    print_source_info()
    starttime = datetime.datetime.now()
    patha = '/tmp/ddl_success_table.log'  # 用来记录DDL创建成功的表
    if os.path.exists(patha):  # 在每次创建表前清空文件
        os.remove(patha)  # 删除文件
    path = '/tmp/ddl_failed_table.log'  # 创建失败的ddl日志文件
    if os.path.exists(path):  # 如果文件存在，每次迁移表前先清除失败的表csv文件
        os.remove(path)  # 删除文件
    if os.path.exists('/tmp/insert_failed_table.log'):  # 在每次创建表前清空文件
        os.remove('/tmp/insert_failed_table.log')
    create_meta_table()  # 创建表结构 串行
    split_success_list()  # 把创建成功的表分成2个list
    # create_table_main_process()  # 并行创建表
    create_meta_constraint()  # 创建约束
    create_meta_foreignkey()  # 创建外键
    async_work()  # 异步迁移数据
    # t = ThreadPoolExecutor(max_workers=2)  # 异步方式迁移数据
    # task1 = t.submit(mig_table_task1, '0')  # 此行下面的函数会以异步方式进行
    # mig_table_task2('1')
    # if task1.done():
    #    print('表数据迁移完毕！\n\n')
    # else:
    #    print('表数据迁移异常，请检查日志！\n\n')
    # mig_table_main_thread()  # 并行迁移数据
    # mig_table_task1('0') # 单独执行迁移数据第一部分
    # mig_table_task2('1') # 单独执行迁移数据第二部分
    auto_increament_col()  # 增加自增列
    create_view()  # 创建视图
    create_comment()  # 添加注释
    endtime = datetime.datetime.now()
    '''
        # 再次对失败的表迁移数据
                is_continue = input('是否再次迁移失败的表：Y|N\n')
        if is_continue == 'Y' or is_continue == 'y':
            try:
                print('开始重新插入失败的表\n')
                mig_failed_table()
            except Exception as e:
                print('插入失败')
        else:
            print('迁移完毕！')
    else:
        print('迁移成功，没有迁移失败的表')
    '''
    mig_summary()
source_db.close()
