# -*- coding: utf-8 -*-
# oracle_mig_mysql.py
# Oracle database migration to MySQL
# CURRENT VERSION
# V1.3.9.2
"""
MODIFY HISTORY
****************************************************
V1.3.9.2
2020.11.12
bug修复、输出调整
****************************************************
V1.3.9.1
2020.11.11
增加异步方式插入表
****************************************************
V1.3.9
2020.11.10
1、在创建表的时候改为使用连接池
2、添加MySQL连接池
****************************************************
V1.3.8.1
2020.11.5
修复多处错误以及测试
****************************************************
V1.3.8
2020.11.4
1、修改创建表为并行创建
****************************************************
V1.3.7.1
2020.11.4
1、增加表对象的comment属性支持
2、优化计数统计
****************************************************
V1.3.7
2020.11.3
1、增加ddl创建comment列字段注释
2、增加Oracle视图创建到MySQL
****************************************************
V1.3.6.3
2020.11.3 11:20
优化异常log记录方式
****************************************************
V1.3.6.2
2020.11.2
1、优化索引创建方式
2、不再对大于1000长度的字符串类型映射为tinytext
3、修复调用批量创建自增列索引，无法跳过异常的问题
****************************************************
V1.3.6.1
2020.11.2
1、由于Oracle的long类型无法使用字符串截取，增加临时表trigger_name用于存储Oracle触发器信息
2、优化输出方式
****************************************************
V1.3.6
2020.10.30
1、增加创建自增列的sql
2、增加源数据库连接信息、表、视图、触发器等信息
3、下个版本需要创建存Oracle触发器定义以及表字段的表
****************************************************
v1.3.5
2020.10.29
1、增加外键创建
2、增加迁移摘要，统计表计数
3、优化输出格式
****************************************************
v1.3.4
2020.10.28
增加创建索引的sql
****************************************************
v1.3.3
2020.10.27
1、解决number字段类型默认值包含括号的问题
2、解决字符串类型默认值包含括号的问题
3、增加输出ddl创建失败的表以及异常捕获语句
4、增加ddl创建成功的表记录到文件
5、修改在迁移时仅读取ddl成功的表
6、优化注释、优化格式界面
****************************************************
v1.3.2.1
2020.10.26
1、增加ddl创建log
2、待修正number类型默认值创建失败问题
****************************************************
v1.3.2
2020.10.23
1、优化lob、number类handler
2、优化主键查询方式
3、修改varchar2超过2000字节字段映射为MySQL tinytext
4、修正number、date类型默认值中包含括号等问题
****************************************************
v1.3.1
2020.10.21
1、使用cx_Oracle增加NumberToDecimal的handler处理
2、修正Oracle浮点类型数据保留小数位问题(小数位超过4位的number类型数据插入到MySQL数据不准确)
3、优化数据库连接以及游标对象名称
****************************************************
v1.3
2020.10.16
1、在线创建表结构、增加主键
2、支持MySQL字符类型、时间类型、数值类型、大字段类型
3、在线迁移数据（在迁移前可先收集下数据库的统计信息方便数值类型平均长度判断）
****************************************************
v1.2
2020.9.30
1、记录迁移失败的表
2、能再次迁移失败的表
****************************************************
v1.1
2020.9.24
1、目标库需要先创建好表结构
2、实现了Oracle blob、nclob大字段插入到MySQL
3、按照每张表顺序进行迁移，如遇字段溢出、类型不匹配等异常会中断
****************************************************
"""
import cx_Oracle
import pymysql
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
from dbutils.pooled_db import PooledDB
from concurrent.futures import ThreadPoolExecutor

MySQLPOOL = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=10,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=3,
    # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=['SET AUTOCOMMIT=1;'],  # 开始会话前执行的命令列表。
    ping=0,
    # ping MySQL服务端，检查是否服务可用。
    host='172.16.4.81',
    port=3306,
    user='root',
    password='Gepoint',
    database='test',
    charset='utf8mb4'
)


class OraclePool:
    """
    1) 这里封装了一些有关oracle连接池的功能;
    2) sid和service_name，程序会自动判断哪个有值，
        若两个都有值，则默认使用service_name；
    3) 关于config的设置，注意只有 port 的值的类型是 int，以下是config样例:
        config = {
            'user':         'maixiaochai',
            'password':     'maixiaochai',
            'host':         '192.168.158.1',
            'port':         1521,
            'sid':          'maixiaochai',
            'service_name': 'maixiaochai'
        }
    """

    def __init__(self, config):
        """
        获得连接池
        :param config:      dict    Oracle连接信息
        """
        self.__pool = self.__get_pool(config)

    @staticmethod
    def __get_pool(config):
        """
        :param config:        dict    连接Oracle的信息
        ---------------------------------------------
        以下设置，根据需要进行配置
        maxconnections=6,   # 最大连接数，0或None表示不限制连接数
        mincached=2,        # 初始化时，连接池中至少创建的空闲连接。0表示不创建
        maxcached=5,        # 连接池中最多允许的空闲连接数，很久没有用户访问，连接池释放了一个，由6个变为5个，
                            # 又过了很久，不再释放，因为该项设置的数量为5
        maxshared=0,        # 在多个线程中，最多共享的连接数，Python中无用，会最终设置为0
        blocking=True,      # 没有闲置连接的时候是否等待， True，等待，阻塞住；False，不等待，抛出异常。
        maxusage=None,      # 一个连接最多被使用的次数，None表示无限制
        setession=[],       # 会话之前所执行的命令, 如["set charset ...", "set datestyle ..."]
        ping=0,             # 0  永远不ping
                            # 1，默认值，用到连接时先ping一下服务器
                            # 2, 当cursor被创建时ping
                            # 4, 当SQL语句被执行时ping
                            # 7, 总是先ping
        """
        dsn = None
        host, port = config.get('host'), config.get('port')

        if 'service_name' in config:
            dsn = cx_Oracle.makedsn(host, port, service_name=config.get('service_name'))

        elif 'sid' in config:
            dsn = cx_Oracle.makedsn(host, port, sid=config.get('sid'))

        pool = PooledDB(
            cx_Oracle,
            mincached=5,
            maxcached=10,
            user=config.get('user'),
            password=config.get('password'),
            dsn=dsn
        )

        return pool

    def __get_conn(self):
        """
        从连接池中获取一个连接，并获取游标。
        :return: conn, cursor
        """
        conn = self.__pool.connection()
        cursor = conn.cursor()

        return conn, cursor

    @staticmethod
    def __reset_conn(conn, cursor):
        """
        把连接放回连接池。
        :return:
        """
        cursor.close()
        conn.close()

    def __execute(self, sql, args=None):
        """
        执行sql语句
        :param sql:     str     sql语句
        :param args:    list    sql语句参数列表
        :param return:  cursor
        """
        conn, cursor = self.__get_conn()

        if args:
            cursor.execute(sql, args)
        else:
            cursor.execute(sql)

        return conn, cursor

    def fetch_all(self, sql, args=None):
        """
        获取全部结果
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        result = cursor.fetchall()
        self.__reset_conn(conn, cursor)

        return result

    def fetch_one(self, sql, args=None):
        """
        获取全部结果
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        result = cursor.fetchone()
        self.__reset_conn(conn, cursor)

        return result

    def fetch_many(self, sql, args=None):
        conn = self.__pool.connection()
        cur = conn.cursor()
        try:
            cur.execute(sql, args)
            while True:
                row = cur.fetchone()
                if row is None:
                    break
                # 在此编写你想做的操作
                return row
        except Exception as e:
            print('异常信息:' + str(e))

    def execute_sql(self, sql, args=None):
        """
        执行SQL语句。
        :param sql:     str     sql语句
        :param args:    list    sql语句参数
        :return:        tuple   fetch结果
        """
        conn, cursor = self.__execute(sql, args)
        conn.commit()
        self.__reset_conn(conn, cursor)

    def __del__(self):
        """
        关闭连接池。
        """
        self.__pool.close()


# NJJBXQ_DJGBZ

config = {
    'user': 'DATATEST',
    'password': 'oracle',
    'host': '172.16.4.81',
    'port': 1521,
    'service_name': 'orcl'
}

oracle_cursor = OraclePool(config)  # Oracle连接池
mysql_conn = MySQLPOOL.connection()  # MySQL连接池
mysql_cursor = mysql_conn.cursor()


# pool2 = PooledDB(cx_Oracle, user='NJJBXQ_DJGBZ', password='11111', dsn='192.168.189.208:1522/orcl11g', mincached=5, maxcached=20)


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


list_table_name = []  # 查完user_tables即当前用户所有表存入list
list_success_table = []  # 创建成功的表存入到list
new_list = []  # 用于存储1分为2的表，将原表分成2个list
# lock = threading.Lock()  # 用于游标在执行时增加以及释放锁
sys.stdout = Logger(stream=sys.stdout)
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  # 设置字符集为UTF8，防止中文乱码
# ora_conn = 'test2/oracle@192.168.189.208:1522/orcl11g'  # test2
ora_conn = 'DATATEST/oracle@172.16.4.81/orcl'  # NJJBXQ_DJGBZ
mysql_conn = '172.16.4.81'
mysql_target_db = 'test'
source_db = cx_Oracle.connect(ora_conn)  # 源库Oracle的数据库连接
target_db = pymysql.connect(mysql_conn, "root", "Gepoint", mysql_target_db)  # 目标库MySQL的数据库连接
source_db_type = 'Oracle'  # 大小写无关，后面会被转为大写
target_db_type = 'MySQL'  # 大小写无关，后面会被转为大写
cur_select = source_db.cursor()  # 源库Oracle查询源表有几列
cur_tblprt = source_db.cursor()  # 生成用于输出表名的游标对象
cur_oracle_result = source_db.cursor()  # 查询Oracle源表的游标结果集
cur_insert_mysql = target_db.cursor()  # 目标库MySQL插入目标表执行的插入sql
cur_createtbl = target_db.cursor()
cur_drop_table = target_db.cursor()  # 在MySQL清除表 drop table if exists
cur_source_constraint = source_db.cursor()
cur_target_constraint = target_db.cursor()
cur_oracle_result.arraysize = 10000  # Oracle数据库游标对象结果集返回的行数即每次获取多少行
cur_insert_mysql.arraysize = 10000  # MySQL数据库批量插入的行数
# 以下为异步迁移数据单独创建了连接
source_db2 = cx_Oracle.connect(ora_conn)  # 源库Oracle的数据库连接
cur_select2 = source_db2.cursor()  # 源库Oracle查询源表有几列
cur_oracle_result2 = source_db2.cursor()  # 查询Oracle源表的游标结果集
cur_oracle_result2.arraysize = 10000
target_db1 = pymysql.connect(mysql_conn, "root", "Gepoint", mysql_target_db)  # 目标库MySQL的数据库连接
target_db2 = pymysql.connect(mysql_conn, "root", "Gepoint", mysql_target_db)  # 目标库MySQL的数据库连接
cur_insert_mysql1 = target_db1.cursor()  # 目标库MySQL插入目标表执行的插入sql
cur_insert_mysql2 = target_db2.cursor()  # 目标库MySQL插入目标表执行的插入sql
cur_insert_mysql1.arraysize = 10000
cur_insert_mysql2.arraysize = 10000


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
cur_oracle_result2.outputtypehandler = dataconvert  # 查询Oracle表数据结果集的游标
cur_source_constraint.outputtypehandler = dataconvert  # 查询Oracle主键以及索引、外键的游标

# 用于记录ddl创建失败的表名
ddl_failed_table_result = []

# 用于统计主键以及索引创建失败的计数
constraint_failed_count = []

# 用于统计外键创建失败的计数
foreignkey_failed_count = []

# 用于统计视图创建失败的计数
view_failed_count = []

# 用于统计注释添加失败的计数
comment_failed_count = []

# 用于统计Oracle中自增列的计数
oracle_autocol_total = []

# 用于统计MySQL中自增列创建失败的计数
autocol_failed_count = []


# source_table = input("请输入源表名称:")    # 手动从键盘获取源表名称
# target_table = input("请输入目标表名称:")  # 手动从键盘获取目标表名称

def list_of_groups(init_list, children_list_len):
    list_of_groups = zip(*(iter(init_list),) * children_list_len)
    end_list = [list(i) for i in list_of_groups]
    count = len(init_list) % children_list_len
    end_list.append(init_list[-count:]) if count != 0 else end_list
    return end_list


def split_list():  # 将user_tables的list结果分为2个小list
    print_table()
    n = round(len(list_table_name) / 2)
    # n = 242
    print(n)
    print('原始list：', list_table_name, '\n')
    new_list.append(list_of_groups(list_table_name, n))
    print('一分为二：', new_list)


def split_success_list():  # 将创建表成功的list结果分为2个小list
    print_table()
    n = round(len(list_success_table) / 2)
    # n = 242
    print(n)
    print('原始list：', list_success_table, '\n')
    new_list.append(list_of_groups(list_success_table, n))
    print('一分为二：', new_list)


# 打印连接信息
def print_source_info():
    print('-' * 50 + 'Oracle->MySQL' + '-' * 50)
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    print('源Oracle数据库连接信息: ' + ora_conn)
    cur_select.execute("""select count(*) from user_tables""")
    source_table_count = cur_select.fetchone()[0]
    cur_select.execute("""select count(*) from user_views""")
    source_view_count = cur_select.fetchone()[0]
    cur_select.execute("""select count(*) from user_triggers where TRIGGER_NAME not like 'BIN$%'""")
    source_trigger_count = cur_select.fetchone()[0]
    cur_select.execute("""
    select count(*) from USER_PROCEDURES where OBJECT_TYPE='PROCEDURE' and OBJECT_NAME  not like 'BIN$%'""")
    source_procedure_count = cur_select.fetchone()[0]
    cur_select.execute(
        """select count(*) from USER_PROCEDURES where OBJECT_TYPE='FUNCTION' and OBJECT_NAME  not like 'BIN$%'""")
    source_function_count = cur_select.fetchone()[0]
    cur_select.execute(
        """select count(*) from USER_PROCEDURES where OBJECT_TYPE='PACKAGE' and OBJECT_NAME  not like 'BIN$%'""")
    source_package_count = cur_select.fetchone()[0]
    print('源表总计: ' + str(source_table_count))
    print('源视图总计: ' + str(source_view_count))
    print('源触发器总计: ' + str(source_trigger_count))
    print('源存储过程总计: ' + str(source_procedure_count))
    print('源数据库函数总计: ' + str(source_function_count))
    print('源数据库包总计: ' + str(source_package_count))
    print('目标MySQL数据库连接信息: ' + 'ip-> ' + mysql_conn + ' database-> ' + mysql_target_db)


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


# 获取在MySQL创建目标表的DDL、增加主键
def create_table(table_name):
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
                                                       ('comment ' + '\'' + commentvalue + '\'') if commentvalue else ''
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


# 用来创建目标索引的函数
def user_constraint():
    #  将创建失败的sql记录到log文件
    # logging.basicConfig(filename='/tmp/constraint_error_table.log')
    cur_source_constraint.execute("""SELECT
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
    for d in cur_source_constraint:
        create_index_sql = d[0]
        print(create_index_sql)
        try:
            cur_target_constraint.execute(create_index_sql)
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


# 创建外键
def user_foreign_key(table_name):
    cur_source_constraint.execute("""SELECT 'ALTER TABLE ' || B.TABLE_NAME || ' ADD CONSTRAINT ' ||
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
    for e in cur_source_constraint:
        create_foreign_key_sql = e[0]
        print(create_foreign_key_sql)
        try:
            cur_target_constraint.execute(create_foreign_key_sql)
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


# 调用user_foreign_key函数批量创建外键
def create_meta_foreignkey():
    table_foreign_key = 'select table_name from USER_CONSTRAINTS where CONSTRAINT_TYPE= \'R\''
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    cur_source_constraint.execute(table_foreign_key)
    for v_result_table in cur_source_constraint:
        table_name = v_result_table[0]
        print('#' * 50 + '开始创建' + table_name + '外键 ' + '#' * 50)
        user_foreign_key(table_name)  # 调用user_foreign_key函数批量创建主键以及索引
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('\033[31m*' * 50 + '外键创建完成' + '*' * 50 + '\033[0m\n\n\n')


# 调用user_constraint函数批量创建主键以及索引
def create_meta_constraint():
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
    print('#' * 50 + '开始创建' + '约束以及索引 ' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    user_constraint()  # 现在改为了一次性创建约束以及索引
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print('\033[31m*' * 50 + '主键约束、索引创建完成' + '*' * 50 + '\033[0m\n\n\n')


# 查找具有自增特性的表以及字段名称
def auto_increament_col():
    print('#' * 50 + '开始增加自增列' + '#' * 50)
    # Oracle中无法对long类型数据截取，创建用于存储触发器字段信息的临时表TRIGGER_NAME
    cur_oracle_result.execute("""
    select count(*) from user_tables where table_name='TRIGGER_NAME'
    """)
    count_num_tri = cur_oracle_result.fetchone()[0]
    if count_num_tri == 1:
        cur_oracle_result.execute("""
            truncate table trigger_name
            """)
        cur_oracle_result.execute("""
                insert into trigger_name select table_name ,to_lob(trigger_body) from user_triggers
                """)

    else:
        cur_oracle_result.execute("""
                    create table trigger_name (table_name varchar2(200),trigger_body clob)
                    """)
        cur_oracle_result.execute("""
        insert into trigger_name select table_name ,to_lob(trigger_body) from user_triggers
        """)

    cur_oracle_result.execute("""
    select 'create  index ids_'||substr(table_name,1,26)||' on '||table_name||'('||upper(substr(substr(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.')), 1, instr(upper(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.'))), ' FROM DUAL;') - 1), 5)) ||');' as sql_create from trigger_name where instr(upper(trigger_body), 'NEXTVAL')>0
    """)  # 在Oracle拼接sql生成用于在MySQL中自增列的索引

    print('创建用于自增列的索引:\n ')
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    for v_increa_index in cur_oracle_result:
        create_autoincrea_index = v_increa_index[0]
        print(create_autoincrea_index)
        try:
            cur_insert_mysql.execute(create_autoincrea_index)
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
    cur_oracle_result.execute("""
    select 'alter table '||table_name||' modify '||upper(substr(substr(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.')), 1, instr(upper(SUBSTR(trigger_body, INSTR(upper(trigger_body), ':NEW.') + 1,length(trigger_body) - instr(trigger_body, ':NEW.'))), ' FROM DUAL;') - 1), 5)) ||' int auto_increment;' from trigger_name where instr(upper(trigger_body), 'NEXTVAL')>0
    """)
    for v_increa_col in cur_oracle_result:
        alter_increa_col = v_increa_col[0]
        print('\n执行sql alter table：\n')
        print(alter_increa_col)
        try:  # 注意下try要在for里面
            cur_insert_mysql.execute(alter_increa_col)
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
    cur_oracle_result.execute("""select count(*) from trigger_name  where instr(upper(trigger_body), 'NEXTVAL')>0""")
    oracle_autocol_total.append(cur_oracle_result.fetchone()[0])  # 将自增列的总数存入list
    cur_oracle_result.execute("""
    drop table trigger_name purge
    """)


# 获取视图定义以及创建
def create_view():
    print('#' * 50 + '开始创建视图' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    # Oracle中无法对long类型数据截取，创建用于存储视图信息的临时表content_view
    cur_oracle_result.execute("""
        select count(*) from user_tables where table_name='CONTENT_VIEW'
        """)
    count_num_view = cur_oracle_result.fetchone()[0]
    if count_num_view == 1:
        cur_oracle_result.execute("""drop table CONTENT_VIEW purge""")
        cur_oracle_result.execute("""create table content_view (view_name varchar2(200),text clob)""")
        cur_oracle_result.execute(
            """insert into content_view(view_name,text) select view_name,to_lob(text) from USER_VIEWS""")
    else:
        cur_oracle_result.execute("""create table content_view (view_name varchar2(200),text clob)""")
        cur_oracle_result.execute(
            """insert into content_view(view_name,text) select view_name,to_lob(text) from USER_VIEWS""")
    cur_source_constraint.execute("""
    select  view_name,'create view '||view_name||' as '||replace(text, '"'  , '') as view_sql from CONTENT_VIEW
    """)
    for e in cur_source_constraint:
        view_name = e[0]
        create_view_sql = e[1]
        print(create_view_sql)
        try:
            cur_target_constraint.execute("""drop view  if exists %s""" % view_name)
            cur_target_constraint.execute(create_view_sql)
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
    cur_oracle_result.execute("""
            drop table content_view purge
            """)


# 数据库对象的comment注释
def create_comment():
    print('#' * 50 + '开始添加comment注释' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    cur_source_constraint.execute("""
    select TABLE_NAME,'alter table '||TABLE_NAME||' comment '||''''||COMMENTS||'''' as create_comment
 from USER_TAB_COMMENTS where COMMENTS is not null
    """)
    for e in cur_source_constraint:
        table_name = e[0]
        create_comment_sql = e[1]
        print(create_comment_sql)
        try:
            print('正在添加' + table_name + '的注释:')
            cur_target_constraint.execute(create_comment_sql)
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
    cur_tblprt.execute(tableoutput_sql)
    for v_table in cur_tblprt:
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


# 批量将Oracle数据插入到MySQL的方法
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
            5000))  # 每次获取2000行，cur_oracle_result.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
        #  print(cur_oracle_result.description)  # 打印Oracle查询结果集字段列表以及类型
        try:
            cur_insert_mysql.executemany(insert_sql, rows)  # 批量插入每次5000行，需要注意的是 rows 必须是 list [] 数据类型
            # mysql_cursor.executemany(insert_sql, rows)
            # mysql_conn.commit() 对于连接池，这里需要显式提交
            target_db.commit()  # 提交
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
def create_meta_table():
    tableoutput_sql = 'select table_name from user_tables  order by table_name  desc'  # 查询需要导出的表
    # cur_tblprt.execute(tableoutput_sql)
    output_table_name = oracle_cursor.fetch_all(tableoutput_sql)
    starttime = datetime.datetime.now()
    for row in output_table_name:
        tbl_name = row[0]
        print('#' * 50 + '开始创建表' + tbl_name + '#' * 50)
        print(tbl_name + '\n')
        create_table(tbl_name)  # 调用Oracle映射到MySQL规则的函数
    print('\033[31m*' * 50 + '表创建完成' + '*' * 50 + '\033[0m\n\n\n')
    endtime = datetime.datetime.now()
    print("表创建耗时\n" + "开始时间:" + str(starttime) + '\n' + "结束时间:" + str(endtime) + '\n' + "消耗时间:" + str(
        (endtime - starttime).seconds) + "秒\n")
    '''
    # 以下为串行方式创建表
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


# 从csv文件读取源库需要迁移的表，调用mig_table(tbl_name, 1)，插入表数据，并输出迁移失败的表
def mig_database():
    filename = '/tmp/ddl_success_table.log'  # 读取要迁移的表，csv文件 list_table_name
    print('-' * 50 + '开始表数据迁移' + '-' * 50)
    with open(filename) as f:
        reader = csv.reader(f)
        for row in reader:
            tbl_name = row[0]
            print('-' * 50 + tbl_name + '正在迁移数据' + '-' * 50)
            # print("\033[31m开始表数据迁移：\033[0m\n")
            print(tbl_name)
            mig_table(tbl_name)
            print(tbl_name + '数据插入完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '\n')
        print('\033[31m*' * 50 + '表数据迁移完成' + '*' * 50 + '\033[0m\n\n\n')
    f.close()


def create_table_task1(name):
    print(name)
    task1_starttime = datetime.datetime.now()
    for v_table_name in new_list[0][int(name)]:
        table_name = v_table_name
        print('#' * 50 + '开始创建表' + table_name + '#' * 50)
        print('task1 ' + table_name + '\n')
        create_table(table_name)  # 调用Oracle映射到MySQL规则的函数
    print('\033[31m*' * 50 + '表创建完成' + '*' * 50 + '\033[0m\n\n\n')
    task1_endtime = datetime.datetime.now()
    task1_exec_time = str((task1_endtime - task1_starttime).seconds)
    # list_task1_time.append(str(task1_endtime))
    # list_task1_time.append(str(task1_exec_time))
    # time.sleep(0.5)
    print("第一部分的表\n" + "开始时间:" + str(task1_starttime) + '\n' + "结束时间:" + str(
        task1_endtime) + '\n' + "消耗时间:" + task1_exec_time + "秒\n")


def create_table_task2(name):
    print(name)
    task2_starttime = datetime.datetime.now()
    for v_table_name in new_list[0][int(name)]:
        table_name = v_table_name
        print('#' * 50 + '开始创建表' + table_name + '#' * 50)
        print('task2 ' + table_name + '\n')
        create_table(table_name)  # 调用Oracle映射到MySQL规则的函数
    print('\033[31m*' * 50 + '表创建完成' + '*' * 50 + '\033[0m\n\n\n')
    task2_endtime = datetime.datetime.now()
    task2_exec_time = str((task2_endtime - task2_starttime).seconds)
    # time.sleep(0.5)
    print("第二部分的表\n" + "开始时间:" + str(task2_starttime) + '\n' + "结束时间:" + str(
        task2_endtime) + '\n' + "消耗时间:" + task2_exec_time + "秒\n")


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


def mig_table_task1(list_index):
    time.sleep(0.1)
    # source_db = cx_Oracle.connect(ora_conn)
    # cur_oracle_result = source_db.cursor
    # cur_select = source_db.cursor
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
                10000))  # 每次获取2000行，cur_oracle_result.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
            #  print(cur_oracle_result.description)  # 打印Oracle查询结果集字段列表以及类型
            try:
                cur_insert_mysql1.executemany(insert_sql, rows)  # 批量插入每次5000行，需要注意的是 rows 必须是 list [] 数据类型
                # mysql_cursor.executemany(insert_sql, rows)
                # mysql_conn.commit() 对于连接池，这里需要显式提交
                target_db1.commit()  # 提交
            except Exception as e:
                time.sleep(0.1)
                # 1print(traceback.format_exc())  # 遇到异常记录到log，会继续迁移下张表
                # 1sql_insert_error1 = traceback.format_exc()
                #  print(tablename, '表记录', rows, '插入失败') 插入失败时输出insert语句
                # print_insert_failed_table(table_name)
                filename = '/tmp/insert_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('/' + '*' * 50 + table_name + ' INSERT ERROR' + '*' * 50 + '/\n')
                f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n\n')
                f.write(insert_sql + '\n\n\n')
                f.write('sql_insert_error1' + '\n\n')
                f.close()
                #v1logging.error(sql_insert_error1)  # 插入失败的sql语句输出到文件/tmp/ddl_failed_table.log

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
                10000))  # 每次获取2000行，cur_oracle_result.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
            #  print(cur_oracle_result.description)  # 打印Oracle查询结果集字段列表以及类型
            try:
                cur_insert_mysql2.executemany(insert_sql, rows)  # 批量插入每次5000行，需要注意的是 rows 必须是 list [] 数据类型
                # mysql_cursor.executemany(insert_sql, rows)
                # mysql_conn.commit() 对于连接池，这里需要显式提交
                target_db2.commit()  # 提交
            except Exception as e:
                time.sleep(0.2)
                # 1print(traceback.format_exc())  # 遇到异常记录到log，会继续迁移下张表
                # 1sql_insert_error2 = traceback.format_exc()
                #  print(tablename, '表记录', rows, '插入失败') 插入失败时输出insert语句
                # print_insert_failed_table(table_name)
                filename = '/tmp/insert_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('/' + '*' * 50 + table_name + ' INSERT ERROR' + '*' * 50 + '/\n')
                f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n\n')
                f.write(insert_sql + '\n\n\n')
                f.write('sql_insert_error2' + '\n\n')
                f.close()
                # 1logging.error(sql_insert_error2)  # 插入失败的sql语句输出到文件/tmp/ddl_failed_table.log

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
    cur_select.execute("""select user from dual""")
    oracle_schema = cur_select.fetchone()[0]
    cur_select.execute("""select count(*) from user_tables""")
    oracle_tab_count = cur_select.fetchone()[0]
    cur_select.execute("""select sum(row_count) from (
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
                           )""")
    oracle_constraint_count = cur_select.fetchone()[0]
    cur_select.execute("""select count(*) from USER_CONSTRAINTS where CONSTRAINT_TYPE='R'""")
    oracle_fk_count = cur_select.fetchone()[0]
    cur_select.execute("""select count(*) from USER_VIEWS""")
    oracle_view_count = cur_select.fetchone()[0]
    oracle_autocol_count = oracle_autocol_total[0]
    # Oracle源表信息

    # MySQL迁移计数
    cur_insert_mysql.execute("""select database()""")
    mysql_database_name = cur_insert_mysql.fetchone()[0]
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
    print('1、目标表创建失败计数: ' + str(table_failed_count))
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
    # split_list()  # 把user_tables结果分成2个list
    # print_table()  # 1、读取user_tables,生成Oracle要迁移的表写入到csv文件，现在不用了
    create_meta_table()  # 2、创建表结构 串行
    split_success_list()  # 把创建成功的表分成2个list
    # create_table_main_process()  # 并行创建表
    create_meta_constraint()  # 3、创建约束
    create_meta_foreignkey()  # 4、创建外键
    # mig_database()  # 5、迁移数据 (只迁移DDL创建成功的表) 串行
    t = ThreadPoolExecutor(max_workers=2)  # 异步方式迁移数据
    task1 = t.submit(mig_table_task1, '0')  # 此行下面的函数会以异步方式进行
    mig_table_task2('1')
    if task1.done():
        print('表数据迁移完毕！\n\n')
    else:
        print('表数据迁移异常，请检查日志！\n\n')
    # mig_table_main_thread()  # 并行迁移数据
    # mig_table_task1('0') # 单独执行迁移数据第一部分
    # mig_table_task2('1') # 单独执行迁移数据第二部分
    auto_increament_col()  # 6、增加自增列
    create_view()  # 7、创建视图
    create_comment()  # 8、添加注释
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
cur_select.close()
cur_insert_mysql.close()
source_db.close()
target_db.close()
