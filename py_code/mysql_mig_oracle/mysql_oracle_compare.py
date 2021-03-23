import argparse
import sys
import textwrap
import time

import cx_Oracle
import pandas as pd
import dmPython
import pymysql
from dbutils.pooled_db import PooledDB
import configparser
import os
import traceback
from sqlalchemy import create_engine
import numpy as np

# 读取配置文件
exepath = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(exepath, 'config.ini'))


def get_mysql(self, name):
    value = config.get('mysql', name)  # 通过config.get拿到配置文件中DATABASE的name的对应值
    return value


def get_oracle(self, name):
    value = config.get('oracle', name)  # 通过config.get拿到配置文件中DATABASE的name的对应值
    return value


# MySQL read config
mysql_host = get_mysql('mysql', 'host')
mysql_port = int(get_mysql('mysql', 'port'))
mysql_user = get_mysql('mysql', 'user')
mysql_passwd = get_mysql('mysql', 'passwd')
mysql_database = get_mysql('mysql', 'database')
mysql_dbchar = get_mysql('mysql', 'dbchar')

# oracle read config
oracle_host = get_oracle('oracle', 'host')
oracle_port = get_oracle('oracle', 'port')
oracle_user = get_oracle('oracle', 'user')
oracle_passwd = get_oracle('oracle', 'passwd')
service_name = get_oracle('oracle', 'service_name')

MySQL_POOL = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=10,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=3,
    # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=['SET AUTOCOMMIT=0;'],  # 开始会话前执行的命令列表。使用连接池执行dml，这里需要显式指定提交，已测试通过
    ping=0,
    # ping MySQL服务端，检查是否服务可用。
    host=mysql_host,
    port=mysql_port,
    user=mysql_user,
    password=mysql_passwd,
    database=mysql_database,
    charset=mysql_dbchar
)

mysql_conn = MySQL_POOL.connection()
mysql_cursor = mysql_conn.cursor()
mysql_cursor.arraysize = 20000
print('MySQL host:', mysql_host)
print('oracle host:', oracle_host)

oracle_conn = cx_Oracle.connect(
    oracle_user + '/' + oracle_passwd + '@' + oracle_host + ':' + oracle_port + '/' + service_name)
oracle_cursor = oracle_conn.cursor()

parser = argparse.ArgumentParser(prog='mysql_oracle_compare',
                                 formatter_class=argparse.RawDescriptionHelpFormatter,
                                 description=textwrap.dedent('''\
VERSION:
V1.0
EXAMPLE:
    EG(1):RUN MIGRATION FETCH AND INSERT 10000 ROWS DATA INTO TABLE:\n ./oracle_to_mysql -b 10000\n
    EG(2):RUN IMPORT CUSTOM TABLE MIGRATION TO MySQL INCLUDE METADATA AND DATA ROWS:\n ./oracle_to_mysql -c true\n
    EG(3):NEED TO CREATE TARGET TABLE BEFORE RUN,THIS ONLY MIG TABLE DATA ROWS,INDEX,TRIGGER:\n ./oracle_to_mysql -c true -d true
    '''))
parser.add_argument('--custom_table', '-c', help='use source_db.txt for compare mysql and dm,DEFAULT FALSE',
                    choices=['true', 'false'], default='false')  # 默认是全表迁移
args = parser.parse_args()
# print("当前路径 -> %s" % os.getcwd())
current_path = os.path.dirname(__file__)
# 判断命令行参数-c是否指定
if args.custom_table.upper() == 'TRUE':
    custom_table = 'true'
    with open(current_path + '/source_db.txt', 'r', encoding='utf-8') as fr, open('/tmp/table.txt', 'w',
                                                                                  encoding='utf-8') as fd:
        row_count = len(fr.readlines())
    if row_count < 1:
        print('!!!请检查当前目录source_db.txt是否有数据库名!!!\n\n\n')
        time.sleep(2)
    #  在当前目录下编辑custom_table.txt，然后对该文件做去掉空行处理，输出到tmp目录
    with open(current_path + '/source_db.txt', 'r', encoding='utf-8') as fr, open('/tmp/table.txt', 'w',
                                                                                  encoding='utf-8') as fd:
        for text in fr.readlines():
            if text.split():
                fd.write(text)
else:
    custom_table = 'false'

list_source_table = []
list_target_table = []

# 创建数据库引擎
engine = create_engine(
    'oracle://' + oracle_user + ':' + oracle_passwd + '@' + oracle_host + ':' + oracle_port + '/' + service_name)


def test():
    # 创建数据库引擎
    engine = create_engine(
        'oracle://' + oracle_user + ':' + oracle_passwd + '@' + oracle_host + ':' + oracle_port + '/' + service_name)
    df = pd.DataFrame()
    df["A"] = [1, 2, 3, 4]
    df["B"] = [11, 22, 33, 44]
    df.to_sql('ooo', con=engine, if_exists='replace', index=False)


def table_prepare():
    oracle_cursor.execute("""select count(*) from user_tables where table_name=upper('data_compare')""")
    table_exist = oracle_cursor.fetchone()[0]
    if table_exist == 1:
        oracle_cursor.execute("""drop table data_compare purge""")
    oracle_cursor.execute("""create table data_compare
(id number ,
source_db_name varchar2(100),
source_table_name varchar2(100),
source_rows number,
target_user varchar2(100),
target_table_name varchar2(100),
target_rows number,
is_success varchar2(10),
compare_time date default sysdate
)""")


def check_db_exist(src_db_name, tgt_user_name):
    mysql_cursor.execute(
        """select count(distinct TABLE_SCHEMA) from information_schema.TABLES where TABLE_SCHEMA='%s' """ % src_db_name)
    src_result = mysql_cursor.fetchone()[0]
    oracle_cursor.execute("""select count(*) from dba_users where username=upper('%s')""" % tgt_user_name)
    trg_result = oracle_cursor.fetchone()[0]
    return src_result, trg_result


def diff_table(src_db_name, tgt_user_name):
    global list_target_table, list_source_table
    list_target_table = []
    list_source_table = []
    mysql_cursor.execute(
        """select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_TYPE='BASE TABLE'
         """ % src_db_name)
    s_out_table = mysql_cursor.fetchall()
    for source_table_name in s_out_table:
        source_table_name = source_table_name[0]
        list_source_table.append(source_table_name.upper())  # 将源表存入比较的list中
    oracle_cursor.execute("""select table_name from dba_tables where owner=upper('%s') """ % tgt_user_name)
    t_out_table = oracle_cursor.fetchall()
    for target_table_name in t_out_table:
        target_table_name = target_table_name[0]
        list_target_table.append(target_table_name.upper())

    # 比较下表数量，找出目标表没有创建的表
    df1 = pd.DataFrame({'table_name': list_source_table})
    df2 = pd.DataFrame({'table_name': list_target_table})
    df3 = df1.merge(df2.drop_duplicates(), how='left', indicator=True)
    df4 = df3[df3['_merge'] == 'left_only']
    df5 = pd.DataFrame(df4, columns=['table_name'])
    if not df5.empty:
        print('以下表在目标数据库不存在\n', df5)
        df5.to_sql('missed_table', con=engine, if_exists='replace', index=False)
        # df.to_sql('表名',con=engine,if_exists='replace/append/fail',index=False)


def data_compare_multi():
    print('开始比较源数据库与目数据库差异')
    db_id = 0
    with open("/tmp/table.txt", "r") as source_db_list:  # 读取文本文件中的源数据库名称
        for source_db in source_db_list:  # 一个个数据库来比较
            table_id = 0
            source_db = source_db.strip('\n')  # 这里需要去掉换行符（\n）
            src_out, trg_out = check_db_exist(source_db, source_db)  # 使用文本文件比较多个数据库时，缺省情况下源数据库名称与目标模式名相同
            if src_out == 0:
                print(source_db, '在源数据库不存在，已跳过并继续比较下一个数据库!')
                continue
            if trg_out == 0:
                print(source_db, '在目标数据库不存在此模式名，已跳过并继续比较下一个数据库!')
                continue

            # 比较下源数据库以及目标数据库表数量是否一致
            try:
                mysql_cursor.execute(
                    """select count(*) from information_schema.TABLES where TABLE_SCHEMA ='%s' and TABLE_TYPE='BASE TABLE'""" % source_db)
                src_table_total = mysql_cursor.fetchone()[0]  # 源表数量
                oracle_cursor.execute(
                    """select count(*) from dba_tables where table_name not in ('MY_MIG_TASK_INFO','DATA_COMPARE','MISSED_TABLE') and owner='%s'""" % source_db)
                tar_table_total = oracle_cursor.fetchone()[0]  # 目标数量
            except Exception as e:
                print(e)
            db_id += 1
            print('\n', db_id, '、源数据库名称:', source_db, '表数量:', src_table_total, '目标模式名:', source_db, '表数量:',
                  tar_table_total)
            # 比较下当前源跟目标库的表总数，如果两者表数量不一致，输出目标数据库缺失的表
            diff_table(source_db, source_db)
            try:
                # 先根据MySQL的表名查每个表的行数
                mysql_cursor.execute(
                    """select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_TYPE='BASE TABLE'
                     """ % source_db)
                out_table = mysql_cursor.fetchall()
                for source_table_name in out_table:
                    source_table_name = source_table_name[0]
                    mysql_cursor.execute("""select count(*) from %s.`%s`""" % (source_db, source_table_name))
                    source_rows = mysql_cursor.fetchone()[0]
                    try:
                        target_user_name = source_db
                        target_table_name = source_table_name
                        oracle_cursor.execute(
                            """select count(*) from dba_tables where owner=upper('%s') and table_name =upper('%s')""" % (
                                target_user_name, target_table_name))
                        target_is_exist = oracle_cursor.fetchone()[0]
                        if target_is_exist == 0:
                            continue
                        else:
                            oracle_cursor.execute(
                                """select count(*) from %s.%s""" % (target_user_name, target_table_name))
                            target_rows = oracle_cursor.fetchone()[0]
                            if source_rows != target_rows:
                                is_success = 'N'
                            else:
                                is_success = 'Y'
                            table_id += 1
                            oracle_cursor.execute("""insert into data_compare
                                                    (id,
                                                                source_db_name,
                                                                source_table_name,
                                                                source_rows,
                                                                target_user,
                                                                target_table_name,
                                                                target_rows,
                                                                is_success
                                                                ) values(%s,'%s','%s',%s,'%s','%s',%s,'%s')""" % (
                                table_id, source_db.upper(), source_table_name.upper(), source_rows,
                                target_user_name.upper(), target_table_name.upper(),
                                target_rows, is_success.upper()))
                    except Exception as e:
                        print(e, ' 请检查表是否存在')

            except Exception as e:
                print(e)


def data_compare_single(sourcedb, target_user):
    table_id = 0
    src_out, trg_out = check_db_exist(sourcedb, target_user)
    if src_out == 0:
        print(sourcedb, '在源数据库不存在\nEXIT!')
        sys.exit()
    elif trg_out == 0:
        print(target_user, '在目标数据库不存在此模式名\nEXIT!')
        sys.exit()
    else:
        try:
            # 输出源数据库以及目标数据库表数量
            mysql_cursor.execute(
                """select count(*) from information_schema.TABLES where TABLE_SCHEMA ='%s' and TABLE_TYPE='BASE TABLE'""" % sourcedb)
            src_table_total = mysql_cursor.fetchone()[0]  # 源表行数
            oracle_cursor.execute(
                """select count(*) from dba_tables where table_name not in ('DATA_COMPARE','MISSED_TABLE','MY_MIG_TASK_INFO') and owner=upper('%s')""" % target_user)
            tar_table_total = oracle_cursor.fetchone()[0]  # 目标行数
            print('\n', '源数据库名称:', sourcedb, '表数量:', src_table_total, '目标模式名:', target_user, '表数量:',
                  tar_table_total)
            # 比较下当前源跟目标库的表总数，如果两者表数量不一致，输出目标数据库缺失的表
            diff_table(sourcedb, target_user)
        except Exception as e:
            print(e)
        print('开始比较数据差异\n源数据库名称:', sourcedb, '目标模式名:', target_user)
        try:
            # 先根据MySQL的表名查每个表的行数
            mysql_cursor.execute(
                """select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_TYPE='BASE TABLE'
                 """ % sourcedb)
            out_table = mysql_cursor.fetchall()
            for source_table_name in out_table:
                source_table_name = source_table_name[0]
                mysql_cursor.execute("""select count(*) from %s.`%s`""" % (sourcedb, source_table_name))
                source_rows = mysql_cursor.fetchone()[0]  # 源表行数
                try:
                    target_user_name = target_user
                    target_table_name = source_table_name
                    oracle_cursor.execute("""select count(*) from %s.%s""" % (target_user_name, target_table_name))
                    target_rows = oracle_cursor.fetchone()[0]  # 目标表行数
                    if source_rows != target_rows:
                        is_success = 'N'
                    else:
                        is_success = 'Y'
                    table_id += 1
                    oracle_cursor.execute("""insert into data_compare
                    (id,
                                source_db_name,
                                source_table_name,
                                source_rows,
                                target_user,
                                target_table_name,
                                target_rows,
                                is_success
                                ) values(%s,'%s','%s',%s,'%s','%s',%s,'%s')""" % (
                        table_id, sourcedb.upper(), source_table_name.upper(), source_rows, target_user_name.upper(),
                        target_table_name.upper(),
                        target_rows, is_success.upper()))
                    oracle_conn.commit()
                except Exception as e:
                    print(e, ',表', '[', source_table_name.upper(), ']', ' 查询表数量异常，请检查目标表是否存在')
        except Exception as e:
            print(e)


table_prepare()
# test()
# diff_table('CNSBZB_V9_KF', 'CNSBZB_V9_KF')
if custom_table.upper() == 'TRUE':
    data_compare_multi()
else:
    input_source_db = input('请输入源数据库名称\n')
    input_target_db = input('请输入目标数据库模式名\n')
    # 后面优化下，检查下源库跟目标库是否存在，再比较
    data_compare_single(input_source_db, input_target_db)
print('数据比较已结束，请查看目标表"DATA_COMPARE"获取表数量详细信息，查询MISSED_TABLE获取缺失的表')
oracle_cursor.close()
oracle_conn.close()
