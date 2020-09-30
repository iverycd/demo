# -*- coding: utf-8 -*-
# oracle_mig_mysql.py
# Oracle database migration to MySQL
# CURRENT VERSION
# V1.2
"""
MODIFY HISTORY
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
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  # 设置字符集为UTF8，防止中文乱码
source_db = cx_Oracle.connect('admin/oracle@192.168.189.208:1522/orcl11g')  # 源库
target_db = pymysql.connect("192.168.189.208", "root", "Gepoint", "test")  # 目标库
source_db_type = 'Oracle'  # 大小写无关，后面会被转为大写
target_db_type = 'MySQL'  # 大小写无关，后面会被转为大写

cur_select = source_db.cursor()  # 源库查询对象
cur_insert = target_db.cursor()  # 目标库插入对象
cur_select.arraysize = 5000  # 数据库游标对象结果集返回到客户端行数
cur_insert.arraysize = 5000


# clob、blob、nclob要在读取源表前加载outputtypehandler属性
def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_NCLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)


# 源库的连接对象需要加载outputtypehandler属性
source_db.outputtypehandler = OutputTypeHandler


# source_table = input("请输入源表名称:")    # 手动从键盘获取源表名称
# target_table = input("请输入目标表名称:")  # 手动从键盘获取目标表名称

# 输出要迁移的表
def print_table():
    cur_tblprt = source_db.cursor()  # 生成用于输出表名的游标对象
    tableoutput_sql = 'select table_name from user_tables  where table_name in (\'TEST4\',\'TEST3\',\'TEST2\') order by table_name  desc'  # 查询需要导出的表
    cur_tblprt.execute(tableoutput_sql)  # 执行
    filename = '/tmp/table_name.csv'
    f = open(filename, 'w')
    for row_table in cur_tblprt:
        table_name = row_table[0]
        f.write(table_name + '\n')
    f.close()


# 输出迁移失败的表
def print_failed_table(table_name):
    filename = '/tmp/failed_table.csv'
    f = open(filename, 'a', encoding='utf-8')
    f.write(table_name + '\n')
    f.close()


# 表插入方法
def mig_table(tablename, write_fail):
    target_table = source_table = tablename
    if source_db_type.upper() == 'ORACLE':
        get_column_length = 'select count(*) from user_tab_columns where table_name= ' + "'" + source_table.upper() + "'"  # 拼接获取源表有多少个列的SQL
    cur_select.execute(get_column_length)  # 执行
    col_len = cur_select.fetchone()  # 获取源表有多少个列
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
    cur_select.execute(select_sql)  # 执行
    print("\033[31m正在执行插入表:\033[0m", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    source_effectrow = 0
    target_effectrow = 0
    while True:
        rows = list(
            cur_select.fetchmany(
                5000))  # 每次获取2000行，由cur_select.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
        try:
            cur_insert.executemany(insert_sql, rows)  # 批量插入每次2000行，需要注意的是 rows 必须是 list [] 数据类型
            target_db.commit()  # 提交
        except Exception as e:
            print(traceback.format_exc())  # 遇到异常记录到log，会继续迁移下张表
            print(tablename, '表记录', rows, '插入失败')
            if write_fail == 1:
                print_failed_table(tablename)
            else:
                continue
        if not rows:
            break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表
        source_effectrow = cur_select.rowcount  # 计数源表插入的行数
        target_effectrow = target_effectrow + cur_insert.rowcount  # 计数目标表插入的行数
    print('源表查询总数:', source_effectrow)
    print('目标插入总数:', target_effectrow)


# 从csv文件读取源库需要迁移的表，通过循环进行所有表的迁移
def mig_database():
    print_table()  # 读取要迁移的表，csv文件
    filename = '/tmp/table_name.csv'
    with open(filename) as f:
        reader = csv.reader(f)
        for row in reader:
            '''
            try:
                tbl_name = row[0]
                print("\033[31m开始迁移：\033[0m" + tbl_name)
                mig_table(tbl_name)
                print(tbl_name + '插入完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                # 如果有异常，比如唯一建约束导致插入失败，可以忽略掉继续下一张表
            except pymysql.err.IntegrityError:
                pass
            continue
            '''
            tbl_name = row[0]
            print("\033[31m开始迁移：\033[0m\n")
            print(tbl_name)
            mig_table(tbl_name, 1)
            print(tbl_name + '插入完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '\n')
    f.close()


# 再次迁移失败的表
def mig_failed_table():
    filename = '/tmp/failed_table.csv'
    with open(filename) as f:
        reader = csv.reader(f)
        for row in reader:
            tbl_name = row[0]
            print("\033[31m开始迁移：\033[0m" + tbl_name)
            mig_table(tbl_name, 0)
            is_continue = input('是否迁移失败的表：Y|N\n')
            if is_continue == 'Y' or is_continue == 'y':
                continue
            else:
                print(tbl_name + '插入完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                sys.exit()
    f.close()


if __name__ == '__main__':
    starttime = datetime.datetime.now()
    path = '/tmp/failed_table.csv'  # 迁移失败的表csv路径，每次迁移表前先清除失败的表csv文件
    if os.path.exists(path):  # 如果文件存在
        os.remove(path)  # 删除文件
    mig_database()
    endtime = datetime.datetime.now()
    print("Oracle迁移数据到MySQL完毕,一共耗时" + str((endtime - starttime).seconds) + "秒")
    print('-' * 100)
    print("迁移失败的表如下：")
    filename = '/tmp/failed_table.csv'  # 输出迁移失败的表
    with open(filename, "r") as f:
        data = f.read()  # 读取文件
        print(data)
    f.close()
    print('-' * 100)
    print('请检查失败的表DDL以及约束')
    is_continue = input('是否再次迁移失败的表：Y|N\n')
    if is_continue == 'Y' or is_continue == 'y':
        try:
            print('开始重新迁移失败的表\n')
            mig_failed_table()
        except Exception as e:
            print('插入失败')
    else:
        print('迁移完毕！')

cur_select.close()
cur_insert.close()
source_db.close()
target_db.close()
