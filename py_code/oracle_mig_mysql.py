# -*- coding: utf-8 -*-
# oracle_mig_mysql.py v1.3
# Oracle database migration to MySQL
"""表迁移的方法，先在源库查询每张表有几个列字段，生成插入到目标库的insert语句，以自定义批量插入方式插入到目标库"""
import cx_Oracle
import pymysql
import os
import time
import csv
import datetime
import sys

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


def print_table():
    cur_tblprt = source_db.cursor()  # 生成用于输出表名的游标对象
    tableoutput_sql = 'select table_name from user_tables  where table_name in (\'TEST\')'  # 查询需要导出的表
    cur_tblprt.execute(tableoutput_sql)  # 执行
    filename = '/tmp/table_name.csv'
    f = open(filename, 'w')
    for row_table in cur_tblprt:
        table_name = row_table[0]
        f.write(table_name + '\n')
    f.close()


def mig_table(tablename):
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
    while True:
        rows = list(
            cur_select.fetchmany(
                5000))  # 每次获取2000行，由cur_select.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
        cur_insert.executemany(insert_sql, rows)  # 批量插入每次2000行，需要注意的是 rows 必须是 list [] 数据类型
        target_db.commit()  # 提交
        if not rows:
            break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表


def mig_database():
    # global row_data
    # sql = "select table_name from user_tables"
    # sql = "select table_name from user_tables where table_name=upper('frame_user')"  # get a list of all tables
    # cur_select.execute(sql)
    # for row_data in cur_select:
    # print('xxx')
    # tableName = row_data[0]
    # 从csv文件读取源库需要迁移的表，通过循环进行所有表的迁移
    print_table()
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
            print("\033[31m开始迁移：\033[0m" + tbl_name)
            mig_table(tbl_name)
            print(tbl_name + '插入完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


if __name__ == '__main__':
    starttime = datetime.datetime.now()
    mig_database()
    endtime = datetime.datetime.now()
    print("Oracle迁移数据到MySQL完毕,一共耗时" + str((endtime - starttime).seconds) + "秒")
cur_select.close()
cur_insert.close()
source_db.close()
target_db.close()

# (<class 'pymysql.err.IntegrityError'>, IntegrityError(1062, "Duplicate entry 'a724326e-d832-4673-820b-7aa63b009a40' for key 'PRIMARY'"), <traceback object at 0x7f0f8c551e48>)
