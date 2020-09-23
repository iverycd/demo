# -*- coding: utf-8 -*-
# pg_mig_mysql.py v1.0 2020.09.10
# postgresql database migration to MySQL
"""表迁移的方法，先在源库查询每张表有几个列字段，生成插入到目标库的insert语句，以自定义批量插入方式插入到目标库"""
import binascii

import pymysql
import time
import csv
import datetime
import psycopg2

source_db = psycopg2.connect(database="admin", user="admin", password="11111", host="192.168.212.245",
                             port="5432")  # 源库
target_db = pymysql.connect("192.168.189.208", "root", "Gepoint", "test")  # 目标库
v_relkind = 'r'  # MySQL中要迁移的数据库名称，用于查询information_schema表名称以及字段数量

cur_select = source_db.cursor()  # 源库查询对象
cur_insert = target_db.cursor()  # 目标库插入对象
cur_select.arraysize = 10000  # 数据库游标对象结果集返回到客户端行数
cur_insert.arraysize = 10000


# 将要迁移的表输出到文本文件
def print_table():
    cur_tblprt = source_db.cursor()  # 生成用于输出表名的游标对象
    tableoutput_sql = 'select tablename from pg_tables where schemaname=\'public\' and tablename=\'test\''  # 查询需要导出的表
    cur_tblprt.execute(tableoutput_sql)  # 执行
    filename = '/tmp/table_name.csv'
    f = open(filename, 'w')
    for row_table in cur_tblprt:
        table_name = row_table[0]
        f.write(table_name + '\n')
    f.close()


# 每张表迁移，可调整每次查询以及插入的行数
'''bytea
def mig_table(tablename):
    target_table = source_table = tablename
    get_column_length = 'select relnatts from pg_class  where relkind=' + "'" + v_relkind + "'" + 'and relname= ' + "'" + source_table.lower() + "'"  # 拼接获取源表有多少个列的SQL
    cur_select.execute(get_column_length)  # 执行
    col_len = cur_select.fetchone()  # 获取源表有多少个列
    col_len = col_len[0]  # 将游标结果数组的值赋值，该值为表列字段总数
    val_str = ''  # 用于生成批量插入的列字段变量
    for i in range(1, col_len):
        val_str = val_str + '%s' + ','
    val_str = val_str + '%s'  # postgresql批量插入语法是 insert into tb_name values(%s,%s,%s,%s)
    insert_sql = 'insert into ' + target_table + ' values(' + val_str + ')'  # 拼接insert into 目标表 values  #目标表插入语句
    select_sql = 'select * from ' + source_table  # 源查询SQL，如果有where过滤条件，在这里拼接
    cur_select.execute(select_sql)  # 执行
    print("\033[31m正在执行插入表:\033[0m", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    v_pic = cur_select.fetchone()  # bytea类型测试
    v_dis = bytes(v_pic[0])  # bytea类型测试
    # while True:
    #    rows = list(
    #        cur_select.fetchmany(
    #            10000))  # 每次获取10000行，由cur_select.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
    #    # cur_insert.executemany(insert_sql, rows)  # 批量插入每次10000行，需要注意的是 rows 必须是 list [] 数据类型
    cur_insert.execute(insert_sql, v_dis)  # bytea类型测试
    target_db.commit()  # 提交
    #    if not rows:
    #        break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表
'''


def mig_table(tablename):
    target_table = source_table = tablename
    get_column_length = 'select relnatts from pg_class  where relkind=' + "'" + v_relkind + "'" + 'and relname= ' + "'" + source_table.lower() + "'"  # 拼接获取源表有多少个列的SQL
    cur_select.execute(get_column_length)  # 执行
    col_len = cur_select.fetchone()  # 获取源表有多少个列
    col_len = col_len[0]  # 将游标结果数组的值赋值，该值为表列字段总数
    val_str = ''  # 用于生成批量插入的列字段变量
    for i in range(1, col_len):
        val_str = val_str + '%s' + ','
    val_str = val_str + '%s'  # postgresql批量插入语法是 insert into tb_name values(%s,%s,%s,%s)
    insert_sql = 'insert into ' + target_table + ' values(' + val_str + ')'  # 拼接insert into 目标表 values  #目标表插入语句
    select_sql = 'select * from ' + source_table  # 源查询SQL，如果有where过滤条件，在这里拼接
    cur_select.execute(select_sql)  # 执行
    print("\033[31m正在执行插入表:\033[0m", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    while True:
        rows = list(
            cur_select.fetchmany(
                10000))  # 每次获取10000行，由cur_select.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
        cur_insert.executemany(insert_sql, rows)  # 批量插入每次10000行，需要注意的是 rows 必须是 list [] 数据类型
        # test cur_insert.execute(insert_sql, v_dis) bytea类型测试
        target_db.commit()  # 提交
        if not rows:
            break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表


# 迁移用主函数
def mig_database():
    # 从csv文件读取源库需要迁移的表，通过循环进行所有表的迁移
    print_table()  # 先将要迁移的表输出到文本
    filename = '/tmp/table_name.csv'
    with open(filename) as f:
        reader = csv.reader(f)
        for row in reader:  # 循环文本文件每一张表
            # try:
            #    tbl_name = row[0]
            #    print("\033[31m开始迁移：\033[0m" + tbl_name)
            #    mig_table(tbl_name)  # 按照文本文件每张表迁移
            #    print(tbl_name + '插入完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            # except pymysql.err.IntegrityError:  # 如果有异常，比如唯一建约束导致插入失败，可以忽略掉继续下一张表
            #    pass
            # continue
            tbl_name = row[0]
            print("\033[31m开始迁移：\033[0m" + tbl_name)
            mig_table(tbl_name)  # 按照文本文件每张表迁移
            print(tbl_name + '插入完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


if __name__ == '__main__':
    starttime = datetime.datetime.now()
    mig_database()  # 调用主函数开始迁移
    endtime = datetime.datetime.now()
    print("postgresql迁移数据到MySQL完毕,一共耗时" + str((endtime - starttime).seconds) + "秒")
cur_select.close()
cur_insert.close()
source_db.close()
target_db.close()

# (<class 'pymysql.err.IntegrityError'>, IntegrityError(1062, "Duplicate entry 'a724326e-d832-4673-820b-7aa63b009a40' for key 'PRIMARY'"), <traceback object at 0x7f0f8c551e48>)
