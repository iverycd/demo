# -*- coding: utf-8 -*-
# oracle_mig_mysql.py
# Oracle database migration to MySQL
# CURRENT VERSION
# V1.3.1
"""
MODIFY HISTORY
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
source_db = cx_Oracle.connect('test2/oracle@192.168.189.208:1522/orcl11g')  # 源库Oracle的数据库连接
target_db = pymysql.connect("192.168.189.208", "root", "Gepoint", "test2")  # 目标库MySQL的数据库连接
source_db_type = 'Oracle'  # 大小写无关，后面会被转为大写
target_db_type = 'MySQL'  # 大小写无关，后面会被转为大写

cur_select = source_db.cursor()  # 源库Oracle查询源表有几列
cur_oracle_result = source_db.cursor()  # 查询Oracle源表的游标结果集
cur_insert_mysql = target_db.cursor()  # 目标库MySQL插入目标表执行的插入sql
cur_drop_table = target_db.cursor()  # 在MySQL清除表 drop table if exists
cur_oracle_result.arraysize = 5000  # Oracle数据库游标对象结果集返回的行数即每次获取多少行
cur_insert_mysql.arraysize = 5000  # MySQL数据库批量插入的行数


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


# 处理Oracle的number类型浮点数据与Python decimal类型的转换
# Python遇到超过3位小数的浮点类型，小数部分只能保留3位，声音会被截断，会造成数据不准确，需要用此handler做转换，可指定数据库连接或者游标对象

def NumberToDecimal(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.DB_TYPE_NUMBER:
        return cursor.var(decimal.Decimal, arraysize=cursor.arraysize)


cur_oracle_result.outputtypehandler = NumberToDecimal


# source_table = input("请输入源表名称:")    # 手动从键盘获取源表名称
# target_table = input("请输入目标表名称:")  # 手动从键盘获取目标表名称


# 获取Oracle的主键字段
def table_primary(table_name):
    cur_table_primary = source_db.cursor()
    cur_table_primary.execute("""SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols
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
    cur_tbl_columns = source_db.cursor()
    cur_tbl_columns.execute("""SELECT A.COLUMN_NAME, A.DATA_TYPE, A.DATA_LENGTH, case when A.DATA_PRECISION is null then -1 else  A.DATA_PRECISION end DATA_PRECISION, case when A.DATA_SCALE is null then -1 else  A.DATA_SCALE end DATA_SCALE,  case when A.NULLABLE ='Y' THEN 'True' ELSE 'False' END as isnull, B.COMMENTS,A.DATA_DEFAULT,case when a.AVG_COL_LEN is null then -1 else a.AVG_COL_LEN end AVG_COL_LEN
            FROM USER_TAB_COLUMNS A LEFT JOIN USER_COL_COMMENTS B 
            ON A.TABLE_NAME=B.TABLE_NAME AND A.COLUMN_NAME=B.COLUMN_NAME 
            WHERE A.TABLE_NAME='%s' ORDER BY COLUMN_ID ASC""" % table_name)
    result = []
    primary_key = table_primary(table_name)
    for column in cur_tbl_columns:  # 按照游标行遍历字段
        '''
        result.append({'column_name': column[0],
                       'type': column[1],
                       'primary': column[0] in primary_key,
                       'length': column[2],
                       'precision': column[3],
                       'scale': column[4],
                       'nullable': column[5],
                       'comment': column[6]})
        '''
        # 对游标cur_tbl_columns中每行的column[1]字段进行平行判断
        # 字符类型映射规则，字符串类型映射为MySQL类型varchar(n)
        if column[1] == 'VARCHAR2' or column[1] == 'CHAR' or column[1] == 'NCHAR' or column[1] == 'NVARCHAR2':
            result.append({'fieldname': column[0],  # 如下为字段的属性值
                           'type': 'VARCHAR' + '(' + str(column[2]) + ')',  # 列字段类型以及长度范围
                           'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                           'default': column[7],  # 字段默认值
                           'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                           }
                          )
        # 时间日期类型映射规则，Oracle date类型映射为MySQL类型datetime
        elif column[1] == 'DATE' or column[1] == 'TIMESTAMP(6)':
            # Oracle 默认值sysdate映射到MySQL默认值current_timestamp
            if column[7] == 'sysdate':
                result.append({'fieldname': column[0],  # 如下为字段的属性值
                               'type': 'DATETIME',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': 'current_timestamp()',  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )
            # 其他时间日期默认值保持不变(原模原样对应)
            else:
                result.append({'fieldname': column[0],  # 如下为字段的属性值
                               'type': 'DATETIME',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )

        # 数值类型映射规则，判断Oracle number类型是否是浮点，是否是整数，转为MySQL的int或者decimal。下面分了3种情况区分整数与浮点
        # column[n] == -1,即DATA_PRECISION，DATA_SCALE，AVG_COL_LEN为null，仅在如下if条件判断是否为空
        elif column[1] == 'NUMBER':
            # 浮点类型判断，如number(5,2)映射为MySQL的DECIMAL(5,2)
            if column[3] > 0 and column[4] > 0:
                result.append({'fieldname': column[0],
                               'type': 'DECIMAL' + '(' + str(column[3]) + ',' + str(column[4]) + ')',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )
            # 整数类型判断，如number(20,0)，如果AVG_COL_LEN比较大，映射为MySQL的bigint
            elif column[3] > 0 and column[4] == 0 and column[8] >= 6:
                result.append({'fieldname': column[0],
                               'type': 'BIGINT',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )
            # 整数类型判断，如number(10,0)，如果AVG_COL_LEN比较小，映射为MySQL的int
            elif column[3] > 0 and column[4] == 0 and column[8] < 6:
                result.append({'fieldname': column[0],
                               'type': 'INT',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )
            # 整数类型判断，如id number,若AVG_COL_LEN比较大，映射为MySQL的bigint
            elif column[3] == -1 and column[4] == -1 and column[8] >= 6:
                result.append({'fieldname': column[0],
                               'type': 'BIGINT',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )
            # 整数类型判断，如id number,若AVG_COL_LEN比较小，映射为MySQL的int
            elif column[3] == -1 and column[4] == -1 and column[8] < 6:
                result.append({'fieldname': column[0],
                               'type': 'INT',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )
            # int整数类型判断，如id int,(oracle的int会自动转为number),若AVG_COL_LEN比较大，映射为MySQL的bigint
            elif column[3] == -1 and column[4] == 0 and column[8] >= 6:
                result.append({'fieldname': column[0],
                               'type': 'BIGINT',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )
            # int整数类型判断，如id int,(oracle的int会自动转为number)若AVG_COL_LEN比较小，映射为MySQL的int
            elif column[3] == -1 and column[4] == 0 and column[8] < 6:
                result.append({'fieldname': column[0],
                               'type': 'INT',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )
        # 大字段映射规则，文本类型大字段映射为MySQL类型longtext
        elif column[1] == 'CLOB' or column[1] == 'NCLOB' or column[1] == 'LONG':
            result.append({'fieldname': column[0],  # 如下为字段的属性值
                           'type': 'LONGTEXT',  # 列字段类型以及长度范围
                           'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                           'default': column[7],  # 字段默认值
                           'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                           }
                          )
        # 大字段映射规则，16进制类型大字段映射为MySQL类型longblob
        elif column[1] == 'BLOB' or column[1] == 'RAW' or column[1] == 'LONG RAW':
            result.append({'fieldname': column[0],  # 如下为字段的属性值
                           'type': 'LONGBLOB',  # 列字段类型以及长度范围
                           'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                           'default': column[7],  # 字段默认值
                           'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                           }
                          )
        else:
            result.append({'fieldname': column[0],  # 如果是非大字段类型，通过括号加上字段类型长度范围
                           'type': column[1] + '(' + str(column[2]) + ')',  # 列字段类型以及长度范围
                           'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                           'default': column[7],  # 字段默认值
                           'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                           }

                          )
    # print(result)
    cur_tbl_columns.close()
    return result


# 获取在MySQL创建目标表的DDL、增加主键
def create_table(table_name):  # new_tbl：即将创建的新表, meta_tbl：源表的表名
    # 在MySQL创建表前先删除存在的表
    drop_target_table = 'drop table if exists ' + table_name
    cur_drop_table.execute(drop_target_table)
    cur_createtbl = target_db.cursor()
    fieldinfos = []
    structs = tbl_columns(table_name)  # 获取源表的表字段信息
    v_pri_key = table_primary(table_name)  # 获取源表的主键字段
    # 以下字段已映射为MySQL字段类型
    for struct in structs:
        defaultvalue = struct.get('default')
        if defaultvalue:
            defaultvalue = "'{0}'".format(defaultvalue) if type(defaultvalue) == 'str' else str(defaultvalue)
        fieldinfos.append('{0} {1} {2} {3}'.format(struct['fieldname'],
                                                   struct['type'],
                                                   # 'primary key' if struct.get('primary') else '',主键在创建表的时候定义
                                                   # ('default ' + '\'' + defaultvalue + '\'') if defaultvalue else '',
                                                   ('default ' + defaultvalue) if defaultvalue else '',
                                                   '' if struct.get('isnull') else 'not null'))
    create_table_sql = 'create table {0} ({1})'.format(table_name, ','.join(fieldinfos))  # 生成创建目标表的sql
    add_pri_key_sql = 'alter table {0} add primary key ({1})'.format(table_name, ','.join(v_pri_key))  # 创建目标表之后增加主键
    cur_createtbl.execute(create_table_sql)
    if v_pri_key:
        cur_createtbl.execute(add_pri_key_sql)


# 仅输出Oracle当前用户的表，即user_tables的table_name
def print_table():
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


# 仅输出迁移失败的表
def print_failed_table(table_name):
    filename = '/tmp/failed_table.csv'
    f = open(filename, 'a', encoding='utf-8')
    f.write(table_name + '\n')
    f.close()


# 批量将Oracle数据插入到MySQL的方法
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
    cur_oracle_result.execute(select_sql)  # 执行
    print("\033[31m正在执行插入表:\033[0m", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    source_effectrow = 0
    target_effectrow = 0
    while True:
        rows = list(
            cur_oracle_result.fetchmany(
                5000))  # 每次获取2000行，cur_oracle_result.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
        #  print(cur_oracle_result.description)  # 打印Oracle查询结果集字段列表以及类型
        try:
            cur_insert_mysql.executemany(insert_sql, rows)  # 批量插入每次5000行，需要注意的是 rows 必须是 list [] 数据类型
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
        source_effectrow = cur_oracle_result.rowcount  # 计数源表插入的行数
        target_effectrow = target_effectrow + cur_insert_mysql.rowcount  # 计数目标表插入的行数
    print('源表查询总数:', source_effectrow)
    print('目标插入总数:', target_effectrow)


# 在MySQL创建表结构以及添加主键
def create_meta_table():
    filename = '/tmp/table_name.csv'
    with open(filename) as f:
        reader = csv.reader(f)
        for row in reader:
            tbl_name = row[0]
            print("\033[31m开始创建表：\033[0m\n")
            print(tbl_name)
            create_table(tbl_name)
            print(tbl_name + '表创建完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '\n')
    f.close()


# 从csv文件读取源库需要迁移的表，调用mig_table(tbl_name, 1)，插入表数据，并输出迁移失败的表
def mig_database():
    filename = '/tmp/table_name.csv'  # 读取要迁移的表，csv文件
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


# 仅用于迁移数据插入失败的表
def mig_failed_table():
    filename = '/tmp/failed_table.csv'
    with open(filename) as f:
        reader = csv.reader(f)
        for row in reader:
            tbl_name = row[0]
            print("\033[31m开始迁移：\033[0m" + tbl_name)
            mig_table(tbl_name, 0)
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


if __name__ == '__main__':
    starttime = datetime.datetime.now()
    path = '/tmp/failed_table.csv'  # 迁移失败的表
    if os.path.exists(path):  # 如果文件存在，每次迁移表前先清除失败的表csv文件
        os.remove(path)  # 删除文件
    print_table()  # 1、生成Oracle要迁移的表写入到csv文件
    create_meta_table()  # 2、创建表结构
    mig_database()  # 3、迁移数据
    endtime = datetime.datetime.now()
    print("Oracle迁移数据到MySQL完毕,一共耗时" + str((endtime - starttime).seconds) + "秒")
    print('-' * 100)
    if os.path.exists(path):  # 判断下/tmp/failed_table.csv是否存在，没有就是没有迁移错误
        print("迁移失败的表如下：")
        filename = '/tmp/failed_table.csv'  # 输出这次迁移失败的表
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
    else:
        print('迁移成功，没有迁移失败的表')

cur_select.close()
cur_insert_mysql.close()
source_db.close()
target_db.close()
