import cx_Oracle
import pymysql
import os
import time
import sys

# 不带循环
# 说明：本脚本用于Oracle与MySQL之间的数据迁移
# 注意：源表与目标表字段数量必须一致
# 使用：脚本默认是从MySQL迁移到Oracle,如果想从Oracle迁移到MySQL,修改source_db,target_db,source_db_type,target_db_type就行
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  # 设置字符集为UTF8，防止中文乱码

'''
mysql to oracle
source_db = pymysql.connect("192.168.56.101", "scott", "tiger", "test")  # 源库
target_db = cx_Oracle.connect('scott/tiger@192.168.3.13/orcl')  # 目标库
source_db_type = 'MySQL'  # 大小写无关，后面会被转为大写
target_db_type = 'Oracle'  # 大小写无关，后面会被转为大写
'''

# oracle to mysql
source_db = cx_Oracle.connect('NJJBXQ_DJGBZ/11111@192.168.189.208:1522/orcl11g')  # 源库
target_db = pymysql.connect("192.168.189.208", "root", "Gepoint", "NJJBXQ_DJGBZ")  # 目标库
source_db_type = 'Oracle'  # 大小写无关，后面会被转为大写
target_db_type = 'MySQL'  # 大小写无关，后面会被转为大写

cur_select = source_db.cursor()  # 源库查询对象
cur_insert = target_db.cursor()  # 目标库插入对象
cur_select.arraysize = 2000
cur_insert.arraysize = 2000

source_table = input("请输入源表名称:")  # 从键盘获取源表名称
target_table = input("请输入目标表名称:")  # 从键盘获取目标表名称

# source_table = target_table = sys.argv[1]


# 要在读blob之前加载outputtypehandler属性
def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.DB_TYPE_CLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_BLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.DB_TYPE_NCLOB:
        return cursor.var(cx_Oracle.DB_TYPE_LONG, arraysize=cursor.arraysize)


# 要在读blob之前加载outputtypehandler属性
source_db.outputtypehandler = OutputTypeHandler

if source_db_type.upper() == 'ORACLE':
    get_column_length = 'select count(*) from user_tab_columns where table_name= ' + "'" + source_table.upper() + "'"  # 拼接获取源表有多少个列的SQL
elif source_db_type.upper() == 'MYSQL':
    get_column_length = 'select * from ' + source_table + ' limit 1'  # 拼接获取源表有多少个列的SQL
cur_select.execute(get_column_length)  # 执行
col_len = cur_select.fetchone()  # 获取源表有多少个列
col_len = col_len[0]
val_str = ''
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
print('开始执行:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
while True:
    rows = list(
        cur_select.fetchmany(2000))  # 每次获取500行，由cur_select.arraysize值决定，MySQL fetchmany 返回的是 tuple 数据类型 所以用list做类型转换
    cur_insert.executemany(insert_sql, rows)  # 批量插入每次500行，需要注意的是 rows 必须是 list [] 数据类型
    target_db.commit()  # 提交
    if not rows:
        break  # 中断循环
cur_select.close()
cur_insert.close()
source_db.close()
target_db.close()
print('执行成功:', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
