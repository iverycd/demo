import cx_Oracle
import pymysql

source_db = cx_Oracle.connect('admin/oracle@192.168.189.208:1522/orcl11g', encoding="UTF-8")
target_db = pymysql.connect("192.168.189.208", "root", "Gepoint", "test")  # 目标库


# 返回源表的主键字段
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


# 返回源表的列字段以及MySQL字段类型映射判断
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
        # 字符类型转换为MySQL的varchar
        if column[1] == 'VARCHAR2' or column[1] == 'CHAR' or column[1] == 'NCHAR' or column[1] == 'NVARCHAR2':
            result.append({'fieldname': column[0],  # 如下为字段的属性值
                           'type': 'VARCHAR' + '(' + str(column[2]) + ')',  # 列字段类型以及长度范围
                           'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                           'default': column[7],  # 字段默认值
                           'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                           }
                          )
        # 判断number类型是否是浮点，是否是整数，转为MySQL的int或者decimal。下面分了3种情况区分整数与浮点
        # column[n] == -1,代表该值为null，方便通过比较大小判断是否为空
        elif column[1] == 'NUMBER':
            if column[3] > 0 and column[4] > 0:  # 浮点类型判断，如number(5,2)映射为MySQL的DECIMAL(5,2)
                result.append({'fieldname': column[0],
                               'type': 'DECIMAL' + '(' + str(column[3]) + ',' + str(column[4]) + ')',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )
            elif column[3] > 0 and column[4] == 0 and column[8] >= 7:  # 整数类型判断，如number(38,0)，映射为MySQL的bigint
                result.append({'fieldname': column[0],
                               'type': 'BIGINT',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )
            elif column[3] > 0 and column[4] == 0 and column[8] < 7:  # 整数类型判断，如number(10,0)，映射为MySQL的int
                result.append({'fieldname': column[0],
                               'type': 'INT',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )
            elif column[3] == -1 and column[4] == -1 and column[8] >= 7:  # 整数类型判断，如id number,若数值较大，AVG_COL_LEN>=7，映射为MySQL的bigint
                result.append({'fieldname': column[0],
                               'type': 'BIGINT',  # 列字段类型以及长度范围
                               'primary': column[0] in primary_key,  # 如果有主键字段返回true，否则false
                               'default': column[7],  # 字段默认值
                               'isnull': column[5]  # 字段是否允许为空，true为允许，否则为false
                               }
                              )
            elif column[3] == -1 and column[4] == -1 and column[8] < 7:  # 整数类型判断，如id number,若数值较小，AVG_COL_LEN<7，映射为MySQL的int
                result.append({'fieldname': column[0],
                               'type': 'INT',  # 列字段类型以及长度范围
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


# 生成创建目标表的拼接sql
def create_table(new_tbl, meta_tbl):  # new_tbl：即将创建的新表, meta_tbl：源表的表名
    cur_createtbl = target_db.cursor()
    fieldinfos = []
    structs = tbl_columns(meta_tbl)  # 获取源表的表字段信息
    v_pri_key = table_primary(meta_tbl)  # 获取源表的主键字段
    for struct in structs:
        defaultvalue = struct.get('default')
        if defaultvalue:
            defaultvalue = "'{0}'".format(defaultvalue) if type(defaultvalue) == 'str' else str(defaultvalue)
        fieldinfos.append('{0} {1} {2} {3}'.format(struct['fieldname'],
                                                   struct['type'],
                                                   # 'primary key' if struct.get('primary') else '',主键在创建表的时候定义
                                                   (
                                                           'default ' + '\'' + defaultvalue + '\'') if defaultvalue else '',
                                                   '' if struct.get('isnull') else 'not null'))
    create_table_sql = 'create table {0} ({1})'.format(new_tbl, ','.join(fieldinfos))  # 生成创建目标表的sql
    add_pri_key_sql = 'alter table {0} add primary key ({1})'.format(new_tbl, ','.join(v_pri_key))  # 创建目标表之后增加主键
    cur_createtbl.execute(create_table_sql)
    if v_pri_key:
        cur_createtbl.execute(add_pri_key_sql)


new_tablename = 'TEST_PJ'
meta_tablename = 'TEST4'
# v_table_primary = table_primary(tablename)
# v_columns = tbl_columns(meta_tablename)
# print(v_table_primary)
# print(v_columns)
create_table(new_tablename, meta_tablename)
