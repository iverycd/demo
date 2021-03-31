# -*- coding: utf-8 -*-
# dm_dts_xml.py
# 批量生成多个dts的xml文件
import os


def mkdir(path):
    # 去除首位空格
    path = path.strip()
    # 去除尾部 \ 符号
    path = path.rstrip("\\")

    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists = os.path.exists(path)

    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        os.makedirs(path)
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        return False


# 定义要创建的目录
mkpath = "/tmp/xml"
mkdir(mkpath)
mig_id = 1614318739305
count = 0
list_db = ['fsoav18_0524', 'fsoav1_anjianju', 'fsoav1_bianban', 'fsoav1_canlian', 'fsoav1_chuangwenban',
           'fsoav1_danganju', 'fsoav1_dangxiao', 'fsoav1_dongping', 'fsoav1_dsyjs', 'fsoav1_dzj', 'fsoav1_fagaiju',
           'fsoav1_fazhiju', 'fsoav1_fulian', 'fsoav1_ggzyjyzx', 'fsoav1_glj', 'fsoav1_gongshanglian', 'fsoav1_guotuju',
           'fsoav1_guoziwei', 'fsoav1_gxqgwh', 'fsoav1_huanbaoju', 'fsoav1_jggw', 'fsoav1_jgswzx', 'fsoav1_jiaotong',
           'fsoav1_jiaoyuju', 'fsoav1_jiashi', 'fsoav1_jiedaichu', 'fsoav1_jingxinonly', 'fsoav1_jishengju',
           'fsoav1_jiwei', 'fsoav1_jsgczjz', 'fsoav1_jxj', 'fsoav1_kejiju', 'fsoav1_kexie', 'fsoav1_laoganju',
           'fsoav1_laogansuo', 'fsoav1_laojiaosuo', 'fsoav1_luyouju', 'fsoav1_maocuhui', 'fsoav1_minzhengju',
           'fsoav1_minzong', 'fsoav1_nongguan', 'fsoav1_nongyeju', 'fsoav1_qiaolian', 'fsoav1_qixiangju',
           'fsoav1_rendaban', 'fsoav1_renfangban', 'fsoav1_rensheju', 'fsoav1_sfb', 'fsoav1_sfj', 'fsoav1_shebao',
           'fsoav1_shegongwei', 'fsoav1_shenjiju', 'fsoav1_shibowuguan', 'fsoav1_shijianban', 'fsoav1_shuiwuju',
           'fsoav1_swb', 'fsoav1_taiban', 'fsoav1_tiyuju', 'fsoav1_tongji', 'fsoav1_tongzhan', 'fsoav1_tuanshiwei',
           'fsoav1_waijingmaoju', 'fsoav1_waiqiaoju', 'fsoav1_weishengju', 'fsoav1_wenguanxin', 'fsoav1_wenhuaguan',
           'fsoav1_wenlian', 'fsoav1_wsjds', 'fsoav1_xinggaiban', 'fsoav1_xuanchuanbu', 'fsoav1_xuezhan',
           'fsoav1_xzfwzx', 'fsoav1_yaojian', 'fsoav1_ysczy', 'fsoav1_zhengfawei', 'fsoav1_zhengxie',
           'fsoav1_zhengyanshi', 'fsoav1_zhujianju', 'fsoav1_zonggonghui', 'fsoav1_zsj', 'fsoav1_zuzhibu',
           'fsoav1_zybfzs', 'fsoav2']
filename = '/tmp/xml/dts.xml'
if os.path.exists(filename):
    os.remove(filename)
dts = open(filename, 'a', encoding='utf-8')
# 生成dts.xml文件 开头
dts.write('<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n')
dts.write('<projects>\n')
dts.write('  <mappings/>\n')
dts.write('  <project id="1603421975873" name="mig">\n')
dts.write('    <description/>\n')
dts.write('    <schedules/>\n')
dts.write('    <transforms>\n')
# 生成dts.xml文件 循环生成中间多个迁移内容 begin
for dbname in list_db:
    dbname = dbname.upper()
    count += 1
    mig_id += 1
    dts.write('      <transform id="{}" name="{}" valid="true">\n'.format(mig_id, dbname))
    dts.write('        <description/>\n')
    dts.write('      </transform>\n')

    # 生成每个任务的迁移步骤,多个xml文件
    filename = '/tmp/xml/' + str(mig_id) + '.xml'
    if os.path.exists(filename):
        os.remove(filename)
    f = open(filename, 'a', encoding='utf-8')
    f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    f.write('\n')
    f.write('<!--本文件由DM数据迁移工具生成,请不要手工修改,生成时间2021-02-26 13:53:57.-->\n')
    f.write('<TransformTask transformer="13"> \n')
    f.write('  <Source type="db" useCustomDriver="true" useDefaultURL="true"> \n')
    f.write('    <Server>19.130.224.13</Server>\n')
    f.write('    <Port>3306</Port>\n')
    f.write("    <DriverPath>D:\\tools\\mysql-connector-java-5.1.46\\mysql-connector-java-5.1.46.jar</DriverPath>\n")
    f.write('    <DriverClassName>com.mysql.jdbc.Driver</DriverClassName>\n')
    f.write(
        '    <URL>jdbc:mysql://19.130.224.13:3306/' + dbname + '?tinyInt1isBit=false&amp;transformedBit1sBoolean=false</URL>\n')
    f.write('    <AuthType>0</AuthType>\n')
    f.write('    <Compress>false</Compress>\n')
    f.write('    <User>root</User>\n')
    f.write('    <Password>8A4FACE31DF083B9E7477AC29DFD0B05</Password>\n')
    f.write('    <Catalog>' + dbname + '</Catalog>\n')
    f.write('  </Source>  \n')
    f.write('  <Destination type="db" useCustomDriver="false" useDefaultURL="true"> \n')
    f.write('    <Server>19.130.188.154</Server>\n')
    f.write('    <Port>5236</Port>\n')
    f.write('    <AuthType>0</AuthType>\n')
    f.write('    <Compress>false</Compress>\n')
    f.write('    <DmType>dm</DmType>\n')
    f.write('    <User>SYSDBA</User>\n')
    f.write('    <Password>F1A54AF6DA530F3F</Password>\n')
    f.write('  </Destination>  \n')
    f.write(
        '  <Mode useDefaultDataTypeMap="true" lengthInChar="1" simple="false" objectNameToUpperCase="true" transformerDefault="false" isQuery="false"> \n')
    f.write('    <DBStrategies>\n')
    f.write('      <Strategy>TRANSFORM_SCHEMAS</Strategy>\n')
    f.write('    </DBStrategies>\n')
    f.write('    <Schema source="" destination="' + dbname + '">\n')
    f.write('      <Strategies>\n')
    f.write('        <Strategy>TRANSFORM_TABLES</Strategy>\n')
    f.write('        <Strategy>TRANSFORM_VIEWS</Strategy>\n')
    f.write('      </Strategies>\n')
    f.write('    </Schema>\n')
    f.write('  </Mode>  \n')
    f.write('  <TransformItems continueWhenError="true" multiThread="true"/>\n')
    f.write('</TransformTask>\n')
    f.close()
    # create_tbs_sql = 'create tablespace ' + dbname + ' datafile ' + '\'' + dbname + '.dbf\'' + 'size 150 autoextend on next 100;'
    # create_user_sql = 'create user ' + dbname + ' identified by 111111111 default tablespace ' + dbname + ';'
    # grant_sql = 'grant dba to ' + dbname + ';'
    # print(create_tbs_sql)
    # print(create_user_sql)
    # print(grant_sql)

# 生成dts.xml文件 循环生成中间多个迁移内容 end
dts.write('      </transform>\n')
dts.write('    </transforms>\n')
dts.write('    <jobs/>\n')
dts.write('  </project>\n')
dts.write('</projects>\n')
dts.close()
print('total db:' + str(count) + ' xml in /tmp/xml')
