# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
--jars /usr/share/java/mysql-connector-java-5.1.39.jar \
--queue project.hongjing \
ra_area_count.py
'''
import os

import MySQLdb
import configparser
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp

def truncate_table(table):
    '''连接mysql，执行一个SQL'''
    db = MySQLdb.connect(host=PROP['ip'], user=PROP['user'], 
                         passwd=PROP['password'], db=PROP['db_name'], 
                         charset="utf8")
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    # 使用execute方法执行SQL语句
    sql = "TRUNCATE TABLE {0}".format(table)
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()
    # 关闭数据库连接
    db.close()
    
    print "清空表{0}成功".format(table)

def get_change_info(col):
    col = sorted(col)
    if len(col) == 2:
        return 0
    elif col[0] == OLD_VERSION:
        return -1
    elif col[0] == NEW_VERSION:
        return 1
    else:
        return 0


def get_change_info_2(col):
    '''
    计算上升与下降人数
    '''
    return dict(
        rise=col.count(1),
        decline=col.count(-1)
    )

def get_xxjr(col):
    if col:
        for each_type in col:
            if u'新兴金融' in each_type:
                num = int(each_type.split(':')[1])
                break
        else:
            num = 0
        return num
    else:
        return 0

def get_wljd(col):
    if col:
        for each_type in col:
            if u'网络借贷' in each_type:
                num = int(each_type.split(':')[1])
                break
        else:
            num = 0
        return num
    else:
        return 0

def get_smjj(col):
    if col:
        for each_type in col:
            if u'私募基金' in each_type:
                num = int(each_type.split(':')[1])
                break
        else:
            num = 0
        return num
    else:
        return 0

def get_jycs(col):
    if col:
        for each_type in col:
            if u'交易场所' in each_type:
                num = int(each_type.split(':')[1])
                break
        else:
            num = 0
        return num
    else:
        return 0
        
def get_rzdb(col):
    if col:
        for each_type in col:
            if u'融资担保' in each_type:
                num = int(each_type.split(':')[1])
                break
        else:
            num = 0
        return num
    else:
        return 0
        
def get_xedk(col):
    if col:
        for each_type in col:
            if u'小额贷款' in each_type:
                num = int(each_type.split(':')[1])
                break
        else:
            num = 0
        return num
    else:
        return 0        

def get_zdgz_num(col):
    '''
    获得 '重点关注' 的企业数量
    '''
    return col.count(u'重点关注')
    
def get_cxjk_num(col):
    '''
    获得 '持续监控' 的企业数量
    '''
    return col.count(u'持续监控')

def get_id():
    return ''
    
def raw_spark_data_flow():
    #注册所有需要用到的udf
    get_xedk_udf = fun.udf(get_xedk, tp.IntegerType())
    get_rzdb_udf = fun.udf(get_rzdb, tp.IntegerType())
    get_jycs_udf = fun.udf(get_jycs, tp.IntegerType())
    get_smjj_udf = fun.udf(get_smjj, tp.IntegerType())
    get_wljd_udf = fun.udf(get_wljd, tp.IntegerType())
    get_xxjr_udf = fun.udf(get_xxjr, tp.IntegerType())
    
    get_zdgz_num_udf = fun.udf(get_zdgz_num, tp.IntegerType())
    get_cxjk_num_udf = fun.udf(get_cxjk_num, tp.IntegerType())
    
    
    get_change_info_2_udf = fun.udf(
        get_change_info_2, 
        tp.MapType(tp.StringType(), tp.IntegerType())
    )
    get_change_info_udf = fun.udf(get_change_info, tp.IntegerType())
    
    #读取原始输入
    old_df =  spark.read.parquet(
        ("{path}"
         "/all_company_info/{version}").format(path=IN_PATH,
                                               version=OLD_VERSION)
    ).fillna(
        {'city': u'无', 'county': u'无', 'province': u'无'}
    )
    new_df =  spark.read.parquet(
        ("{path}"
         "/all_company_info/{version}").format(path=IN_PATH,
                                               version=NEW_VERSION)
    ).fillna(
        {'city': u'无', 'county': u'无', 'province': u'无'}
    )

    #高危企业数
    high_risk_count_df = new_df.select(
        'province',
        'city',
        'county',
        'bbd_qyxx_id'
    ).where(
        new_df.risk_rank == u'高危预警'
    ).groupBy(
        ['province', 'city', 'county']
    ).count(
    ).withColumnRenamed(
        'count', 'high_risk_num'
    ).fillna(
        {'city': u'无', 'county': u'无', 'province': u'无'}
    ).cache()
    
    #重点关注企业数
    focus_on_count_df = new_df.select(
        'province',
        'city',
        'county',
        'bbd_qyxx_id'
    ).where(
        new_df.risk_rank == u'重点关注'
    ).groupBy(
        ['province', 'city', 'county']
    ).count(
    ).withColumnRenamed(
        'count', 'focus_on_num'
    ).fillna(
        {'city': u'无', 'county': u'无', 'province': u'无'}
    ).cache()
    
    #持续监控企业数   
    constantly_monitor_count_df = new_df.select(
        'province',
        'city',
        'county',
        'bbd_qyxx_id'
    ).where(
        new_df.risk_rank == u'持续监控'
    ).groupBy(
        ['province', 'city', 'county']
    ).count(
    ).withColumnRenamed(
        'count', 'constantly_monitor_num'
    ).fillna(
        {'city': u'无', 'county': u'无', 'province': u'无'}
    ).cache()
    
    
    #监控企业数
    supervise_count_df = new_df.select(
        'province',
        'city',
        'county',
        'bbd_qyxx_id'
    ).groupBy(
        ['province', 'city', 'county']
    ).count(
    ).withColumnRenamed(
        'count', 'supervise_num'
    ).fillna(
        {'city': u'无', 'county': u'无', 'province': u'无'}
    ).cache()
    
    #新兴金融、网络借贷、私募基金、交易场所
    raw_types_num_df = new_df.select(
        'province',
        'city',
        'county',
        'company_type'
    ).groupBy(
        ['province', 'city', 'county', 'company_type']
    ).count()
    tid_types_num_df = raw_types_num_df.select(
        'province', 
        'city', 
        'county',
        fun.concat_ws(':', 'company_type', 
                      'count').alias('company_type_merge')
    ).groupBy(
        ['province', 'city', 'county']
    ).agg(
        {'company_type_merge': 'collect_list'}
    ).withColumnRenamed(
        'collect_list(company_type_merge)', 'company_type_merge'
    ).fillna(
        {'city': u'无', 'county': u'无', 'province': u'无'}
    ).cache()
    
    #新增高危企业、减少高危企业
    tmp_new_df = new_df.select(
        'province',
        'city',
        'county',
        'bbd_qyxx_id',
        'company_type',
        'data_version'
    ).where(
        new_df.risk_rank == u'高危预警'
    )
    tmp_old_df = old_df.select(
        'province',
        'city',
        'county',
        'bbd_qyxx_id',
        'company_type',
        'data_version'
    ).where(
        old_df.risk_rank == u'高危预警'
    )
    tmp_new_2_df = tmp_new_df.union(
        tmp_old_df
    ).groupBy(
        ['province', 'city', 'county', 'bbd_qyxx_id']
    ).agg(
        {'data_version': 'collect_list'}
    ).select(
        'province',
        'city',
        'county',
        'bbd_qyxx_id',
        'collect_list(data_version)',
        get_change_info_udf(
            'collect_list(data_version)').alias('risk_change')
    ).groupBy(
        ['province', 'city', 'county']
    ).agg(
        {'risk_change': 'collect_list'}
    ).select(
        'province',
        'city',
        'county',
        get_change_info_2_udf(
            'collect_list(risk_change)'
        ).alias('risk_change_num')
    )
    tmp_new_3_df = tmp_new_2_df.select(
        'province',
        'city',
        'county',
        tmp_new_2_df.risk_change_num.getItem('decline').alias('risk_decline_num'),
        tmp_new_2_df.risk_change_num.getItem('rise').alias('risk_rise_num')
    ).fillna(
        {'city': u'无', 'county': u'无', 'province': u'无'}
    ).cache()
    
    #各行业新增高危企业、减少高危企业
    #新兴金融、网络借贷、私募基金、交易场所、融资担保、小额贷款
    tmp_new_6_df = tmp_new_df.union(
        tmp_old_df
    ).groupBy(
        ['province', 'city', 'county', 'bbd_qyxx_id', 'company_type']
    ).agg(
        {'data_version': 'collect_list'}
    ).select(
        'province',
        'city',
        'county',
        'bbd_qyxx_id',
        'company_type',
        'collect_list(data_version)',
        get_change_info_udf(
            'collect_list(data_version)').alias('risk_change')
    ).groupBy(
        ['province', 'city', 'county', 'company_type']
    ).agg(
        {'risk_change': 'collect_list'}
    ).select(
        'province',
        'city',
        'county',
        'company_type',
        get_change_info_2_udf(
            'collect_list(risk_change)'
        ).alias('risk_change_num')
    )
    
    tmp_new_7_df = tmp_new_6_df.select(
        'province',
        'city',
        'county',
        'company_type',
        tmp_new_6_df.risk_change_num.getItem('decline').alias('risk_decline_num'),
        tmp_new_6_df.risk_change_num.getItem('rise').alias('risk_rise_num')
    ).fillna(
        {'city': u'无', 'county': u'无', 'province': u'无'}
    ).cache()

    #选择不同的行业
    os.system(
        ("hadoop fs -rmr " 
         "{path}").format(path=TMP_PATH))
    tmp_new_7_df.where(
        tmp_new_7_df.company_type == u'新兴金融'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_xxjr_change_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_7_df.where(
        tmp_new_7_df.company_type == u'网络借贷'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_wljd_change_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_7_df.where(
        tmp_new_7_df.company_type == u'私募基金'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_smjj_change_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_7_df.where(
        tmp_new_7_df.company_type == u'交易场所'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_jycs_change_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_7_df.where(
        tmp_new_7_df.company_type == u'融资担保'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_rzdb_change_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_7_df.where(
        tmp_new_7_df.company_type == u'小额贷款'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_xedk_change_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
        
    tmp_xxjr_change_df = spark.read.parquet(
        "{path}/tmp_xxjr_change_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_wljd_change_df = spark.read.parquet(
        "{path}/tmp_wljd_change_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )  
    tmp_smjj_change_df = spark.read.parquet(
        "{path}/tmp_smjj_change_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_jycs_change_df = spark.read.parquet(
        "{path}/tmp_jycs_change_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_rzdb_change_df = spark.read.parquet(
        "{path}/tmp_rzdb_change_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_xedk_change_df = spark.read.parquet(
        "{path}/tmp_xedk_change_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    
    #监控企业变动情况
    tmp_new_df = new_df.select(
        'province',
        'city',
        'county',
        'bbd_qyxx_id',
        'company_type',
        'data_version'
    )
    tmp_old_df = old_df.select(
        'province',
        'city',
        'county',
        'bbd_qyxx_id',
        'company_type',
        'data_version'
    )
    tmp_new_4_df = tmp_new_df.union(
        tmp_old_df
    ).groupBy(
        ['province', 'city', 'county', 'bbd_qyxx_id']
    ).agg(
        {'data_version': 'collect_list'}
    ).select(
        'province',
        'city',
        'county',
        'bbd_qyxx_id',
        'collect_list(data_version)',
        get_change_info_udf(
            'collect_list(data_version)').alias('risk_change')
    ).groupBy(
        ['province', 'city', 'county']
    ).agg(
        {'risk_change': 'collect_list'}
    ).select(
        'province',
        'city',
        'county',
        get_change_info_2_udf(
            'collect_list(risk_change)').alias('risk_change_num')
    )
    tmp_new_5_df = tmp_new_4_df.select(
        'province',
        'city',
        'county',
        tmp_new_4_df.risk_change_num.getItem(
            'decline').alias('all_decline_num'),
        tmp_new_4_df.risk_change_num.getItem('rise').alias('all_rise_num')
    ).fillna(
        {'city': u'无', 'county': u'无', 'province': u'无'}
    ).cache()
    
    #各行业监控企业变动情况
    #新兴金融、网络借贷、私募基金、交易场所、融资担保、小额贷款
    tmp_new_8_df = tmp_new_df.union(
        tmp_old_df
    ).groupBy(
        ['province', 'city', 'county', 'bbd_qyxx_id', 'company_type']
    ).agg(
        {'data_version': 'collect_list'}
    ).select(
        'province',
        'city',
        'county',
        'bbd_qyxx_id',
        'company_type',
        'collect_list(data_version)',
        get_change_info_udf(
            'collect_list(data_version)').alias('risk_change')
    ).groupBy(
        ['province', 'city', 'county', 'company_type']
    ).agg(
        {'risk_change': 'collect_list'}
    ).select(
        'province',
        'city',
        'county',
        'company_type',
        get_change_info_2_udf(
            'collect_list(risk_change)').alias('risk_change_num')
    )
    tmp_new_9_df = tmp_new_8_df.select(
        'province',
        'city',
        'county',
        'company_type',
        tmp_new_8_df.risk_change_num.getItem(
            'decline').alias('all_decline_num'),
        tmp_new_8_df.risk_change_num.getItem('rise').alias('all_rise_num')
    ).fillna(
        {'city': u'无', 'county': u'无', 'province': u'无'}
    ).cache()

    #选择不同的行业
    tmp_new_9_df.where(
        tmp_new_9_df.company_type == u'新兴金融'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_xxjr_overall_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_9_df.where(
        tmp_new_9_df.company_type == u'网络借贷'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_wljd_overall_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_9_df.where(
        tmp_new_9_df.company_type == u'私募基金'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_smjj_overall_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_9_df.where(
        tmp_new_9_df.company_type == u'交易场所'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_jycs_overall_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_9_df.where(
        tmp_new_9_df.company_type == u'融资担保'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_rzdb_overall_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_9_df.where(
        tmp_new_9_df.company_type == u'小额贷款'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_xedk_overall_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )


    tmp_xxjr_overall_df = spark.read.parquet(
        "{path}/tmp_xxjr_overall_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_wljd_overall_df = spark.read.parquet(
        "{path}/tmp_wljd_overall_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_smjj_overall_df = spark.read.parquet(
        "{path}/tmp_smjj_overall_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_jycs_overall_df = spark.read.parquet(
        "{path}/tmp_jycs_overall_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_rzdb_overall_df = spark.read.parquet(
        "{path}/tmp_rzdb_overall_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_xedk_overall_df = spark.read.parquet(
        "{path}/tmp_xedk_overall_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)          
    )
    
    
    #各行业持续监控、重点关注企业
    #新兴金融、网络借贷、私募基金、交易场所、融资担保、小额贷款  
    tmp_new_10_df = new_df.select(
        'province',
        'city',
        'county',
        'company_type',
        'risk_rank'
    ).groupBy(
        ['province', 'city', 'county', 'company_type']
    ).agg(
        {'risk_rank': 'collect_list'}
    ).withColumnRenamed(
        'collect_list(risk_rank)', 'risk_rank'
    ).withColumn(
         'zdgz_num', get_zdgz_num_udf('risk_rank')
    ).withColumn(
        'cxjk_num', get_cxjk_num_udf('risk_rank')
    ).fillna(
        {'city': u'无', 'county': u'无', 'province': u'无'}
    ).cache()
    
    #选择不同的行业
    tmp_new_10_df.where(
        tmp_new_10_df.company_type == u'新兴金融'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_xxjr_monitoring_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)
    )
    
    tmp_new_10_df.where(
        tmp_new_10_df.company_type == u'网络借贷'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_wljd_monitoring_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_10_df.where(
        tmp_new_10_df.company_type == u'私募基金'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_smjj_monitoring_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_10_df.where(
        tmp_new_10_df.company_type == u'交易场所'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_jycs_monitoring_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_10_df.where(
        tmp_new_10_df.company_type == u'融资担保'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_rzdb_monitoring_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    tmp_new_10_df.where(
        tmp_new_10_df.company_type == u'小额贷款'
    ).coalesce(
        10    
    ).write.parquet(
        "{path}/tmp_xedk_monitoring_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)            
    )
    
        
    tmp_xxjr_monitoring_df = spark.read.parquet(
        "{path}/tmp_xxjr_monitoring_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)
    )
    tmp_wljd_monitoring_df = spark.read.parquet(
        "{path}/tmp_wljd_monitoring_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)
    )
    tmp_smjj_monitoring_df = spark.read.parquet(
        "{path}/tmp_smjj_monitoring_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)
    )
    tmp_jycs_monitoring_df = spark.read.parquet(
        "{path}/tmp_jycs_monitoring_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)
    )
    tmp_rzdb_monitoring_df = spark.read.parquet(
        "{path}/tmp_rzdb_monitoring_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)
    )
    tmp_xedk_monitoring_df = spark.read.parquet(
        "{path}/tmp_xedk_monitoring_df/"
        "{version}".format(path=TMP_PATH,
                           version=NEW_VERSION)
    )
    
    #组合所有的字段
    tid_new_df = new_df.dropDuplicates(
        ['province', 'city', 'county']
    ).join(
        high_risk_count_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        focus_on_count_df,
        ['province', 'city', 'county'],
        'left_outer'            
    ).join(
        constantly_monitor_count_df,
        ['province', 'city', 'county'],
        'left_outer'            
    ).join(
        supervise_count_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tid_types_num_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_new_3_df,
        ['province', 'city', 'county'],
        'left_outer'    
    ).join(
        tmp_new_5_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_xxjr_change_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_wljd_change_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_smjj_change_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_jycs_change_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_rzdb_change_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_xedk_change_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_xxjr_overall_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_wljd_overall_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_smjj_overall_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_jycs_overall_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_rzdb_overall_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_xedk_overall_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_xxjr_monitoring_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_wljd_monitoring_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_smjj_monitoring_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_jycs_monitoring_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_rzdb_monitoring_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tmp_xedk_monitoring_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).select(
        new_df.province,
        new_df.city,
        new_df.county,
        high_risk_count_df.high_risk_num,
        focus_on_count_df.focus_on_num,
        constantly_monitor_count_df.constantly_monitor_num,
        supervise_count_df.supervise_num,
        get_xxjr_udf(tid_types_num_df.company_type_merge).alias('xxjr'),
        get_smjj_udf(tid_types_num_df.company_type_merge).alias('smjj'),
        get_wljd_udf(tid_types_num_df.company_type_merge).alias('wljd'),
        get_jycs_udf(tid_types_num_df.company_type_merge).alias('jycs'),
        get_rzdb_udf(tid_types_num_df.company_type_merge).alias('rzdb'),
        get_xedk_udf(tid_types_num_df.company_type_merge).alias('xedk'),
        tmp_new_3_df.risk_decline_num,
        tmp_new_3_df.risk_rise_num,
        tmp_new_5_df.all_decline_num,
        tmp_new_5_df.all_rise_num,
        tmp_xxjr_change_df.risk_decline_num.alias('other_lessen_high_risk'),
        tmp_xxjr_change_df.risk_rise_num.alias('other_add_high_risk'),
        tmp_wljd_change_df.risk_decline_num.alias('net_lessen_high_risk'),
        tmp_wljd_change_df.risk_rise_num.alias('net_add_high_risk'),        
        tmp_smjj_change_df.risk_decline_num.alias('private_fund_lessen_high_risk'),
        tmp_smjj_change_df.risk_rise_num.alias('private_fund_add_high_risk'),               
        tmp_jycs_change_df.risk_decline_num.alias('trade_place_lessen_high_risk'),
        tmp_jycs_change_df.risk_rise_num.alias('trade_place_add_high_risk'),   
        tmp_rzdb_change_df.risk_decline_num.alias('financing_guarantee_lessen_high_risk'),
        tmp_rzdb_change_df.risk_rise_num.alias('financing_guarantee_add_high_risk'),  
        tmp_xedk_change_df.risk_decline_num.alias('petty_loan_lessen_high_risk'),
        tmp_xedk_change_df.risk_rise_num.alias('petty_loan_add_high_risk'),  
        tmp_xxjr_overall_df.all_decline_num.alias('other_lessen_monitor'),
        tmp_xxjr_overall_df.all_rise_num.alias('other_add_monitor'),
        tmp_wljd_overall_df.all_decline_num.alias('net_lessen_monitor'),
        tmp_wljd_overall_df.all_rise_num.alias('net_add_monitor'),
        tmp_smjj_overall_df.all_decline_num.alias('private_fund_lessen_monitor'),
        tmp_smjj_overall_df.all_rise_num.alias('private_fund_add_monitor'),
        tmp_jycs_overall_df.all_decline_num.alias('trade_place_lessen_monitor'),
        tmp_jycs_overall_df.all_rise_num.alias('trade_place_add_monitor'),        
        tmp_rzdb_overall_df.all_decline_num.alias('financing_guarantee_lessen_monitor'),
        tmp_rzdb_overall_df.all_rise_num.alias('financing_guarantee_add_monitor'),           
        tmp_xedk_overall_df.all_decline_num.alias('petty_loan_lessen_monitor'),
        tmp_xedk_overall_df.all_rise_num.alias('petty_loan_add_monitor'),
        tmp_xxjr_monitoring_df.zdgz_num.alias('other_focus_on'),
        tmp_xxjr_monitoring_df.cxjk_num.alias('other_sustain_monitor'),
        tmp_wljd_monitoring_df.zdgz_num.alias('net_focus_on'),
        tmp_wljd_monitoring_df.cxjk_num.alias('net_sustain_monitor'),
        tmp_smjj_monitoring_df.zdgz_num.alias('private_fund_focus_on'),
        tmp_smjj_monitoring_df.cxjk_num.alias('private_fund_sustain_monitor'),
        tmp_jycs_monitoring_df.zdgz_num.alias('trade_place_focus_on'),
        tmp_jycs_monitoring_df.cxjk_num.alias('trade_place_sustain_monitor'),        
        tmp_rzdb_monitoring_df.zdgz_num.alias('financing_guarantee_focus_on'),
        tmp_rzdb_monitoring_df.cxjk_num.alias('financing_guarantee_sustain_monitor'),
        tmp_xedk_monitoring_df.zdgz_num.alias('petty_loan_focus_on'),
        tmp_xedk_monitoring_df.cxjk_num.alias('petty_loan_sustain_monitor'),
        fun.current_timestamp().alias('gmt_create'),
        fun.current_timestamp().alias('gmt_update')
    ).cache()
    
    return tid_new_df
    
def spark_data_flow():
    '''
    构建最终输出df
    '''
    get_id_udf = fun.udf(get_id, tp.StringType())
    
    tid_new_df = raw_spark_data_flow()
    prd_new_df = tid_new_df.select(
        get_id_udf().alias('id'),
        tid_new_df.province,
        tid_new_df.city,
        tid_new_df.county.alias('area'),
        tid_new_df.high_risk_num.alias('high_risk'),
        tid_new_df.focus_on_num.alias('focus_on'),
        tid_new_df.constantly_monitor_num.alias('sustain_monitor'),
        tid_new_df.risk_rise_num.alias('add_high_risk'),
        tid_new_df.risk_decline_num.alias('lessen_high_risk'),
        tid_new_df.supervise_num.alias('monitor'),
        tid_new_df.all_rise_num.alias('add_monitor'),
        tid_new_df.all_decline_num.alias('lessen_monitor'),
        tid_new_df.xxjr.alias('rising_financial'),
        tid_new_df.wljd.alias('net_loan'),
        tid_new_df.smjj.alias('private_fund'),
        tid_new_df.jycs.alias('trade_place'),
        tid_new_df.rzdb.alias('financing_guarantee'),
        tid_new_df.xedk.alias('petty_loan'),
        'net_add_high_risk',
        'net_lessen_high_risk',
        'net_add_monitor',
        'net_lessen_monitor',
        'net_focus_on',
        'net_sustain_monitor',
        'private_fund_add_high_risk',
        'private_fund_lessen_high_risk',
        'private_fund_add_monitor',
        'private_fund_lessen_monitor',
        'private_fund_focus_on',
        'private_fund_sustain_monitor',
        'trade_place_add_high_risk',
        'trade_place_lessen_high_risk',
        'trade_place_add_monitor',
        'trade_place_lessen_monitor',
        'trade_place_focus_on',
        'trade_place_sustain_monitor',
        'financing_guarantee_add_high_risk',
        'financing_guarantee_lessen_high_risk',
        'financing_guarantee_add_monitor',
        'financing_guarantee_lessen_monitor',
        'financing_guarantee_focus_on',
        'financing_guarantee_sustain_monitor',
        'petty_loan_add_high_risk',
        'petty_loan_lessen_high_risk',
        'petty_loan_add_monitor',
        'petty_loan_lessen_monitor',
        'petty_loan_focus_on',
        'petty_loan_sustain_monitor',
        'other_add_high_risk',
        'other_lessen_high_risk',
        'other_add_monitor',
        'other_lessen_monitor',
        'other_focus_on',
        'other_sustain_monitor',
        tid_new_df.gmt_create,
        tid_new_df.gmt_update
    ).fillna(
        {'city': u'无', 'area': u'无', 'province': u'无'}
    ).fillna(
        0
    )
    
    return prd_new_df
    
def run():
    prd_df = spark_data_flow()
    
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "ra_area_count/{version}").format(path=OUT_PATH, 
                                           version=NEW_VERSION))

    prd_df.repartition(
        10
    ).rdd.map(
        lambda r:
            '\t'.join([
                r.id,
                r.province,
                r.city,
                r.area,
                str(r.high_risk),
                str(r.focus_on),
                str(r.sustain_monitor),
                str(r.add_high_risk),
                str(r.lessen_high_risk),
                str(r.monitor),
                str(r.add_monitor),
                str(r.lessen_monitor),
                str(r.rising_financial),
                str(r.net_loan),
                str(r.private_fund),
                str(r.trade_place),
                str(r.financing_guarantee),
                str(r.petty_loan),
                str(r.net_add_high_risk),
                str(r.net_lessen_high_risk),
                str(r.net_add_monitor),
                str(r.net_lessen_monitor),
                str(r.net_focus_on),
                str(r.net_sustain_monitor),
                str(r.private_fund_add_high_risk),
                str(r.private_fund_lessen_high_risk),
                str(r.private_fund_add_monitor),
                str(r.private_fund_lessen_monitor),
                str(r.private_fund_focus_on),
                str(r.private_fund_sustain_monitor),
                str(r.trade_place_add_high_risk),
                str(r.trade_place_lessen_high_risk),
                str(r.trade_place_add_monitor),
                str(r.trade_place_lessen_monitor),
                str(r.trade_place_focus_on),
                str(r.trade_place_sustain_monitor),
                str(r.financing_guarantee_add_high_risk),
                str(r.financing_guarantee_lessen_high_risk),
                str(r.financing_guarantee_add_monitor),
                str(r.financing_guarantee_lessen_monitor),
                str(r.financing_guarantee_focus_on),
                str(r.financing_guarantee_sustain_monitor),
                str(r.petty_loan_add_high_risk),
                str(r.petty_loan_lessen_high_risk),
                str(r.petty_loan_add_monitor),
                str(r.petty_loan_lessen_monitor),
                str(r.petty_loan_focus_on),
                str(r.petty_loan_sustain_monitor),
                str(r.other_add_high_risk),
                str(r.other_lessen_high_risk),
                str(r.other_add_monitor),
                str(r.other_lessen_monitor),
                str(r.other_focus_on),
                str(r.other_sustain_monitor),
                r.gmt_create.strftime('%Y-%m-%d %H:%M:%S'),
                r.gmt_update.strftime('%Y-%m-%d %H:%M:%S')
            ])
    ).saveAsTextFile(
        "{path}/ra_area_count/{version}".format(path=OUT_PATH,
                                                version=NEW_VERSION)
    )
 
    #输出到mysql
    if IS_INTO_MYSQL:
        truncate_table('ra_area_count')
        os.system(
        ''' 
        sqoop export \
        --connect {url} \
        --username {user} \
        --password '{password}' \
        --table {table} \
        --export-dir {path}/{table}/{version} \
        --input-fields-terminated-by '\\t' 
        '''.format(
                url=URL,
                user=PROP['user'],
                password=PROP['password'],
                table=TABLE,
                path=OUT_PATH,
                version=NEW_VERSION
            )
        )
       
        print '\n************\n导入大成功SUCCESS !!\n************\n'

def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "50g")
    conf.set("spark.executor.instances", 10)
    conf.set("spark.executor.cores", 10)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 1000)
    conf.set("spark.sql.shuffle.partitions", 1000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)

    spark = SparkSession \
        .builder \
        .appName("hgongjing2_four_raw_all_info") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 

if __name__ == '__main__':
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #用于比较的两个数据版本,取最近6个月
    VERSION_LIST = eval(conf.get('common', 'RELATION_VERSIONS'))[-6:]
    VERSION_LIST.sort()
    OLD_VERSION, NEW_VERSION = VERSION_LIST[-2:]
    
    #结果存一份在HDFS，同时判断是否输出到mysql
    IN_PATH = conf.get('all_company_info', 'OUT_PATH')
    TMP_PATH = conf.get('ra_area_count', 'TMP_PATH')
    OUT_PATH = conf.get('to_mysql', 'OUT_PATH')
    IS_INTO_MYSQL = conf.getboolean('to_mysql', 'IS_INTO_MYSQL')
    
    #mysql输出信息
    TABLE = 'ra_area_count'
    URL = conf.get('mysql', 'URL')
    PROP = eval(conf.get('mysql', 'PROP'))
    
    spark = get_spark_session()
    
    run()