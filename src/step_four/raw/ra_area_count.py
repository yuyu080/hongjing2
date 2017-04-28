# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
--jars /usr/share/java/mysql-connector-java-5.1.39.jar \
ra_area_count.py
'''


from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp


def get_change_info(col):
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

def get_id():
    return 
    
def raw_spark_data_flow():
    #注册所有需要用到的udf
    get_jycs_udf = fun.udf(get_jycs, tp.IntegerType())
    get_smjj_udf = fun.udf(get_smjj, tp.IntegerType())
    get_wljd_udf = fun.udf(get_wljd, tp.IntegerType())
    get_xxjr_udf = fun.udf(get_xxjr, tp.IntegerType())
    get_change_info_2_udf = fun.udf(
        get_change_info_2, 
        tp.MapType(tp.StringType(), tp.IntegerType())
    )
    get_change_info_udf = fun.udf(get_change_info, tp.IntegerType())
    
    #读取原始输入
    old_df =  spark.read.parquet(
        ("/user/antifraud/hongjing2/dataflow/step_three/prd"
        "/all_company_info/{version}").format(version=OLD_VERSION))
    new_df =  spark.read.parquet(
        ("/user/antifraud/hongjing2/dataflow/step_three/prd"
        "/all_company_info/{version}").format(version=NEW_VERSION))    

    #高危企业数
    high_risk_count_df = new_df.select(
        'province',
        'city',
        'county',
        'company_name'
    ).where(
        new_df.risk_rank == u'高危预警'
    ).groupBy(
        ['province', 'city', 'county']
    ).count(
    ).withColumnRenamed(
        'count', 'high_risk_num'
    ).cache()
    
    #监控企业数
    supervise_count_df = new_df.select(
        'province',
        'city',
        'county',
        'company_name'
    ).groupBy(
        ['province', 'city', 'county']
    ).count(
    ).withColumnRenamed(
        'count', 'supervise_num'
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
        fun.concat_ws(':', 'company_type', 'count').alias('company_type_merge')
    ).groupBy(
        ['province', 'city', 'county']
    ).agg(
        {'company_type_merge': 'collect_list'}
    ).withColumnRenamed(
        'collect_list(company_type_merge)', 'company_type_merge'
    ).cache()
    
    #新增高危企业、减少高危企业
    tmp_new_df = new_df.select(
        'province',
        'city',
        'county',
        'company_name',
        'data_version'
    ).where(
        new_df.risk_rank == u'高危预警'
    )
    tmp_old_df = old_df.select(
        'province',
        'city',
        'county',
        'company_name',
        'data_version'
    ).where(
        old_df.risk_rank == u'高危预警'
    )
    tmp_new_2_df = tmp_new_df.union(
        tmp_old_df
    ).groupBy(
        ['province', 'city', 'county', 'company_name']
    ).agg(
        {'data_version': 'collect_list'}
    ).select(
        'province',
        'city',
        'county',
        'company_name',
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
        get_change_info_2_udf('collect_list(risk_change)').alias('risk_change_num')
    )
    tmp_new_3_df = tmp_new_2_df.select(
        'province',
        'city',
        'county',
        tmp_new_2_df.risk_change_num.getItem('decline').alias('risk_decline_num'),
        tmp_new_2_df.risk_change_num.getItem('rise').alias('risk_rise_num')
    ).cache()
    
    #监控企业变动情况
    tmp_new_df = new_df.select(
        'province',
        'city',
        'county',
        'company_name',
        'data_version'
    )
    tmp_old_df = old_df.select(
        'province',
        'city',
        'county',
        'company_name',
        'data_version'
    )
    tmp_new_4_df = tmp_new_df.union(
        tmp_old_df
    ).groupBy(
        ['province', 'city', 'county', 'company_name']
    ).agg(
        {'data_version': 'collect_list'}
    ).select(
        'province',
        'city',
        'county',
        'company_name',
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
    ).cache()
    
    #组合所有的字段
    tid_new_df = new_df.dropDuplicates(
        ['province', 'city', 'county']
    ).join(
        high_risk_count_df,
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
    ).select(
        new_df.province,
        new_df.city,
        new_df.county,
        high_risk_count_df.high_risk_num,
        supervise_count_df.supervise_num,
        get_xxjr_udf(tid_types_num_df.company_type_merge).alias('xxjr'),
        get_smjj_udf(tid_types_num_df.company_type_merge).alias('smjj'),
        get_wljd_udf(tid_types_num_df.company_type_merge).alias('wljd'),
        get_jycs_udf(tid_types_num_df.company_type_merge).alias('jycs'),
        tmp_new_3_df.risk_decline_num,
        tmp_new_3_df.risk_rise_num,
        tmp_new_5_df.all_decline_num,
        tmp_new_5_df.all_rise_num,
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
        'province',
        'city',
        tid_new_df.county.alias('area'),
        tid_new_df.high_risk_num.alias('high_risk'),
        tid_new_df.risk_rise_num.alias('add_high_risk'),
        tid_new_df.risk_decline_num.alias('lessen_high_risk'),
        tid_new_df.supervise_num.alias('monitor'),
        tid_new_df.all_rise_num.alias('add_monitor'),
        tid_new_df.all_decline_num.alias('lessen_monitor'),
        tid_new_df.xxjr.alias('rising_financial'),
        tid_new_df.wljd.alias('net_loan'),
        tid_new_df.jycs.alias('trade_place'),
        tid_new_df.gmt_create,
        tid_new_df.gmt_update
    ).fillna(
        0
    ).fillna(
        {'city': u'无', 'area': u'无', 'province': u'无'}
    )
    
    return prd_new_df
    
def run():
    prd_df = spark_data_flow()
    prd_df.repartition(
        1
    ).write.jdbc(url=URL, table=TABLE, 
                 mode="append", properties=PROP)
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
    #用于比较的两个数据版本
    OLD_VERSION = '20170117'
    NEW_VERSION = '20170403'
    
    #mysql输出信息
    TABLE = 'ra_area_count'
    URL = "jdbc:mysql://10.10.20.180:3306/airflow?characterEncoding=UTF-8"
    PROP = {"user": "airflow", 
            "password":"airflow", 
            "driver": "com.mysql.jdbc.Driver"}
    
    spark = get_spark_session()
    
    run()