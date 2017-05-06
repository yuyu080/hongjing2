# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
--jars /usr/share/java/mysql-connector-java-5.1.39.jar \
ra_high_company.py
'''
import os

from pyspark.sql import Window
from pyspark.sql.functions import rank
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp

def get_into_date(date_list):
    '''
    企业“进入日期”
    '''
    date_list.sort()
    return date_list[0]

def is_new(col):
    '''
    企业是否是新进入企业
    '''
    new_status, old_status = False, False
    for each_info in col:
        if NEW_VERSION in each_info:
            new_status = True
        if OLD_VERSION in each_info:
            old_status = True
    return 1 if new_status and not old_status else 0

def is_rise(col):
    '''
    企业风险值是否升高
    '''
    result_dict = {}
    for each_info in col:
        version, risk_index = each_info.split(':')
        result_dict[version] = float(risk_index)
    #这里需要考虑几种特殊情况
    version_list = result_dict.keys()
    if (len(version_list) == 1 or 
                NEW_VERSION not in version_list):
        return 0
    else:
        is_rise = result_dict[NEW_VERSION] - result_dict.get(OLD_VERSION, 0)
        if is_rise < 0:
            return -1
        elif is_rise > 0:
            return 1
        else:
            return 0

def get_df(version):
    '''
    获取单个版本df的高危企业
    '''
    raw_df = spark.read.parquet(
        ("{path}"
         "/all_company_info/{version}").format(path=IN_PATH,
                                               version=version))
    tid_df = raw_df.where(
        raw_df.risk_rank == u'高危预警'
    )
    
    return tid_df

def raw_spark_data_flow():
    df_list = []
    for each_version in VERSION_LIST:
        each_df = get_df(each_version)
        df_list.append(each_df)
    
    #将多个df合并
    tid_df = eval(
        "df_list[{0}]".format(0) + 
        "".join([
                ".union(df_list[{0}])".format(df_index) 
                for df_index in range(1, len(df_list))])
    )    

    return tid_df
    
def spark_data_flow():
    '''
    输入每个一个版本列表，计算高危企业进入时间，输出一个最终df
    '''
    get_into_date_udf = fun.udf(get_into_date, tp.StringType())
    is_new_udf = fun.udf(is_new, tp.IntegerType())
    is_rise_udf = fun.udf(is_rise, tp.IntegerType())    
    
    tid_df = raw_spark_data_flow()
    
    #计算企业的“进入时间”
    tmp_df = tid_df.groupBy(
        'company_name'
    ).agg(
        {'data_version': 'collect_list'}
    ).select(
        'company_name',
        'collect_list(data_version)',
        get_into_date_udf(
            'collect_list(data_version)').alias('join_date')
    )
    
    #企业易燃指数是否上升：比较最近2个版本
    #企业是否是新进入榜单企业：包含特殊规则
    tmp_2_df = tid_df.select(
        'company_name',
        fun.concat_ws(':', 'data_version', 
                           'risk_index').alias('risk_index_with_date')
    ).groupBy(
        'company_name'
    ).agg(
        {'risk_index_with_date': 'collect_list'}
    ).select(
        'company_name',
        is_new_udf(
            'collect_list(risk_index_with_date)'
        ).alias('is_new'),
        is_rise_udf(
            'collect_list(risk_index_with_date)'
        ).alias('rise')
    )

    #合并数据
    #选取每个企业时间最靠后的那个版本的risk_index作为易燃指数
    window = Window.partitionBy(
        "company_name"
    ).orderBy(
        tid_df.data_version.desc()
    )
    
    prd_df = tid_df.select(
        'bbd_qyxx_id',
        'province',
        'city',
        'county',
        'company_name',
        'risk_index',
        'company_type',
        'data_version',
        rank().over(window).alias('rank')
    ).where(
        'rank == 1'
    ).join(
        tmp_df,
        'company_name', 
    ).join(
        tmp_2_df,
        'company_name'
    ).select(
        tid_df.bbd_qyxx_id.alias('id'),
        tid_df.province,
        tid_df.city,
        tid_df.county.alias('area'),
        tid_df.bbd_qyxx_id.alias('company_id'),
        tid_df.company_name.alias('company'),
        tid_df.risk_index,
        tmp_2_df.rise,
        tmp_2_df.is_new,
        tmp_df.join_date,
        tid_df.company_type.alias('industry'),
        fun.concat_ws(
            '', tid_df.city, tid_df.county
        ).alias('register_area'),
        fun.current_timestamp().alias('gmt_create'),
        fun.current_timestamp().alias('gmt_update')
    ).fillna(
        u'无'
    ).fillna(
        {'city': u'无', 'area': u'无', 'province': u'无'}
    ).dropDuplicates(
        ['id']
    )

    
    return prd_df
    
def run():
    prd_df = spark_data_flow()
    
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "ra_high_company/{version}").format(path=OUT_PATH, 
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
                r.company_id,
                r.company,
                str(r.risk_index),
                str(r.rise),
                str(r.is_new),
                r.join_date,
                r.industry,
                r.register_area,
                r.gmt_create.strftime('%Y-%m-%d %H:%M:%S'),
                r.gmt_update.strftime('%Y-%m-%d %H:%M:%S')
            ])
    ).saveAsTextFile(
        "{path}/ra_high_company/{version}".format(path=OUT_PATH,
                                                  version=NEW_VERSION)
    )
    
    #输出到mysql
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
    #输入参数
    IN_PATH = '/user/antifraud/hongjing2/dataflow/step_three/prd'
    VERSION_LIST = ['20170117', '20170403']
    VERSION_LIST.sort()
    OLD_VERSION, NEW_VERSION = VERSION_LIST[-2:]
    OUT_PATH = '/user/antifraud/hongjing2/dataflow/step_four/raw'
    
    #mysql输出信息
    TABLE = 'ra_high_company'
    URL = "jdbc:mysql://10.10.20.180:3306/airflow?characterEncoding=UTF-8"
    PROP = {"user": "airflow", 
            "password":"airflow", 
            "driver": "com.mysql.jdbc.Driver"}
    
    spark = get_spark_session()
    
    run()