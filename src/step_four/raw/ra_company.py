# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
--jars /usr/share/java/mysql-connector-java-5.1.39.jar \
ra_company.py
'''
import os
import json

import configparser
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp
from pyspark.sql import Row

def get_xgxx_change(old_xgxx, new_xgxx):
    '''
    获取相关信息的变动情况，并按照一定格式输出
    '''
    if old_xgxx and new_xgxx:
        old_xgxx_obj = json.loads(old_xgxx)
        new_xgxx_obj = json.loads(new_xgxx)
        
        all_keys = new_xgxx_obj.keys()
        
        #获取最终输出列表
        result = [
            {
                "type": each_key,
                "value": new_xgxx_obj[each_key],
                "isupdate": get_risk_change(
                    old_xgxx_obj[each_key],
                    new_xgxx_obj[each_key]
                )
            }
            for each_key in all_keys
        ]
        return  json.dumps(result, ensure_ascii=False)
    else:
        return u'无'

def get_risk_change(old_score, new_score):
    if old_score and new_score:
        is_rise = new_score - old_score
    else:
        is_rise = 0.
    if is_rise < 0:
        return -1
    elif is_rise > 0:
        return 1
    else:
        return 0

def get_risk_sequence_version(iter_obj):
    '''
    获取每个企业各个日期，每个子键的风险总分
    '''
    result = {}
    for each_obj in iter_obj:
        version, json_obj = each_obj.split('&&')
        py_obj = json.loads(json_obj)
        for k, v in py_obj.iteritems():
            if result.has_key(k):
                result[k][version] = v['total_score']
            else:
                result[k] = {version: v['total_score']}
    return json.dumps(result, ensure_ascii=False)

def get_df(version):
    '''
    获取单个版本df的高危企业
    '''
    raw_df = spark.read.parquet(
        ("{path}"
         "/all_company_info/{version}").format(path=IN_PATH,
                                               version=version))
    return raw_df
    
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

def tid_spark_data_flow():
    '''
    构建df的某些字段
    '''
    #构建udf
    get_risk_change_udf = fun.udf(get_risk_change, tp.IntegerType())
    get_xgxx_change_udf = fun.udf(get_xgxx_change, tp.StringType())
    
    #原始输入
    old_df = get_df(OLD_VERSION)
    new_df = get_df(NEW_VERSION)
    raw_df = raw_spark_data_flow()        
        
    #易燃指数是否上升
    tmp_new_df = new_df.select(
        'company_name',
        'risk_index'
    )
    tmp_old_df = old_df.select(
        'company_name',
        'risk_index'
    )
    tmp_new_2_df = tmp_new_df.join(
        tmp_old_df,
        'company_name',
        'left_outer'
    ).select(
        'company_name',
        tmp_new_df.risk_index.alias('new'),
        tmp_old_df.risk_index.alias('old'),
        get_risk_change_udf(
            tmp_old_df.risk_index, 
            tmp_new_df.risk_index
        ).alias('is_rise')
    )

    #相关信息变更情况
    tmp_new_3_df = new_df.select(
        'company_name',
        'xgxx_info'
    )
    tmp_old_3_df = old_df.select(
        'company_name',
        'xgxx_info'
    )
    tmp_new_4_df = tmp_new_3_df.join(
        tmp_old_3_df,
        'company_name',
        'left_outer'
    ).select(
        'company_name',
        tmp_new_3_df.xgxx_info,
        tmp_old_3_df.xgxx_info,
        get_xgxx_change_udf(
            tmp_old_3_df.xgxx_info,
            tmp_new_3_df.xgxx_info
        ).alias('xgxx_info_with_change')
    )
    
    #易燃指数时序图，涉及多版本，多子键的计算
    #risk_sequence_version
    tid_df = raw_df.select(
        'bbd_qyxx_id',
        'company_name',
        fun.concat_ws(
            '&&', 'data_version', 'risk_composition'
        ).alias('risk_with_version')
    ).rdd.map(
        lambda r:
            ((r.company_name, r.bbd_qyxx_id), 
             r.risk_with_version)
    ).groupByKey(
    ).map(
        lambda (k, iter_obj): Row(
            company_name=k[0],
            bbd_qyxx_id=k[1],
            risk_sequence_version=get_risk_sequence_version(iter_obj)
        )
    ).toDF(
    )
    
    #组合所有字段最终输出
    tid_new_df = new_df.join(
        tmp_new_2_df,
        'company_name',
        'left_outer'
    ).join(
        tmp_new_4_df,
        'company_name',
        'left_outer'
    ).join(
        tid_df,
        'company_name',
        'left_outer'
    ).select(
        new_df.bbd_qyxx_id,
        new_df.province,
        new_df.city,
        new_df.county,
        new_df.company_name,
        new_df.risk_index,
        new_df.risk_rank,
        tmp_new_2_df.is_rise,
        new_df.company_type,
        new_df.risk_composition,
        new_df.risk_tags,
        tid_df.risk_sequence_version,
        tmp_new_4_df.xgxx_info_with_change
    )

    return tid_new_df
    
def spark_data_flow():
    '''
    构建最终输出df
    '''        
    tid_new_df = tid_spark_data_flow()

    
    prd_new_df = tid_new_df.where(
        tid_new_df.bbd_qyxx_id.isNotNull()
    ).select(
        tid_new_df.bbd_qyxx_id.alias('id'),
        'province',
        'city',
        tid_new_df.county.alias('area'),
        tid_new_df.company_name.alias('company'),
        fun.round('risk_index', 1).alias('risk_index'),
        tid_new_df.risk_rank.alias('risk_level'),
        tid_new_df.is_rise.alias('rise'),
        tid_new_df.company_type.alias('industry'),
        tid_new_df.risk_composition.alias('index_radar'),
        tid_new_df.risk_tags.alias('risk_scan'),
        tid_new_df.risk_sequence_version.alias('index_sort'),
        tid_new_df.xgxx_info_with_change.alias('company_detail'),
        fun.current_timestamp().alias('gmt_create'),
        fun.current_timestamp().alias('gmt_update')
    ).fillna(
        u'无'
    ).fillna(
        {'city': u'无', 'area': u'无', 'province': u'无'}
    ).dropDuplicates(
        ['id']
    )    
    return prd_new_df

def run():
    prd_df = spark_data_flow()

    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "ra_company/{version}").format(path=OUT_PATH, 
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
                r.company,
                str(r.risk_index),
                r.risk_level,
                str(r.rise),
                r.industry,
                r.index_radar,
                r.risk_scan,
                r.index_sort,
                r.company_detail,
                r.gmt_create.strftime('%Y-%m-%d %H:%M:%S'),
                r.gmt_update.strftime('%Y-%m-%d %H:%M:%S')
            ])
    ).saveAsTextFile(
        "{path}/ra_company/{version}".format(path=OUT_PATH,
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
    conf.set("spark.executor.memory", "20g")
    conf.set("spark.executor.instances", 15)
    conf.set("spark.executor.cores", 5)
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
    
    #用于比较的两个数据版本
    VERSION_LIST = eval(conf.get('common', 'RELATION_VERSIONS'))
    VERSION_LIST.sort()
    OLD_VERSION, NEW_VERSION = VERSION_LIST[-2:]
    IN_PATH = '/user/antifraud/hongjing2/dataflow/step_three/prd'
    OUT_PATH = '/user/antifraud/hongjing2/dataflow/step_four/raw'
    
    #mysql输出信息
    TABLE = conf.get('mysql', 'TABLE')
    URL = conf.get('mysql', 'URL')
    PROP = eval(conf.get('mysql', 'PROP'))
    
    spark = get_spark_session()
    
    run()