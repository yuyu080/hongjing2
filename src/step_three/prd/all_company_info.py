# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
all_company_info.py {version}
'''
import sys
import os
import json

import configparser
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp
from pyspark.sql import Row

def get_json_obj(row):
    json_obj = json.dumps(
        {u'工商变更': row['gsbg'],
         u'诉讼信息': row['ssxx'],
         u'行政处罚': row['xzcf'],
         u'失信信息': row['sxxx'],
         u'经营异常': row['jyyc'],
         u'分支机构': row['fzjg'],
         u'风险关联方': row['fxglf'] if row['fxglf'] else 0
        },
        ensure_ascii=False
    )
    return Row(
        bbd_qyxx_id=row['bbd_qyxx_id'],
        company_name=row['company_name'],
        xgxx_info=json_obj
    )

def get_data_version():
    return RELATION_VERSION

def raw_spark_data_flow():
    '''
    根据step_one_prd的计算结果合并前各行业企业的所有信息，
    并输出单一时间点的最终结果
    '''
    
    #所有种类企业的合集
    sample_df = spark.read.parquet(
         ("{path}"
          "/ljr_sample/{version}").format(path=IN_PATH_ONE,
                                          version=RELATION_VERSION))
    
    raw_nf_df = spark.read.parquet(
        ("{path}"
         "/nf_feature_tags/{version}").format(path=IN_PATH_TWO,
                                              version=RELATION_VERSION))
    
    raw_p2p_df = spark.read.parquet(
        ("{path}"
         "/p2p_feature_tags/{version}").format(path=IN_PATH_TWO,
                                               version=RELATION_VERSION)) 
    
    raw_pe_df = spark.read.parquet(
        ("{path}"
         "/pe_feature_tags/{version}").format(path=IN_PATH_TWO,
                                              version=RELATION_VERSION))
    
    raw_ex_df = spark.read.parquet(
        ("{path}"
         "/ex_feature_tags/{version}").format(path=IN_PATH_TWO,
                                              version=RELATION_VERSION))
    
    tmp_p2p_df = sample_df.where(sample_df.company_type == u'网络借贷')
    tmp_pe_df = sample_df.where(sample_df.company_type == u'私募基金')
    tmp_ex_df = sample_df.where(sample_df.company_type == u'交易场所')
    tmp_nf_df = sample_df.where(
        sample_df.company_type.isin(TYPE_LIST)
    )
 
    #确保一个公司只会存在一个company_type
    tid_nf_df = raw_nf_df.join(
        tmp_nf_df,
        ['bbd_qyxx_id']
    ).select(
        raw_nf_df.company_name,
        raw_nf_df.bbd_qyxx_id,
        raw_nf_df.risk_tags,
        raw_nf_df.risk_index,
        raw_nf_df.risk_composition,
        raw_nf_df.province,
        raw_nf_df.city,
        raw_nf_df.county,
        raw_nf_df.is_black,
        tmp_nf_df.company_type
    )
    
    tid_p2p_df = raw_p2p_df.join(
        tmp_p2p_df,
        ['bbd_qyxx_id']
    ).select(
        raw_p2p_df.company_name,
        raw_p2p_df.bbd_qyxx_id,
        raw_p2p_df.risk_tags,
        raw_p2p_df.risk_index,
        raw_p2p_df.risk_composition,
        raw_p2p_df.province,
        raw_p2p_df.city,
        raw_p2p_df.county,
        raw_p2p_df.is_black,
        tmp_p2p_df.company_type
    )
    
    tid_pe_df = raw_pe_df.join(
        tmp_pe_df,
        ['bbd_qyxx_id']
    ).select(
        raw_pe_df.company_name,
        raw_pe_df.bbd_qyxx_id,
        raw_pe_df.risk_tags,
        raw_pe_df.risk_index,
        raw_pe_df.risk_composition,
        raw_pe_df.province,
        raw_pe_df.city,
        raw_pe_df.county,
        raw_pe_df.is_black,
        tmp_pe_df.company_type
    )
    
    tid_ex_df = raw_ex_df.join(
        tmp_ex_df,
        ['bbd_qyxx_id']
    ).select(
        raw_ex_df.company_name,
        raw_ex_df.bbd_qyxx_id,
        raw_ex_df.risk_tags,
        raw_ex_df.risk_index,
        raw_ex_df.risk_composition,
        raw_ex_df.province,
        raw_ex_df.city,
        raw_ex_df.county,
        raw_ex_df.is_black,
        tmp_ex_df.company_type
    )
    
    prd_df = tid_nf_df.union(
        tid_p2p_df
    ).union(
        tid_pe_df
    ).union(
        tid_ex_df
    ).dropDuplicates(
        ['bbd_qyxx_id']
    ).cache()
    
    #在prd_df的基础上，获取每个公司的风险等级
    risk_list = prd_df.select(
        'risk_index'
    ).rdd.map(
        lambda r: r.risk_index
    ).collect()
    risk_list.sort(reverse=True)
    high_risk_point_index = int(len(risk_list) * HIGH_RISK_RATIO)
    middle_risk_point_index = int(len(risk_list) * MIDDLE_RISK_RATIO)
    
    MIDDLE_RISK_POINT = risk_list[middle_risk_point_index]
    HIGH_RISK_POINT = risk_list[high_risk_point_index]
    
    def get_risk_rank(risk_index):
        if risk_index <= MIDDLE_RISK_POINT:
            return u'持续监控'
        elif MIDDLE_RISK_POINT < risk_index <= HIGH_RISK_POINT:
            return u'重点关注'
        elif HIGH_RISK_POINT < risk_index:
            return u'高危预警'
    get_risk_rank_udf = fun.udf(get_risk_rank, tp.StringType())    

    #对prd_df新增一列
    prd_df = prd_df.withColumn('risk_rank', get_risk_rank_udf('risk_index'))

    return prd_df
        
def spark_data_flow():
    '''
    输出一个版本的整合数据
    '''
    #输入
    tid_nf_df = raw_spark_data_flow()
    
    #获取目标公司的相关信息
    #这里计算流程较长，为了就是获取风险关联企业：
    relation_df_one = spark.read.parquet(
        ("{path}"
         "/common_company_info_merge"
         "/{version}").format(path=IN_PATH_THREE,
                              version=RELATION_VERSION))
    relation_df_two = spark.read.parquet(
        ("{path}"
         "/common_company_info_merge_v2"
         "/{version}").format(path=IN_PATH_THREE,
                              version=RELATION_VERSION))    
    
    
    high_risk_df = tid_nf_df.select(
        'bbd_qyxx_id',
        'risk_index',
        'risk_rank'
    ).where(
        tid_nf_df.risk_rank == u'高危预警'
    ).dropDuplicates(
        ['bbd_qyxx_id']
    )
    
    tmp_df = relation_df_one.select(
        'a', 'b', 'c', 'b_isperson', 
        'c_isperson', 'a_name', 'b_name', 'c_name'
    ).union(
        relation_df_two.select(
            'a', 'b', 'c', 'b_isperson', 
            'c_isperson', 'a_name', 'b_name', 'c_name'
        )
    ).dropDuplicates(
       [ 'a', 'b', 'c']
    ).select(
        'a', 'b', 'c', 'b_isperson', 
        'c_isperson', 'a_name', 'b_name', 'c_name'
    ).cache()
    
    tmp_2_df = tmp_df.select(
        'a', 'b', 'b_name', 'a_name'
    ).where(
        'b_isperson == 0'
    ).union(
        tmp_df.select(
            'a', 'c', 'c_name', 'a_name'
        ).where(
            'c_isperson == 0'
        )
    ).dropDuplicates(
        ['a', 'b']
    )
    
    tmp_3_df = tmp_2_df.join(
        high_risk_df,
        high_risk_df.bbd_qyxx_id == tmp_2_df.b,
        'left_outer'
    ).select(
        tmp_2_df.a,
        tmp_2_df.b,
        tmp_2_df.a_name,
        tmp_2_df.b_name,
        fun.when(
            high_risk_df.risk_rank.isNull(), 0
        ).otherwise(
            1
        ).alias(
            'is_high'
        )
    ).groupBy(
        tmp_2_df.a
    ).agg(
        {'is_high': 'sum'}
    ).withColumnRenamed(
        'sum(is_high)', 'relation_high_num'
    ).cache()
    
    common_static_feature_df_one = spark.read.json(
        ("{path}"
         "/common_static_feature_distribution"
         "/{version}").format(path=IN_PATH_FOUR,
                              version=RELATION_VERSION))
    common_static_feature_df_two = spark.read.json(
        ("{path}"
         "/common_static_feature_distribution_v2"
         "/{version}").format(path=IN_PATH_FOUR,
                              version=RELATION_VERSION))
    common_static_feature_df = common_static_feature_df_one.select(
        common_static_feature_df_one.bbd_qyxx_id,
        common_static_feature_df_one.company_name,
        (common_static_feature_df_one.feature_6.getItem('c_1') + 
        common_static_feature_df_one.feature_6.getItem('c_2') +
        common_static_feature_df_one.feature_6.getItem('c_3') +
        common_static_feature_df_one.feature_6.getItem('c_4') +
        common_static_feature_df_one.feature_6.getItem('c_5') +
        common_static_feature_df_one.feature_6.getItem('c_6') 
        ).alias('gsbg'),
        (common_static_feature_df_one.feature_10.getItem('0').getItem('ktgg') +
        common_static_feature_df_one.feature_10.getItem('0').getItem('rmfygg') +
        common_static_feature_df_one.feature_10.getItem('0').getItem('zgcpwsw')
        ).alias('ssxx'),
        (common_static_feature_df_one.feature_11.getItem('0').getItem('xzcf') +
        common_static_feature_df_one.feature_14.getItem('0').getItem('circxzcf')
        ).alias('xzcf'),
        (common_static_feature_df_one.feature_12.getItem('0').getItem('dishonesty') +
        common_static_feature_df_one.feature_12.getItem('0').getItem('zhixing')
        ).alias('sxxx'),
        (common_static_feature_df_one.feature_13.getItem('0').getItem('jyyc')
        ).alias('jyyc'),
        (common_static_feature_df_one.feature_18.getItem('d')
        ).alias('fzjg')
    ).union(
        common_static_feature_df_two.select(
            common_static_feature_df_two.bbd_qyxx_id,
            common_static_feature_df_two.company_name,
            (common_static_feature_df_two.feature_6.getItem('c_1') + 
            common_static_feature_df_two.feature_6.getItem('c_2') +
            common_static_feature_df_two.feature_6.getItem('c_3') +
            common_static_feature_df_two.feature_6.getItem('c_4') +
            common_static_feature_df_two.feature_6.getItem('c_5') +
            common_static_feature_df_one.feature_6.getItem('c_6') 
            ).alias('gsbg'),
            (common_static_feature_df_two.feature_10.getItem('0').getItem('ktgg') +
            common_static_feature_df_two.feature_10.getItem('0').getItem('rmfygg') +
            common_static_feature_df_two.feature_10.getItem('0').getItem('zgcpwsw')
            ).alias('ssxx'),
            (common_static_feature_df_two.feature_11.getItem('0').getItem('xzcf') +
            common_static_feature_df_two.feature_14.getItem('0').getItem('circxzcf')
            ).alias('xzcf'),
            (common_static_feature_df_two.feature_12.getItem('0').getItem('dishonesty') +
            common_static_feature_df_two.feature_12.getItem('0').getItem('zhixing')
            ).alias('sxxx'),
            (common_static_feature_df_two.feature_13.getItem('0').getItem('jyyc')
            ).alias('jyyc'),
            (common_static_feature_df_two.feature_18.getItem('d')
            ).alias('fzjg')
        )
    )
    
    raw_xgxx_info = common_static_feature_df.join(
        tmp_3_df,
        common_static_feature_df.bbd_qyxx_id == tmp_3_df.a,
        'left_outer'
    ).select(
        common_static_feature_df.bbd_qyxx_id,
        common_static_feature_df.company_name,
        'gsbg',
        'ssxx',
        'xzcf',
        'sxxx',
        'jyyc',
        'fzjg',
        tmp_3_df.relation_high_num.alias('fxglf')
    ).rdd.map(
        get_json_obj
    ).toDF(
    )
        
    #输出
    #已basic的名字为准
    basic_df = spark.read.parquet(
         ("{path}"
          "/basic/{version}").format(path=IN_PATH_ONE,
                                     version=RELATION_VERSION))
    get_data_version_udf = fun.udf(get_data_version, tp.StringType())
    prd_nf_df = tid_nf_df.join(
        raw_xgxx_info,
        raw_xgxx_info.company_name == tid_nf_df.company_name,
        'left_outer'
    ).join(
        basic_df,
        basic_df.bbd_qyxx_id == tid_nf_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        fun.when(
            basic_df.company_name.isNotNull(), basic_df.company_name
        ).otherwise(
            tid_nf_df.company_name
        ).alias('company_name'),
        tid_nf_df.bbd_qyxx_id,
        tid_nf_df.risk_tags,
        tid_nf_df.risk_index,
        tid_nf_df.risk_composition,
        tid_nf_df.province,
        tid_nf_df.city,
        tid_nf_df.county,
        tid_nf_df.is_black,
        tid_nf_df.company_type,
        raw_xgxx_info.xgxx_info,
        tid_nf_df.risk_rank,
        get_data_version_udf().alias('data_version'),
        fun.current_timestamp().alias('gmt_create'),
        fun.current_timestamp().alias('gmt_update')
    ).dropDuplicates(
        ['bbd_qyxx_id']    
    )
    return prd_nf_df

def run():
    prd_df = spark_data_flow()
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "all_company_info/{version}").format(path=OUT_PATH, 
                                           version=RELATION_VERSION))    
    prd_df.repartition(10).write.parquet(         
        ("{path}/"
         "all_company_info/{version}").format(path=OUT_PATH, 
                                           version=RELATION_VERSION))

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
        .appName("hgongjing2_three_prd_all_info") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 

if __name__ == '__main__':
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #输入参数
    TYPE_LIST = eval(conf.get('input_sample_data', 'TYPE_LIST'))
    HIGH_RISK_RATIO = conf.getfloat('all_company_info', 'HIGH_RISK_RATIO')
    MIDDLE_RISK_RATIO = conf.getfloat('all_company_info', 'MIDDLE_RISK_RATIO')
    RELATION_VERSION = sys.argv[1]

    IN_PATH_ONE = conf.get('common_company_info', 'OUT_PATH')
    IN_PATH_TWO = conf.get('feature_tags', 'OUT_PATH')
    IN_PATH_THREE = conf.get('common_company_info_merge', 'OUT_PATH')
    IN_PATH_FOUR = conf.get('common_company_feature', 'OUT_PATH')
    OUT_PATH = conf.get('all_company_info', 'OUT_PATH')
    
    spark = get_spark_session()
    
    run()