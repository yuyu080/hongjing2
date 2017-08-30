# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
all_info_merge.py {version}
'''

import sys
import os

from pyspark.sql import functions as fun
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def get_read_path(file_name):
    return "{path}/{file_name}/{version}".format(version=RELATION_VERSION,
                                                 file_name=file_name,
                                                 path=IN_PATH_ONE)

def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "60g")
    conf.set("spark.executor.instances", 20)
    conf.set("spark.executor.cores", 15)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 4000)
    conf.set("spark.sql.shuffle.partitions", 4000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)
    
    spark = SparkSession \
        .builder \
        .appName("hgongjing2_all_info_merge") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark  


def spark_data_flow(tidversion):
    #读取相关信息数据
    basic_df = spark.read.parquet(
        get_read_path('basic')
    )
    #失信被执行人
    dishonesty_df = spark.read.parquet(
        get_read_path('dishonesty')
    )
    #执行人数
    zhixing_df = spark.read.parquet(
        get_read_path('zhixing')
    )
    #行政处罚
    xzcf_df = spark.read.parquet(
        get_read_path('xzcf')
    )
    #经营异常
    jyyc_df = spark.read.parquet(
        get_read_path('jyyc')
    )
    #开庭公告
    ktgg_df = spark.read.parquet(
        get_read_path('ktgg')
    )
    #法院公告
    rmfygg_df = spark.read.parquet(
        get_read_path('rmfygg')
    )
    #裁判文书
    zgcpwsw_df = spark.read.parquet(
        get_read_path('zgcpwsw')
    )
    #变更信息
    bgxx_df = spark.read.parquet(
        get_read_path('bgxx')
    )
    #黑企业名单
    black_df = spark.read.parquet(
        get_read_path('black_company')
    ).withColumnRenamed(
        'company_type', 'is_black_company'
    )
    #step three的输出结果
    all_company_info = spark.read.parquet(   
        "{path}/"
        "all_company_info/"
        "{version}".format(path=IN_PATH_TWO, 
                           version=RELATION_VERSION)
    )
    tid_all_company_info = all_company_info.where(
        all_company_info.risk_rank == u'高危预警'
    ).select(
        'bbd_qyxx_id',
        fun.when(
            all_company_info.risk_rank == u'高危预警', True
        ).alias('is_high_company')
    )
    #类金融名单
    ljr_sample_df = spark.read.parquet(
        get_read_path('ljr_sample')
    )
    tid_ljr_sample_df = ljr_sample_df.select(
        'bbd_qyxx_id',
        fun.when(
            ljr_sample_df.company_type != u'', True
        ).alias('is_leijinrong')
    )
    #民间借贷
    lawsuit_df = spark.read.parquet(
        get_read_path('lawsuit')    
    )
    #公司字号
    namefrag_df = spark.read.parquet(
        '{path}/all_namefrag/{version}'.format(path=IN_PATH_THREE,
                                               version=RELATION_VERSION)
    )
    #关联方
    relation_df = spark.read.parquet(
        '{path}/all_relation/{version}'.format(path=IN_PATH_THREE,
                                               version=RELATION_VERSION)
    )

    #合并中间数据结果
    tid_info_merge = basic_df.select(
        basic_df.bbd_qyxx_id,
        basic_df.company_name,
        basic_df.enterprise_status,
        basic_df.address,
        basic_df.company_province
    ).join(
        black_df,
        black_df.company_name == basic_df.company_name,
        'left_outer'
    ).join(
        namefrag_df,
        namefrag_df.company_name == basic_df.company_name,
        'left_outer'
    ).join(
        lawsuit_df,
        lawsuit_df.bbd_qyxx_id == basic_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        tid_ljr_sample_df,
        tid_ljr_sample_df.bbd_qyxx_id == basic_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        tid_all_company_info,
        tid_all_company_info.bbd_qyxx_id == basic_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        bgxx_df,
        bgxx_df.bbd_qyxx_id == basic_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        zgcpwsw_df,
        zgcpwsw_df.bbd_qyxx_id == basic_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        rmfygg_df,
        rmfygg_df.bbd_qyxx_id == basic_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        ktgg_df,
        ktgg_df.bbd_qyxx_id == basic_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        jyyc_df,
        jyyc_df.bbd_qyxx_id == basic_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        xzcf_df,
        xzcf_df.bbd_qyxx_id == basic_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        zhixing_df,
        zhixing_df.bbd_qyxx_id == basic_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        dishonesty_df,
        dishonesty_df.bbd_qyxx_id == basic_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        basic_df.bbd_qyxx_id,
        basic_df.company_name,
        basic_df.enterprise_status,
        basic_df.address,
        'namefrag',
        'dishonesty_num'
        ,'zhixing_num'
        ,'xzcf_num'
        ,'jyyc_num'
        ,'ktgg_num'
        ,'rmfygg_num'
        ,'zgcpwsw_num'
        ,'bgxx_dict'
        ,'is_black_company'
        ,'is_high_company'
        ,'is_leijinrong'
        ,'lawsuit_num'
        ,'company_province'
    ).dropDuplicates(
        ['bbd_qyxx_id']
    ).cache(
    )

    #构建属性图
    tid_relation_2_df = relation_df.join(
        tid_info_merge,
        tid_info_merge.bbd_qyxx_id == relation_df.a,
        'right_outer'
    ).select(
        tid_info_merge.bbd_qyxx_id.alias('a'),
        'b',
        'c',
        'b_degree',
        'c_degree',
        'bc_relation',
        'b_isperson',
        'c_isperson',
        tid_info_merge.company_name.alias('a_name'),
        'b_name',
        'c_name',
        tid_info_merge.namefrag.alias('a_namefrag'),
        tid_info_merge.dishonesty_num.alias('a_dishonesty'),
        tid_info_merge.zhixing_num.alias('a_zhixing'),
        tid_info_merge.xzcf_num.alias('a_xzcf'),
        tid_info_merge.jyyc_num.alias('a_jyyc'),
        tid_info_merge.ktgg_num.alias('a_ktgg'),
        tid_info_merge.rmfygg_num.alias('a_rmfygg'),
        tid_info_merge.zgcpwsw_num.alias('a_zgcpwsw'),
        tid_info_merge.bgxx_dict.alias('a_bgxx'),
        tid_info_merge.lawsuit_num.alias('a_lending')
    )
    
    tid_relation_3_df = tid_relation_2_df.join(
        tid_info_merge,
        tid_info_merge.bbd_qyxx_id == tid_relation_2_df.b,
        'left_outer'
    ).select(
        'a',
        'b',
        'c',
        'b_degree',
        'c_degree',
        'bc_relation',
        'b_isperson',
        'c_isperson',
        'a_name',
        'b_name',
        'c_name',
        'a_namefrag',
        'a_dishonesty', 'a_zhixing', 'a_xzcf',
        'a_jyyc', 'a_ktgg', 'a_rmfygg',
        'a_zgcpwsw', 'a_bgxx', 'a_lending',
        tid_info_merge.dishonesty_num.alias('b_dishonesty'),
        tid_info_merge.zhixing_num.alias('b_zhixing'),
        tid_info_merge.xzcf_num.alias('b_xzcf'),
        tid_info_merge.jyyc_num.alias('b_jyyc'),
        tid_info_merge.ktgg_num.alias('b_ktgg'),
        tid_info_merge.rmfygg_num.alias('b_rmfygg'),
        tid_info_merge.zgcpwsw_num.alias('b_zgcpwsw'),
        tid_info_merge.bgxx_dict.alias('b_bgxx'),
        tid_info_merge.address.alias('b_address'),
        tid_info_merge.is_black_company.alias('b_is_black_company'),
        tid_info_merge.is_high_company.alias('b_is_high_company'),
        tid_info_merge.is_leijinrong.alias('b_is_leijinrong'),
        tid_info_merge.enterprise_status.alias('b_estatus'),
        tid_info_merge.lawsuit_num.alias('b_lending'),
        tid_info_merge.company_province.alias('b_company_province')
    )
    
    tid_relation_4_df = tid_relation_3_df.join(
        tid_info_merge,
        tid_info_merge.bbd_qyxx_id == tid_relation_2_df.c,
        'left_outer'    
    ).select(
        'a', 'b', 'c',
        'b_degree', 'c_degree', 'bc_relation',
        'b_isperson', 'c_isperson',
        'a_name', 'b_name', 'c_name',
        'a_namefrag',   
        'a_dishonesty', 'a_zhixing', 'a_xzcf',
        'a_jyyc', 'a_ktgg', 'a_rmfygg',
        'a_zgcpwsw', 'a_bgxx', 'a_lending',  
        'b_dishonesty', 'b_zhixing', 'b_xzcf',
        'b_jyyc', 'b_ktgg', 'b_rmfygg',
        'b_zgcpwsw', 'b_bgxx', 'b_address',
        'b_is_black_company', 'b_is_high_company',
        'b_is_leijinrong', 'b_estatus', 'b_lending',
        'b_company_province',
        tid_info_merge.dishonesty_num.alias('c_dishonesty'),
        tid_info_merge.zhixing_num.alias('c_zhixing'),
        tid_info_merge.xzcf_num.alias('c_xzcf'),
        tid_info_merge.jyyc_num.alias('c_jyyc'),
        tid_info_merge.ktgg_num.alias('c_ktgg'),
        tid_info_merge.rmfygg_num.alias('c_rmfygg'),
        tid_info_merge.zgcpwsw_num.alias('c_zgcpwsw'),
        tid_info_merge.bgxx_dict.alias('c_bgxx'),
        tid_info_merge.address.alias('c_address'),
        tid_info_merge.is_black_company.alias('c_is_black_company'),
        tid_info_merge.is_high_company.alias('c_is_high_company'),
        tid_info_merge.is_leijinrong.alias('c_is_leijinrong'),
        tid_info_merge.enterprise_status.alias('c_estatus'),
        tid_info_merge.lawsuit_num.alias('c_lending'),
        tid_info_merge.company_province.alias('c_company_province')
    )

    return tid_relation_4_df

def run():
    tid_df = spark_data_flow(RELATION_VERSION)

    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "all_info_merge"
         "/{version}").format(path=OUT_PATH, 
                              version=RELATION_VERSION))    
    tid_df.coalesce(
        300
    ).write.parquet(
        "{path}/"
        "all_info_merge/"
        "{version}".format(path=OUT_PATH, 
                           version=RELATION_VERSION)
    )


if __name__ == '__main__':  
    import configparser
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    #输入参数
    IN_PATH_ONE = "/user/antifraud/hongjing2/dataflow/step_one/raw/"
    IN_PATH_TWO = "/user/antifraud/hongjing2/dataflow/step_three/prd/"
    IN_PATH_THREE = "/user/antifraud/hongjing2/dataflow/step_four/tid/raw/"
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_four/tid/tid/"

    
    spark = get_spark_session()

    run()