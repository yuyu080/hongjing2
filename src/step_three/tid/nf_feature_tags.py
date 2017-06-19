# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
nf_feature_tags.py {version}
'''

import sys
import os
import json

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Row

def get_tags(row):
    
    def get_behavior_risk(risk_name=u"经营行为风险"):
        tags = []
        if row['feature_6']['c_1'] >= 2:
            tags.append(u'法定代表人变更频繁')
        if row['feature_6']['c_2'] >= 2:
            tags.append(u'股东变更频繁')
        if row['feature_6']['c_3'] >= 2:
            tags.append(u'注册资本变更频繁')
        if row['feature_6']['c_4'] >= 2:
            tags.append(u'高管变更频繁')
        if row['feature_6']['c_5'] >= 2:
            tags.append(u'经营范围变更频繁')
        if row['feature_18']['d'] >= 2:
            tags.append(u'分支机构数量较多')
        if row['feature_22']['z'] >= 2:
            tags.append(u'企业利益一致行动法人较多')
        return {
            risk_name: tags
        }
    
    def get_company_strength_risk(risk_name=u'综合实力风险'):
        tags = []
        if row['feature_8']['t_1'] and row['feature_8']['t_1'] <= 700:
            tags.append(u'公司成立时间较短')
        if (row['feature_7']['e'] and 
                row['feature_7']['e_1'] / row['feature_7']['e'] >= 0.3):
            tags.append(u'大专及大专以下或不限专业招聘比例较高')
        if row['feature_17']['x'] >= 3:
            tags.append(u'对外投资公司数量较多')
        return {
            risk_name: tags
        }
    
    def get_credit_risk(risk_name=u'企业诚信风险'):
        tags = []
        if (row['feature_10']['0']['ktgg'] + 
                row['feature_10']['0']['rmfygg'] +
                row['feature_10']['0']['zgcpwsw']) >= 10:
            tags.append(u'企业诉讼文书数量较多')
        if row['feature_10']['0']['lending']:
            tags.append(u'企业存在“民间借贷”类诉讼文书')
        if row['feature_11']['0']['xzcf']:
            tags.append(u'企业受到行政处罚')
        if row['feature_12']['0']['zhixing']:
            tags.append(u'企业存在被执行人信息')
        if row['feature_12']['0']['dishonesty']:
            tags.append(u'企业存在失信被执行人信息')
        if row['feature_13']['0']['jyyc']:
            tags.append(u'企业存在经营异常信息')
        return {
            risk_name: tags
        }
    
    def get_all_nums(feature_name, distances=['0', '1', '2', '3']):
        return sum(
            map(
                sum, 
                [v.asDict().values() 
                     for k ,v in row[feature_name].asDict().iteritems() 
                     if k in distances]))
    def get_some_nums(feature_name, value_name, distances=['0', '1', '2', '3']):
        return sum([
                v.asDict().get(value_name, 0)
                for k, v in row[feature_name].asDict().iteritems()
                if k in distances])         
    def get_static_relationship_risk(risk_name=u'静态关联方风险'):
        tags = []
        if get_all_nums('feature_10', distances=['0', '1', '2']) >= 15:
            tags.append(u'二度内关联方诉讼文书数量较多')
        if get_some_nums('feature_10', 'lending') >= 3:
            tags.append(u'三度内关联方存在较多“民间借贷”类诉讼文书')
        if get_some_nums('feature_11', 'xzcf', distances=['0', '1', '2']):
            tags.append(u'二度内关联方存在受行政处罚企业')
        if get_some_nums('feature_12', 'zhixing'):
            tags.append(u'三度内关联方存在被执行人企业数量较多')
        if get_some_nums('feature_12', 'dishonesty'):
            tags.append(u'三度内关联方存在失信被执行人企业')
        if get_some_nums('feature_13', 'estatus', distances=['0', '1', '2']):
            tags.append(u'二度内关联方存在吊销企业')
        if get_some_nums('feature_13', 'jyyc', distances=['0', '1', '2']):
            tags.append(u'二度内关联方存在经营异常企业')
        if row['feature_8']['t_2'] and row['feature_8']['t_2'] <= 365:
            tags.append(u'二度内法人关联方平均成立时间较短')
        if row['feature_15']['x_1'] >= 10:
            tags.append(u'二度内单个关联自然人中最大投资企业数量较多')
        if row['feature_15']['x_2'] >= 25:
            tags.append(u'二度内单个关联法人最大投资企业数量较多')
        if ((row['feature_16']['y_1'] + row['feature_16']['y_2']) /
               (row['feature_16']['y_1'] + row['feature_16']['y_2'] +
                row['feature_16']['x_1'] + row['feature_16']['x_2'] + 0.01)) >= 0.9:
            tags.append(u'二度内关联方中自然人比例较高')
        if row['feature_21']['n'] >= 2:
            tags.append(u'二度内关联企业中地址相同公司较多')
        if (row['feature_23']['b_1'] + row['feature_23']['b_2']) :
            tags.append(u'二度内关联方中存在黑名单企业')
        return {
            risk_name: tags
        }
            
    def get_dynamic_relationship_risk(risk_name=u'动态关联方风险'):
        tags = []
        if row['feature_26']['a_1'] > 2 or row['feature_26']['a_1'] < -0.7:
            tags.append(u'近三个月内自然人关联节点变动较大')
        if row['feature_26']['a_4'] > 0.1 or row['feature_26']['a_4'] < -0.1:
            tags.append(u'近三个月内对外投资公司数量变动较大')
        if row['feature_26']['a_5'] > 0.5:
            tags.append(u'近三个月内公司分支机构数量变动较大')
        if row['feature_26']['a_6'] > 0.5:
            tags.append(u'近三个月内利益一致行动法人的数量变动较大')
        if row['feature_26']['c_1'] > 1 or row['feature_26']['c_1'] < -0.5:
            tags.append(u'近三个月内低学历招聘人数变动较大')
        if row['feature_26']['d_2'] > 2 or row['feature_26']['d_2'] < -0.7:
            tags.append(u'近三个月内3度及3度以下核心自然人（前3）控制节点总数变动较大')
        return {
            risk_name: tags
        }
    
    result = {}
    result.update(get_behavior_risk())
    result.update(get_company_strength_risk())
    result.update(get_credit_risk())
    result.update(get_static_relationship_risk())    
    if row['feature_26']:
        result.update(get_dynamic_relationship_risk())
    else:
        result.update({u'动态关联方风险': []})
        
    final_out_dict = {
        '--': result
    }
    
    return json.dumps(final_out_dict, ensure_ascii=False)

def spark_data_flow():
    raw_nf_feature_df = spark.read.parquet(
        ("/user/antifraud/hongjing2/dataflow/step_two/raw"
         "/nf_feature_merge/{version}").format(version=RELATION_VERSION))
    nf_info_df = spark.read.parquet(
        ("/user/antifraud/hongjing2/dataflow/step_three/raw"
         "/nf_info_merge/{version}").format(version=RELATION_VERSION))
    
    tid_nf_feature_df = raw_nf_feature_df.rdd.map(
        lambda r: Row(
            company_name=r['company_name'],
            bbd_qyxx_id=r['bbd_qyxx_id'],
            risk_tags=get_tags(r)
        )
    ).toDF().cache()
    
    prd_nf_feature_df = tid_nf_feature_df.join(
        nf_info_df,
        nf_info_df.company_name == tid_nf_feature_df.company_name
    ).select(
        tid_nf_feature_df.company_name,
        tid_nf_feature_df.bbd_qyxx_id,
        tid_nf_feature_df.risk_tags,
        nf_info_df.risk_index,
        nf_info_df.risk_composition,
        nf_info_df.province,
        nf_info_df.city,
        nf_info_df.county,
        nf_info_df.is_black
    ).dropDuplicates(
        ['company_name']
    )
    return prd_nf_feature_df


def run():
    tid_df = spark_data_flow()
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "nf_feature_tags/{version}").format(path=OUT_PATH, 
                                             version=RELATION_VERSION))    
    tid_df.repartition(10).write.parquet(         
        ("{path}/"
         "nf_feature_tags/{version}").format(path=OUT_PATH, 
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
        .appName("hgongjing2_three_raw_nf_info") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark

if __name__ == '__main__':
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_three/tid/"
    
    spark = get_spark_session()
    
    run()