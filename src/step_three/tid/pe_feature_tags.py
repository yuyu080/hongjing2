# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
pe_feature_tags.py {version}
'''
import sys
import os
import math
import json

import configparser
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Row


def get_tags(row):
    
    def get_pe_compliance_risk(risk_name=u'监管合规性风险'):
        tags = []
        if row['pe_feature_2'] == 100:
            tags.append(u'要求提交法律意见书')
        if row['pe_feature_3'] == 100:
            tags.append(u'全部高管无基金从业资格')
        if row['pe_feature_3'] == 50:
            tags.append(u'部分高管无基金从业资格')
        return {
            risk_name: tags
        }
    
    def get_pe_strength_risk(risk_name=u'私募综合实力风险'):
        tags = []
        if row['pe_feature_4'] == 100:
            tags.append(u'管理机构为非会员单位')
        if row['pe_feature_5'] == 70:
            tags.append(u'创业投资基金、其他投资基金')
        if row['pe_feature_6'] == 80:
            tags.append(u'人员数量小于10人')
        return {
            risk_name: tags
        }
    
    def get_pe_management_risk(risk_name=u'私募基金管理风险'):
        tags = []
        if row['pe_feature_7'] == 100:
            tags.append(u'基金异常清盘个数大于（等于）1')
        if row['pe_feature_8'] == 100:
            tags.append(u'暂行办法实施后基金没有发行过基金产品')
        if  round(
                math.log(
                    1 / (1 - row['pe_feature_9']/100 + 0.001), 
                    math.e)) <= 3:
            tags.append(u'发行基金个数小于（等于）3')
        if int(row['pe_feature_10']['risk']) == 100:
            tags.append(row['pe_feature_10']['info'])
        return {
            risk_name: tags
        }
    
    def get_organization_management_risk(risk_name=u'管理机构风险'):
        tags = []
        if row['feature_6']['c_1'] >= 3:
            tags.append(u'法定代表人变更频繁')
        if row['feature_6']['c_2'] >= 3:
            tags.append(u'股东变更频繁')
        if row['feature_6']['c_3'] >= 3:
            tags.append(u'注册资本变更频繁')
        if row['feature_6']['c_4'] >= 3:
            tags.append(u'高管变更频繁')
        if row['feature_6']['c_5'] >= 3:
            tags.append(u'经营范围变更频繁')
        if row['feature_8']['t_1'] and row['feature_8']['t_1'] <= 365:
            tags.append(u'公司成立时间较短')
        if row['feature_18']['d'] >= 3:
            tags.append(u'分支机构数量较多')
        if (row['feature_7']['e'] and 
                row['feature_7']['e_1'] / row['feature_7']['e'] >= 0.5):
            tags.append(u'大专及大专以下或不限专业招聘比例较高')
        if row['feature_17']['x'] >= 3:
            tags.append(u'对外投资公司数量较多')        
        if (row['feature_10']['0']['ktgg'] >= 2 or 
                row['feature_10']['0']['rmfygg'] >= 2 or
                row['feature_10']['0']['zgcpwsw'] >= 2):
            tags.append(u'企业诉讼文书数量较多')
        if row['feature_11']['0']['xzcf']:
            tags.append(u'企业受到行政处罚')
        if row['feature_12']['0']['zhixing']:
            tags.append(u'企业存在被执行人信息')
        if row['feature_12']['0']['dishonesty']:
            tags.append(u'企业存在失信被执行人信息')
        if row['feature_13']['0']['jyyc']:
            tags.append(u'企业存在经营异常信息')
        if row['feature_14']['0']['circxzcf']:
            tags.append(u'企业受到银监会行政处罚')
        return {
            risk_name: tags
        }
 
    def get_all_nums(feature_name):
        return sum(
            map(
                sum, 
                [v.asDict().values() 
                     for k ,v in row[feature_name].asDict().iteritems() 
                     if k in ['0', '1', '2', '3']]))
    def get_some_nums(feature_name, value_name):
        return sum([
                v.asDict().get(value_name, 0) 
                for k, v in row[feature_name].asDict().iteritems() 
                if k in ['0', '1', '2', '3']])
    def get_static_organization_relationship_risk(risk_name=u'机构关联方风险'):
        tags = []
        if get_all_nums('feature_10') >= 8 :
            tags.append(u'关联方诉讼文书数量较多')
        if get_some_nums('feature_11', 'xzcf'):
            tags.append(u'关联方存在受行政处罚企业')
        if get_some_nums('feature_12', 'zhixing'):
            tags.append(u'关联方存在被执行人企业数量较多')
        if get_some_nums('feature_12', 'dishonesty'):
            tags.append(u'关联方存在失信被执行人企业')
        if get_some_nums('feature_13', 'estatus'):
            tags.append(u'关联方存在吊销企业')
        if get_some_nums('feature_13', 'jyyc'):
            tags.append(u'关联方存在经营异常企业')
        if get_some_nums('feature_14', 'circxzcf'):
            tags.append(u'关联方存在银监会行政处罚企业')            
        if row['feature_8']['t_2'] and row['feature_8']['t_2'] <= 365:
            tags.append(u'法人关联方平均成立时间较短')
        if row['feature_15']['x_1'] >= 10:
            tags.append(u'单个关联自然人中最大投资企业数量较多')
        if row['feature_15']['x_2'] >= 30:
            tags.append(u'单个关联法人最大投资企业数量较多')
        if (row['feature_16']['y_1'] + row['feature_16']['y_2'] + 
               row['feature_16']['y_3']) >= 30:
            tags.append(u'关联方中自然人数量较多')
        if sum([
                v 
                for k ,v in row['feature_20'].asDict().iteritems() 
                if k != 'g']) >= 5:
            tags.append(u'风险行业关联企业数量较多')
        if row['feature_21']['n'] >= 3:
            tags.append(u'关联企业中地址相同公司较多')
        if (row['feature_23']['b_1'] + row['feature_23']['b_2'] + 
                row['feature_23']['b_3']) :
            tags.append(u'关联方中存在黑名单企业')
        return tags

    def get_dynamic_organization_relationship_risk(risk_name=u'机构关联方风险'):
        tags = []
        if (row['feature_24']['x_1'] + row['feature_24']['x_2'] +
                row['feature_24']['x_3']) >= 5:
            tags.append(u'关联方新成立子公司数量较多')
        if abs(row['feature_26']['x']-row['feature_26']['y']) >= 6:
            tags.append(u'核心自然人控制节点数量变化较大')
        if abs(row['feature_27']['x']-row['feature_27']['y']) >= 0.1:
            tags.append(u'利益一致行动法人占比变化较大')
        if abs(row['feature_28']['x']-row['feature_28']['y']) >= 5:
            tags.append(u'关联方聚集区域关联节点数量变化较大')
        return tags
        
    result = {}
    result.update(get_pe_compliance_risk())
    result.update(get_pe_strength_risk())
    result.update(get_pe_management_risk())
    if row['feature_1']:
        result.update(get_organization_management_risk())
    else:
        result.update({u'管理机构风险': []})
    if (row['feature_25'] and row['feature_26'] and 
            row['feature_27'] and row['feature_28']):
        static_tags = get_static_organization_relationship_risk()
        dynamic_tags = get_dynamic_organization_relationship_risk()
        result[u'机构关联方风险'] = static_tags + dynamic_tags
    elif row['feature_1']:
        result[u'机构关联方风险'] = get_static_organization_relationship_risk()
    else:
        result[u'机构关联方风险'] = []

    final_out_dict = {
        '--': result
    }
        
    return json.dumps(final_out_dict, ensure_ascii=False)

def spark_data_flow():
    raw_pe_feature_df = spark.read.parquet(
        ("{path}/"
         "pe_feature_merge/{version}").format(path=IN_PATH_ONE,
                                              version=RELATION_VERSION))
    pe_info_df = spark.read.parquet(
        ("{path}/"
         "pe_info_merge/{version}").format(path=IN_PATH_TWO,
                                           version=RELATION_VERSION))
    
    tid_pe_feature_df = raw_pe_feature_df.rdd.map(
        lambda r: Row(
            company_name=r['company_name'],
            bbd_qyxx_id=r['bbd_qyxx_id'],
            risk_tags=get_tags(r)
        )
    ).toDF()
    prd_pe_feature_df = tid_pe_feature_df.join(
        pe_info_df,
        pe_info_df.bbd_qyxx_id == tid_pe_feature_df.bbd_qyxx_id
    ).select(
        tid_pe_feature_df.company_name,
        tid_pe_feature_df.bbd_qyxx_id,
        tid_pe_feature_df.risk_tags,
        pe_info_df.risk_index,
        pe_info_df.risk_composition,
        pe_info_df.province,
        pe_info_df.city,
        pe_info_df.county,
        pe_info_df.is_black
    )
    return prd_pe_feature_df

def run():
    tid_df = spark_data_flow()
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "pe_feature_tags/{version}").format(path=OUT_PATH, 
                                             version=RELATION_VERSION))    
    tid_df.repartition(10).write.parquet(         
        ("{path}/"
         "pe_feature_tags/{version}").format(path=OUT_PATH, 
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
        .appName("hgongjing2_three_raw_pe_info") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark

if __name__ == '__main__':
    conf = configparser.ConfigParser()
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")

    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    IN_PATH_ONE = conf.get('feature_merge', 'OUT_PATH')
    IN_PATH_TWO = conf.get('info_merge', 'OUT_PATH')
    OUT_PATH = conf.get('feature_tags', 'OUT_PATH')
    
    spark = get_spark_session()
    
    run()