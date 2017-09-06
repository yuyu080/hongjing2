# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.hongjing \
p2p_feature_tags.py {version}
'''
import sys
import os
import json

import configparser
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Row


def get_tags(row):
    
    def get_p2p_trading_exchange_risk(risk_name=u"网络借贷行业风险"):
        tags = []
        if row['p2p_feature_16'] >= 0.1:
            tags.append(u'承诺收益高于10%')
        if row['p2p_feature_7'] >= 30:
            tags.append(u'平均标满时间较长')
        if 0 < row['p2p_feature_8'] <= 50:
            tags.append(u'总投资人数较少')
        if 0 < row['p2p_feature_17'] <= 200:
            tags.append(u'总借款人数较少')
        if row['p2p_feature_18'] == 0:
            tags.append(u'不可债权转让')
        if row['p2p_feature_19'] == 0:
            tags.append(u'未经银行托管')
        if row['p2p_feature_20'] == 0:
            tags.append(u'无风险准备金存管')
        return {
            risk_name: tags
        }
        
    def get_p2p_company_risk(risk_name=u"综合实力风险"):
        tags = []
        if (row['feature_8']['t_1'] and 
                row['feature_8']['t_1'] < 700):
            tags.append(u'公司成立时间较短')
        if (row['feature_7']['e'] and 
                row['feature_7']['e_1'] / row['feature_7']['e'] >= 0.3):
            tags.append(u'低学历人员招聘比例较高')
        if row['feature_17']['x'] >= 2:
            tags.append(u'对外投资公司数量较多')
        return {
            risk_name: tags
        }
    
    def get_p2p_trading_risk(risk_name=u"企业行为风险"):
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
        if row['feature_22']['d'] >= 2:
            tags.append(u'企业利益一致行动法人较多')
        if row['feature_18']['d'] >= 2:
            tags.append(u'分支机构数量较多')
        if row['feature_10']['0']['lending']:
            tags.append(u'企业存在“民间借贷”类诉讼文书')
        if row['feature_12']['0']['dishonesty']:
            tags.append(u'企业存在失信被执行人信息')            
        if row['feature_12']['0']['zhixing']:
            tags.append(u'企业存在被执行人信息')
        if row['feature_11']['0']['xzcf']:
            tags.append(u'企业受到行政处罚')
        if row['feature_13']['0']['jyyc']:
            tags.append(u'企业存在经营异常信息')           
        if (row['feature_10']['0']['ktgg'] or 
                row['feature_10']['0']['rmfygg'] or
                row['feature_10']['0']['zgcpwsw']):
            tags.append(u'企业存在诉讼文书')
        return {
            risk_name: tags
        }   

    def get_all_nums(feature_name, value_names, distances=['1', '2', '3']):
        return sum([v.asDict().get(each_value_name, 0)
                     for k ,v in row[feature_name].asDict().iteritems()
                     for each_value_name in value_names
                     if k in distances])
    def get_some_nums(feature_name, value_name, distances=['1', '2', '3']):
        return sum([
                v.asDict().get(value_name, 0)
                for k, v in row[feature_name].asDict().iteritems()
                if k in distances])      
    def get_p2p_static_relationship_risk(risk_name=u'静态关联方风险'):
        tags = []
        if get_some_nums('feature_10', 'lending_1'):
            tags.append(u'关联方存在涉“民间借贷”类诉讼企业')
        if get_some_nums('feature_12', 'dishonesty_1'):
            tags.append(u'关联方存在失信被执行人企业')
        if get_some_nums('feature_12', 'zhixing_1'):
            tags.append(u'关联方存在被执行人企业')
        if get_some_nums('feature_13', 'estatus'):
            tags.append(u'关联方存在吊销企业')
        if get_some_nums('feature_11', 'xzcf_1', distances=['1', '2']):
            tags.append(u'关联方存在受行政处罚企业')
        if get_some_nums('feature_13', 'jyyc_1', distances=['1', '2']):
            tags.append(u'关联方存在经营异常企业')
        if get_all_nums('feature_10', 
                        ['ktgg_1', 'rmfygg_1', 'zgcpwsw_1'],
                        distances=['1', '2']) >= 10:
            tags.append(u'关联方涉诉企业较多')
        if row['feature_8']['t_2'] and row['feature_8']['t_2'] <= 800:
            tags.append(u'法人关联方平均成立时间较短')
        if row['feature_15']['x_1'] >= 20:
            tags.append(u'单个关联自然人最大投资企业数量较多')
        if row['feature_15']['x_2'] >= 40:
            tags.append(u'单个关联法人最大投资企业数量较多')
        if ((row['feature_16']['y_1'] + row['feature_16']['y_2']) /
               (row['feature_16']['x_1'] + row['feature_16']['x_2'] + 
                0.01)) >= 1.:
            tags.append(u'关联方中自然人比例较高')
        if row['feature_21']['n']:
            tags.append(u'关联企业中存在地址相同企业')
        if (row['feature_23']['b_1'] + row['feature_23']['b_2'] + 
            row['feature_23']['b_3']):
            tags.append(u'关联方中存在黑名单企业')
        return {
            risk_name: tags
        }

    def get_p2p_dynamic_relationship_risk(risk_name=u'动态关联方风险'):        
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
    result.update(get_p2p_trading_exchange_risk())

    if row['feature_1']:
        result.update(get_p2p_company_risk())
        result.update(get_p2p_trading_risk())
        result.update(get_p2p_static_relationship_risk())
    else:
        result.update({u"综合实力风险": []})
        result.update({u'企业行为风险': []})
        result.update({u'静态关联方风险': []})

    if row['feature_26']:
        result.update(get_p2p_dynamic_relationship_risk())
    else:
        result.update({u'动态关联方风险': []}) 
        
    final_out_dict = {
        row['platform_name']: result
    }
        
    return final_out_dict

def merge_each_platform(iter_obj):
    '''
    合并P2P行业多个平台的tags
    '''
    result = {}
    for each_dict in iter_obj:
        result.update(each_dict)
    return json.dumps(result, ensure_ascii=False)

def spark_data_flow():
    raw_p2p_feature_df = spark.read.parquet(
        ("{path}/"
         "p2p_feature_merge/{version}").format(path=IN_PATH_ONE,
                                               version=RELATION_VERSION))
    p2p_info_df = spark.read.parquet(
        ("{path}/"
         "p2p_info_merge/{version}").format(path=IN_PATH_TWO,
                                            version=RELATION_VERSION))
    
    tid_p2p_feature_df = raw_p2p_feature_df.rdd.map(
        lambda r: (
            (r['company_name'], r['bbd_qyxx_id']),
            get_tags(r)
        )
    ).groupByKey(
    ).map(
        lambda (k, iter_obj): Row(
            company_name=k[0],
            bbd_qyxx_id=k[1],
            risk_tags=merge_each_platform(iter_obj)
        )
    ).toDF()
        
    prd_p2p_feature_df = tid_p2p_feature_df.join(
        p2p_info_df,
        p2p_info_df.bbd_qyxx_id == tid_p2p_feature_df.bbd_qyxx_id
    ).select(
        tid_p2p_feature_df.company_name,
        tid_p2p_feature_df.bbd_qyxx_id,
        tid_p2p_feature_df.risk_tags,
        p2p_info_df.risk_index,
        p2p_info_df.risk_composition,
        p2p_info_df.province,
        p2p_info_df.city,
        p2p_info_df.county,
        p2p_info_df.is_black
    ).dropDuplicates(
        ['bbd_qyxx_id']
    )
    return prd_p2p_feature_df

def run():
    tid_df = spark_data_flow()
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "p2p_feature_tags/{version}").format(path=OUT_PATH, 
                                             version=RELATION_VERSION))    
    tid_df.repartition(10).write.parquet(         
        ("{path}/"
         "p2p_feature_tags/{version}").format(path=OUT_PATH, 
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
        .appName("hgongjing2_three_raw_p2p_info") \
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