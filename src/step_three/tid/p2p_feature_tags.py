# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
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
    
    def get_platform_compliance_risk(risk_name=u"平台合规性风险"):
        tags = []
        if row['p2p_feature_19'] == 100:
            tags.append(u'可自动投标')
        if row['p2p_feature_20'] == 100:
            tags.append(u'可债权转让')
        if row['p2p_feature_21'] == 100:
            tags.append(u'未经银行托管')
        return {
            risk_name: tags
        }
    
    def get_trading_feature_risk(risk_name=u"交易指标风险"):
        tags = []
        if row['p2p_feature_12'] >= 12 * 365:
            tags.append(u'存在12月以上标')
        if row['p2p_feature_12'] >= 24 * 365:
            tags.append(u'存在24月以上标')
        if row['p2p_feature_8'] >= 15:
            tags.append(u'承诺收益高于15%')
        if row['p2p_feature_8'] >= 20:
            tags.append(u'承诺收益高于20%')
        if row['p2p_feature_3'] >= 2000:
            tags.append(u'投资人数大于2000人')
        if row['p2p_feature_14'] >= 2000:
            tags.append(u'借款人数大于2000人')
        if row['p2p_feature_16'] >= 50000:
            tags.append(u'贷款余额大于5亿')
        if row['p2p_feature_6'] >= 1000:
            tags.append(u'平均月成交金额大于1000万')
        if row['p2p_feature_17'] >= 500:
            tags.append(u'人均借款金额大于500万')
        if row['p2p_feature_18'] >= 0.15:
            tags.append(u'前十大借款人金额大于15%')
        if row['p2p_feature_15'] >= 0.15:
            tags.append(u'前十大投资人金额大于15%')
        return {
            risk_name: tags
        }
    
    def get_company_strength_risk(risk_name=u"综合实力风险"):
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
        return {
            risk_name: tags
        }
            
    def get_reputation_risk(risk_name=u'声誉风险'):
        tags = []
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
    def get_relationship_risk(risk_name=u'平台关联方风险'):
        tags = []
        #静态
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
        #动态
        if (row['feature_25'] and row['feature_26'] and 
                row['feature_27'] and row['feature_28']):
            if (row['feature_24']['x_1'] + row['feature_24']['x_2'] +
                    row['feature_24']['x_3']) >= 5:
                tags.append(u'关联方新成立子公司数量较多')
            if abs(row['feature_26']['x']-row['feature_26']['y']) >= 6:
                tags.append(u'核心自然人控制节点数量变化较大')
            if abs(row['feature_27']['x']-row['feature_27']['y']) >= 0.1:
                tags.append(u'利益一致行动法人占比变化较大')
            if abs(row['feature_28']['x']-row['feature_28']['y']) >= 5:
                tags.append(u'关联方聚集区域关联节点数量变化较大')

        return {
            risk_name: tags
        }
        
    result = {}
    result.update(get_trading_feature_risk())
    result.update(get_platform_compliance_risk())
    if row['feature_1']:
        result.update(get_company_strength_risk())
        result.update(get_reputation_risk())
        result.update(get_relationship_risk())
    else:
        result.update({u"综合实力风险": []})
        result.update({u'声誉风险': []})
        result.update({u'平台关联方风险': []})

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