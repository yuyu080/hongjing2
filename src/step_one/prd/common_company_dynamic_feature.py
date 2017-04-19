# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
common_company_dynamic_feature.py

'''

import json
import os
from collections import Counter
import configparser

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from common_company_static_feature import FeatureConstruction

class DynamicFeatureConstruction(FeatureConstruction):
    '''
    计算特征的函数集
    '''      
    def __fault_tolerant(func):
        '''
        一个用于容错的装饰器
        '''
        @classmethod
        def wappen(cls, *args, **kwargs):
            try:
                return func(cls, *args, **kwargs)
            except Exception, e:
                return (
                    "{func_name} has a errr : {excp}"
                ).format(func_name=func.__name__, excp=e)
        return wappen    
    
    @classmethod
    def get_feature_25(cls):
        '''
        平台稳态运营风险
        '''
        nature_person_num = sum([
                1
                for node, attr in  cls.DIG.nodes_iter(data=True)
                if (attr['is_human'] == 1
                        and attr['distance'] <= 3)
        ])
        legal_person_num = sum([
                1
                for node, attr in  cls.DIG.nodes_iter(data=True)
                if (attr['is_human'] == 0
                        and attr['distance'] <= 3)
        ])
        return dict(
            nature_person_num=nature_person_num,
            legal_person_num=legal_person_num
        )
    
    @classmethod
    def get_feature_26(cls):
        '''
        平台核心资本运作风险
        '''
        some_person_sets = [
            node
            for node, attr in cls.DIG.nodes_iter(data=True) 
            if (attr['is_human'] == 1
                    and attr['distance'] <= 3)]
        person_out_degree = cls.DIG.out_degree(
            some_person_sets).values()
        person_out_degree.sort(reverse=True)
        if not person_out_degree:
            person_out_degree.append(0)

        return dict(
            kernel_control_num=sum(person_out_degree[:3])
        )
    
    @classmethod
    def get_feature_27(cls):
        '''
        可持续性风险
        '''
        def is_similarity(str_1, str_2):
            '''
            判断两个字符串是否有连续2个字相同
            '''
            try:
                token_1 = [
                    str_1[index] + str_1[index+1] 
                    for index,data in enumerate(str_1) 
                    if index < len(str_1) - 1]
                is_similarity = sum([
                        1 for each_token in token_1 
                        if each_token in str_2])
                return True if is_similarity > 0 else False        
            except:
                return False
            
        #目标公司的名称frag
        tar_company_frag = cls.resultiterable.data[0].a_namefrag
        #三度以内，所有关联方的节点集合(不包含自身)
        relation_set = [
                attr['name']
                for node, attr in cls.DIG.nodes_iter(data=True) 
                if attr['distance'] <= 3]
        #relation_set.remove(tar_company)
        common_interests_num = sum([
                1 
                for node_name in relation_set 
                if is_similarity(tar_company_frag, node_name)
        ])
        total_legal_person_num = sum([
                1
                for node, attr in cls.DIG.nodes_iter(data=True) 
                if (attr['distance'] <= 3 and 
                       attr['is_human'] == 0)
        ])
        if total_legal_person_num:
            ratio = round(
                common_interests_num / total_legal_person_num, 2
            )
        else:
            ratio = 0
            
        return dict(
            ratio=ratio
        )
        
    @classmethod
    def get_feature_28(cls):
        '''
        平台跨区域舞弊风险
        '''
        province_list = filter(
            None, [
                attr['province']
                for node, attr in cls.DIG.nodes_iter(data=True)
                if (attr['distance'] <= 3 and 
                       attr['is_human'] == 0)]
        )
        province_distribution = Counter(province_list)
        province_top4_num = sum(
            map(lambda (k, v): v, province_distribution.most_common(4)))
        
        return dict(
            province_top4_num=province_top4_num
        )
    
    
    @classmethod
    def get_some_feature(cls, resultiterable, feature_nums):
        #创建类变量
        cls.resultiterable = resultiterable
        cls.tarcompany = cls.resultiterable.data[0].a
        cls.DIG = cls.create_graph_udf(cls.resultiterable, 1)
        cls.G = cls.create_graph_udf(cls.resultiterable, 0)
        
        feature_list = [
            ('feature_{0}'.format(feature_index), 
                     eval('cls.get_feature_{0}()'.format(feature_index)))
            for feature_index in feature_nums]
        feature_list.append(('bbd_qyxx_id', cls.tarcompany))
        feature_list.append(('company_name', 
                             cls.resultiterable.data[0].a_name))
        
        return dict(feature_list) 

    
def get_dynamic_risk(data):
    '''
    计算动态风险
    '''
    old_data, new_data = data
    #平台稳态运营风险
    if old_data['feature_25']['nature_person_num']:
        nature_person_risk = ((new_data['feature_25']['nature_person_num'] - 
                                   old_data['feature_25']['nature_person_num']) / 
                              old_data['feature_25']['nature_person_num'])
    else:
        nature_person_risk = 0.
    if old_data['feature_25']['legal_person_num']:
        legal_person_risk = ((new_data['feature_25']['legal_person_num'] - 
                                  old_data['feature_25']['legal_person_num']) / 
                             old_data['feature_25']['legal_person_num'])
    else:
        legal_person_risk = 0.
    feature_25 = 5. * (nature_person_risk + legal_person_risk)
    
    #平台核心资本运作风险
    if old_data['feature_26']['kernel_control_num']:
        feature_26 = (15. * 
                      new_data['feature_26']['kernel_control_num'] / 
                      old_data['feature_26']['kernel_control_num'])
    else:
        feature_26 = 0.
        
    #可持续性风险
    if old_data['feature_27']['ratio']:
        feature_27 = (4. * 
                      new_data['feature_27']['ratio'] / 
                      old_data['feature_27']['ratio'])
    else:
        feature_27 = 0.
        
    #平台跨区域舞弊风险
    if old_data['feature_28']['province_top4_num']:
        feature_28 = (15. * 
                      new_data['feature_28']['province_top4_num'] / 
                      old_data['feature_28']['province_top4_num'])
    else:
        feature_28 = 0.
        
    return dict(
        feature_25={'p': round(feature_25, 2)},
        feature_26={'h': round(feature_26, 2)},
        feature_27={'c': round(feature_27, 2)},
        feature_28={'k': round(feature_28, 2)}
    )
    
def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "50g")
    conf.set("spark.executor.instances", 20)
    conf.set("spark.executor.cores", 10)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 1000)
    conf.set("spark.sql.shuffle.partitions", 1000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)
    conf.set("spark.submit.pyFiles", STATIC_FILE)
    
    spark = SparkSession \
        .builder \
        .appName("hgongjing2_one_prd_common_dynamic") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark   
    
def spark_data_flow(tid_old_version, tid_new_version):
    '''
    dataflow
    '''    
    #输入
    #算法设计直接用rdd
    tid_old_df = spark.read.parquet(
        ("{path}/"
        "common_company_info_merge/{version}").format(path=IN_PATH,
                                                      version=tid_old_version))
    tid_old_rdd = tid_old_df.rdd
    tid_new_df = spark.read.parquet(
        ("{path}/"
        "common_company_info_merge/{version}").format(path=IN_PATH,
                                                      version=tid_new_version))
    tid_new_rdd = tid_new_df.rdd    
        
    #最终计算流程
    tid_new_rdd_2 = tid_new_rdd.map(lambda row: (row.a_name, row)) \
        .groupByKey() \
        .repartition(600) \
        .filter(lambda r: len(r[1].data) <= 150000) \
        .cache()
    tid_old_rdd_2 = tid_old_rdd.map(lambda row: (row.a_name, row)) \
        .groupByKey() \
        .repartition(600) \
        .filter(lambda r: len(r[1].data) <= 150000) \
        .cache()

    feature_new_list = tid_new_rdd_2.mapValues(
        lambda v: DynamicFeatureConstruction.get_some_feature(
            v, [_ for _ in range(25, 29)]))
    feature_old_list = tid_old_rdd_2.mapValues(
        lambda v: DynamicFeatureConstruction.get_some_feature(
            v, [_ for _ in range(25, 29)]))
    
    dynamic_risk_data = feature_old_list.join(
        feature_new_list
    ).mapValues(
        get_dynamic_risk
    ).map(
        lambda (k, v): dict({'company_name': k}, **v)
    ).map(
        lambda data: json.dumps(data, ensure_ascii=False)
    )

    return dynamic_risk_data

def run(tid_old_version, tid_new_version):
    '''
    格式化输出
    '''
    dynamic_risk_data = spark_data_flow(tid_old_version, tid_new_version)
    os.system(
        ("hadoop fs -rmr "
        "{path}/"
        "common_dynamic_feature_distribution/{version}").format(path=OUT_PATH, 
                                                                version=tid_new_version))
    dynamic_risk_data.repartition(10).saveAsTextFile(
        ("{path}/"
        "common_dynamic_feature_distribution/{version}").format(path=OUT_PATH, 
                                                                version=tid_new_version))
 
    
if __name__ == '__main__':  
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.conf")
    
    #输入参数
    IN_PATH = conf.get('step_one', 'prd_in_path')
    OUT_PATH = conf.get('step_one', 'prd_out_path')
    STATIC_FILE = ("/data5/antifraud/Hongjing2.0/src/step_one/prd/"
                   "common_company_static_feature.py")
    
    spark = get_spark_session()

    
    RELATION_VERSIONS = eval(conf.get('step_one', 'RELATION_VERSIONS'))
    for index, relation_version in RELATION_VERSIONS:
        if index < 3:
            tid_old_version = RELATION_VERSIONS[0]
            tid_new_version = relation_version
        else:
            tid_old_version = RELATION_VERSIONS[index-3]
            tid_new_version = relation_version
            
        run(tid_old_version, tid_new_version)

