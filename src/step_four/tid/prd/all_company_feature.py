# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 15g \
--queue project.hongjing \
all_company_feature.py {version}
'''
import sys
import os

import json
from operator import itemgetter
from collections import OrderedDict, Counter

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import networkx as nx
import numpy as np


class FeatureConstruction(object):
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
    
    @__fault_tolerant    
    def create_graph_udf(cls, relations, is_directed):
        '''
        根据关联方的结构创建有向、无向图
        '''
        def init_graph(edge_list, node_list, is_directed=0):
            #网络初始化
            G = nx.DiGraph() if is_directed == 1 else nx.Graph()    
            #增加带属性的节点
            for node in node_list:
                G.add_node(node[0], attr_dict=node[1])
            #增加带属性的边
            G.add_edges_from(edge_list)
            return G
            
        #生成一个图
        company_correlative_edges = [
            (row.b, row.c, {'is_invest': row.bc_relation}) for row in relations]
                                     
        company_correlative_nodes = [(
                row.b, 
                dict(
                    is_human=row.b_isperson,
                    is_black=row.b_is_black_company,
                    is_high=row.b_is_high_company,
                    is_new_finance=row.b_is_leijinrong,
                    distance=row.b_degree,
                    name=row.b_name,
                    bgxx=row.b_bgxx,
                    ktgg=row.b_ktgg,
                    zgcpwsw=row.b_zgcpwsw,
                    rmfygg=row.b_rmfygg,
                    lending=row.b_lending,
                    xzcf=row.b_xzcf,
                    zhixing=row.b_zhixing,
                    dishonesty=row.b_dishonesty,
                    jyyc=row.b_jyyc,
                    address=row.b_address,
                    estatus=row.b_estatus,
                    province=row.b_company_province,
                    high_relation=row.b_high_relation_num,
                    black_relation=row.b_black_relation_num
                ))  for row in relations] + [(
                row.c, 
                dict(
                    is_human=row.c_isperson,
                    is_black=row.c_is_black_company,
                    is_high=row.c_is_high_company,
                    is_new_finance=row.c_is_leijinrong,                    
                    distance = row.c_degree,
                    name = row.c_name,
                    bgxx=row.c_bgxx,
                    ktgg = row.c_ktgg,
                    zgcpwsw = row.c_zgcpwsw,
                    rmfygg = row.c_rmfygg,
                    lending = row.c_lending,
                    xzcf = row.c_xzcf,
                    zhixing = row.c_zhixing,
                    dishonesty = row.c_dishonesty,
                    jyyc = row.c_jyyc,
                    address = row.c_address,
                    estatus = row.c_estatus,
                    province=row.c_company_province,
                    high_relation=row.c_high_relation_num,
                    black_relation=row.c_black_relation_num
                )) for row in relations]
        
        if is_directed == 1:
            g = init_graph(
                company_correlative_edges,
                company_correlative_nodes, is_directed = 1)
        else:
            g = init_graph(
                company_correlative_edges, 
                company_correlative_nodes, is_directed = 0)
        return g

    @__fault_tolerant
    def get_feature_xgxx(cls):
        '''
        【计算基础数据】
        法律诉讼风险：开庭公告、裁判文书、非法集资裁判文书、法院公告、民间借贷
        行政处罚
        被执行风险
        异常经营风险：经营异常、吊销&注销
        银监会行政处罚
        ''' 
        def get_certain_distance_name_info(distance, document_types):
            '''
            出现公司名单集合
            '''
            all_array = []
            
            for each_document in document_types:
                each_list = []
                for node, attr in cls.DIG.nodes_iter(data=True):
                    if (attr['is_human'] == 0  
                            and attr['distance'] <= distance
                            and node != cls.tarcompany):
                        if attr[each_document]:
                            each_list.append(attr['name'])
                all_array.append(each_list)
            
            #处理某一个distance不存在节点的情况
            if not all_array:
                all_array = [[]]*len(document_types)
                
            return all_array
        
        
        def get_certain_distance_add_info(distance, document_types):
            '''
            出现次数
            '''
            all_array = []
            #处理某一个distance不存在节点的情况
            all_array.append([0]*len(document_types))            
            for node, attr in cls.DIG.nodes_iter(data=True):
                if (attr['is_human'] == 0 
                        and attr['distance'] <= distance  
                        and node != cls.tarcompany):
                    each_array = map(lambda x: 1 if x else 0, 
                                     [attr[each_document] 
                                     for each_document in document_types])
                    all_array.append(each_array)
                else:
                    continue
            documents_num = np.sum(all_array, axis=0)
            return documents_num
        
        def get_feature_name(distance, name):
            '''
            获取特征名
            '''
            return  '{distance}d_{name}'.format(distance=distance, 
                                                name=name)
        
        
        matrx = dict()
        
        xgxx_type = ['ktgg', 'zgcpwsw', 'rmfygg', 
                             'lending', 'xzcf', 'zhixing', 
                             'dishonesty', 'jyyc', 'bgxx',
                             'is_black', 'is_high', 'is_new_finance',
                             'estatus']
        
        xgxx_type_num = ['ktgg_num', 'zgcpwsw_num', 'rmfygg_num', 
                             'lending_num', 'xzcf_num', 'zhixing_num', 
                             'dishonesty_num', 'jyyc_num', 'bgxx_num',
                             'black_num', 'high_risk_num', 'new_finance_num',
                             'estatus_num']

        xgxx_type_name = ['ktgg_name', 'zgcpwsw_name', 'rmfygg_name', 
                             'lending_name', 'xzcf_name', 'zhixing_name', 
                             'dishonesty_name', 'jyyc_name', 'bgxx_name',
                             'black_name', 'high_risk_name', 'new_finance_name',
                             'estatus_name']
        
        for each_distance in xrange(1, 4):
            xgxx_name_list = get_certain_distance_name_info(each_distance,
                                                            xgxx_type)
            
            xgxx_num_list = get_certain_distance_add_info(each_distance, 
                                                          xgxx_type)
            matrx[each_distance] = dict(zip([get_feature_name(each_distance, each_name) 
                                             for each_name in xgxx_type_num], 
                                             xgxx_num_list))
            
        return reduce(lambda a,b: dict(a, **b), matrx.values())

    
    @classmethod
    def get_feature_1(cls):
        '''
        关联方统计特征
        '''
        return cls.get_feature_xgxx()

    @classmethod
    def get_feature_2(cls):
        '''
        目标公司特征
        '''
        def get_property_num(property_name):
            tar_data = cls.resultiterable.data[0]
            num = tar_data['a_{0}'.format(property_name)]
            return num if num else 0

        def get_bgxx_symbol(bgxx_name):
            if u'法定代表人' in bgxx_name:
                return 'fddbr'
            elif u'股东' in bgxx_name:
                return 'gd'
            elif u'注册资本' in bgxx_name:
                return 'zczb'
            elif u'高管' in bgxx_name:
                return 'gg'
            elif u'经营范围' in bgxx_name:
                return 'jyfw'
            else:
                return 'else_type'
        
        bgxx_dict = OrderedDict([
                ('fddbr', 0),
                ('gd', 0),
                ('zczb', 0),
                ('gg', 0),
                ('jyfw', 0),
                ('else_type', 0)
            ]
        )
        
        if get_property_num('bgxx'):
            for bgxx_name, bgxx_num in get_property_num('bgxx').iteritems():
                bgxx_dict[get_bgxx_symbol(bgxx_name)] += int(bgxx_num)
        
        return dict(
            tar_lending_num = get_property_num('lending'),
            tar_dishonesty_num = get_property_num('dishonesty'),
            tar_zhixing_num = get_property_num('zhixing'),
            tar_xzcf_num = get_property_num('xzcf'),
            tar_jyyc_num = get_property_num('jyyc'),
            tar_ktgg_num = get_property_num('ktgg'),
            tar_rmfygg_num = get_property_num('rmfygg'),
            tar_zgcpwsw_num = get_property_num('zgcpwsw'),
            tar_ssws_num = (
              get_property_num('ktgg') + 
              get_property_num('rmfygg') + 
              get_property_num('zgcpwsw')             
            ),
            tar_out_drgree=cls.DIG.out_degree(cls.tarcompany),
            tar_bgxx_total_num=sum(bgxx_dict.values()),
            tar_bgxx_fddbr_num=bgxx_dict['fddbr'],
            tar_bgxx_gd_num=bgxx_dict['gd'],
            tar_bgxx_zczb_num=bgxx_dict['zczb'],
            tar_bgxx_gg_num=bgxx_dict['gg'],
            tar_bgxx_jyfw_num=bgxx_dict['jyfw']
        )
        
    @classmethod
    def get_feature_3(cls):
        '''
        关联方存在诉讼文书的企业数
        '''
        def get_num(obj):
            '''
            统计总和
            '''
            result = sum(filter(None, obj))
            return 1 if result else 0
        
        
        def get_certain_distance_num(distance):
            return sum(
                [get_num([attr['ktgg'], attr['rmfygg'], attr['zgcpwsw']])
                 for node, attr in cls.DIG.nodes_iter(data=True)
                 if attr['distance'] <= distance
                 and node != cls.tarcompany]
            )
        
        def get_certain_distance_name(distance):
            return [
                attr['name']
                for node, attr in cls.DIG.nodes_iter(data=True)
                if attr['distance'] <= distance
                and node != cls.tarcompany
                and (attr['ktgg'] or attr['rmfygg'] or attr['zgcpwsw'])
            ]
        
        sswx_num = {'{0}d_sswx_num'.format(each_distance): 
                                get_certain_distance_num(each_distance) 
                                for each_distance in range(1, 4)}
        
        sswx_name = {'{0}d_sswx_name'.format(each_distance): 
                                get_certain_distance_name(each_distance) 
                                for each_distance in range(1, 4)}
        
        return sswx_num
            
    @classmethod
    def get_feature_4(cls):
        '''
        关联方与目标企业利益一致的企业数
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

        def get_certain_distance_info(distance, tar_company, return_name): 
            some_relation_set = [
                attr['name'] 
                for node, attr in cls.DIG.nodes_iter(data=True) 
                if attr['distance'] <= distance
                and node != cls.tarcompany]
            
            common_interests_name = [
                node_name
                for node_name in some_relation_set 
                if is_similarity(tar_company_frag, node_name)]
            
            return common_interests_name if return_name else len(common_interests_name)
            
        #目标公司的名称frag
        #tar_company = cls.resultiterable.data[0].a_name
        tar_company_frag = cls.resultiterable.data[0].a_namefrag
        
        #三度以内，所有关联方的节点集合(不包含自身)
        same_common_interests_num = {
            '{0}d_common_interests_num'.format(each_distance): 
            get_certain_distance_info(each_distance, tar_company_frag, False)
            for each_distance in range(1, 4)}
        
        cls.same_common_interests_name = {
            '{0}d_common_interests_name'.format(each_distance): 
            get_certain_distance_info(each_distance, tar_company_frag, True) 
            for each_distance in range(1, 4)}

        return same_common_interests_num
        
    @classmethod
    def get_feature_5(cls):
        '''
        关联方地址相同
        '''
        def get_certain_distance_info(distance, return_name): 
            legal_person_address = {
                attr['name']: attr['address']
                for node, attr in cls.DIG.nodes_iter(data=True)
                if attr['distance'] <= distance
                and attr['is_human'] == 0
                and attr['address']
                and attr['name']}
                            
            c = Counter(filter(lambda x: x is not None and len(x) >= 21,
                               legal_person_address.values()))
            n = c.most_common(1)
            common_address_num = n[0][1] if len(n) > 0 else 0
            common_address_name = n[0][0] if len(n) > 0 else ''
            
            if return_name:
                return [
                    company_name 
                    for company_name, address 
                    in legal_person_address.iteritems()
                    if address == common_address_name]
            else:
                return common_address_num
        
        same_address_num = {'{0}d_same_address_num'.format(each_distance): 
                            get_certain_distance_info(each_distance, False)
                            for each_distance in range(1, 4)}
        
        cls.same_address_name = {'{0}d_same_address_name'.format(each_distance): 
                                 get_certain_distance_info(each_distance, True) 
                                 for each_distance in range(1, 4)}
        
        return same_address_num
    
    @classmethod
    def get_feature_6(cls):
        '''
        关联方1/2/3度，按标签分类统计
        '''
        def get_certain_node_info(node):
            '''
            获取某个节点的相关信息
            '''
            def get_some_attr(attr_name):
                value = cls.DIG.node[node][attr_name]
                return value if value else 0
            
            bgxx_info=get_some_attr('bgxx')
            if bgxx_info:
                bgxx_num = sum(map(int, filter(None, bgxx_info.values())))
            else:
                bgxx_num = bgxx_info
            
            return dict(
                lending_num=get_some_attr('lending'),
                dishonesty_num=get_some_attr('dishonesty'),
                zhixing_num=get_some_attr('zhixing'),
                xzcf_num=get_some_attr('xzcf'),
                jyyc_num=get_some_attr('jyyc'),
                ssws_num=(
                    get_some_attr('ktgg') + 
                    get_some_attr('rmfygg') + 
                    get_some_attr('zgcpwsw')                
                ),
                bgxx_num=bgxx_num,
                out_degree=cls.DIG.out_degree(node),
                black_relation_num=get_some_attr('black_relation'),
                high_relation_num=get_some_attr('high_relation')
            )
            
        
        def get_certain_distance_info(distance):
            '''
            获取某度内的节点企业信息:
            
            1d_relation_info: {
             'total':{
                'f82c6505b4844c2ab58a345253ca435b'：{'lending_num': 2, 
                                                    'zhixing_num': 0, 
                                                    'ssws_num': 8}
                '490564e547f156bb9cd4835d3bd1826e': {...}
                ...
                }
             'black'：{
                'f82c6505b4844c2ab58a345253ca435b'：{'lending_num': 2, 
                                                    'zhixing_num': 0, 
                                                    'ssws_num': 8}
                '490564e547f156bb9cd4835d3bd1826e': {...}
                ...
                }
            }
            2d_relation_info
            3d_relation_info
            
            total 全部关联方,
            black 黑名单关联方,
            high_risk 高风险关联方,
            new_finance 新金融关联方,
            common_interests 利益一致关联方,
            common_address 地址相同关联方,
            estate 注吊销关联方的
            '''
            def get_special_node_info(attr_name):
                return {
                node: get_certain_node_info(node)
                for node, attr in cls.DIG.nodes_iter(data=True)
                if attr['distance'] <= distance
                and node != cls.tarcompany
                and attr[attr_name]
            }
            
            # 只需要返回1度的全部企业
            if distance == 1:
                total = {
                    node: get_certain_node_info(node)
                    for node, attr in cls.DIG.nodes_iter(data=True)
                    if attr['distance'] <= distance
                    and node != cls.tarcompany
                }
            else:
                total = {}
                
            black = get_special_node_info('is_black')
            high_risk = get_special_node_info('is_high')
            new_finance = get_special_node_info('is_new_finance')
            estate = get_special_node_info('estatus')
            
            #特殊处理, 获取利益一致公司名单
            certain_nodes_name = cls.same_common_interests_name[
                '{0}d_common_interests_name'.format(distance)]
            common_interests = {
                node: get_certain_node_info(node)
                for node, attr in cls.DIG.nodes_iter(data=True)
                if attr['distance'] <= distance
                and node != cls.tarcompany
                and attr['name'] in certain_nodes_name
            }
            
            #特殊处理，获取地址相同公司名单
            certain_nodes_name = cls.same_address_name[
                '{0}d_same_address_name'.format(distance)]
            common_address = {
                node: get_certain_node_info(node)
                for node, attr in cls.DIG.nodes_iter(data=True)
                if attr['distance'] <= distance
                and node != cls.tarcompany
                and attr['name'] in certain_nodes_name
            }
            
            return dict(
                total=total,
                black=black,
                high_risk=high_risk,
                new_finance=new_finance,
                common_interests=common_interests,
                common_address=common_address,
                estate=estate
            )
        
        xgxx_num_list = {
            '{0}d_relation_info'.format(each_distance): 
                get_certain_distance_info(each_distance)
            for each_distance in range(1, 4)
        }
    
        return xgxx_num_list
    
    @classmethod
    def get_feature_7(cls):
        '''
        关联方聚集地
        '''
        
        def get_certain_distance_info(distance):
            province_distribution = [
                attr['province']
                for ndoe, attr in cls.DIG.nodes_iter(data=True)
                if attr['distance'] <= distance
                and attr['province']
            ]
            cont = Counter(province_distribution)
            province_info = cont.most_common(3)
            
            return map(itemgetter(0), province_info)
            
        result = {
            '1d_gather_place': get_certain_distance_info(1),
            '2d_gather_place': get_certain_distance_info(2),
            '3d_gather_place': get_certain_distance_info(3)
        }
        
        return result
        
    
    @classmethod
    def get_some_feature(cls, resultiterable, feature_nums):
        #创建类变量
        cls.resultiterable = resultiterable
        cls.tarcompany = cls.resultiterable.data[0].a
        cls.DIG = cls.create_graph_udf(cls.resultiterable, 1)
        
        feature_list = [
            eval('cls.get_feature_{0}()'.format(feature_index))
            for feature_index in feature_nums]
        feature_list.append(dict([('bbd_qyxx_id', cls.tarcompany)]))
        feature_list.append(dict([('company_name', 
                                   cls.resultiterable.data[0].a_name)]))
        
        return json.dumps(reduce(lambda a,b: dict(a, **b), feature_list), 
                          ensure_ascii=False)
    
def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "65g")
    conf.set("spark.executor.instances", 25)
    conf.set("spark.executor.cores", 15)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 6000)
    conf.set("spark.sql.shuffle.partitions", 6000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)
    
    spark = SparkSession \
        .builder \
        .appName("hgongjing2_all_company_feature") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark     
    
def spark_data_flow(tidversion):
    
    tid_df = spark.read.parquet(
        "{path}/all_info_merge/{version}".format(path=IN_PATH,
                                                 version=tidversion))
    tid_add_df = spark.read.parquet(
        "{path}/som_company_relation_info/{version}".format(path=IN_PATH,
                                                            version=tidversion)
    ).cache()
    
    #在tid_df的基础上增加两列
    tid_df_cols = tid_df.columns
    tid_df_cols.remove('a')
    tid_df_cols.insert(0, tid_df.a)
    tid_df_cols.append(
        tid_add_df.high_relation_company_num.alias('b_high_relation_num'))
    tid_df_cols.append(
        tid_add_df.black_relation_company_num.alias('b_black_relation_num'))
    
    
    tid_df = tid_df.join(
        tid_add_df,
        tid_add_df.a == tid_df.b,
        'left_outer'
    ).select(
        tid_df_cols
    )
    
    #再增加两列
    tid_df_cols = tid_df.columns
    tid_df_cols.remove('a')
    tid_df_cols.insert(0, tid_df.a)
    tid_df_cols.append(
        tid_add_df.high_relation_company_num.alias('c_high_relation_num'))
    tid_df_cols.append(
        tid_add_df.black_relation_company_num.alias('c_black_relation_num'))
    
    tid_df = tid_df.join(
        tid_add_df,
        tid_add_df.a == tid_df.c,
        'left_outer'
    ).select(
        tid_df_cols
    )

    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "tid_df"
         "/{version}").format(path=TMP_PATH, 
                              version=RELATION_VERSION))
    tid_df.coalesce(
        100
    ).write.parquet(
         "{path}/"
         "tid_df/{version}".format(version=RELATION_VERSION,
                                   path=TMP_PATH))
    tid_df = spark.read.parquet(
         "{path}/"
         "tid_df/{version}".format(version=RELATION_VERSION,
                                   path=TMP_PATH))
    tid_rdd = tid_df.rdd
        
    #最终计算流程
    tid_rdd_2 = tid_rdd.map(lambda row: (row.a, row)) \
        .groupByKey() \
        .filter(lambda r: len(r[1].data) <= 100000) \
        .cache()
    
    import signal
    def handler(signum, frame):
        raise AssertionError
    
    def fault_tolerant(func):
        def wappen(*args):
            try:
                return func(*args)
            except:
                return {}
        return wappen
    
    @fault_tolerant
    def time_out(data):
        signal.signal(signal.SIGALRM, handler)
        signal.alarm(1000)
        result = FeatureConstruction.get_some_feature(
            data,[_ for _ in range(1, 8)])
        signal.alarm(0)
        return result        
        
    feature_list = tid_rdd_2.mapValues(
        lambda data: FeatureConstruction.get_some_feature(
            data,[_ for _ in range(1, 8)])
    ).map(
        itemgetter(1)
    )
    
    return feature_list

def run():
    '''
    格式化输出
    '''
    pd_df = spark_data_flow(RELATION_VERSION)
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "all_company_feature"
         "/{version}").format(path=OUT_PATH, 
                              version=RELATION_VERSION))
    pd_df.repartition(30).saveAsTextFile(
        ("{path}/"
        "all_company_feature"
        "/{version}").format(path=OUT_PATH, 
                             version=RELATION_VERSION))
    

if __name__ == '__main__':  
    import configparser
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    #输入参数
    IN_PATH = "/user/antifraud/hongjing2/dataflow/step_four/tid/tid/"
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_four/tid/prd/"
    TMP_PATH = "/user/antifraud/hongjing2/dataflow/step_four/tid/tmp/"
    
    spark = get_spark_session()

    run()