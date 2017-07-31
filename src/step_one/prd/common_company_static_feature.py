# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
common_company_static_feature.py {version}
'''
import json
import datetime
import requests
import sys
import os
from operator import itemgetter
from collections import Counter, defaultdict, OrderedDict

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
                    distance = row.b_degree,
                    name = row.b_name,
                    isSOcompany = row.b_isSOcompany,
                    esdate = row.b_regtime,
                    ktgg = row.b_ktgg,
                    zgcpwsw = row.b_zgcpwsw,
                    rmfygg = row.b_rmfygg,
                    lending = row.b_lending,
                    xzcf = row.b_xzcf,
                    zhixing = row.b_zhixing,
                    dishonesty = row.b_dishonesty,
                    jyyc = row.b_jyyc,
                    circxzcf = row.b_circxzcf,
                    opescope = row.b_opescope,
                    address = row.b_address,
                    estatus = row.b_estatus,
                    province = row.b_province
                ))  for row in relations] + [(
                row.c, 
                dict(
                    is_human=row.c_isperson,
                    is_black=row.c_is_black_company,
                    distance = row.c_degree,
                    name = row.c_name,
                    isSOcompany = row.c_isSOcompany,
                    esdate = row.c_regtime,
                    ktgg = row.c_ktgg,
                    zgcpwsw = row.c_zgcpwsw,
                    rmfygg = row.c_rmfygg,
                    lending = row.c_lending,
                    xzcf = row.c_xzcf,
                    zhixing = row.c_zhixing,
                    dishonesty = row.c_dishonesty,
                    jyyc = row.c_jyyc,
                    circxzcf = row.c_circxzcf,
                    opescope = row.c_opescope,
                    address = row.c_address,
                    estatus = row.c_estatus,
                    province = row.c_province
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
    def get_feature_1(cls):
        '''
        企业背景风险
        '''
        one_relation_set = [
            node for node, attr in cls.DIG.nodes_iter(data=True) 
            if attr['distance'] == 1]
        shareholder = [
            src_node for src_node, des_node, edge_attr 
            in cls.DIG.edges_iter(one_relation_set, data=True) 
            if des_node == cls.tarcompany 
            and edge_attr['is_invest'] == 'INVEST']
        os_shareholder = [
            node for node in shareholder 
            if cls.DIG.node[node]['isSOcompany'] is True 
            and cls.DIG.node[node]['is_human'] == 0]
        anti_os_shareholder = [
            node for node in shareholder 
            if cls.DIG.node[node]['isSOcompany'] is False 
            and cls.DIG.node[node]['is_human'] == 0]
        nature_shareholder = [
            node for node in shareholder 
            if cls.DIG.node[node]['is_human'] == 1]
        is_ipo_tarcompany = (
            1 if cls.resultiterable.data[0].a_isIPOcompany is True else 0)
    
        x = len(os_shareholder)
        y = len(anti_os_shareholder)
        z = len(nature_shareholder)
        w = len(shareholder)
        r_i = is_ipo_tarcompany
        r = (0.2*x + 0.5*y + z) * 100. * (2 - r_i) / (2*w + 0.001) 
        
        return dict(
            x=x,
            y=y,
            z=z,
            w=w,
            r_i=r_i,
            r=round(r, 2) if r else 100
        )


    @__fault_tolerant
    def get_feature_2(cls):
        '''
        资本风险
        '''
        x = cls.resultiterable.data[0].a_regcap 
        y = cls.resultiterable.data[0].a_realcap
        
        x = x / 10000 if x != 'NULL' and x is not None else 0
        y = y / 10000 if y != 'NULL' and y is not None else 0
        
        if 0 <= x < 500:
            c_i = 100
        elif 500 <= x < 1000:
            c_i = 60
        elif 1000 <= x < 5000:
            c_i = 30
        elif 5000 <= x:
            c_i = 10
        
        p_i = 1 if y / (x + 0.001) >= 0.5 else 0
        
        return  dict(
            x=x,
            y=y,
            c=c_i * (1 - p_i/2.)
        )
    
    @__fault_tolerant
    def get_feature_3(cls):
        '''
        公司地域风险
        '''
        province_black_num = cls.resultiterable.data[0].a_province_black_num
        province_leijinrong_num = (
            cls.resultiterable.data[0].a_province_leijinrong_num)
        province_black_num = province_black_num if province_black_num else 0
        province_leijinrong_num = (
            province_leijinrong_num if province_leijinrong_num else 0)
        
        return dict(
            j_1=province_black_num,
            j_2=province_leijinrong_num,
            j=round(province_black_num / (province_leijinrong_num + 0.0001), 4)
        )
    
    @__fault_tolerant
    def get_feature_4(cls):
        '''
        知识产权风险（专利和商标）
        '''
        k_1 = cls.resultiterable.data[0].a_zhuanli
        k_2 = cls.resultiterable.data[0].a_shangbiao 
        k_1 = k_1 if k_1 != 'NULL' and k_1 is not None else 0
        k_2 = k_2 if k_1 != 'NULL' and k_2 is not None else 0
        k_3 = k_1 + k_2
        
        if k_3 == 0:
            k = 100
        elif 1 <= k_3 < 5:
            k = 60
        elif 5 <= k_3 < 10:
            k = 30
        elif 10 <= k_3:
            k = 10
        
        return dict(
            k_1=k_1,
            k_2=k_2,
            k=k
        )
        
    @__fault_tolerant
    def get_feature_5(cls):
        '''
        域名备案风险
        '''
        icp = cls.resultiterable.data[0].a_ICP
        url = cls.resultiterable.data[0].a_url
        
        c_i = 1 if icp != 'NULL' and icp is not None else 0   
        try:
            if url is not None and url != 'NULL':
                status_code = (
                    requests.get(url, allow_redirects=False).status_code)
                p_i = 1 if status_code == 200 else 0
            else:
                p_i = 0
        except:
            p_i = 0
        
        l = (1 - c_i/2.) * (1 - p_i/2.) * 100
        
        return dict(
            c_i=c_i,
            p_i=p_i,
            l=l
        )
        
    @__fault_tolerant
    def get_feature_6(cls):
        '''
        工商变更风险
        '''
        def get_bgxx_symbol(bgxx_name):
            if u'法定代表人' in bgxx_name:
                return 'c_1'
            elif u'股东' in bgxx_name:
                return 'c_2'
            elif u'注册资本' in bgxx_name:
                return 'c_3'
            elif u'高管' in bgxx_name:
                return 'c_4'
            elif u'经营范围' in bgxx_name:
                return 'c_5'
            else:
                return 'c_6'
        
        default_result = OrderedDict([
                ('c_1', 0),
                ('c_2', 0),
                ('c_3', 0),
                ('c_4', 0),
                ('c_5', 0),
                ('c_6', 0),
                ('r', 0),
                ('z', 10)
            ]
        )
        
        if cls.resultiterable.data[0].a_bgxx is not None:
            for bgxx_name, bgxx_num in cls.resultiterable.data[0].a_bgxx.iteritems():
                default_result[get_bgxx_symbol(bgxx_name)] += int(bgxx_num)
            default_result['r'] = np.dot(default_result.values(), 
                                         [2, 1, 1, 1, 2, 0, 0, 0])
        
            if default_result['r'] == 0:
                default_result['z'] = 10
            elif 1 <= default_result['r'] < 5:
                default_result['z'] = 30
            elif 5 <= default_result['r'] < 10:
                default_result['z'] = 60
            elif 10 <= default_result['r']:
                default_result['z'] = 100
                
        default_result.pop('c_6')
        
        return dict(default_result)
        
    @__fault_tolerant
    def get_feature_7(cls):
        '''
        人才结构风险
        '''        
        def get_recruit_num(each_recruit):
            if u'本科' in each_recruit:
                return 'e_2'
            elif u'硕士' in each_recruit:
                return 'e_3'
            elif u'博士' in each_recruit:
                return 'e_3'
            else:
                return 'e_1'
            
        default_result = OrderedDict([
                ('e_1', 0),
                ('e_2', 0),
                ('e_3', 0),
                ('e', 0),
                ('r', 0),
                ('z', 10)
            ]
        )
        
        if cls.resultiterable.data[0].a_recruit is not None:
            for education_name, education_num in \
                    cls.resultiterable.data[0].a_recruit.iteritems():
                default_result[get_recruit_num(education_name)] += int(education_num)
            default_result['e'] = sum(
                map(int ,cls.resultiterable.data[0].a_recruit.values()))
            default_result['r'] = round(
                np.dot(default_result.values(), 
                       [100., 60., 30., 0, 0, 0]) / default_result['e'], 
                       2)
            
            if default_result['r'] == 0:
                default_result['z'] = 10
            elif 0 < default_result['r'] < 60:
                default_result['z'] = 30
            elif 60 <= default_result['r'] < 80:
                default_result['z'] = 60
            elif 80 <= default_result['r']:
                default_result['z'] = 100         
            
        return dict(default_result)
        
    @__fault_tolerant
    def get_feature_8(cls):
        '''
        公司运营持续风险
        '''
        esdate_relation_set = [
            (datetime.date.today() - attr['esdate']).days 
            for node, attr in cls.DIG.nodes_iter(data=True) 
            if attr['distance'] <= 3 
            and attr['is_human'] == 0 
            and attr['esdate']]
        
        
        if esdate_relation_set:
            t_2 = round(np.average(esdate_relation_set), 2)
        else:
            t_2 = 0.
        try:
            tar_date = cls.DIG.node[cls.tarcompany]['esdate']
            t_1 = (datetime.date.today() - tar_date).days 
        except:
            t_1 = 0.
        t = (t_1+t_2) / 2.
        
        if 0 <= t < 365:
            y = 100
        elif 365 <= t < 1825:
            y = 60
        elif 1825 <= t < 3650:
            y = 30
        elif 3650 <= t:
            y = 10
        else:
            y = 100
            
        return dict(
            t_1=t_1,
            t_2=t_2,
            y=y
        )

        
    @__fault_tolerant
    def get_feature_9(cls):
        '''
        招标和中标风险
        '''
        n_1 = cls.resultiterable.data[0].a_zhaobiao
        n_2 = cls.resultiterable.data[0].a_zhongbiao
        n_1 = n_1 if n_1 is not None else 0
        n_2 = n_2 if n_2 is not None else 0
        
        n = n_1 + n_2
        
        if n == 0:
            n = 100
        elif 1 <= n < 5:
            n = 60
        elif 5<= n < 10:
            n = 30
        else:
            n = 10
        
        return  dict(
            n_1=n_1,
            n_2=n_2,
            n=n
        )
        
    @__fault_tolerant
    def get_feature_xgxx(cls):
        '''
        【计算基础数据】
        法律诉讼风险：开庭公告、裁判文书、法院公告、民间借贷
        行政处罚
        被执行风险
        异常经营风险：经营异常、吊销&注销
        银监会行政处罚
        ''' 
        def get_certain_distance_all_info(distance, document_types):
            all_array = []
            #处理某一个distance不存在节点的情况
            all_array.append([0]*len(document_types))
            for node, attr in cls.DIG.nodes_iter(data=True):
                if attr['is_human'] == 0  and attr['distance'] == distance:
                    each_array = map(lambda x: x if x else 0, 
                                     [attr[each_document] 
                                         for each_document in document_types])
                    all_array.append(each_array)
                else:
                    continue
            documents_num = np.sum(all_array, axis=0)
            return documents_num
        
        matrx = dict()
        xgxx_type = ['ktgg', 'zgcpwsw', 'rmfygg', 
                     'lending', 'xzcf', 'zhixing', 
                     'dishonesty', 'jyyc', 'circxzcf', 
                     'estatus']

        for each_distance in xrange(0, 4):
            xgxx_num_list = get_certain_distance_all_info(each_distance, 
                                                          xgxx_type)
            matrx[each_distance] = dict(zip(xgxx_type, xgxx_num_list))
            
        return matrx
    
    @__fault_tolerant
    def filter_xgxx_type(cls, tar_xgxx):
        '''
        过滤相关信息
        '''
        risk = dict()
        for each_distance, xgxx_statistics in cls.xgxx_distribution.iteritems():
            tar_xgxx_statistics = dict([
                    (each_xgxx_type, each_xgxx_num)
                    for each_xgxx_type, each_xgxx_num 
                    in xgxx_statistics.iteritems() 
                    if each_xgxx_type in tar_xgxx])
            risk[each_distance] = tar_xgxx_statistics
        return risk
    
    @__fault_tolerant
    def get_feature_10(cls):
        '''
        法律诉讼风险：开庭公告、裁判文书、法院公告、民间借贷
        '''
        tar_xgxx = ['ktgg', 'zgcpwsw', 'rmfygg', 'lending']
        risk = cls.filter_xgxx_type(tar_xgxx)
        
        risk['r'] = round(
            np.dot(
                map(sum, [
                        risk[each_distance].values() 
                        for each_distance in xrange(0, 4)]), 
                [1., 1/2., 1/3., 1/4.]), 2)
        
        if risk['r'] == 0:
            risk['z'] = 10
        elif 0 < risk['r'] <= 10:
            risk['z'] = 30
        elif 10 < risk['r'] <= 30:
            risk['z'] = 60
        elif 30 < risk['r']:
            risk['z'] = 100
        
        return risk
            
    @__fault_tolerant
    def get_feature_11(cls):
        '''
        行政处罚风险
        '''
        tar_xgxx = ['xzcf']
        risk = cls.filter_xgxx_type(tar_xgxx)
        
        risk['r'] = round(
            np.dot(
                map(sum, [risk[each_distance].values() 
                    for each_distance in xrange(0, 4)]), 
                [1., 1/2., 1/3., 1/4.]), 2)
        
        if risk['r'] == 0:
            risk['z'] = 10
        elif 0 < risk['r'] <= 10:
            risk['z'] = 30
        elif 10 < risk['r'] <= 30:
            risk['z'] = 60
        elif 30 < risk['r']:
            risk['z'] = 100
        
        return risk
    
    @__fault_tolerant
    def get_feature_12(cls):
        '''
        被执行风险：被执行、失信被执行
        '''
        tar_xgxx = ['zhixing', 'dishonesty']
        risk = cls.filter_xgxx_type(tar_xgxx)        
        
        risk['r'] = round(
            np.dot(
                map(
                    lambda x: x['zhixing'] + 2. * x['dishonesty'], 
                    [risk[each_distance] for each_distance in xrange(0, 4)]), 
                [1., 1/2., 1/3., 1/4.]), 2)
        
        if risk['r'] == 0:
            risk['z'] = 10
        elif 0 < risk['r'] <= 10:
            risk['z'] = 30
        elif 10 < risk['r'] <= 30:
            risk['z'] = 60
        elif 30 < risk['r']:
            risk['z'] = 100        
        
        return risk
        
    @__fault_tolerant
    def get_feature_13(cls):
        '''
        异常经营风险
        '''
        tar_xgxx = ['jyyc', 'estatus']
        risk = cls.filter_xgxx_type(tar_xgxx)        
        
        risk['r'] = round(
            np.dot(
                map(
                    lambda x: x['jyyc'] + 2. * x['estatus'], 
                    [risk[each_distance] for each_distance in xrange(0, 4)]), 
                [1., 1/2., 1/3., 1/4.]), 2)

        if risk['r'] == 0:
            risk['z'] = 10
        elif 0 < risk['r'] <= 10:
            risk['z'] = 30
        elif 10 < risk['r'] <= 30:
            risk['z'] = 60
        elif 30 < risk['r']:
            risk['z'] = 100
        
        return risk        
        
    @__fault_tolerant
    def get_feature_14(cls):
        '''
        银监会行政处罚
        '''
        tar_xgxx = ['circxzcf']
        risk = cls.filter_xgxx_type(tar_xgxx)        
        
        risk['r'] = round(
            np.dot(
                map(
                    lambda x: x['circxzcf'], 
                    [risk[each_distance] for each_distance in xrange(0, 4)]), 
                [1., 1/2., 1/3., 1/4.]), 2)

        if risk['r'] == 0:
            risk['z'] = 10
        elif 0 < risk['r'] <= 10:
            risk['z'] = 30
        elif 10 < risk['r'] <= 30:
            risk['z'] = 60
        elif 30 < risk['r']:
            risk['z'] = 100        
    
        return risk            
        
    @__fault_tolerant
    def get_feature_15(cls):
        '''
        实际控制人风险
        '''
        def get_degree_distribution(is_human):
            some_person_sets = [
                node
                for node, attr in cls.DIG.nodes_iter(data=True) 
                if attr['is_human'] == is_human
                and attr['distance'] <= 3]
            person_out_degree = cls.DIG.out_degree(some_person_sets).values()
            if not person_out_degree:
                person_out_degree.append(0)
            return person_out_degree
        
        nature_person_distribution = get_degree_distribution(1)
        legal_person_distribution = get_degree_distribution(0)
        
        nature_max_control = max(nature_person_distribution)
        legal_max_control = max(legal_person_distribution)        
        nature_avg_control = round(np.average(nature_person_distribution), 2)
        legal_avg_control = round(np.average(legal_person_distribution), 2)
        
        total_legal_num = len([
                node 
                for node, attr in cls.DIG.nodes_iter(data=True) 
                if attr['is_human'] == 0])
        
        risk = round(((
                    2*(nature_max_control + nature_avg_control) + 
                    (legal_max_control + legal_avg_control)) /
                (2*total_legal_num + 0.001)), 2)
        
        if 0 <= risk <= 0.5:
            r = 10
        elif 0.5 < risk <= 1:
            r = 30
        elif 1 < risk <= 3:
            r = 60  
        elif 3 < risk:
            r = 100
        else:
            r = 10
        
        return dict(
            x_1=nature_max_control,
            x_2=legal_max_control,
            y_1=nature_avg_control,
            y_2=legal_avg_control,
            z=total_legal_num,
            s=risk,
            r=r
        )
    
    @__fault_tolerant
    def get_feature_16(cls):
        '''
        公司扩张路径风险
        '''
        def get_node_set(is_human):
            return Counter([
                    attr['distance']
                    for node, attr in cls.DIG.nodes_iter(data=True)
                    if attr['is_human'] == is_human])
        
        nature_person_distribution = get_node_set(1)
        legal_person_distribution = get_node_set(0)
        
        nature_person_num = [
            nature_person_distribution.get(each_distance, 0)
            for each_distance in range(1, 4)]
        legal_person_num = [
            legal_person_distribution.get(each_distance, 0)
            for each_distance in range(1, 4)]
        
        risk = round(np.sum(
                np.divide(
                    [
                        np.divide(
                            np.sum(nature_person_num[:each_distance]), 
                            np.sum(legal_person_num[:each_distance]), 
                            dtype=float)
                        for each_distance in range(1, 4)], 
                    np.array([1, 2, 3], dtype=float))), 2)
        
        if 0 <= risk <= 1:
            r = 10
        elif 1 < risk <= 3:
            r = 30
        elif 3 < risk <= 5:
            r = 60  
        elif 5 < risk:
            r = 100
        else:
            r = 10
        
        return dict(
            x_1=legal_person_num[0],
            x_2=legal_person_num[1],
            x_3=legal_person_num[2],
            y_1=nature_person_num[0],
            y_2=nature_person_num[1],
            y_3=nature_person_num[2],
            z=risk if risk < 10000 else 0,
            r=r
        )


    @__fault_tolerant
    def get_feature_17(cls):
        '''
        关联方中心集聚风险
        '''
        one_relation_set = [
            node for node, attr in cls.DIG.nodes_iter(data=True) 
            if attr['distance'] == 1]
        legal_person_shareholder = len([
                src_node 
                for src_node, des_node, edge_attr 
                in cls.DIG.edges_iter(one_relation_set, data=True) 
                if des_node == cls.tarcompany 
                and edge_attr['is_invest'] == 'INVEST'
                and cls.DIG.node[src_node]['is_human'] == 0])
        legal_person_subsidiary = len([
                des_node 
                for src_node, des_node, edge_attr 
                in cls.DIG.edges_iter(data=True)
                if src_node == cls.tarcompany 
                and edge_attr['is_invest'] == 'INVEST'
                and cls.DIG.node[des_node]['is_human'] == 0])
        
        risk =(
            legal_person_subsidiary -
            legal_person_shareholder ) 
        
        if risk < 0:
            r = 10
        elif risk == 0:
            r = 30
        elif 0 < risk <= 3:
            r = 60
        elif 3 < risk:
            r = 100
        else:
            r = 100        
        
        return dict(
            x=legal_person_subsidiary,
            y=legal_person_shareholder,
            z=risk,
            r=r
        )

    @__fault_tolerant
    def get_feature_18(cls):
        '''
        分支机构过度扩张风险
        '''
        risk = cls.resultiterable.data[0].a_fzjg
        risk = risk if risk is not None else 0

        if  risk == 0:
            r = 10
        elif risk == 1:
            r = 30
        elif 1 < risk <= 3:
            r = 60  
        elif 3 < risk:
            r = 100
        else:
            r = 10
        
        return dict(
            d=risk if risk < 100 else 100, 
            z=r
        )
    
    @__fault_tolerant
    def get_feature_19(cls):
        '''
        关联方结构稳定风险
        '''
        def get_relation_num():
            return Counter([
                    attr['distance']
                    for node, attr in cls.DIG.nodes(data=True)])
        
        #目标企业各个度的节点的数量
        relations_num = get_relation_num()
        relation_three_num = relations_num.get(3, 0)
        relation_two_num = relations_num.get(2, 0)
        relation_one_num = relations_num.get(1, 0)
        relation_zero_num = relations_num.get(0, 1)
        
        x = np.array([
                relation_zero_num, 
                relation_one_num, 
                relation_two_num, 
                relation_three_num]).astype(float)
        
        y_2 = x[2] / (x[1]+x[2]) if x[1]+x[2] else 0.
        y_3 = x[3] / (x[1]+x[2]+x[3]) if x[1]+x[2]+x[3] else 0.
        risk = y_2/2 + y_3/3
        
        if 0 <= risk <= 0.1:
            r = 10
        elif 0.1 < risk <= 0.3:
            r = 30
        elif 0.3 < risk <= 0.5:
            r = 60  
        elif 0.5 < risk:
            r = 100      
        else:
            r = 10
        
        return dict(
            x_1=x[1],
            x_2=x[2],
            x_3=x[3],
            y_2=y_2,
            y_3=y_3,
            z=risk if risk else 0,
            r=r
        )        
    
    @__fault_tolerant
    def get_feature_20(cls):
        '''
        潜在违规融资风险
        '''
        def get_relation_risk_num(keyword_type):
            return Counter(
                [
                    attr['distance']
                    for node, attr in cls.DIG.nodes_iter(data=True) 
                    if attr['is_human'] == 0 and 
                    attr['opescope'] == keyword_type
                ]
            )
        
        k_2_num = get_relation_risk_num('k_2')
        k_1_num = get_relation_risk_num('k_1')
        
        x = np.array([
                k_2_num.get(each_distance, 0) 
                for each_distance in xrange(1, 4)])
        y = np.array([
                k_1_num.get(each_distance, 0) 
                for each_distance in xrange(1, 4)])
    
        risk = round(
            np.sum(
                np.divide(np.add(x, y), [1, 2, 3], dtype=float)), 2)
    
        if  risk == 0:
            r = 10
        elif 0 < risk < 5:
            r = 30
        elif 5 <= risk < 10:
            r = 60  
        elif 10 <= risk:
            r = 100     
        else:
            r = 10
    
        return dict(
            x_1=x[0],
            x_2=x[1],
            x_3=x[2],
            y_1=y[0],
            y_2=y[1],
            y_3=y[2],
            g=r
        )
    
    @__fault_tolerant
    def get_feature_21(cls):
        '''
        关联方地址集中度风险
        '''
        legal_person_address = [
            attr['address'] 
            for node, attr in cls.DIG.nodes_iter(data=True) 
            if attr['distance'] <= 3 
            and attr['is_human'] == 0]
        c = Counter(
            filter(
                lambda x: x is not None and len(x) >= 21,  
                legal_person_address))
        n = c.most_common(1)
        n= n[0][1] if len(n) > 0 else 1
        risk = n -1
        
        if  risk == 0:
            z = 10
        elif risk == 1:
            z = 30
        elif risk == 2:
            z = 60  
        elif 2 < risk:
            z = 100             
        
        return dict(
            n=risk,
            y=risk,
            z=z
        )
    
    @__fault_tolerant
    def get_feature_22(cls):
        '''
        利益一致行动法人风险
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
        common_interests_list = [
            1 
            for node_name in relation_set 
            if is_similarity(tar_company_frag, node_name)]
        
        risk = sum(common_interests_list)
        if risk == 0:
            z = 10
        elif risk == 1:
            z = 30
        elif 1 < risk <= 3:
            z = 60
        elif 3 < risk:
            z = 100
        else:
            z = 10
        
        return dict(
            d=risk, 
            z=z
        )        
        
    @__fault_tolerant
    def get_feature_23(cls):
        '''
        关联方信誉风险
        '''
        def get_black_num(distance):
            '''
            某一度黑企业的总数
            '''
            return sum([
                    1
                    for node, attr in cls.DIG.nodes_iter(data=True)
                    if (attr['is_black'] and attr['distance'] == distance 
                            and attr['is_human'] == 0)
            ])
        
        black_relation_set =[
            get_black_num(each_distance) 
            for each_distance in range(1, 4)]
        risk = np.dot(black_relation_set, [1, 1/2., 1/3.])
        if risk == 0:
            z = 10
        elif 0 < risk < 0.5:
            z = 30
        elif 0.5 <= risk < 1:
            z = 60
        elif 1 <= risk:
            z =100
        else:
            z = 10
        
        return dict(
            b_1=black_relation_set[0],
            b_2=black_relation_set[1],
            b_3=black_relation_set[2],
            y=risk,
            z=z
        )
    
    @__fault_tolerant
    def get_feature_24(cls):
        '''
        短期逐利风险
        '''
        def get_max_established(distance, timedelta):
            '''
            获取在某distance关联方企业中，某企业在任意timedelta内投资成立公司数量的最大值
            '''
            #用于获取最大连续时间数，这里i的取值要仔细琢磨一下，这里输入一个时间差序列与时间差
            def get_date_density(difference_list, timedelta):
                time_density_list = []
                for index, date in enumerate(difference_list):
                    if date < timedelta:
                        s = 0
                        i = 1
                        while(s < timedelta and i <= len(difference_list) - index):
                            i += 1
                            s = sum(difference_list[index:index+i])
                        time_density_list.append(i)
                    else:
                        continue
                return max(time_density_list) if len(time_density_list) >0 else 0            

            #distance所有节点集合
            relation_set = [
                node 
                for node, attr in cls.DIG.nodes(data=True) 
                if attr['distance'] == distance]
            investment_dict = defaultdict(lambda : [])     
            
            if len(cls.DIG.edge) > 1:
                for src_node, des_node, edge_attr in (
                        cls.DIG.edges_iter(relation_set, data=True)):
                    if (edge_attr.get('is_invest', 0) == 'INVEST' 
                            and cls.DIG.node[des_node]['distance'] == distance
                            and cls.DIG.node[des_node]['esdate'] is not None 
                            and cls.DIG.node[src_node]['is_human'] == 0):
                        #将所有节点投资的企业的成立时间加进列表中
                        investment_dict[src_node].append(
                            cls.DIG.node[des_node]['esdate'])
             
            #目标企业所有节点所投资的企业时间密度字典
            all_date_density_dict = {}
            
            for node, date_list in investment_dict.iteritems():
                #构建按照时间先后排序的序列
                date_strp_list = sorted(date_list)
                if len(date_strp_list) > 1:
                    #构建时间差的序列，例如：[256, 4, 5, 1, 2, 33, 6, 5, 4, 73]
                    date_difference_list = [
                        (date_strp_list[index + 1] - date_strp_list[index]).days 
                        for index in range(len(date_strp_list) -1)]
                    #计算某法人节点在timedelta天之内有多少家公司成立
                    es_num = get_date_density(date_difference_list, timedelta)
                    if all_date_density_dict.has_key(es_num):
                        all_date_density_dict[es_num].append(node)
                    else:
                        all_date_density_dict[es_num] = [node]              
                else:
                    continue
            keys = all_date_density_dict.keys()        
            max_num = max(keys) if len(keys) > 0 else 0
            
            return max_num
        
        x = [
            get_max_established(each_distance, 180) 
            for each_distance in xrange(1, 4)]
        
        risk = round(
            np.sum(
                np.divide(x, [1, 2, 3], dtype=float)), 2)
        
        if  risk == 0:
            r = 10
        elif 0 < risk <= 0.7:
            r = 30
        elif 0.7 < risk <= 1:
            r = 60  
        elif 1 < risk:
            r = 100       
        else:
            r = 10
        
        return dict(
            x_1=x[0],
            x_2=x[1],
            x_3=x[2],
            z=r,
            d=risk
        )
    
    @classmethod
    def get_some_feature(cls, resultiterable, feature_nums):
        #创建类变量
        cls.resultiterable = resultiterable
        cls.tarcompany = cls.resultiterable.data[0].a
        cls.DIG = cls.create_graph_udf(cls.resultiterable, 1)
        cls.xgxx_distribution = cls.get_feature_xgxx()
        
        feature_list = [
            ('feature_{0}'.format(feature_index), 
                     eval('cls.get_feature_{0}()'.format(feature_index)))
            for feature_index in feature_nums]
        feature_list.append(('bbd_qyxx_id', cls.tarcompany))
        feature_list.append(('company_name', cls.resultiterable.data[0].a_name))
        
        return json.dumps(dict(feature_list), ensure_ascii=False)

    
def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "25g")
    conf.set("spark.executor.instances", 30)
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
        .appName("hgongjing2_one_prd_common_static") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark        
    
def spark_data_flow(tidversion):
    '''
    dataflow
    '''
    #输入
    #算法设计直接用rdd
    tid_df = spark.read.parquet(
        "{path}/common_company_info_merge/{version}".format(path=IN_PATH,
                                                            version=tidversion))
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
                return 'acer'
        return wappen

    @fault_tolerant
    def time_out(data):
        signal.signal(signal.SIGALRM, handler)
        signal.alarm(60)
        result = FeatureConstruction.get_some_feature(data, 
                                                      [_ for _ in range(1, 25)])
        signal.alarm(0)
        return result

    def calculate(data):
        return FeatureConstruction.get_some_feature(data, 
                                                    [_ for _ in range(1, 25)])
        
    feature_list = tid_rdd_2.mapValues(
        time_out
    ).map(
        itemgetter(1)
    )
    
    return feature_list

def run(relation_version):
    '''
    格式化输出
    '''
    pd_df = spark_data_flow(relation_version)
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "common_static_feature_distribution"
         "/{version}").format(path=OUT_PATH, 
                              version=relation_version))
    pd_df.repartition(10).saveAsTextFile(
        ("{path}/"
        "common_static_feature_distribution"
        "/{version}").format(path=OUT_PATH, 
                            version=relation_version))

    
if __name__ == '__main__':  
    import configparser
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")

    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    #输入参数
    IN_PATH = conf.get('common_company_info_merge', 'OUT_PATH')
    OUT_PATH = conf.get('common_company_feature', 'OUT_PATH')
    
    spark = get_spark_session()

    run(relation_version=RELATION_VERSION)
    