# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
common_company_static_feature_v2.py {version}
'''
import sys
import os
import datetime
import requests
import json
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
                    isIPOcompany = row.b_isIPOcompany,
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
                    province = row.b_province,
                    is_fzjg=row.b_is_fzjg,
                    is_new_finance=row.b_is_new_finance,
                    zgcpwsw_specific=row.b_zgcpwsw_specific,
                    common_interests=row.b_is_common_interests,
                    common_address=row.b_is_common_address
                ))  for row in relations] + [(
                row.c, 
                dict(
                    is_human=row.c_isperson,
                    is_black=row.c_is_black_company,
                    distance = row.c_degree,
                    name = row.c_name,
                    isSOcompany = row.c_isSOcompany,
                    isIPOcompany = row.c_isIPOcompany,
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
                    province = row.c_province,
                    is_fzjg=row.c_is_fzjg,
                    is_new_finance=row.c_is_new_finance,
                    zgcpwsw_specific=row.c_zgcpwsw_specific,
                    common_interests=row.c_is_common_interests,
                    common_address=row.c_is_common_address
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
            node 
            for node, attr in cls.DIG.nodes_iter(data=True) 
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
        new_finance = [
            node for ndoe in shareholder
            if cls.DIG.node[node]['is_new_finance']]
        ipo_company = [
            node for ndoe in shareholder
            if cls.DIG.node[node]['isIPOcompany']]
        out_degree_obj = cls.DIG.out_degree()
        if isinstance(out_degree_obj, dict):
            out_degree_list = out_degree_obj.values()
            out_degree_list.sort()
        else:
            out_degree_list = [0]
        
        is_ipo_tarcompany = (
            1 if cls.resultiterable.data[0].a_isIPOcompany is True else 0)
        is_common_interests = (
            1 if cls.resultiterable.data[0].a_is_common_interests else 0)
        is_common_address = (
            1 if cls.resultiterable.data[0].a_is_common_address else 0)
        
        x = len(os_shareholder)
        y = len(anti_os_shareholder)
        z = len(nature_shareholder)
        w = len(new_finance)
        n = len(ipo_company)
        r_i = is_ipo_tarcompany
        r_1 = is_common_interests
        r_2 = is_common_address
        r_3 = out_degree_list[-1]
        r_4 = sum(out_degree_list[-3:])
        
        r = (0.2*x + 0.5*y + z) * 100. * (2 - r_i) / (2*w + 0.001)
        
        return dict(
            x=x,
            y=y,
            z=z,
            n=n,
            w=w,
            r_1=r_1,
            r_2=r_2,
            r_3=r_3,
            r_4=r_4,
            r_i=r_i,
            r=round(r, 2) if r else 100
        )


    @__fault_tolerant
    def get_feature_2(cls):
        '''
        1、资本风险
        2、目标公司任意6个月内注册资本最大增额
        '''
        def get_date_density(difference_list, timedelta):
            '''计算最大连续时间段，并返回他们的index'''
            time_density_list = []
            for index, date in enumerate(difference_list):
                if date < timedelta:
                    s = 0
                    indexes = [index]
                    i = 1
                    while(s < timedelta and i <= len(difference_list) - index):
                        indexes.append(index+i)
                        i += 1
                        s = sum(difference_list[index:index+i])
                    time_density_list.append(indexes)
                else:
                    continue
            if time_density_list:
                ma = 0
                for each_data in time_density_list:
                    if len(each_data) > ma:
                        ma = len(each_data)
                    else:
                        continue
            return time_density_list
    
        def get_date_list(date_list):
            '''获得时间差序列'''
            date_strp_list = sorted(date_list)
            if len(date_strp_list) > 1:
                #构建时间差的序列，例如：[256, 4, 5, 1, 2, 33, 6, 5, 4, 73]
                date_difference_list = [
                    (date_strp_list[index + 1] - date_strp_list[index]).days 
                    for index in range(len(date_strp_list) -1)]
            else:
                date_difference_list = []
            return date_difference_list

                
        captial_list = cls.resultiterable.data[0].a_bgxx_capital
        result = []
        
        if captial_list and len(captial_list) > 1:
            captial_list = sorted(captial_list, key=lambda r: r[0])
            date_list = map(
                lambda r: datetime.datetime.strptime(
                    r[0], "%Y-%m-%d"), captial_list)
            max_date_indexes = get_date_density(get_date_list(date_list), 180)
            for each_index in max_date_indexes:  
                result.append(
                    abs(
                        float(captial_list[each_index[-1]][1]) - 
                        float(captial_list[each_index[0]][1] )))
        elif captial_list and len(captial_list) ==1:
            result.append(float(captial_list[0][1]))
            
            
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
            x_1=max(result) if result else 0,
            y=y,
            c=c_i * (1 - p_i/2.)
        )
    
    @__fault_tolerant
    def get_feature_3(cls):
        '''
        资本背景风险
        '''
        def get_relation_set(distance):
            return [
                attr['name']
                for node, attr in cls.DIG.nodes_iter(data=True)
                if attr['distance'] == distance]

        from wtyh_company_type_20160816 import my_dict
        import numpy as np
        
        ratio = np.array([1, 1, 0.75, 0.5])
        reducing_score = np.array([-20, -40, -30, -15])
        
        #记录目标企业关联方中的所有企业类别
        matrx = np.zeros([4, 4], dtype=int)
        
        #计算关联方的数据
        degree_zero_list = get_relation_set(0)
        degree_one_list = get_relation_set(1)
        degree_two_list = get_relation_set(2)
        degree_three_list = get_relation_set(3)
        
        for degree_zero_company in degree_zero_list:
            if my_dict.has_key(degree_zero_company):
                row_index = my_dict[degree_zero_company]
                col_index = 0
                matrx[row_index][col_index] += 1

        
        for degree_one_company in degree_one_list:
            if my_dict.has_key(degree_one_company):
                row_index = my_dict[degree_one_company]
                col_index = 1
                matrx[row_index][col_index] += 1    

        
        for degree_two_company in degree_two_list:
            if my_dict.has_key(degree_two_company):
                row_index = my_dict[degree_two_company]
                col_index = 2
                matrx[row_index][col_index] += 1    

        
        for degree_three_company in degree_three_list:
            if my_dict.has_key(degree_three_company):
                row_index = my_dict[degree_three_company]
                col_index = 3
                matrx[row_index][col_index] += 1

        
        #资本背景风险有一个特殊的判断条件：必须小于等于0
        B = reducing_score.dot(matrx).dot(ratio) 
        B = B if B >= -30 else -30
        
        #各度的条件
        data = matrx.sum(axis=1).tolist()
        
        return dict(
            z=B + 30,
            x_0=data[0],
            x_1=data[1],
            x_2=data[2],
            x_3=data[3],
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
        
        return dict(
            k_1=k_1,
            k_2=k_2,
            k=k_3
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
                status_code = requests.get(url, 
                                           allow_redirects=False).status_code 
                p_i = 1 if status_code == 200 else 0
            else:
                p_i = 0
        except:
            p_i = 0
        
        l = c_i * p_i
        
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
                ('z', 0)
            ]
        )
        
        if cls.resultiterable.data[0].a_bgxx is not None:
            for bgxx_name, bgxx_num in (
                    cls.resultiterable.data[0].a_bgxx.iteritems()):
                default_result[get_bgxx_symbol(bgxx_name)] += int(bgxx_num)
            default_result['z'] = np.dot(default_result.values(), 
                                         [2, 1, 1, 1, 2, 0, 0])
                
        default_result.pop('c_6')
        
        return dict(default_result)
        
    @__fault_tolerant
    def get_feature_7(cls):
        '''
        1、人才结构风险
        2、目标公司招聘行业分布
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
                ('e_4', 0),
                ('e_5', 0),
                ('e_6', 0),
                ('e', 0),
                ('z', 0)
            ]
        )

        recruit_data = cls.resultiterable.data[0].a_recruit_industry
        if recruit_data:
            for key, value in recruit_data.iteritems():
                if u'金融' in key:
                    default_result['e_4'] += int(value)
                if (u'租赁' or u'商务') in key:
                    default_result['e_5'] += int(value)
                if (u'信息传输' or u'软件' or u'信息技术') in key:
                    default_result['e_6'] += int(value)
        
        if cls.resultiterable.data[0].a_recruit is not None:
            for education_name, education_num in (
                    cls.resultiterable.data[0].a_recruit.iteritems()):
                default_result[
                    get_recruit_num(education_name)] += int(education_num)
            default_result['e'] = sum(
                map(int ,cls.resultiterable.data[0].a_recruit.values()))
            default_result['z'] = (
                default_result['e_1'] / default_result['e'] * 15.)
            
        return dict(default_result)
        
    @__fault_tolerant
    def get_feature_8(cls):
        '''
        公司运营持续风险
        '''
        import math
        
        esdate_relation_set = [
            (datetime.date.today() - attr['esdate']).days 
            for node, attr in cls.DIG.nodes_iter(data=True) 
            if 0 < attr['distance'] <= 3 
            and attr['is_human'] == 0 
            and attr['esdate']]
        t_2 = round(np.average(esdate_relation_set), 2)
        
        if (cls.DIG.node.get(cls.tarcompany, 0) and 
                    cls.DIG.node[cls.tarcompany]['esdate']):
            t_1 = (datetime.date.today() - 
                       cls.DIG.node[cls.tarcompany]['esdate']).days 
        else :
            t_1 = 0
       
        leagal_person_set = [
            (datetime.date.today() - attr['esdate']).days 
            for node, attr in cls.DIG.nodes_iter(data=True) 
            if attr['distance'] == 1
            and attr['is_human'] == 0 
            and attr['esdate']
        ]
        t_3 = round(np.average(leagal_person_set), 2)
    
        return dict(
            t_2=t_2 if not math.isnan(t_2) else 0,
            t_1=t_1,
            t_3=t_3 if not math.isnan(t_3) else 0,
            y=t_1
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
        
        return  dict(
            n_1=n_1,
            n_2=n_2,
            n=n
        )
        
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
        def get_certain_distance_all_info(distance, document_types):
            '''
            总条数
            '''
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
        
        def get_certain_distance_add_info(distance, document_types):
            '''
            出现次数
            '''
            all_array = []
            #处理某一个distance不存在节点的情况
            all_array.append([0]*len(document_types))            
            for node, attr in cls.DIG.nodes_iter(data=True):
                if attr['is_human'] == 0  and attr['distance'] == distance:
                    each_array = map(lambda x: 1 if x else 0, 
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
                     'estatus', 'zgcpwsw_specific']
        
        xgxx_add_type = ['ktgg_1', 'zgcpwsw_1', 'rmfygg_1', 
                         'lending_1', 'xzcf_1', 'zhixing_1', 
                         'dishonesty_1', 'jyyc_1',
                         'estatus_1', 'zgcpwsw_specific_1']

        for each_distance in xrange(0, 4):
            xgxx_num_list = get_certain_distance_all_info(each_distance, 
                                                          xgxx_type)
            xgxx_add_num_list = get_certain_distance_add_info(each_distance, 
                                                              xgxx_type)
            matrx[each_distance] = dict(zip(xgxx_type, xgxx_num_list)+
                                        zip(xgxx_add_type, xgxx_add_num_list))
            
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
        1、法律诉讼风险：开庭公告、裁判文书、法院公告、民间借贷
        2、某度关联方中非法集资裁判文书总数
        '''
        tar_xgxx = ['ktgg', 'zgcpwsw', 'rmfygg', 'lending', 'zgcpwsw_specific',
                    'ktgg_1', 'zgcpwsw_1', 'rmfygg_1', 
                    'lending_1', 'zgcpwsw_specific_1']
        risk = cls.filter_xgxx_type(tar_xgxx)
        
        risk['z'] = round(
            np.dot(
                map(sum, [
                        risk[each_distance].values() 
                        for each_distance in xrange(0, 4)]), 
                [1., 1/2., 1/3., 1/4.]), 2)
        
        return risk
            
    @__fault_tolerant
    def get_feature_11(cls):
        '''
        行政处罚风险
        '''
        tar_xgxx = ['xzcf', 'xzcf_1']
        risk = cls.filter_xgxx_type(tar_xgxx)
        
        risk['z'] = round(
            np.dot(
                map(sum, [risk[each_distance].values() 
                        for each_distance in xrange(0, 4)]), 
                [1., 1/2., 1/3., 1/4.]), 2)
        
        return risk
    
    @__fault_tolerant
    def get_feature_12(cls):
        '''
        被执行风险：被执行、失信被执行
        '''
        tar_xgxx = ['zhixing', 'dishonesty', 'zhixing_1', 
                    'dishonesty_1']
        risk = cls.filter_xgxx_type(tar_xgxx)        
        
        risk['z'] = round(
            np.dot(
                map(
                    lambda x: x['zhixing'] + 2. * x['dishonesty'], 
                    [risk[each_distance] for each_distance in xrange(0, 4)]), 
                [1., 1/2., 1/3., 1/4.]), 2)     
        
        return risk
        
    @__fault_tolerant
    def get_feature_13(cls):
        '''
        异常经营风险
        '''
        tar_xgxx = ['jyyc', 'estatus', 'jyyc_1', 'estatus_1']
        risk = cls.filter_xgxx_type(tar_xgxx)        
        
        risk['z'] = round(
            np.dot(
                map(
                    lambda x: x['jyyc'] + 2. * x['estatus'], 
                    [risk[each_distance] for each_distance in xrange(0, 4)]), 
                [1., 1/2., 1/3., 1/4.]), 2)
        
        return risk        
        
    @__fault_tolerant
    def get_feature_14(cls):
        '''
        银监会行政处罚
        '''
        tar_xgxx = ['circxzcf']
        risk = cls.filter_xgxx_type(tar_xgxx)        
        
        risk['z'] = round(
            np.dot(
                map(
                    lambda x: x['circxzcf'], 
                    [risk[each_distance] for each_distance in xrange(0, 4)]), 
                [1., 1/2., 1/3., 1/4.]), 2)
     
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
                    2*(nature_max_control + legal_max_control) + 
                    (nature_avg_control + legal_avg_control)) /
                (2*total_legal_num + 0.001)), 2)
        
        return dict(
            x_1=nature_max_control,
            x_2=legal_max_control,
            y_1=nature_avg_control,
            y_2=legal_avg_control,
            z=total_legal_num,
            r=risk
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
        
        def get_node_set_two(company_type):
            return Counter([
                    attr['distance']
                    for node, attr in cls.DIG.nodes_iter(data=True)
                    if attr['is_human'] == 0
                    and attr[company_type]])
        
        
        nature_person_distribution = get_node_set(1)
        legal_person_distribution = get_node_set(0)
        so_company_distribution = get_node_set_two('isSOcompany')
        ipo_company_distribution = get_node_set_two('isIPOcompany')
        
        nature_person_num = [
            nature_person_distribution.get(each_distance, 0)
            for each_distance in range(1, 4)]
        legal_person_num = [
            legal_person_distribution.get(each_distance, 0)
            for each_distance in range(1, 4)]
        so_company_num = [
            so_company_distribution.get(each_distance, 0)
            for each_distance in range(1, 4)]
        ipo_company_num = [
            ipo_company_distribution.get(each_distance, 0)
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
        
        return dict(
            x_1=legal_person_num[0],
            x_2=legal_person_num[1],
            x_3=legal_person_num[2],
            y_1=nature_person_num[0],
            y_2=nature_person_num[1],
            y_3=nature_person_num[2],
            v_1=so_company_num[0],
            v_2=so_company_num[1],
            v_3=so_company_num[2],
            w_1=ipo_company_num[0],
            w_2=ipo_company_num[1],
            w_3=ipo_company_num[2],
            r=risk if risk < 10000 else 0
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
        
        return dict(
            x=legal_person_subsidiary,
            y=legal_person_shareholder,
            r=risk
        )

    @__fault_tolerant
    def get_feature_18(cls):
        '''
        分支机构过度扩张风险
        '''
        risk = cls.resultiterable.data[0].a_fzjg
        risk = risk if risk is not None else 0
        
        return dict(
            d=risk, 
            z=risk
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
        
        y_2 = x[2] / (x[1]+x[2])
        y_3 = x[3] / (x[1]+x[2]+x[3])
        risk = y_2/2 + y_3/3
        
        return dict(
            x_1=x[1],
            x_2=x[2],
            x_3=x[3],
            y_2=y_2,
            y_3=y_3,
            z=risk if risk else 0
        )        
    
    @__fault_tolerant
    def get_feature_20(cls):
        '''
        1、潜在违规融资风险
        2、某度关联方中新金融企业数量
        '''    
        def get_relation_info(distance):
            return sum([
                1
                for node, attr in cls.DIG.nodes_iter(data=True)
                if attr['distance'] == distance 
                and attr['is_new_finance']
            ])
        
        def get_relation_risk_num(keyword_type):
            return Counter([attr['distance']
                    for node, attr in cls.DIG.nodes_iter(data=True) 
                    if attr['is_human'] == 0 and 
                    attr['opescope'] == keyword_type])
        
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
    
        return dict(
            x_1=x[0],
            x_2=x[1],
            x_3=x[2],
            y_1=y[0],
            y_2=y[1],
            y_3=y[2],
            z_1=get_relation_info(1),
            z_2=get_relation_info(2),
            z_3=get_relation_info(3),
            g=risk
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
                 
        return dict(
            n=risk,
            y=risk
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
        #tar_company = cls.resultiterable.data[0].a_name
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
        
        return dict(
            d=risk, 
            z=risk
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
        
        def get_common_interests_num(distance):
            '''
            与黑名单库中任意一家企业存在利益一致行动关系
            '''
            return sum([
                    1
                    for node, attr in cls.DIG.nodes_iter(data=True)
                    if attr['distance'] == distance 
                    and attr['common_interests']
            ])
        
        def get_common_address_num(distance):
            '''
            与黑名单库中任意一家企业地址相同
            '''
            return sum([
                    1
                    for node, attr in cls.DIG.nodes_iter(data=True)
                    if attr['distance'] == distance 
                    and attr['common_address']
            ])
        
        black_relation_set =[
            get_black_num(each_distance) 
            for each_distance in range(1, 4)]
        common_interests_set=[
            get_common_interests_num(each_distance) 
            for each_distance in range(1, 4)]
        common_address_set=[
            get_common_address_num(each_distance) 
            for each_distance in range(1, 4)]
        
        risk = np.dot(black_relation_set, [1, 1/2., 1/3.])
        
        return dict(
            b_1=black_relation_set[0],
            b_2=black_relation_set[1],
            b_3=black_relation_set[2],
            c_1=common_interests_set[0],
            c_2=common_interests_set[1],
            c_3=common_interests_set[2],
            d_1=common_address_set[0],
            d_2=common_address_set[1],
            d_3=common_address_set[2],
            y=risk
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
            for each_distance in xrange(0, 4)]
        
        legal_person_num = sum([
            1 
            for node, attr in cls.DIG.nodes(data=True) 
            if not attr['is_human']])
        
        risk = round(
            np.sum(
                np.divide(x, [1, 2, 3, 4], dtype=float)), 2) / legal_person_num * 15
        
        return dict(
            x_0=x[0],
            x_1=x[1],
            x_2=x[2],
            x_3=x[3],
            w=legal_person_num,
            z=risk
        )
    
    @__fault_tolerant
    def get_feature_25(cls):
        '''
        目标公司在任意6个月内分支机构数量的最大值
        '''
        
        def get_max_established(distance, timedelta):
            '''
            获取在某distance关联方企业中，某企业在任意timedelta内投资成立公司（分支机构）数量的最大值
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
                            and cls.DIG.node[src_node]['is_human'] == 0
                            and cls.DIG.node[des_node]['is_fzjg']):
                        #将所有节点投资的企业（分支机构）的成立时间加进列表中
                        investment_dict[src_node].append(cls.DIG.node[des_node]['esdate'])
             
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
            for each_distance in xrange(0, 4)]
        
        return dict(
            x_0=x[0],
            x_1=x[1],
            x_2=x[2],
            x_3=x[3]
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
    conf.set("spark.submit.pyFiles", WTYH_DEDUCTION_COMPANY_LIST)
    
    spark = SparkSession \
        .builder \
        .appName("hgongjing2_one_prd_common_static_v2") \
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
        "{path}/common_company_info_merge_v2/{version}".format(path=IN_PATH,
                                                               version=tidversion))
    tid_rdd = tid_df.rdd
        
    #最终计算流程
    tid_rdd_2 = tid_rdd.map(lambda row: (row.a_name, row)) \
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
        result = FeatureConstruction.get_some_feature(
            data,[_ for _ in range(1, 26)])
        signal.alarm(0)
        return result        
        
    feature_list = tid_rdd_2.mapValues(
        time_out
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
         "common_static_feature_distribution_v2"
         "/{version}").format(path=OUT_PATH, 
                              version=RELATION_VERSION))
    pd_df.repartition(10).saveAsTextFile(
        ("{path}/"
        "common_static_feature_distribution_v2"
        "/{version}").format(path=OUT_PATH, 
                             version=RELATION_VERSION))

if __name__ == '__main__':  
    #中间结果版本
    RELATION_VERSION = sys.argv[1]
    
    #输入参数
    IN_PATH = "/user/antifraud/hongjing2/dataflow/step_one/tid/"
    OUT_PATH = "/user/antifraud/hongjing2/dataflow/step_one/prd/"
    WTYH_DEDUCTION_COMPANY_LIST = (
        "hdfs://bbdc6ha/user/antifraud/source/company_type_for_capital_risk/"
        "wtyh_company_type_20160816.py"
    )
    
    spark = get_spark_session()

    run()