# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 15g \
common_company_info_merge_v2.py {version}
'''

import os
import sys

import configparser
from pyspark.sql.types import (StructType, StructField, 
                               StringType, IntegerType, 
                               BooleanType)
from pyspark.sql import functions as fun
from pyspark.sql import  types as tp
from pyspark.sql import SparkSession, Row
from pyspark.conf import SparkConf

def is_common_interests(frag, company_name):
    '''
    目标工公司是否与企业存在利益一致行动关系
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
            return 1 if is_similarity > 0 else 0        
        except:
            return 0
    return is_similarity(frag, company_name)


def is_common_address(address1, address2):
    '''
    是否有相同的address
    '''
    return 1 if address1 == address2 else 0

def get_read_path(file_name, version):
    return "{path}/{file_name}/{version}".format(version=version,
                                                 file_name=file_name,
                                                 path=IN_PATH)

def is_invest(edge_iter):
    '''
    将关联图中的各种关系变成：投资/非投资关系
    '''
    for each_edge in edge_iter:
        if each_edge.bc_relation == 'INVEST':
            return Row(
                a=each_edge.a,
                b=each_edge.b,
                c=each_edge.c,
                b_degree=each_edge.b_degree,
                c_degree=each_edge.c_degree,
                bc_relation=each_edge.bc_relation,
                b_isperson=each_edge.b_isperson,
                c_isperson=each_edge.c_isperson,
                a_name=each_edge.a_name,
                b_name=each_edge.b_name,
                c_name=each_edge.c_name)
    else:
        return Row(
            a=edge_iter.data[0].a,
            b=edge_iter.data[0].b,
            c=edge_iter.data[0].c,
            b_degree=edge_iter.data[0].b_degree,
            c_degree=edge_iter.data[0].c_degree,
            bc_relation='UNINVEST',
            b_isperson=edge_iter.data[0].b_isperson,
            c_isperson=edge_iter.data[0].c_isperson,
            a_name=edge_iter.data[0].a_name,
            b_name=edge_iter.data[0].b_name,
            c_name=edge_iter.data[0].c_name)
            
def raw_spark_data_flow():
    '''
    新版关联方处理（投资与被投资关系）
    
    2.0：读取所以输入数据
    2.1：解析关联方，获取全量公司列表 
        (sample_df + relation_df) -> tid_df
    2.2：将所有公司信息计算出来 
        tid_df -> (tid_company_list_df + ...) -> tid_company_info_df
    2.3：根据样本列表构建属性图 
        (sample_df + tid_df + tid_company_info_df) -> tid_company_merge_df
    
    '''
    is_new_finance_udf = fun.udf(lambda r: True, tp.BooleanType())
    
    #2.0 输入数据读取
        
    sample_df = spark.read.parquet(
        get_read_path(file_name='ljr_sample', 
                      version=LEIJINRONG_VERSION))

    so_df = spark.read.parquet(
        get_read_path(file_name='qyxx_state_owned_enterprise_background', 
                      version=SO_VERSION))

    basic_df = spark.read.parquet(
        get_read_path(file_name='basic', 
                      version=BASIC_VERSION))

    zhuanli_count_df = spark.read.parquet(
        get_read_path(file_name='zhuanli', 
                      version=ZHUANLI_VERSION))

    shangbiao_count_df = spark.read.parquet(
        get_read_path(file_name='shangbiao', 
                      version=SHANGBIAO_VERSION))

    domain_website_df = spark.read.parquet(
        get_read_path(file_name='domain_website', 
                      version=DOMAIN_WEBSITE_VERSION))

    bgxx_df = spark.read.parquet(
        get_read_path(file_name='bgxx', 
                      version=BGXX_VERSION))
    
    bgxx_capital_df = spark.read.parquet(
        get_read_path(file_name='bgxx_capital', 
                      version=BGXX_VERSION))    

    recruit_df = spark.read.parquet(
        get_read_path(file_name='recruit', 
                      version=RECRUIT_VERSION))
    
    recruit_industry_df = spark.read.parquet(
        get_read_path(file_name='recruit_industry', 
                      version=RECRUIT_VERSION))
    
    zhaobiao_count_df = spark.read.parquet(
        get_read_path(file_name='zhaobiao', 
                      version=ZHAOBIAO_VERSION))

    zhongbiao_count_df = spark.read.parquet(
        get_read_path(file_name='zhongbiao', 
                      version=ZHONGBIAO_VERSION))

    ktgg_count_df = spark.read.parquet(
        get_read_path(file_name='ktgg', 
                      version=KTGG_VERSION))

    zgcpwsw_count_df = spark.read.parquet(
        get_read_path(file_name='zgcpwsw', 
                      version=ZGCPWSW_VERSION))

    zgcpwsw_specific_count_df = spark.read.parquet(
        get_read_path(file_name='zgcpwsw_specific', 
                      version=ZGCPWSW_VERSION))

    rmfygg_count_df = spark.read.parquet(
        get_read_path(file_name='rmfygg', 
                      version=RMFYGG_VERSION))

    lawsuit_count_df = spark.read.parquet(
        get_read_path(file_name='lawsuit', 
                      version=RMFYGG_VERSION))
    
    xzcf_count_df = spark.read.parquet(
        get_read_path(file_name='xzcf', 
                      version=XZCF_VERSION))

    zhixing_count_df = spark.read.parquet(
        get_read_path(file_name='zhixing', 
                      version=ZHIXING_VERSION))
    
    dishonesty_count_df = spark.read.parquet(
        get_read_path(file_name='dishonesty', 
                      version=DISHONESTY_VERSION))
    
    jyyc_count_df = spark.read.parquet(
        get_read_path(file_name='jyyc', 
                      version=JYYC_VERSION))

    circxzcf_count_df = spark.read.parquet(
        get_read_path(file_name='circxzcf', 
                      version=CIRCXZCF_VERSION))

    fzjg_count_df = spark.read.parquet(
        get_read_path(file_name='fzjg', 
                      version=FZJG_VERSION))

    fzjg_name_df = spark.read.parquet(
        get_read_path(file_name='fzjg_name', 
                      version=FZJG_VERSION))

    namefrag_df = spark.read.parquet(
        get_read_path(file_name='namefrag', 
                      version=LEIJINRONG_VERSION))
    
    black_df = spark.read.parquet(
        get_read_path(file_name='black_company', 
                      version=BLACK_VERSION))
    
    black_province_df = spark.read.parquet(
        get_read_path(file_name='black_province', 
                      version=BLACK_VERSION))

    leijinrong_province_df = spark.read.parquet(
        get_read_path(file_name='leijinrong_province', 
                      version=LEIJINRONG_VERSION))
    
    new_finance_df = spark.read.parquet(
        get_read_path(file_name='ljr_sample', 
                      version=LEIJINRONG_VERSION)
    ).withColumn(
        'is_new_finance', is_new_finance_udf('bbd_qyxx_id')
    )

    #原始关联方
    relation_df = spark.sql(
        '''SELECT 
        bbd_qyxx_id                          a,
        source_bbd_id                       b,
        destination_bbd_id                c,
        company_name                     a_name,
        source_name                         b_name,
        destination_name                  c_name,
        source_degree                       b_degree,
        destination_degree               c_degree,
        relation_type                         bc_relation,
        source_isperson                    b_isperson,
        destination_isperson            c_isperson
        FROM 
        dw.off_line_relations 
        WHERE 
        dt='{version}'  
        AND
        source_degree <= 3
        AND
        destination_degree <= 3
        '''.format(version=RELATION_VERSION)
    )
    
    
    #2.1 解析关联方，获取全量公司列表
    #由于历史关联方的更新问题，这里从sample中选取最新的bbd_qyxx_id
    tid_df = sample_df.join(
        relation_df,
        fun.trim(relation_df.a_name) == fun.trim(sample_df.company_name),
        'left_outer'
    ).select(
        sample_df.bbd_qyxx_id.alias('a'),
        'b','c',
        'b_degree','c_degree', 'bc_relation' ,
        'b_isperson','c_isperson',
        sample_df.company_name.alias('a_name'),
        'b_name','c_name'
    )
    
    glf_schema = StructType([
            StructField('a',StringType(),True),
            StructField('b',StringType(),True),
            StructField('c',StringType(),True),
            StructField('b_degree',IntegerType(),True),
            StructField('c_degree',IntegerType(),True),
            StructField('bc_relation',StringType(),True),
            StructField('b_isperson',IntegerType(),True),
            StructField('c_isperson',IntegerType(),True),
            StructField('a_name',StringType(),True),
            StructField('b_name',StringType(),True),
            StructField('c_name',StringType(),True)])
    
    tid_df = tid_df.rdd.map(lambda r: ((r.a, r.b, r.c), r)) \
        .groupByKey().mapValues(is_invest) \
        .map(lambda r: r[1]) \
        .toDF(schema=glf_schema) \
        .cache()
        
    tid_company_list_df = tid_df.select(
        'a', 'a_name'
    ).union(
        tid_df.where(
            tid_df.b_isperson == 0
        ).select(
            'b', 'b_name'
        )
    ).union(
        tid_df.where(
            tid_df.c_isperson == 0
        ).select(
            'c', 'c_name'
        )
    ).withColumnRenamed(
        'a', 'bbd_qyxx_id'
    ).withColumnRenamed(
        'a_name', 'company_name'
    ).dropDuplicates(
        ['bbd_qyxx_id']
    ).cache()
    
    #2.2 合并所有公司的相关信息
    #国企
    tid_company_info_df = tid_company_list_df.join(
        so_df,
        tid_company_list_df.bbd_qyxx_id == so_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_list_df.bbd_qyxx_id,
        tid_company_list_df.company_name,
        fun.when(
            so_df.company_type.isNotNull(), True
        ).otherwise(False).alias('isSOcompany')
    )
        
    #基本信息
    tid_company_info_df = tid_company_info_df.join(
        basic_df,
        basic_df.bbd_qyxx_id == tid_company_info_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany',
        fun.when(
            basic_df.ipo_company != 'NULL', True
        ).otherwise(False).alias('isIPOcompany'),
        basic_df.realcap_amount.alias('realcap'),
        basic_df.regcap_amount.alias('regcap'),
        basic_df.esdate.alias('regtime'),
        basic_df.operate_scope.alias('opescope'),
        basic_df.address.alias('address'),
        basic_df.enterprise_status.alias('estatus'),
        basic_df.company_province.alias('province')    
    )
        
    
    #专利
    tid_company_info_df = tid_company_info_df.join(
        zhuanli_count_df,
        tid_company_info_df.bbd_qyxx_id == zhuanli_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        zhuanli_count_df.zhuanli_num.alias('zhuanli')
    )
    
    #商标
    tid_company_info_df = tid_company_info_df.join(
        shangbiao_count_df,
        tid_company_info_df.bbd_qyxx_id == shangbiao_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli',
        shangbiao_count_df.shangbiao_num.alias('shangbiao')
    )
    
    #域名与网址
    tid_company_info_df = tid_company_info_df.join(
        domain_website_df,
        tid_company_info_df.bbd_qyxx_id == domain_website_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao',
        domain_website_df.homepage_url.alias('url'),
        domain_website_df.record_license.alias('ICP')
    )
    
    #变更信息
    tid_company_info_df = tid_company_info_df.join(
        bgxx_df,
        tid_company_info_df.bbd_qyxx_id == bgxx_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP',
        bgxx_df.bgxx_dict.alias('bgxx')
    )
    
    #招聘信息
    tid_company_info_df = tid_company_info_df.join(
        recruit_df,
        tid_company_info_df.bbd_qyxx_id == recruit_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 
        recruit_df.recruit_dict.alias('recruit')
    )
    
    #招标信息
    tid_company_info_df = tid_company_info_df.join(
        zhaobiao_count_df,
        tid_company_info_df.bbd_qyxx_id == zhaobiao_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        zhaobiao_count_df.zhaobiao_num.alias('zhaobiao') 
    )
    
    #中标信息
    tid_company_info_df = tid_company_info_df.join(
        zhongbiao_count_df,
        tid_company_info_df.bbd_qyxx_id == zhongbiao_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao',
        zhongbiao_count_df.zhongbiao_num.alias('zhongbiao') 
    )
    
    #开庭公告
    tid_company_info_df = tid_company_info_df.join(
        ktgg_count_df,
        tid_company_info_df.bbd_qyxx_id == ktgg_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 
        ktgg_count_df.ktgg_num.alias('ktgg') 
    )
    
    #裁判文书
    tid_company_info_df = tid_company_info_df.join(
        zgcpwsw_count_df,
        tid_company_info_df.bbd_qyxx_id == zgcpwsw_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg',
        zgcpwsw_count_df.zgcpwsw_num.alias('zgcpwsw') 
    )
    
    #法院公告
    tid_company_info_df = tid_company_info_df.join(
        rmfygg_count_df,
        tid_company_info_df.bbd_qyxx_id == rmfygg_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg',
        'zgcpwsw',
        rmfygg_count_df.rmfygg_num.alias('rmfygg') 
    )
    
    #民间借贷
    tid_company_info_df = tid_company_info_df.join(
        lawsuit_count_df,
        tid_company_info_df.bbd_qyxx_id == lawsuit_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg',
        'zgcpwsw', 'rmfygg',
        lawsuit_count_df.lawsuit_num.alias('lending') 
    )
    
    #行政处罚
    tid_company_info_df = tid_company_info_df.join(
        xzcf_count_df,
        tid_company_info_df.bbd_qyxx_id == xzcf_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg',
        'zgcpwsw', 'rmfygg', 'lending',
        xzcf_count_df.xzcf_num.alias('xzcf') 
    )
    
    #被执行
    tid_company_info_df = tid_company_info_df.join(
        zhixing_count_df,
        tid_company_info_df.bbd_qyxx_id == zhixing_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf', 
        zhixing_count_df.zhixing_num.alias('zhixing') 
    )
    
    #失信被执行
    tid_company_info_df = tid_company_info_df.join(
        dishonesty_count_df,
        tid_company_info_df.bbd_qyxx_id == dishonesty_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 
        dishonesty_count_df.dishonesty_num.alias('dishonesty') 
    )
    
    #经营异常
    tid_company_info_df = tid_company_info_df.join(
        jyyc_count_df,
        tid_company_info_df.bbd_qyxx_id == jyyc_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 
        jyyc_count_df.jyyc_num.alias('jyyc')
    )
    
    #银监会处罚
    tid_company_info_df = tid_company_info_df.join(
        circxzcf_count_df,
        tid_company_info_df.bbd_qyxx_id == circxzcf_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 'jyyc',
        circxzcf_count_df.circxzcf_num.alias('circxzcf')
    )
    
    #分支机构
    tid_company_info_df = tid_company_info_df.join(
        fzjg_count_df,
        tid_company_info_df.bbd_qyxx_id == fzjg_count_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 'jyyc',
        'circxzcf', 
        fzjg_count_df.fzjg_num.alias('fzjg')
    )
    
    #公司字号
    tid_company_info_df = tid_company_info_df.join(
        namefrag_df,
        tid_company_info_df.company_name == namefrag_df.company_name,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 'jyyc',
        'circxzcf', 'fzjg',
        namefrag_df.namefrag.alias('namefrag')
    )
    
    #某省份黑企业数
    tid_company_info_df = tid_company_info_df.join(
        black_province_df,
        tid_company_info_df.province == black_province_df.company_province,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 'jyyc',
        'circxzcf', 'fzjg', 'namefrag',
        black_province_df.province_black_num
    )
    
    #某省份类金融企业数
    tid_company_info_df = tid_company_info_df.join(
        leijinrong_province_df,
        tid_company_info_df.province == leijinrong_province_df.company_province,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 'jyyc',
        'circxzcf', 'fzjg', 'namefrag',
        'province_black_num',
        leijinrong_province_df.province_leijinrong_num
    )
    
    #是否是黑企业
    tid_company_info_df = tid_company_info_df.join(
        black_df,
        tid_company_info_df.company_name == black_df.company_name,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 'jyyc',
        'circxzcf', 'fzjg', 'namefrag',
        'province_black_num', 'province_leijinrong_num',
        fun.when(
            black_df.company_type == 'black', True
        ).otherwise(False).alias('is_black_company')
    )    
    
    #是否是分支机构
    tid_company_info_df = tid_company_info_df.join(
        fzjg_name_df,
        fzjg_name_df.fzjg_name == tid_company_info_df.company_name,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 'jyyc',
        'circxzcf', 'fzjg', 'namefrag',
        'province_black_num', 'province_leijinrong_num',
        'is_black_company',
        fzjg_name_df.is_fzjg
    )

    #招聘行业数据
    tid_company_info_df = tid_company_info_df.join(
        recruit_industry_df,
        tid_company_info_df.bbd_qyxx_id == recruit_industry_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 'jyyc',
        'circxzcf', 'fzjg', 'namefrag',
        'province_black_num', 'province_leijinrong_num',
        'is_black_company', 'is_fzjg',
        recruit_industry_df.recruit_dict.alias('recruit_industry')
    )

    #注册资本变更详情
    tid_company_info_df = tid_company_info_df.join(
        bgxx_capital_df,
        bgxx_capital_df.bbd_qyxx_id == tid_company_info_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 'jyyc',
        'circxzcf', 'fzjg', 'namefrag',
        'province_black_num', 'province_leijinrong_num',
        'is_black_company', 'is_fzjg', 'recruit_industry',
        bgxx_capital_df.tid_list.alias('bgxx_capital')
    )

    #是否是新金融企业
    tid_company_info_df = tid_company_info_df.join(
        new_finance_df,
        new_finance_df.bbd_qyxx_id == tid_company_info_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 'jyyc',
        'circxzcf', 'fzjg', 'namefrag',
        'province_black_num', 'province_leijinrong_num',
        'is_black_company', 'is_fzjg', 'recruit_industry',
        'bgxx_capital',
        new_finance_df.is_new_finance.alias('is_new_finance')
    )
    
    #非法集资案件数
    tid_company_info_df = tid_company_info_df.join(
        zgcpwsw_specific_count_df,
        (zgcpwsw_specific_count_df.bbd_qyxx_id == 
            tid_company_info_df.bbd_qyxx_id),
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 'jyyc',
        'circxzcf', 'fzjg', 'namefrag',
        'province_black_num', 'province_leijinrong_num',
        'is_black_company', 'is_fzjg', 'recruit_industry',
        'bgxx_capital', 'is_new_finance',
        zgcpwsw_specific_count_df.zgcpwsw_specific_num.alias(
            'zgcpwsw_specific'
        )
    ).cache()

    #某公司是否与黑名单库中任意一家企业存在利益一致行动关系（是1否0）
    #是否与黑名单库中任意一家企业地址相同（是1否0）
    def get_company_namefrag(iterator):
        '''
        构建DAG；这里因为涉及到加载词典，只能用mapPartition，不然IO开销太大
        '''
        try:
            from dafei_keyword import KeywordExtr
            _obj = KeywordExtr("city", "1gram.words", 
                               "2gram.words", "new.work.words")
            keyword_list = []
            for row in iterator:
                keyword_list.append(
                    (row.company_name, _obj.clean(row.company_name)))
            return keyword_list
        except Exception, e:
            return e
            
    all_company_namefrag_df = tid_company_info_df.select(
        'company_name'
    ).rdd.repartition(
        100
    ).mapPartitions(
        get_company_namefrag
    ).map(
        lambda r: Row(company_name=r[0], namefrag=r[1])
    ).toDF(
    ).cache()
        
    all_company_address_df = all_company_namefrag_df.join(
        basic_df,
        basic_df.company_name == all_company_namefrag_df.company_name,
        'left_outer'
    ).select(
        basic_df.bbd_qyxx_id,
        all_company_namefrag_df.namefrag,
        basic_df.address.alias('all_company_address')
    ).dropDuplicates(
        ['bbd_qyxx_id']
    ).cache()
    
    black_address_df = black_df.join(
        basic_df,
        basic_df.company_name == black_df.company_name,
        'left_outer'
    ).select(
        black_df.company_name,
        basic_df.address.alias('black_address')
    ).dropDuplicates(
        ['company_name']
    ).cache()
    
    #中间结果落地
    os.system(
        "hadoop fs -rmr "
        "{path}/*".format(path=TMP_PATH))
    all_company_address_df.repartition(10).write.parquet(
        "{path}/"
        "all_company_address_df/{version}".format(version=RELATION_VERSION,
                                                  path=TMP_PATH))
    black_address_df.repartition(10).write.parquet(
        "{path}/"
        "black_address_df/{version}".format(version=RELATION_VERSION,
                                            path=TMP_PATH))
    tid_company_info_df.repartition(10).write.parquet(
        "{path}/"
        "tid_company_info_df/{version}".format(version=RELATION_VERSION,
                                               path=TMP_PATH))
    tid_df.repartition(10).write.parquet(
        "{path}/"
        "tid_df/{version}".format(version=RELATION_VERSION,
                                  path=TMP_PATH))

def tid_spark_data_flow():
    is_common_interests_udf = fun.udf(is_common_interests, tp.IntegerType())
    is_common_address_udf = fun.udf(is_common_address, tp.IntegerType())    
    
    all_company_address_df = spark.read.parquet(
        "{path}/"
        "all_company_address_df/{version}".format(version=RELATION_VERSION,
                                                  path=TMP_PATH)
    ).dropDuplicates(
        ['bbd_qyxx_id']
    )
    black_address_df = spark.read.parquet(
        "{path}/black_address_df/{version}".format(version=RELATION_VERSION,
                                                   path=TMP_PATH))
    
    #数据落地是为了做笛卡尔积
    all_company_address_df.join(
        black_address_df
    ).select(
        'bbd_qyxx_id',
        is_common_interests_udf('namefrag', 
                                'company_name').alias('is_common_interests'),
        is_common_address_udf('all_company_address', 
                              'black_address').alias('is_common_address')
    ).cache(
    ).where(
        'is_common_interests = 1 or  is_common_address = 1'
    ).dropDuplicates(
        ['bbd_qyxx_id']
    ).repartition(
        10
    ).write.parquet(
        "{path}/"
        "some_black_info_df/{version}".format(version=RELATION_VERSION,
                                              path=TMP_PATH)
    )
    
def prd_spark_data_flow():
    some_black_info_df = spark.read.parquet(
        "{path}/"
        "some_black_info_df/{version}".format(version=RELATION_VERSION,
                                              path=TMP_PATH))
    tid_company_info_df = spark.read.parquet(
        "{path}/"
        "tid_company_info_df/{version}".format(version=RELATION_VERSION,
                                               path=TMP_PATH))
    tid_df = spark.read.parquet(
        "{path}/"
        "tid_df/{version}".format(version=RELATION_VERSION,
                                  path=TMP_PATH))
    
    sample_df = spark.read.parquet(
        get_read_path(file_name='ljr_sample', 
                      version=LEIJINRONG_VERSION))

    tid_company_info_df = tid_company_info_df.join(
        some_black_info_df,
        some_black_info_df.bbd_qyxx_id == tid_company_info_df.bbd_qyxx_id,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id,
        tid_company_info_df.company_name,
        'isSOcompany', 'isIPOcompany', 'realcap',
        'regcap', 'regtime', 'opescope',
        'address', 'estatus', 'province',
        'zhuanli', 'shangbiao', 'url',
        'ICP', 'bgxx', 'recruit', 
        'zhaobiao', 'zhongbiao', 'ktgg', 
        'zgcpwsw', 'rmfygg', 'lending', 'xzcf',
        'zhixing', 'dishonesty', 'jyyc',
        'circxzcf', 'fzjg', 'namefrag',
        'province_black_num', 'province_leijinrong_num',
        'is_black_company', 'is_fzjg', 'recruit_industry',
        'bgxx_capital', 'is_new_finance',
        'zgcpwsw_specific', 'is_common_interests', 
        'is_common_address'
    ).dropDuplicates(
        ['bbd_qyxx_id']
    )

    #2.3：根据样本列表构建属性图
    ##获取关联方
    ##这里有个特殊逻辑，需要过滤非通用部分的企业
    def filter_company_type(company_type, type_list=TYPE_LIST):
        '''保留在TYPE_LIST中的企业'''
        if company_type in type_list:
            return True
        else:
            return False
    filter_company_type_udf = fun.udf(
        filter_company_type, BooleanType()
    )
    
    tid_company_merge_df = sample_df.where(
        filter_company_type_udf(sample_df.company_type)
    ).join(
        tid_df,
        fun.trim(tid_df.a_name) == fun.trim(sample_df.company_name),
        'left_outer'
    ).select(
        'a', 'b', 'c',
        'b_degree', 'c_degree', 'bc_relation',
        'b_isperson', 'c_isperson',
        sample_df.company_name.alias('a_name'),
        'b_name', 'c_name'
    )
    ##目标公司信息
    tid_company_merge_df = tid_company_merge_df.join(
        tid_company_info_df,
        tid_company_info_df.company_name == tid_company_merge_df.a_name,
        'left_outer'
    ).select(
        tid_company_info_df.bbd_qyxx_id.alias('a'), 
        'b', 'c',
        'b_degree', 'c_degree', 'bc_relation',
        'b_isperson', 'c_isperson', 'a_name',
        'b_name', 'c_name',
        tid_company_info_df.isIPOcompany.alias('a_isIPOcompany'), 
        tid_company_info_df.realcap.alias('a_realcap'),
        tid_company_info_df.regcap.alias('a_regcap'), 
        tid_company_info_df.zhuanli.alias('a_zhuanli'), 
        tid_company_info_df.shangbiao.alias('a_shangbiao'), 
        tid_company_info_df.url.alias('a_url'), 
        tid_company_info_df.ICP.alias('a_ICP'), 
        tid_company_info_df.bgxx.alias('a_bgxx'), 
        tid_company_info_df.recruit.alias('a_recruit'), 
        tid_company_info_df.zhaobiao.alias('a_zhaobiao'), 
        tid_company_info_df.zhongbiao.alias('a_zhongbiao'), 
        tid_company_info_df.fzjg.alias('a_fzjg'), 
        tid_company_info_df.namefrag.alias('a_namefrag'), 
        tid_company_info_df.province_black_num.alias('a_province_black_num'), 
        tid_company_info_df.province_leijinrong_num.alias('a_province_leijinrong_num'),
        tid_company_info_df.recruit_industry.alias('a_recruit_industry'),
        tid_company_info_df.bgxx_capital.alias('a_bgxx_capital'),
        tid_company_info_df.is_common_interests.alias('a_is_common_interests'),
        tid_company_info_df.is_common_address.alias('a_is_common_address')
    )
    #投资方信息
    tid_company_merge_df = tid_company_merge_df.join(
        tid_company_info_df,
        tid_company_info_df.bbd_qyxx_id == tid_company_merge_df.b,
        'left_outer'
    ).select(
        'a', 'b', 'c',
        'b_degree', 'c_degree', 'bc_relation',
        'b_isperson', 'c_isperson', 'a_name',
        'b_name', 'c_name', 'a_isIPOcompany',
        'a_realcap', 'a_regcap', 'a_zhuanli',
        'a_shangbiao', 'a_url', 'a_ICP',
        'a_bgxx', 'a_recruit', 'a_zhaobiao',
        'a_zhongbiao', 'a_fzjg', 'a_namefrag',
        'a_province_black_num', 'a_province_leijinrong_num',
        'a_recruit_industry', 'a_bgxx_capital', 
        'a_is_common_interests', 'a_is_common_address',
        tid_company_info_df.isSOcompany.alias('b_isSOcompany'),
        tid_company_info_df.isSOcompany.alias('b_isIPOcompany'),
        tid_company_info_df.is_black_company.alias('b_is_black_company'),
        tid_company_info_df.regtime.alias('b_regtime'),
        tid_company_info_df.ktgg.alias('b_ktgg'),
        tid_company_info_df.zgcpwsw.alias('b_zgcpwsw'),
        tid_company_info_df.rmfygg.alias('b_rmfygg'),
        tid_company_info_df.lending.alias('b_lending'),
        tid_company_info_df.xzcf.alias('b_xzcf'),
        tid_company_info_df.zhixing.alias('b_zhixing'),
        tid_company_info_df.dishonesty.alias('b_dishonesty'),
        tid_company_info_df.jyyc.alias('b_jyyc'),
        tid_company_info_df.estatus.alias('b_estatus'),
        tid_company_info_df.circxzcf.alias('b_circxzcf'),
        tid_company_info_df.opescope.alias('b_opescope'),
        tid_company_info_df.address.alias('b_address'),
        tid_company_info_df.province.alias('b_province'),
        tid_company_info_df.is_fzjg.alias('b_is_fzjg'),
        tid_company_info_df.is_new_finance.alias('b_is_new_finance'), 
        tid_company_info_df.zgcpwsw_specific.alias('b_zgcpwsw_specific'),
        tid_company_info_df.is_common_interests.alias('b_is_common_interests'),
        tid_company_info_df.is_common_address.alias('b_is_common_address')
    )
    #被投资方信息
    tid_company_merge_df = tid_company_merge_df.join(
        tid_company_info_df,
        tid_company_info_df.bbd_qyxx_id == tid_company_merge_df.c,
        'left_outer'
    ).select(
        'a', 'b', 'c',
        'b_degree', 'c_degree', 'bc_relation',
        'b_isperson', 'c_isperson', 'a_name',
        'b_name', 'c_name', 'a_isIPOcompany',
        'a_realcap', 'a_regcap', 'a_zhuanli',
        'a_shangbiao', 'a_url', 'a_ICP',
        'a_bgxx', 'a_recruit', 'a_zhaobiao',
        'a_zhongbiao', 'a_fzjg', 'a_namefrag',    
        'a_province_black_num', 'a_province_leijinrong_num',
        'a_recruit_industry', 'a_bgxx_capital',
        'a_is_common_interests', 'a_is_common_address',
        'b_isSOcompany' , 'b_isIPOcompany', 'b_is_black_company', 'b_regtime',
        'b_ktgg', 'b_zgcpwsw', 'b_rmfygg', 'b_lending',   
        'b_xzcf' , 'b_zhixing', 'b_dishonesty',   
        'b_jyyc' , 'b_estatus', 'b_circxzcf',   
        'b_opescope', 'b_address', 'b_province', 'b_is_fzjg',
        'b_is_new_finance', 'b_zgcpwsw_specific',
        'b_is_common_interests', 'b_is_common_address',
        tid_company_info_df.isSOcompany.alias('c_isSOcompany'),
        tid_company_info_df.isSOcompany.alias('c_isIPOcompany'),
        tid_company_info_df.is_black_company.alias('c_is_black_company'),
        tid_company_info_df.regtime.alias('c_regtime'),
        tid_company_info_df.ktgg.alias('c_ktgg'),
        tid_company_info_df.zgcpwsw.alias('c_zgcpwsw'),
        tid_company_info_df.rmfygg.alias('c_rmfygg'),
        tid_company_info_df.lending.alias('c_lending'),
        tid_company_info_df.xzcf.alias('c_xzcf'),
        tid_company_info_df.zhixing.alias('c_zhixing'),
        tid_company_info_df.dishonesty.alias('c_dishonesty'),
        tid_company_info_df.jyyc.alias('c_jyyc'),
        tid_company_info_df.estatus.alias('c_estatus'),
        tid_company_info_df.circxzcf.alias('c_circxzcf'),
        tid_company_info_df.opescope.alias('c_opescope'),
        tid_company_info_df.address.alias('c_address'),
        tid_company_info_df.province.alias('c_province'),
        tid_company_info_df.is_fzjg.alias('c_is_fzjg'),
        tid_company_info_df.is_new_finance.alias('c_is_new_finance'),
        tid_company_info_df.zgcpwsw_specific.alias('c_zgcpwsw_specific'),
        tid_company_info_df.is_common_interests.alias('c_is_common_interests'),
        tid_company_info_df.is_common_address.alias('c_is_common_address')
    )

    os.system(
        "hadoop fs -rmr "
        "{path}/"
        "common_company_info_merge_v2/"
        "{version}".format(version=RELATION_VERSION,
                           path=OUT_PATH))
    
    tid_company_merge_df.repartition(10).write.parquet(
        "{path}/"
        "common_company_info_merge_v2/"
        "{version}".format(version=RELATION_VERSION,
                           path=OUT_PATH))
    
    print "SUCESS ！！"    


def run():
    raw_spark_data_flow()
    tid_spark_data_flow()
    prd_spark_data_flow()

def get_spark_session():
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 15)
    conf.set("spark.executor.memory", "60g")
    conf.set("spark.executor.instances", 20)
    conf.set("spark.executor.cores", 10)
    conf.set("spark.python.worker.memory", "3g")
    conf.set("spark.default.parallelism", 1500)
    conf.set("spark.sql.shuffle.partitions", 1500)
    conf.set("spark.broadcast.blockSize", 1024)
    conf.set("spark.sql.crossJoin.enabled", True)
    conf.set("spark.executor.extraJavaOptions",
             "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")    
    conf.set("spark.submit.pyFiles", 
             "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/dafei_keyword.py")
    conf.set("spark.files", 
                ("hdfs://bbdc6ha/user/antifraud/source/keyword_demo/city,"
                 "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/1gram.words,"
                 "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/2gram.words,"
                 "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/new.work.words"))
    
    spark = SparkSession \
        .builder \
        .appName("hgongjing2_one_tid_common_v2") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()    
    
    return spark

if __name__ == "__main__":
    conf = configparser.ConfigParser()  
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
 
    #中间结果版本与关联方版本相同
    RELATION_VERSION = sys.argv[1]
        
    #输入数据版本
    SO_VERSION = RELATION_VERSION
    BASIC_VERSION = RELATION_VERSION
    ZHUANLI_VERSION = RELATION_VERSION
    SHANGBIAO_VERSION = RELATION_VERSION
    DOMAIN_WEBSITE_VERSION = RELATION_VERSION
    BGXX_VERSION = RELATION_VERSION
    RECRUIT_VERSION = RELATION_VERSION
    ZHAOBIAO_VERSION = RELATION_VERSION
    ZHONGBIAO_VERSION = RELATION_VERSION
    KTGG_VERSION = RELATION_VERSION
    ZGCPWSW_VERSION = RELATION_VERSION
    RMFYGG_VERSION = RELATION_VERSION
    XZCF_VERSION = RELATION_VERSION
    ZHIXING_VERSION = RELATION_VERSION
    DISHONESTY_VERSION = RELATION_VERSION
    JYYC_VERSION = RELATION_VERSION
    CIRCXZCF_VERSION = RELATION_VERSION
    FZJG_VERSION = RELATION_VERSION
    BLACK_VERSION = RELATION_VERSION
    LEIJINRONG_VERSION = RELATION_VERSION
    
    #输入输出路径
    IN_PATH = conf.get('common_company_info', 'OUT_PATH')
    OUT_PATH = conf.get('common_company_info_merge', 'OUT_PATH')
    TMP_PATH = conf.get('common_company_info_merge', 'TMP_PATH')

    #除了TYPE_LIST中的企业外，其余企业还是用xgboost，因此这里需要将他们筛选出来
    TYPE_LIST = conf.get('input_sample_data', 'TYPE_LIST')

    #sparkSession
    spark = get_spark_session()
    
    run()
    
    