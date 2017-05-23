# -*- coding: utf-8 -*-

import configparser
import subprocess
import sys

import MySQLdb

def truncate_table(table):
    '''连接mysql，执行一个SQL'''
    db = MySQLdb.connect(host=PROP['ip'], user=PROP['user'], 
                         passwd=PROP['password'], db=PROP['db_name'], 
                         charset="utf8")
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    # 使用execute方法执行SQL语句
    sql = "TRUNCATE TABLE {0}".format(table)
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()
    # 关闭数据库连接
    db.close()
    
    print "清空表{0}成功".format(table)

def execute_some_step(step_name, step_child_name, file_name, version):
    '''提交某个spark-job'''
    execute_result = subprocess.call(
        '''
        /opt/spark-2.0.2/bin/spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 15g \
        --jars /usr/share/java/mysql-connector-java-5.1.39.jar \
        --driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
        {path}/{step_child_name}/{file_name} {version}
        '''.format(path=IN_PATH+step_name, 
                   step_child_name=step_child_name,
                   file_name=file_name,
                   version=version),
        shell=True
    )
    return execute_result

def is_success(result, step_name, step_child_name, file_name, version):
    '''根据计算结果判断是否出错，如果出错则退出程序并打印错误信息'''
    if result:
        print "\n******************************\n"
        sys.exit(
            '''
            {step_name}|{step_child_name}|{file_name}|{version} \
            has a error !!!
            '''.format(
                step_name=step_name,
                step_child_name=step_child_name,
                file_name=file_name,
                version=version
            )
        )
        
def step_zero(step_child_name, version):
    if step_child_name == 'prd':
        result = execute_some_step('step_zero', step_child_name, 
                                   'input_sample_data.py', version)
        is_success(result, 'step_zero', 
                   step_child_name, 'input_sample_data.py', version)
        
def step_one(step_child_name, old_version, new_version):
    if step_child_name == 'raw':
        result = execute_some_step('step_one', step_child_name, 
                                   'common_company_info.py', new_version)
        is_success(result, 'step_one', 
                   step_child_name, 'common_company_info.py', new_version)

    if step_child_name == 'tid':
        result = execute_some_step('step_one', step_child_name, 
                                   'common_company_info_merge.py', new_version)
        is_success(result, 'step_one', step_child_name, 
                   'common_company_info_merge.py', new_version)

    if step_child_name == 'prd':
        result_one = execute_some_step('step_one', step_child_name, 
                                       'common_company_static_feature.py', 
                                       new_version)
        is_success(result_one, 'step_one', step_child_name, 
                   'common_company_static_feature.py', new_version)

        result_two = execute_some_step('step_one', step_child_name, 
                                       'common_company_dynamic_feature.py', 
                                       old_version + ' ' +new_version)
        is_success(result_two, 'step_one', step_child_name, 
                   'common_company_dynamic_feature.py', 
                   old_version + ' ' + new_version)
        
        result_three = execute_some_step('step_one', step_child_name, 
                                         'ex_company_feature.py', 
                                         new_version)
        is_success(result_three, 'step_one', step_child_name, 
                   'ex_company_feature.py', 
                   new_version)

        result_four = execute_some_step('step_one', step_child_name, 
                                         'p2p_company_feature.py', 
                                         new_version)
        is_success(result_four, 'step_one', step_child_name, 
                   'p2p_company_feature.py', 
                   new_version)
        
        result_five = execute_some_step('step_one', step_child_name, 
                                         'pe_company_feature.py', 
                                         new_version)
        is_success(result_five, 'step_one', step_child_name, 
                   'pe_company_feature.py', 
                   new_version)
        
def step_two(version):
    result_one = execute_some_step('step_two', 'raw', 
                                   'nf_feature_merge.py',
                                   version)
    is_success(result_one, 'step_two', 'raw', 
               'nf_feature_merge.py', 
               version)
    
    result_two = execute_some_step('step_two', 'prd', 
                                   'nf_risk_score.py',
                                   version)
    is_success(result_two, 'step_two', 'prd', 
               'nf_risk_score.py', 
               version)
    
    result_three = execute_some_step('step_two', 'raw', 
                                   'ex_feature_merge.py',
                                   version)
    is_success(result_three, 'step_two', 'raw', 
               'ex_feature_merge.py', 
               version)
    
    result_four = execute_some_step('step_two', 'prd', 
                                   'ex_risk_score.py',
                                   version)
    is_success(result_four, 'step_two', 'prd', 
               'ex_risk_score.py', 
               version)
    
    result_five = execute_some_step('step_two', 'raw', 
                                    'p2p_feature_merge.py',
                                    version)
    is_success(result_five, 'step_two', 'raw', 
               'p2p_feature_merge.py', 
               version)
    
    result_six = execute_some_step('step_two', 'prd', 
                                   'p2p_risk_score.py',
                                   version)
    is_success(result_six, 'step_two', 'prd', 
               'p2p_risk_score.py', 
               version)

    result_seven = execute_some_step('step_two', 'raw', 
                                    'pe_feature_merge.py',
                                    version)
    is_success(result_seven, 'step_two', 'raw', 
               'pe_feature_merge.py', 
               version)
    
    result_eight = execute_some_step('step_two', 'prd', 
                                     'pe_risk_score.py',
                                     version)
    is_success(result_eight, 'step_two', 'prd', 
               'pe_risk_score.py', 
               version)
    
def step_three(step_child_name, version):
    if step_child_name == 'raw':
#==============================================================================
#         result_one = execute_some_step('step_three', 'raw', 
#                                          'ex_info_merge.py',
#                                          version)
#         is_success(result_one, 'step_three', 'raw',
#                    'ex_info_merge.py', 
#                    version)
# 
#         result_two = execute_some_step('step_three', 'raw', 
#                                          'nf_info_merge.py',
#                                          version)
#         is_success(result_two, 'step_three', 'raw',
#                    'nf_info_merge.py',
#                    version)
#         
#==============================================================================
        result_three = execute_some_step('step_three', 'raw', 
                                         'p2p_info_merge.py',
                                         version)
        is_success(result_three, 'step_three', 'raw',
                   'p2p_info_merge.py',
                   version)
        
#==============================================================================
#         result_four = execute_some_step('step_three', 'raw', 
#                                          'pe_info_merge.py',
#                                          version)
#         is_success(result_four, 'step_three', 'raw',
#                    'pe_info_merge.py',
#                    version)
#         
#==============================================================================
    if step_child_name == 'tid':
#==============================================================================
#         result_five = execute_some_step('step_three', 'tid', 
#                                          'ex_feature_tags.py',
#                                          version)
#         is_success(result_five, 'step_three', 'tid',
#                    'ex_feature_tags.py',
#                    version)
#         
#         result_six = execute_some_step('step_three', 'tid', 
#                                          'nf_feature_tags.py',
#                                          version)
#         is_success(result_six, 'step_three', 'tid',
#                    'nf_feature_tags.py',
#                    version)
#==============================================================================
        
        result_seven = execute_some_step('step_three', 'tid', 
                                         'p2p_feature_tags.py',
                                         version)
        is_success(result_seven, 'step_three', 'tid',
                   'p2p_feature_tags.py',
                   version)
        
#==============================================================================
#         result_eight = execute_some_step('step_three', 'tid', 
#                                          'pe_feature_tags.py',
#                                          version)
#         is_success(result_eight, 'step_three', 'tid',
#                    'pe_feature_tags.py',
#                    version)
#==============================================================================
        
    if step_child_name == 'prd':
        result_nine = execute_some_step('step_three', 'prd', 
                                        'all_company_info.py',
                                        version)
        is_success(result_nine, 'step_three', 'prd',
                   'all_company_info.py',
                   version)
       
def step_four():
    pass       
      
def run(is_history_back):
    '''
    是否计算历史版本
    '''
    if is_history_back:
        #多版本计算
        for index, relation_version in enumerate(RELATION_VERSIONS):
            if index < 3:
                old_version = RELATION_VERSIONS[0]
                new_version = relation_version
            else:
                old_version = RELATION_VERSIONS[index-3]
                new_version = relation_version        
            
#==============================================================================
#             step_zero('prd', new_version)
#             #这里的old_version是用于计算动态风险的，时间间隔为一个季度
#             step_one('raw', 
#                      old_version=old_version, 
#                      new_version=new_version)
#             step_one('tid', 
#                      old_version=old_version, 
#                      new_version=new_version)
#             step_one('prd', 
#                      old_version=old_version, 
#                      new_version=new_version)
#             
#             step_two(new_version)
#==============================================================================
            
            step_three('raw', new_version)
            step_three('tid', new_version)
            step_three('prd', new_version)            
    else:
        #单版本计算
        #获取用于计算动态风险的时间版本
        new_index = RELATION_VERSIONS.index(NEW_VERSION)
        if new_index < 3:
            dynamic_old_version = RELATION_VERSIONS[0]
        else:
            dynamic_old_version = RELATION_VERSIONS[new_index-3]

        step_zero('prd', NEW_VERSION)
        
        step_one('raw', 
                 old_version=dynamic_old_version, 
                 new_version=NEW_VERSION)
        step_one('tid', 
                 old_version=dynamic_old_version, 
                 new_version=NEW_VERSION)
        step_one('prd', 
                 old_version=dynamic_old_version, 
                 new_version=NEW_VERSION)

        step_two(new_version)
        
        step_three('raw', NEW_VERSION)
        step_three('tid', NEW_VERSION)
        step_three('prd', NEW_VERSION)



def into_mysql(version):
    '''合并所有历史版本，写入mysql，在写之前需要清空表'''
    
    truncate_table('ra_time_sque') 
    result_one = execute_some_step('step_four', 'raw', 
                                     'ra_time_sque.py',
                                     version)
    is_success(result_one, 'step_four', 'step_four',
               'ra_time_sque.py',
               version)
    
    truncate_table('ra_company')
    result_two = execute_some_step('step_four', 'raw', 
                                   'ra_company.py',
                                   version)
    is_success(result_two, 'step_four', 'raw',
               'ra_company.py',
               version)
    
    truncate_table('ra_high_company')
    result_three = execute_some_step('step_four', 'raw', 
                                     'ra_high_company.py',
                                     version)
    is_success(result_three, 'step_four', 'raw',
               'ra_high_company.py',
               version)

    truncate_table('ra_area_count')
    result_four = execute_some_step('step_four', 'raw', 
                                     'ra_area_count.py',
                                     version)
    is_success(result_four, 'step_four', 'raw',
               'ra_area_count.py',
               version)
    
    truncate_table('ra_gather_place')
    result_five = execute_some_step('step_four', 'raw', 
                                    'ra_gather_place.py',
                                    version)
    is_success(result_five, 'step_four', 'raw',
               'ra_gather_place.py',
               version)

    truncate_table('ra_black_white')
    result_five = execute_some_step('step_four', 'raw', 
                                    'ra_black_white.py',
                                    version)
    is_success(result_five, 'step_four', 'raw',
               'ra_black_white.py',
               version)

if __name__ == '__main__':
    conf = configparser.ConfigParser()
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")

    IN_PATH = conf.get('work_flow', 'IN_PATH')
    RELATION_VERSIONS = eval(conf.get('common', 'RELATION_VERSIONS'))
    RELATION_VERSIONS.sort()
    OLD_VERSION, NEW_VERSION = RELATION_VERSIONS[-2:]

    #mysql输出信息
    TABLE = 'ra_company'
    URL = conf.get('mysql', 'URL')
    PROP = eval(conf.get('mysql', 'PROP'))

    run(is_history_back=True)
    into_mysql(NEW_VERSION)