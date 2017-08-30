#!/usr/bin/python
# -*- coding: utf-8 -*-

from util_Log.logger  import logging
from config.conf import config

import subprocess
import os
import sys

reload(sys)
sys.setdefaultencoding("utf-8")
logger = logging.getLogger(__name__)
class HbaseUtils:

#\001
    @staticmethod
    def bulkloadToHbase(dataPath,tabName,columns,hbaseshell,delimiter):
        #创建hbase表 命名规则 tablename_yyyymmm
        p=subprocess.Popen('hbase shell  ../hbaseshell/'+hbaseshell,stdout=subprocess.PIPE,shell=True)
        logging.debug(p.stdout.readlines())
        # 生成hfile
        ghfile_path = config.getConf('hadoop','ghfile_path')
        hadoop_root_dir = config.getConf('hadoop','root_dir')
        if(ghfile_path == '/' or  ghfile_path == '\\' or len(ghfile_path) < 4):
            return
        os.system("hadoop fs -rm -f -r "+  ghfile_path)
        command_create_hfile = 'hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dmapreduce.map.output.compress=true  -Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec   -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1024   -Dimporttsv.separator="'+delimiter+'" -Dimporttsv.bulk.output='+hadoop_root_dir+ghfile_path+' -Dimporttsv.columns='+columns+' ' + tabName + ' ' +hadoop_root_dir + dataPath
        logging.info("command_create_hfile ===> " + command_create_hfile)
        p=subprocess.Popen(command_create_hfile,stdout=subprocess.PIPE,shell=True)
        logging.debug( p.stdout.readlines())
        # 导入hbase
        command_import = 'hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1024 '+hadoop_root_dir+ghfile_path+ ' ' +tabName
        p=subprocess.Popen(command_import,stdout=subprocess.PIPE,shell=True)
        logging.debug( p.stdout.readlines())
    