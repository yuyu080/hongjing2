#-*- coding: UTF-8 -*-
from ConfigParser import ConfigParser
import os

class ConfigUtils(object):
     def __init__(self,fileName):
         super(ConfigUtils, self).__init__()
         try:
             self.config = ConfigParser()
             self.config.read(fileName)
             active = self.config.get("profiles","active")
             cfgFiles = [os.path.dirname(os.path.realpath(__file__)) + "/application.cfg",
                         os.path.dirname(os.path.realpath(__file__)) + "/application_"+active+".cfg"]
             self.config.read(cfgFiles)
         except IOError,e:
             print  e
     def getConf(self,section, option):
         value = self.config.get(section, option)
         if(value is None or value.strip() == ''):
             return None
         return value
     def getLogConfig(self):
         return  os.path.dirname(os.path.realpath(__file__)) + "/log.json"


config = ConfigUtils(os.path.dirname(os.path.realpath(__file__)) + "/application.cfg")
