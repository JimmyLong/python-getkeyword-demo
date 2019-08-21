#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/7/29 17:11
# @Author : jimmy
# @File : ConfigParam.py
# @Software: PyCharm
import os
import configparser

# 配置参数类
class ConfigParam(object):
    def __init__(self):
        # 读取配置文件目录
        self.cur_path = os.path.dirname(os.path.relpath(__file__))
        self.config_path = os.path.join(self.cur_path, 'config.ini')

        # 读取配置文件
        self.conf = configparser.ConfigParser()
        self.conf.read(self.config_path)

    def getWordLibraryFolderPath(self):
        return self.conf.get('file_path', 'word_library_folder_path')

    def getStopWordsFilePath(self):
        return self.conf.get('file_path', 'stop_words_file_path')

    def getGetKeyWordsByAppointDayOutputFolderPath(self):
        return self.conf.get('file_path', 'get_key_word_appoint_day_output_folder_path')

    def getGetKeyWordsByUserFansOutputFolderPath(self):
        return self.conf.get('file_path', 'get_key_word_user_fans_output_folder_path')

    def getGetKeyWordsByMarketingEventTrackOutputFolderPath(self):
        return self.conf.get('file_path', 'get_key_word_marketing_event_track_output_folder_path')

    def getGetCommunityResultStatisticsUserFansInputFolderPath(self):
        return self.conf.get('file_path', 'get_community_result_statistics_user_fans_input_folder_path')

    def getGetCommunityResultStatisticsMarketingEventTrackInputFolderPath(self):
        return self.conf.get('file_path', 'get_community_result_statistics_marketing_event_track_input_folder_path')

    def getMySqlConnPort(self):
        return int(self.conf.get('mysql', 'conn_port'))

    def getMySqlConnUserName(self):
        return self.conf.get('mysql', 'conn_user')

    def getMySqlConnUserPassWord(self):
        return self.conf.get('mysql', 'conn_password')

    def getMySqlSelectMblogInfosSQL(self):
        return self.conf.get('mysql', 'select_mblog_sql')

    def getMySqlSelectFansTotal(self):
        return self.conf.get('mysql', 'select_fans_total_sql')

    def getRedisConnIpAddress(self):
        return self.conf.get('redis', 'redis_ip_address')

    def getRedisConnPort(self):
        return self.conf.get('redis', 'redis_port')

    def getRedisConnPassword(self):
        return self.conf.get('redis', 'redis_password')

    def getLocalHostIpAddress(self):
        return self.conf.get('localhost', 'local_ip_address')

    def getLocalHostDataBaseName(self):
        return self.conf.get('localhost', 'local_data_base_name')

