#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/8/1 14:21
# @Author : jimmy
# @File : GetCommunityResultStatistics.py
# @Software: PyCharm
from FlagMessage import FlagMessage
from ConfigParam import ConfigParam
from MySqlHelper import MyPymysqlPool
import time
import os

class CommunityResultStatisticsByUserFans(object):

    def __init__(self):
        self.flag_object = FlagMessage()
        self.config_object = ConfigParam()
        # 默认状态为正在处理
        self.state_flag = 2

    # 设置IP地址、原始数据存放位置
    def setParam(self, order_number, static_info_ip_address, static_info_data_base_name, static_info_data_table_name, communtry_results_ip_address, communtry_results_url):
        self.mysql_object = MyPymysqlPool(static_info_ip_address, static_info_data_base_name, 'root', 'cldev')
        self.static_info_data_table_name = static_info_data_table_name
        self.communtry_results_ip_address = communtry_results_ip_address
        self.communtry_results_url = communtry_results_url
        self.order_number = order_number
        # 默认状态为正在处理
        self.state_flag = 2

    # 读数据表内内容
    def readDataFromDataBase(self, mysql_object, static_info_data_table_name, order_number):
        mblog_sql = self.config_object.getMySqlSelectFansTotal() + ' ' + str(static_info_data_table_name) + ' where order_number = \'' + str(order_number) + '\''
        results = mysql_object.getAll(mblog_sql)
        fans_total = int(results[0]['fans_total'])

        # 释放资源
        mysql_object.end()
        mysql_object.dispose()
        return fans_total

    # 读取文件下所有分群文件
    def readTxtFileByFolderPath(self, folder_path):
        file_list = []
        for root, dict_names, file_names in os.walk(folder_path):
            for file_name in file_names:
                if '.txt' in file_name:
                    file_list.append(folder_path + file_name)
        return file_list

    # 读取分群结果内容行数
    def readTxtDataRow(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                results = f.read()
            f.close()
        except:
            with open(file_path, 'r', encoding='gbk') as f:
                results = f.read()
            f.close()
        data_list = results.split('\n')
        data_list = list(filter(None, data_list))
        data_list = list(set(data_list))
        return len(data_list)

    # 统计分群结果
    def getCommunityResultStatistics(self, community_result_file_list, communtry_results_url):
        community_sum_total = 0
        communtries_number = 0
        community_size_info_dict = {}
        for community_result_file in community_result_file_list:
            # 统计每个群内人数
            community_total = self.readTxtDataRow(community_result_file)
            community_number = str(community_result_file).replace(communtry_results_url, '').replace('.txt', '')
            try:
                number = community_size_info_dict[community_number]
            except:
                number = 0
            number += community_total
            community_size_info_dict[community_number] = number
        # 进行单人群的排除
        community_size_info_new_dict = {}
        for key_name in community_size_info_dict.keys():
            if community_size_info_dict[key_name] > 2:
                community_size_info_new_dict[key_name] = community_size_info_dict[key_name]
                # 统计总人数
                community_sum_total += community_size_info_dict[key_name]
                # 统计群落数
                communtries_number += 1
        return community_sum_total, communtries_number, community_size_info_new_dict

    # 流程函数
    def main(self, get_community_result_mysql_object):
        start = time.time()
        try:
            # 更新MQL
            sql = '''update `fans_community_result` set `process_state` = %s where `order_number` = %s'''
            count = get_community_result_mysql_object.update(sql, ('processing', str(self.order_number)))
            if count > 0:
                get_community_result_mysql_object.end()
            else:
                get_community_result_mysql_object.end(option='rollback')
            # 读取粉丝总人数
            fans_total = self.readDataFromDataBase(self.mysql_object, self.static_info_data_table_name, self.order_number)
            # 读取文件下所有分群文件
            community_result_file_list = self.readTxtFileByFolderPath(self.communtry_results_url)
            # 统计分群结果
            community_sum_total, communtries_number, community_size_info_dict = self.getCommunityResultStatistics(community_result_file_list, self.communtry_results_url)
            self.state_flag = 1
            # 更新MQL
            sql = '''update `fans_community_result` set `process_state` = %s, `community_total` = %s, `community_size_info` = %s, `not_community_user_number` = %s where `order_number` = %s'''
            count = get_community_result_mysql_object.update(sql, ('finish', communtries_number, str(community_size_info_dict), int(fans_total - community_sum_total), str(self.order_number)))
            if count > 0:
                get_community_result_mysql_object.end()
            else:
                get_community_result_mysql_object.end(option='rollback')
        except Exception as e:
            print(e)
            self.state_flag = 0
            # 删除MQL
            sql = '''delete from `fans_community_result` where `order_number` = %s'''
            count = get_community_result_mysql_object.delete(sql, (str(self.order_number)))
            if count > 0:
                get_community_result_mysql_object.end()
            else:
                get_community_result_mysql_object.end(option='rollback')
        end = time.time()
        print('操作总耗时：%s' % (end - start))

class CommunityResultStatisticsByMarketingEventTrack(object):

    def __init__(self):
        self.flag_object = FlagMessage()
        self.config_object = ConfigParam()
        # 默认状态为正在处理
        self.state_flag = 2

    # 设置IP地址、原始数据存放位置
    def setParam(self, order_number, original_data_ip_address, original_data_base_name, original_data_aittitude_table_name, original_data_comment_table_name, original_data_repost_table_name, communtry_results_ip_address, communtry_results_url):
        self.mysql_object = MyPymysqlPool(original_data_ip_address, original_data_base_name, 'root', '123456')
        self.original_data_aittitude_table_name = original_data_aittitude_table_name
        self.original_data_comment_table_name = original_data_comment_table_name
        self.original_data_repost_table_name = original_data_repost_table_name
        self.communtry_results_ip_address = communtry_results_ip_address
        self.communtry_results_url = communtry_results_url
        self.order_number = order_number
        # 默认状态为正在处理
        self.state_flag = 2

    # 读数据表内内容
    def readDataFromDataBase(self, mysql_object, data_table_name):
        mblog_sql = 'select uid from ' + str(data_table_name)
        results = mysql_object.getAll(mblog_sql)
        results = [result['uid'] for result in results]
        results = list(set(results))

        mysql_object.end()
        return results

    # 读取文件下所有分群文件
    def readTxtFileByFolderPath(self, folder_path):
        file_list = []
        for root, dict_names, file_names in os.walk(folder_path):
            for file_name in file_names:
                if '.txt' in file_name:
                    file_list.append(folder_path + file_name)
        return file_list

    # 读取分群结果内容行数
    def readTxtDataRow(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                results = f.read()
            f.close()
        except:
            with open(file_path, 'r', encoding='gbk') as f:
                results = f.read()
            f.close()
        data_list = results.split('\n')
        data_list = list(filter(None, data_list))
        data_list = list(set(data_list))
        return len(data_list)

    # 统计分群结果
    def getCommunityResultStatistics(self, community_result_file_list, communtry_results_url):
        community_sum_total = 0
        communtries_number = 0
        community_size_info_dict = {}
        for community_result_file in community_result_file_list:
            # 统计每个群内人数
            community_total = self.readTxtDataRow(community_result_file)
            community_number = str(community_result_file).replace(communtry_results_url, '').replace('.txt', '')
            try:
                number = community_size_info_dict[community_number]
            except:
                number = 0
            number += community_total
            community_size_info_dict[community_number] = number
        # 进行单人群的排除
        community_size_info_new_dict = {}
        for key_name in community_size_info_dict.keys():
            if community_size_info_dict[key_name] > 2:
                community_size_info_new_dict[key_name] = community_size_info_dict[key_name]
                # 统计总人数
                community_sum_total += community_size_info_dict[key_name]
                # 统计群落数
                communtries_number += 1
        return community_sum_total, communtries_number, community_size_info_new_dict

    # 流程函数
    def main(self, get_community_result_mysql_object):
        start = time.time()
        try:
            # 更新MQL
            sql = '''update `mblog_track_community_result` set `process_state` = %s where `order_number` = %s'''
            count = get_community_result_mysql_object.update(sql, ('processing', str(self.order_number)))
            if count > 0:
                get_community_result_mysql_object.end()
            else:
                get_community_result_mysql_object.end(option='rollback')
            # 读取点赞人群
            aittitude_list = self.readDataFromDataBase(self.mysql_object, self.original_data_aittitude_table_name)
            # 读取评论人群
            comment_list = self.readDataFromDataBase(self.mysql_object, self.original_data_comment_table_name)
            # 读取转发人群
            repost_list = self.readDataFromDataBase(self.mysql_object, self.original_data_repost_table_name)
            # 释放资源
            self.mysql_object.dispose()
            # 触达人群总计
            touch_up_list = []
            for aittitude_people in aittitude_list:
                touch_up_list.append(aittitude_people)
            for comment_people in comment_list:
                touch_up_list.append(comment_people)
            for repost_people in repost_list:
                touch_up_list.append(repost_people)
            touch_up_list = list(set(touch_up_list))
            # 读取粉丝总人数
            touch_up_total = len(touch_up_list)
            # 读取文件下所有分群文件
            community_result_file_list = self.readTxtFileByFolderPath(self.communtry_results_url)
            # 统计分群结果
            community_sum_total, communtries_number, community_size_info_dict = self.getCommunityResultStatistics(
                community_result_file_list, self.communtry_results_url)
            self.state_flag = 1
            # 更新MQL
            sql = '''update `mblog_track_community_result` set `process_state` = %s, `community_total` = %s, `community_size_info` = %s, `not_community_user_number` = %s where `order_number` = %s'''
            count = get_community_result_mysql_object.update(sql, (
            'finish', communtries_number, str(community_size_info_dict), int(touch_up_total - community_sum_total),
            str(self.order_number)))
            if count > 0:
                get_community_result_mysql_object.end()
            else:
                get_community_result_mysql_object.end(option='rollback')
        except Exception as e:
            print(e)
            self.state_flag = 0
            # 删除MQL
            sql = '''delete from `mblog_track_community_result` where `order_number` = %s'''
            count = get_community_result_mysql_object.delete(sql, (str(self.order_number)))
            if count > 0:
                get_community_result_mysql_object.end()
            else:
                get_community_result_mysql_object.end(option='rollback')
        end = time.time()
        print('操作总耗时：%s' % (end - start))