#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/7/29 17:21
# @Author : jimmy
# @File : GetKeyWords.py
# @Software: PyCharm
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import time
import threading
from FlagMessage import FlagMessage
import re
import os
from ConfigParam import ConfigParam
import csv
from MySqlHelper import MyPymysqlPool
import datetime

csv.field_size_limit(500 * 1024 * 1024)
# 分词(每日更新)
class KeyWordsByAppointDay(object):
    def __init__(self):
        self.monitor_lock = threading.Lock()
        self.flag_object = FlagMessage()
        self.config_object = ConfigParam()
        # 默认状态为正在处理
        self.state_flag = 2

    # 设置IP地址、原始数据存放位置、分词工具对象
    def setParam(self, ip_address, data_base_name, data_table_name, data_time, pro_obj):
        self.mysql_object = MyPymysqlPool(ip_address, data_base_name, 'root', '123456')
        self.data_table_name = data_table_name
        self.data_time = data_time
        self.pro_obj = pro_obj
        self.output_basic_path = self.config_object.getGetKeyWordsByAppointDayOutputFolderPath() + str(self.data_time) + '/'
        self.output_path = self.output_basic_path + str(self.data_time) + '.csv'
        # 默认状态为正在处理
        self.state_flag = 2

    # 读数据表内内容
    def readDataFromDataBase(self, mysql_object, data_table_name):
        mblog_sql = self.config_object.getMySqlSelectMblogInfosSQL() + ' ' + str(data_table_name)
        results = mysql_object.getAll(mblog_sql)
        data_list = []
        for result in results:
            # uid, mid, text, retweeted_text, created_at
            if result['retweeted_text'] is None:
                new_result = {'uid': result['uid'], 'mid': result['mid'], 'text': str(result['text']) + '。', 'created_at': result['created_at']}
            else:
                new_result = {'uid': result['uid'], 'mid': result['mid'], 'text': str(result['text']) + '。' + str(result['retweeted_text']), 'created_at': result['created_at']}
            data_list.append(new_result)

        # 释放资源
        mysql_object.end()
        mysql_object.dispose()
        return data_list

    # 求词语集合与给定词库结合的交集
    def getLable(self, words, poiResult):
        # 提取关键词
        wordDict = {}
        # 找到可以打标签的词
        words = [word for word in words if word in poiResult]
        for word in words:
            try:
                number = wordDict[word]
            except:
                number = 0
            number += 1
            wordDict[word] = number
        return wordDict

    # 将标签类型和标签值写入Dict
    def lablePossing(self, uLabel, words, Result, flag):
        wordDict = self.getLable(words, Result)
        if wordDict:
            uLabel[flag] = wordDict
        return uLabel

    # 清洗博文
    def cleanBlogData(self, blogText):
        # 正则匹配特殊符号
        pat = re.compile('(?<=\>).*?(?=\<)')
        blogNewText = ''.join(pat.findall(blogText))
        if blogNewText == '':
            pass
        else:
            blogText = blogNewText
        blogText = blogText.replace(' ', '')
        blogText = blogText.replace('\n', '')
        # 正则匹配话题
        weiboTopic = re.findall(r'#([\u4e00-\u9fa5\w\-]+)#', blogText)
        for topic in weiboTopic:
            topicAll = '#' + topic + '#'
            blogText = blogText.replace(topicAll, '')
        blogText = blogText.replace(' ', '')
        return blogText

    # 提取标签
    def getLabelFromBlog(self, obj, blog):
        uLabel = {}
        # 清洗博文
        blog = self.cleanBlogData(blog)
        # 分词
        results = obj.pesg.cut(blog)
        words = [s.word for s in results]
        stop_word = set(words) & obj.stop_words
        # 去除停留词
        words = [word for word in words if word not in stop_word]
        for keyName in obj.word_library.keys():
            # 提取标签
            uLabel = self.lablePossing(uLabel, words, obj.word_library[keyName], keyName)
        return uLabel

    # 执行函数
    def processFun(self, data):
        try:
            # 处理uid下所有博文
            data_new_list = []
            classification_list = ['ebook', 'enterprise', 'music', 'film', 'city', 'product', 'brand', 'star', 'app']
            uLabel = self.getLabelFromBlog(self.pro_obj, data['text'])
            row_data = [data['uid'], data['mid']]
            for classification_name in classification_list:
                try:
                    class_value_dict = uLabel[classification_name]
                except:
                    class_value_dict = {}
                row_data.append(class_value_dict)
            # 时间标准化
            if ' ' in str(data['created_at']):
                number = str(data['created_at']).count(':', 0, len(str(data['created_at'])))
                if number >= 2:
                    created_at_standardization = str(data['created_at'])
                elif number == 1:
                    created_at_standardization = str(data['created_at']) + ':00'
                else:
                    created_at_standardization = str(data['created_at']) + ':00:00'
                created_at_standardization = datetime.datetime.strptime(created_at_standardization, '%Y-%m-%d %H:%M:%S')
            else:
                created_at_standardization = datetime.datetime.strptime(str(data['created_at']), '%Y-%m-%d')
            row_data.append(created_at_standardization)
            row_data.append('')
            data_new_list.append(row_data)
            return {'flag': True, 'error_message': '', 'data': data_new_list, 'uid': data['uid']}
        # 提高系统健硕性
        except Exception as e:
            return {'flag': False, 'error_message': str(e), 'data': '', 'uid': data['uid']}

    # 写结果函数
    def writeResultFun(self, output_path, data_list):
        with open(output_path, 'a+', newline='', encoding='utf-8') as f:
            f_csv = csv.writer(f)
            for data in data_list:
                f_csv.writerow(data)
        f.close()

    # 监控函数
    def monitorFun(self, res):
        res = res.result()
        # 锁机制
        self.monitor_lock.acquire()
        try:
            # 写结果阶段
            self.writeResultFun(self.output_path, res['data'])
            # 公共标识量改变阶段
            if res['flag'] == True:
                self.flag_object.updataSuccessValue()
            else:
                self.flag_object.updataFailValue()
        except:
            self.flag_object.updataFailValue()
        self.monitor_lock.release()

    # 读取文件夹下所有文件
    def readFileList(self, folder_path):
        file_list = os.listdir(folder_path)
        file_list = [folder_path + f for f in file_list]
        return file_list

    # 流程函数
    def main(self, get_key_words_state_mysql_object):
        start = time.time()
        try:
            self.flag_object.setSuccessValue(0)
            self.flag_object.setFailValue(0)
            # 更新MQL
            sql = '''update `get_key_words_state_appoint_day` set `process_state` = %s where `data_time` = %s'''
            count = get_key_words_state_mysql_object.update(sql, ('processing', str(self.data_time)))
            if count > 0:
                get_key_words_state_mysql_object.end()
            else:
                get_key_words_state_mysql_object.end(option='rollback')
            # 读取文件内容
            data_list = self.readDataFromDataBase(self.mysql_object, self.data_table_name)
            # 写表头
            if os.path.isdir(self.output_basic_path):
                pass
            else:
                os.mkdir(self.output_basic_path)
            with open(self.output_path, 'a+', newline='', encoding='utf-8') as f:
                f_csv = csv.writer(f)
                f_csv.writerow(['uid', 'mid', 'ebook', 'enterprise', 'music', 'film', 'city', 'product', 'brand', 'star', 'app', 'created_at', 'weight'])
            f.close()
            # 通过开启线程池内线程来完成任务
            self.threading_pool = ThreadPoolExecutor(int(multiprocessing.cpu_count()))
            for data in data_list:
                self.threading_pool.submit(self.processFun, data).add_done_callback(self.monitorFun)
            # 所有任务完成执行(此次不考虑各线程内子任务是否执行成功与否)
            self.threading_pool.shutdown()
            self.state_flag = 1
            # 更新MQL
            sql = '''update `get_key_words_state_appoint_day` set `process_state` = %s, `data_base_name` = %s, `result_data_url` = %s, `result_success_number` = %s, `result_fail_number` = %s where `data_time` = %s'''
            count = get_key_words_state_mysql_object.update(sql, ('finish', self.config_object.getLocalHostDataBaseName(), self.output_basic_path, self.flag_object.getSuccessValue(), self.flag_object.getFailValue(), str(self.data_time)))
            if count > 0:
                get_key_words_state_mysql_object.end()
            else:
                get_key_words_state_mysql_object.end(option='rollback')
        except Exception as e:
            print(e)
            self.state_flag = 0
            # 删除MQL
            sql = '''delete from `get_key_words_state_appoint_day` where `data_time` = %s'''
            count = get_key_words_state_mysql_object.delete(sql, (str(self.data_time)))
            if count > 0:
                get_key_words_state_mysql_object.end()
            else:
                get_key_words_state_mysql_object.end(option='rollback')
        end = time.time()
        print('操作总耗时：%s' % (end - start))
    # 获取状态值(执行状态标识位、执行进度比、执行成功数、执行失败数)
    def getState(self):
        return int(self.state_flag), int(self.flag_object.getSuccessValue()), int(self.flag_object.getFailValue())

# 分词(账户粉丝更新)
class KeyWordsByUserFans(object):
    def __init__(self):
        self.monitor_lock = threading.Lock()
        self.flag_object = FlagMessage()
        self.config_object = ConfigParam()
        # 默认状态为正在处理
        self.state_flag = 2

    # 设置IP地址、原始数据存放位置、分词工具对象
    def setParam(self, ip_address, data_base_name, data_table_name, order_number, pro_obj):
        self.mysql_object = MyPymysqlPool(ip_address, data_base_name, 'root', '123456')
        self.data_table_name = data_table_name
        self.order_number = order_number
        self.pro_obj = pro_obj
        self.output_basic_path = self.config_object.getGetKeyWordsByUserFansOutputFolderPath() + str(self.order_number) + '/'
        self.output_path = self.output_basic_path + str(self.order_number) + '.csv'
        # 默认状态为正在处理
        self.state_flag = 2

    # 读数据表内内容
    def readDataFromDataBase(self, mysql_object, data_table_name):
        mblog_sql = self.config_object.getMySqlSelectMblogInfosSQL() + ' ' + str(data_table_name)
        results = mysql_object.getAll(mblog_sql)
        data_list = []
        for result in results:
            # uid, mid, text, retweeted_text, created_at
            if result['retweeted_text'] is None:
                new_result = {'uid': result['uid'], 'mid': result['mid'], 'text': str(result['text']) + '。', 'created_at': result['created_at']}
            else:
                new_result = {'uid': result['uid'], 'mid': result['mid'], 'text': str(result['text']) + '。' + str(result['retweeted_text']), 'created_at': result['created_at']}
            data_list.append(new_result)

        # 释放资源
        mysql_object.end()
        mysql_object.dispose()
        return data_list

    # 求词语集合与给定词库结合的交集
    def getLable(self, words, poiResult):
        # 提取关键词
        wordDict = {}
        # 找到可以打标签的词
        words = [word for word in words if word in poiResult]
        for word in words:
            try:
                number = wordDict[word]
            except:
                number = 0
            number += 1
            wordDict[word] = number
        return wordDict

    # 将标签类型和标签值写入Dict
    def lablePossing(self, uLabel, words, Result, flag):
        wordDict = self.getLable(words, Result)
        if wordDict:
            uLabel[flag] = wordDict
        return uLabel

    # 清洗博文
    def cleanBlogData(self, blogText):
        # 正则匹配特殊符号
        pat = re.compile('(?<=\>).*?(?=\<)')
        blogNewText = ''.join(pat.findall(blogText))
        if blogNewText == '':
            pass
        else:
            blogText = blogNewText
        blogText = blogText.replace(' ', '')
        blogText = blogText.replace('\n', '')
        # 正则匹配话题
        weiboTopic = re.findall(r'#([\u4e00-\u9fa5\w\-]+)#', blogText)
        for topic in weiboTopic:
            topicAll = '#' + topic + '#'
            blogText = blogText.replace(topicAll, '')
        blogText = blogText.replace(' ', '')
        return blogText

    # 提取标签
    def getLabelFromBlog(self, obj, blog):
        uLabel = {}
        # 清洗博文
        blog = self.cleanBlogData(blog)
        # 分词
        results = obj.pesg.cut(blog)
        words = [s.word for s in results]
        stop_word = set(words) & obj.stop_words
        # 去除停留词
        words = [word for word in words if word not in stop_word]
        for keyName in obj.word_library.keys():
            # 提取标签
            uLabel = self.lablePossing(uLabel, words, obj.word_library[keyName], keyName)
        return uLabel

    # 执行函数
    def processFun(self, data):
        try:
            # 处理uid下所有博文
            data_new_list = []
            classification_list = ['ebook', 'enterprise', 'music', 'film', 'city', 'product', 'brand', 'star', 'app']
            uLabel = self.getLabelFromBlog(self.pro_obj, data['text'])
            row_data = [data['uid'], data['mid']]
            for classification_name in classification_list:
                try:
                    class_value_dict = uLabel[classification_name]
                except:
                    class_value_dict = {}
                row_data.append(class_value_dict)
            # 时间标准化
            if ' ' in str(data['created_at']):
                number = str(data['created_at']).count(':', 0, len(str(data['created_at'])))
                if number >= 2:
                    created_at_standardization = str(data['created_at'])
                elif number == 1:
                    created_at_standardization = str(data['created_at']) + ':00'
                else:
                    created_at_standardization = str(data['created_at']) + ':00:00'
                created_at_standardization = datetime.datetime.strptime(created_at_standardization, '%Y-%m-%d %H:%M:%S')
            else:
                created_at_standardization = datetime.datetime.strptime(str(data['created_at']), '%Y-%m-%d')
            row_data.append(created_at_standardization)
            row_data.append('')
            data_new_list.append(row_data)
            return {'flag': True, 'error_message': '', 'data': data_new_list, 'uid': data['uid']}
        # 提高系统健硕性
        except Exception as e:
            return {'flag': False, 'error_message': str(e), 'data': '', 'uid': data['uid']}

    # 写结果函数
    def writeResultFun(self, output_path, data_list):
        with open(output_path, 'a+', newline='', encoding='utf-8') as f:
            f_csv = csv.writer(f)
            for data in data_list:
                f_csv.writerow(data)
        f.close()

    # 监控函数
    def monitorFun(self, res):
        res = res.result()
        # 锁机制
        self.monitor_lock.acquire()
        try:
            # 写结果阶段
            self.writeResultFun(self.output_path, res['data'])
            # 公共标识量改变阶段
            if res['flag'] == True:
                self.flag_object.updataSuccessValue()
            else:
                self.flag_object.updataFailValue()
        except:
            self.flag_object.updataFailValue()
        self.monitor_lock.release()

    # 读取文件夹下所有文件
    def readFileList(self, folder_path):
        file_list = os.listdir(folder_path)
        file_list = [folder_path + f for f in file_list]
        return file_list

    # 流程函数
    def main(self, get_key_words_state_mysql_object):
        start = time.time()
        try:
            self.flag_object.setSuccessValue(0)
            self.flag_object.setFailValue(0)
            # 更新MQL
            sql = '''update `get_key_words_state_user_fans` set `process_state` = %s where `order_number` = %s'''
            count = get_key_words_state_mysql_object.update(sql, ('processing', str(self.order_number)))
            if count > 0:
                get_key_words_state_mysql_object.end()
            else:
                get_key_words_state_mysql_object.end(option='rollback')
            # 读取文件内容
            data_list = self.readDataFromDataBase(self.mysql_object, self.data_table_name)
            # 写表头
            if os.path.isdir(self.output_basic_path):
                pass
            else:
                os.mkdir(self.output_basic_path)
            with open(self.output_path, 'a+', newline='', encoding='utf-8') as f:
                f_csv = csv.writer(f)
                f_csv.writerow(['uid', 'mid', 'ebook', 'enterprise', 'music', 'film', 'city', 'product', 'brand', 'star', 'app', 'created_at', 'weight'])
            f.close()
            # 通过开启线程池内线程来完成任务
            self.threading_pool = ThreadPoolExecutor(int(multiprocessing.cpu_count()))
            for data in data_list:
                self.threading_pool.submit(self.processFun, data).add_done_callback(self.monitorFun)
            # 所有任务完成执行(此次不考虑各线程内子任务是否执行成功与否)
            self.threading_pool.shutdown()
            self.state_flag = 1
            # 更新MQL
            sql = '''update `get_key_words_state_user_fans` set `process_state` = %s, `data_base_name` = %s, `result_data_url` = %s, `result_success_number` = %s, `result_fail_number` = %s where `order_number` = %s'''
            count = get_key_words_state_mysql_object.update(sql, ('finish', self.config_object.getLocalHostDataBaseName(), self.output_basic_path, self.flag_object.getSuccessValue(), self.flag_object.getFailValue(), str(self.order_number)))
            if count > 0:
                get_key_words_state_mysql_object.end()
            else:
                get_key_words_state_mysql_object.end(option='rollback')
        except Exception as e:
            print(e)
            self.state_flag = 0
            # 删除MQL
            sql = '''delete from `get_key_words_state_user_fans` where `order_number` = %s'''
            count = get_key_words_state_mysql_object.delete(sql, (str(self.order_number)))
            if count > 0:
                get_key_words_state_mysql_object.end()
            else:
                get_key_words_state_mysql_object.end(option='rollback')
        end = time.time()
        print('操作总耗时：%s' % (end - start))
    # 获取状态值(执行状态标识位、执行进度比、执行成功数、执行失败数)
    def getState(self):
        return int(self.state_flag), int(self.flag_object.getSuccessValue()), int(self.flag_object.getFailValue())

# 分词(营销事件追踪更新)
class KeyWordsByMarketingEventTrack(object):
    def __init__(self):
        self.monitor_lock = threading.Lock()
        self.flag_object = FlagMessage()
        self.config_object = ConfigParam()
        # 默认状态为正在处理
        self.state_flag = 2

    # 设置IP地址、原始数据存放位置、分词工具对象
    def setParam(self, ip_address, data_base_name, data_table_name, order_number, pro_obj):
        self.mysql_object = MyPymysqlPool(ip_address, data_base_name, 'root', '123456')
        self.data_table_name = data_table_name
        self.order_number = order_number
        self.pro_obj = pro_obj
        self.output_basic_path = self.config_object.getGetKeyWordsByMarketingEventTrackOutputFolderPath() + str(self.order_number) + '/'
        self.output_path = self.output_basic_path + str(self.order_number) + '.csv'
        # 默认状态为正在处理
        self.state_flag = 2

    # 读数据表内内容
    def readDataFromDataBase(self, mysql_object, data_table_name):
        mblog_sql = self.config_object.getMySqlSelectMblogInfosSQL() + ' ' + str(data_table_name)
        print(mblog_sql)
        results = mysql_object.getAll(mblog_sql)
        data_list = []
        for result in results:
            # uid, mid, text, retweeted_text, created_at
            if result['retweeted_text'] is None:
                new_result = {'uid': result['uid'], 'mid': result['mid'], 'text': str(result['text']) + '。', 'created_at': result['created_at']}
            else:
                new_result = {'uid': result['uid'], 'mid': result['mid'], 'text': str(result['text']) + '。' + str(result['retweeted_text']), 'created_at': result['created_at']}
            data_list.append(new_result)

        # 释放资源
        mysql_object.end()
        mysql_object.dispose()
        return data_list

    # 求词语集合与给定词库结合的交集
    def getLable(self, words, poiResult):
        # 提取关键词
        wordDict = {}
        # 找到可以打标签的词
        words = [word for word in words if word in poiResult]
        for word in words:
            try:
                number = wordDict[word]
            except:
                number = 0
            number += 1
            wordDict[word] = number
        return wordDict

    # 将标签类型和标签值写入Dict
    def lablePossing(self, uLabel, words, Result, flag):
        wordDict = self.getLable(words, Result)
        if wordDict:
            uLabel[flag] = wordDict
        return uLabel

    # 清洗博文
    def cleanBlogData(self, blogText):
        # 正则匹配特殊符号
        pat = re.compile('(?<=\>).*?(?=\<)')
        blogNewText = ''.join(pat.findall(blogText))
        if blogNewText == '':
            pass
        else:
            blogText = blogNewText
        blogText = blogText.replace(' ', '')
        blogText = blogText.replace('\n', '')
        # 正则匹配话题
        weiboTopic = re.findall(r'#([\u4e00-\u9fa5\w\-]+)#', blogText)
        for topic in weiboTopic:
            topicAll = '#' + topic + '#'
            blogText = blogText.replace(topicAll, '')
        blogText = blogText.replace(' ', '')
        return blogText

    # 提取标签
    def getLabelFromBlog(self, obj, blog):
        uLabel = {}
        # 清洗博文
        blog = self.cleanBlogData(blog)
        # 分词
        results = obj.pesg.cut(blog)
        words = [s.word for s in results]
        stop_word = set(words) & obj.stop_words
        # 去除停留词
        words = [word for word in words if word not in stop_word]
        for keyName in obj.word_library.keys():
            # 提取标签
            uLabel = self.lablePossing(uLabel, words, obj.word_library[keyName], keyName)
        return uLabel

    # 执行函数
    def processFun(self, data):
        try:
            # 处理uid下所有博文
            data_new_list = []
            classification_list = ['ebook', 'enterprise', 'music', 'film', 'city', 'product', 'brand', 'star', 'app']
            uLabel = self.getLabelFromBlog(self.pro_obj, data['text'])
            row_data = [data['uid'], data['mid']]
            for classification_name in classification_list:
                try:
                    class_value_dict = uLabel[classification_name]
                except:
                    class_value_dict = {}
                row_data.append(class_value_dict)
            # 时间标准化
            if ' ' in str(data['created_at']):
                number = str(data['created_at']).count(':', 0, len(str(data['created_at'])))
                if number >= 2:
                    created_at_standardization = str(data['created_at'])
                elif number == 1:
                    created_at_standardization = str(data['created_at']) + ':00'
                else:
                    created_at_standardization = str(data['created_at']) + ':00:00'
                created_at_standardization = datetime.datetime.strptime(created_at_standardization, '%Y-%m-%d %H:%M:%S')
            else:
                created_at_standardization = datetime.datetime.strptime(str(data['created_at']), '%Y-%m-%d')
            row_data.append(created_at_standardization)
            row_data.append('')
            data_new_list.append(row_data)
            return {'flag': True, 'error_message': '', 'data': data_new_list, 'uid': data['uid']}
        # 提高系统健硕性
        except Exception as e:
            return {'flag': False, 'error_message': str(e), 'data': '', 'uid': data['uid']}

    # 写结果函数
    def writeResultFun(self, output_path, data_list):
        with open(output_path, 'a+', newline='', encoding='utf-8') as f:
            f_csv = csv.writer(f)
            for data in data_list:
                f_csv.writerow(data)
        f.close()

    # 监控函数
    def monitorFun(self, res):
        res = res.result()
        # 锁机制
        self.monitor_lock.acquire()
        try:
            # 写结果阶段
            self.writeResultFun(self.output_path, res['data'])
            # 公共标识量改变阶段
            if res['flag'] == True:
                self.flag_object.updataSuccessValue()
            else:
                self.flag_object.updataFailValue()
        except:
            self.flag_object.updataFailValue()
        self.monitor_lock.release()

    # 读取文件夹下所有文件
    def readFileList(self, folder_path):
        file_list = os.listdir(folder_path)
        file_list = [folder_path + f for f in file_list]
        return file_list

    # 流程函数
    def main(self, get_key_words_state_mysql_object):
        start = time.time()
        try:
            self.flag_object.setSuccessValue(0)
            self.flag_object.setFailValue(0)
            # 更新MQL
            sql = '''update `get_key_words_state_marketing_event_track` set `process_state` = %s where `order_number` = %s'''
            count = get_key_words_state_mysql_object.update(sql, ('processing', str(self.order_number)))
            if count > 0:
                get_key_words_state_mysql_object.end()
            else:
                get_key_words_state_mysql_object.end(option='rollback')
            # 读取文件内容
            data_list = self.readDataFromDataBase(self.mysql_object, self.data_table_name)
            # 写表头
            if os.path.isdir(self.output_basic_path):
                pass
            else:
                os.mkdir(self.output_basic_path)
            with open(self.output_path, 'a+', newline='', encoding='utf-8') as f:
                f_csv = csv.writer(f)
                f_csv.writerow(['uid', 'mid', 'ebook', 'enterprise', 'music', 'film', 'city', 'product', 'brand', 'star', 'app', 'created_at', 'weight'])
            f.close()
            # 通过开启线程池内线程来完成任务
            self.threading_pool = ThreadPoolExecutor(int(multiprocessing.cpu_count()))
            for data in data_list:
                self.threading_pool.submit(self.processFun, data).add_done_callback(self.monitorFun)
            # 所有任务完成执行(此次不考虑各线程内子任务是否执行成功与否)
            self.threading_pool.shutdown()
            self.state_flag = 1
            # 更新MQL
            sql = '''update `get_key_words_state_marketing_event_track` set `process_state` = %s, `data_base_name` = %s, `result_data_url` = %s, `result_success_number` = %s, `result_fail_number` = %s where `order_number` = %s'''
            count = get_key_words_state_mysql_object.update(sql, ('finish', self.config_object.getLocalHostDataBaseName(), self.output_basic_path, self.flag_object.getSuccessValue(), self.flag_object.getFailValue(), str(self.order_number)))
            if count > 0:
                get_key_words_state_mysql_object.end()
            else:
                get_key_words_state_mysql_object.end(option='rollback')
        except Exception as e:
            print(e)
            self.state_flag = 0
            # 删除MQL
            sql = '''delete from `get_key_words_state_marketing_event_track` where `order_number` = %s'''
            count = get_key_words_state_mysql_object.delete(sql, (str(self.order_number)))
            if count > 0:
                get_key_words_state_mysql_object.end()
            else:
                get_key_words_state_mysql_object.end(option='rollback')
        end = time.time()
        print('操作总耗时：%s' % (end - start))
    # 获取状态值(执行状态标识位、执行进度比、执行成功数、执行失败数)
    def getState(self):
        return int(self.state_flag), int(self.flag_object.getSuccessValue()), int(self.flag_object.getFailValue())