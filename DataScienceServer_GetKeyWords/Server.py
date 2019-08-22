#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/7/29 17:13
# @Author : jimmy
# @File : Server.py
# @Software: PyCharm
from flask import Flask, request, json, jsonify
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from ConfigParam import ConfigParam
from UserProfileWordLibrary import UserprofileWordLibrary
import os
from GetKeyWords import KeyWordsByAppointDay, KeyWordsByUserFans, KeyWordsByMarketingEventTrack
from GetCommunityResultStatistics import CommunityResultStatisticsByUserFans, CommunityResultStatisticsByMarketingEventTrack
import queue
from MySqlHelper import MyPymysqlPool
import time
import redis

app = Flask(__name__)
work_pool = ThreadPoolExecutor(int(multiprocessing.cpu_count()))
get_key_words_by_appoint_day_queue = queue.Queue(maxsize=0)
config_object = ConfigParam()
mysql_object = MyPymysqlPool(config_object.getLocalHostIpAddress(), config_object.getLocalHostDataBaseName(), 'root', '123456')
# 分词接口对象(每日更新、用户粉丝、营销事件追踪)
get_key_words_by_appoint_day_object = KeyWordsByAppointDay()
get_key_words_by_user_fans_object = KeyWordsByUserFans()
get_key_words_by_marketing_event_track_object = KeyWordsByMarketingEventTrack()
redis_pool = redis.ConnectionPool(host=config_object.getRedisConnIpAddress(), port=config_object.getRedisConnPort(), password=config_object.getRedisConnPassword(), decode_responses=True)
redis_object = redis.Redis(connection_pool=redis_pool)

# 读取文件夹下所有文件
def readFileList(folder_path):
    file_list = os.listdir(folder_path)
    file_list = [folder_path + f for f in file_list]
    return file_list

# 加载词库，并实例化词库对象
word_library_path = []
word_library_path.append(config_object.getStopWordsFilePath())
for file_path in readFileList(config_object.getWordLibraryFolderPath()):
    word_library_path.append(file_path)

pro_obj = UserprofileWordLibrary.instance(word_library_path)

# 分词功能(每日更新)
def getKeyWordByAppointDay():
    # 轮询Redis
    while True:
        result = redis_object.spop('weibo:get_key_word_appoint_day_job:get_key_word_appoint_day_param')
        # 无值时休眠1秒
        if result is None:
            time.sleep(1)
        else:
            if not isinstance(result, str):
                result = result.decode('utf-8')
            res = eval(result)
            print('新任务开始分词')
            ip_address = res['ip_address']
            data_base_name = res['data_base_name']
            data_table_name = res['data_table_name']
            data_time = res['data_time']
            get_key_words_by_appoint_day_object.setParam(ip_address, data_base_name, data_table_name,
                                                         data_time, pro_obj)
            get_key_words_by_appoint_day_object.main(mysql_object)
            print('%s 分词完毕' % data_time)

# 分词功能(账户粉丝)
def getKeyWordByUserFans():
    # 轮询Redis
    while True:
        result = redis_object.spop('weibo:get_key_word_user_fans_job:get_key_word_user_fans_param')
        # 无值时休眠1秒
        if result is None:
            time.sleep(1)
        else:
            if not isinstance(result, str):
                result = result.decode('utf-8')
            res = eval(result)
            print('新任务开始分词')
            ip_address = res['ip_address']
            data_base_name = res['data_base_name']
            data_table_name = res['data_table_name']
            order_number = res['order_number']
            get_key_words_by_user_fans_object.setParam(ip_address, data_base_name, data_table_name,
                                                       order_number, pro_obj)
            get_key_words_by_user_fans_object.main(mysql_object)
            print('%s 分词完毕' % order_number)

# 分词功能(营销事件追踪)
def getKeyWordByMarketingEventTrack():
    # 轮询Redis
    while True:
        result = redis_object.spop('weibo:get_key_word_marketing_event_track_job:get_key_word_marketing_event_track_param')
        # 无值时休眠1秒
        if result is None:
            time.sleep(1)
        else:
            if not isinstance(result, str):
                result = result.decode('utf-8')
            res = eval(result)
            print('新任务开始分词')
            ip_address = res['ip_address']
            data_base_name = res['data_base_name']
            data_table_name = res['data_table_name']
            order_number = res['order_number']
            get_key_words_by_marketing_event_track_object.setParam(ip_address, data_base_name, data_table_name,
                                                       order_number, pro_obj)
            get_key_words_by_marketing_event_track_object.main(mysql_object)
            print('%s 分词完毕' % order_number)

# 异步开启线程
work_pool.submit(getKeyWordByAppointDay)
work_pool.submit(getKeyWordByUserFans)
work_pool.submit(getKeyWordByMarketingEventTrack)

@app.route('/stratGetKeyWordsWorkByAppointDay/', methods=['POST'])
def stratGetKeyWordsWorkByAppointDay():
    # 从request中获取表单请求的参数信息
    data_time = request.form['dateTime']
    ip_address = request.form['originalDataIpAddress']
    data_base_name = request.form['originalDataBaseName']
    data_table_name = request.form['originalDataBaseTable']
    results = mysql_object.getAll(sql='''select * from get_key_words_state_appoint_day where data_time = %s''', param=str(data_time))
    if results != False:
        if results[0]['process_state'] == 'finish':
            mysql_object.end()
            response_message = {'isSuccess': 1, 'processState': 100.00, 'keyWordIpAddress': results[0]['result_ip_address'],
                               'keyWordDataUrl': results[0]['result_data_url'], 'dateTime': data_time}
            return jsonify(response_message)
        elif results[0]['process_state'] == 'waiting':
            mysql_object.end()
            response_message = {'isSuccess': 2, 'processState': 0, 'keyWordIpAddress': '', 'keyWordDataUrl': '',
                               'dateTime': data_time}
            return jsonify(response_message)
        elif results[0]['process_state'] == 'processing':
            mysql_object.end()
            is_success, success_number, fail_number = get_key_words_by_appoint_day_object.getState()
            process_state = 50.00
            response_message = {'isSuccess': 2, 'processState': process_state, 'keyWordIpAddress': results[0]['result_ip_address'],
                               'keyWordDataUrl': results[0]['result_data_url'], 'dateTime': data_time}
            return jsonify(response_message)
        else:
            mysql_object.end()
            response_message = {'message': results[0]['process_state']}
            return jsonify(response_message)
    else:
        # 插入MQL
        sql = '''insert into `get_key_words_state_appoint_day` (`data_time`, `process_state`, `result_ip_address`, `data_base_name`, `data_table_name`, `result_data_url`, `result_success_number`, `result_fail_number`) values (%s, %s, %s, %s, %s, %s, %s, %s)'''
        count = mysql_object.insert(sql, (data_time, 'waiting', '192.168.2.19', data_base_name, data_table_name, 'null', 0, 0))
        if count > 0:
            mysql_object.end()
        else:
            mysql_object.end(option='rollback')
        # 插入Redis
        redis_str = str({'data_time': data_time, 'ip_address': ip_address, 'data_base_name': data_base_name, 'data_table_name': data_table_name})
        redis_object.sadd('weibo:get_key_word_appoint_day_job:get_key_word_appoint_day_param', redis_str)
        # 返回值
        response_message = {'isSuccess': 2, 'processState': 0, 'keyWordIpAddress': '', 'keyWordDataUrl': '', 'dateTime': data_time}
        return jsonify(response_message)

@app.route('/stratGetKeyWordsWorkByUserFans/', methods=['POST'])
def stratGetKeyWordsWorkByUserFans():
    # 从request中获取表单请求的参数信息
    order_number = request.form['orderNumber']
    ip_address = request.form['originalDataIpAddress']
    data_base_name = request.form['originalDataBaseName']
    data_table_name = request.form['originalDataBaseTable']
    results = mysql_object.getAll(sql='''select * from get_key_words_state_user_fans where order_number = %s''', param=str(order_number))
    if results != False:
        if results[0]['process_state'] == 'finish':
            mysql_object.end()
            response_message = {'isSuccess': 1, 'processState': 100.00, 'keyWordIpAddress': results[0]['result_ip_address'],
                               'keyWordDataUrl': results[0]['result_data_url'], 'orderNumber': order_number}
            return jsonify(response_message)
        elif results[0]['process_state'] == 'waiting':
            mysql_object.end()
            response_message = {'isSuccess': 2, 'processState': 0, 'keyWordIpAddress': '', 'keyWordDataUrl': '',
                               'orderNumber': order_number}
            return jsonify(response_message)
        elif results[0]['process_state'] == 'processing':
            mysql_object.end()
            is_success, success_number, fail_number = get_key_words_by_user_fans_object.getState()
            process_state = 50.00
            response_message = {'isSuccess': 2, 'processState': process_state, 'keyWordIpAddress': results[0]['result_ip_address'],
                               'keyWordDataUrl': results[0]['result_data_url'], 'orderNumber': order_number}
            return jsonify(response_message)
        else:
            mysql_object.end()
            response_message = {'message': results[0]['process_state']}
            return jsonify(response_message)
    else:
        # 插入MQL
        sql = '''insert into `get_key_words_state_user_fans` (`order_number`, `process_state`, `result_ip_address`, `data_base_name`, `data_table_name`, `result_data_url`, `result_success_number`, `result_fail_number`) values (%s, %s, %s, %s, %s, %s, %s, %s)'''
        count = mysql_object.insert(sql, (order_number, 'waiting', '192.168.2.19', data_base_name, data_table_name, 'null', 0, 0))
        if count > 0:
            mysql_object.end()
        else:
            mysql_object.end(option='rollback')
        # 插入Redis
        redis_str = str({'order_number': order_number, 'ip_address': ip_address, 'data_base_name': data_base_name, 'data_table_name': data_table_name})
        redis_object.sadd('weibo:get_key_word_user_fans_job:get_key_word_user_fans_param', redis_str)
        # 返回值
        response_message = {'isSuccess': 2, 'processState': 0, 'keyWordIpAddress': '', 'keyWordDataUrl': '', 'orderNumber': order_number}
        return jsonify(response_message)

@app.route('/stratGetKeyWordsWorkByMarketingEventTrack/', methods=['POST'])
def stratGetKeyWordsWorkByMarketingEventTrack():
    # 从request中获取表单请求的参数信息
    order_number = request.form['orderNumber']
    ip_address = request.form['originalDataIpAddress']
    data_base_name = request.form['originalDataBaseName']
    data_table_name = request.form['originalDataBaseTable']
    results = mysql_object.getAll(sql='''select * from get_key_words_state_marketing_event_track where order_number = %s''', param=str(order_number))
    if results != False:
        if results[0]['process_state'] == 'finish':
            mysql_object.end()
            response_message = {'isSuccess': 1, 'processState': 100.00, 'keyWordIpAddress': results[0]['result_ip_address'],
                               'keyWordDataUrl': results[0]['result_data_url'],
                               'orderNumber': order_number}
            return jsonify(response_message)
        elif results[0]['process_state'] == 'waiting':
            mysql_object.end()
            response_message = {'isSuccess': 2, 'processState': 0, 'keyWordIpAddress': '', 'keyWordDataUrl': '',
                               'orderNumber': order_number}
            return jsonify(response_message)
        elif results[0]['process_state'] == 'processing':
            mysql_object.end()
            is_success, success_number, fail_number = get_key_words_by_marketing_event_track_object.getState()
            process_state = 50.00
            response_message = {'isSuccess': 2, 'processState': process_state, 'keyWordIpAddress': results[0]['result_ip_address'],
                               'keyWordDataUrl': results[0]['result_data_url'],
                               'orderNumber': order_number}
            return jsonify(response_message)
        else:
            mysql_object.end()
            response_message = {'message': results[0]['process_state']}
            return jsonify(response_message)
    else:
        # 插入MQL
        sql = '''insert into `get_key_words_state_marketing_event_track` (`order_number`, `process_state`, `result_ip_address`, `data_base_name`, `data_table_name`, `result_data_url`, `result_success_number`, `result_fail_number`) values (%s, %s, %s, %s, %s, %s, %s, %s)'''
        count = mysql_object.insert(sql, (order_number, 'waiting', '192.168.2.19', data_base_name, data_table_name, 'null', 0, 0))
        if count > 0:
            mysql_object.end()
        else:
            mysql_object.end(option='rollback')
        # 插入Redis
        redis_str = str({'order_number': order_number, 'ip_address': ip_address, 'data_base_name': data_base_name, 'data_table_name': data_table_name})
        redis_object.sadd('weibo:get_key_word_marketing_event_track_job:get_key_word_marketing_event_track_param', redis_str)
        # 返回值
        response_message = {'isSuccess': 2, 'processState': 0, 'keyWordIpAddress': '', 'keyWordDataUrl': '', 'orderNumber': order_number}
        return jsonify(response_message)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)