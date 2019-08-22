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
import os
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
# 统计分群静态结果()
get_community_result_statistics_by_user_fans_object = CommunityResultStatisticsByUserFans()
get_community_result_statistics_by_marketing_event_track_object = CommunityResultStatisticsByMarketingEventTrack()
redis_pool = redis.ConnectionPool(host=config_object.getRedisConnIpAddress(), port=config_object.getRedisConnPort(), password=config_object.getRedisConnPassword(), decode_responses=True)
redis_object = redis.Redis(connection_pool=redis_pool)

# 读取文件夹下所有文件
def readFileList(folder_path):
    file_list = os.listdir(folder_path)
    file_list = [folder_path + f for f in file_list]
    return file_list

# 分群统计信息功能(账户粉丝)
def getCommunityResultStatisticsByUserFans():
    # 轮询Redis
    while True:
        result = redis_object.spop('weibo:get_community_result_statistics_user_fans_job:get_community_result_statistics_user_fans_param')
        # 无值时休眠1秒
        if result is None:
            time.sleep(1)
        else:
            if not isinstance(result, str):
                result = result.decode('utf-8')
            res = eval(result)
            print('新任务开始统计')
            order_number = res['order_number']
            static_info_ip_address = res['static_info_ip_address']
            static_info_data_base_name = res['static_info_data_base_name']
            static_info_data_table_name = res['static_info_data_table_name']
            communtry_results_ip_address = res['communtry_results_ip_address']
            communtry_results_url = res['communtry_results_url']
            get_community_result_statistics_by_user_fans_object.setParam(order_number, static_info_ip_address, static_info_data_base_name, static_info_data_table_name, communtry_results_ip_address, communtry_results_url)
            get_community_result_statistics_by_user_fans_object.main(mysql_object)
            print('%s 统计完毕' % order_number)

# 分群统计信息功能(营销事件追踪)
def getCommunityResultStatisticsByMarketingEventTrack():
    # 轮询Redis
    while True:
        result = redis_object.spop('weibo:get_community_result_statistics_marketing_event_track_job:get_community_result_statistics_marketing_event_track_param')
        # 无值时休眠1秒
        if result is None:
            time.sleep(1)
        else:
            if not isinstance(result, str):
                result = result.decode('utf-8')
            res = eval(result)
            print('新任务开始统计')
            order_number = res['order_number']
            original_data_ip_address = res['original_data_ip_address']
            original_data_base_name = res['original_data_base_name']
            original_data_aittitude_table_name = res['original_data_aittitude_table_name']
            original_data_comment_table_name = res['original_data_comment_table_name']
            original_data_repost_table_name = res['original_data_repost_table_name']
            communtry_results_ip_address = res['communtry_results_ip_address']
            communtry_results_url = res['communtry_results_url']
            get_community_result_statistics_by_marketing_event_track_object.setParam(order_number, original_data_ip_address,
                                                                         original_data_base_name,
                                                                         original_data_aittitude_table_name,
                                                                         original_data_comment_table_name,
                                                                         original_data_repost_table_name,
                                                                         communtry_results_ip_address,
                                                                         communtry_results_url)
            get_community_result_statistics_by_marketing_event_track_object.main(mysql_object)
            print('%s 统计完毕' % order_number)

# 异步开启线程
work_pool.submit(getCommunityResultStatisticsByUserFans)
work_pool.submit(getCommunityResultStatisticsByMarketingEventTrack)

@app.route('/userFans/CommunityResultsStatistics/', methods=['POST'])
def stratGetCommunityResultsStatisticsByUserFans():
    # 从request中获取表单请求的参数信息
    order_number = request.form['orderNumber']
    static_info_ip_address = request.form['staticInfoIpAddress']
    static_info_data_base_name = request.form['staticInfoDataBaseName']
    static_info_data_table_name = request.form['staticInfoDataBaseTable']
    communtry_results_ip_address = request.form['communityResultsIpAddress']
    communtry_results_url = request.form['communityResultsUrl']
    if str(communtry_results_url).endswith('/'):
        pass
    else:
        communtry_results_url = communtry_results_url + '/'
    results = mysql_object.getAll(sql='''select * from fans_community_result where order_number = %s''', param=str(order_number))
    if results != False:
        if results[0]['process_state'] == 'finish':
            mysql_object.end()
            response_message = {'isSuccess': 1, 'processState': 100.00, 'orderNumber': str(order_number), 'statisticsDataIpAddress': '192.168.2.35', 'statisticsDataBaseName': 'weibo', 'statisticsDataBaseTable': 'fans_community_result'}
            return jsonify(response_message)
        elif results[0]['process_state'] == 'waiting':
            mysql_object.end()
            response_message = {'isSuccess': 2, 'processState': 0.0, 'orderNumber': str(order_number), 'statisticsDataIpAddress': '', 'statisticsDataBaseName': '', 'statisticsDataBaseTable': ''}
            return jsonify(response_message)
        elif results[0]['process_state'] == 'processing':
            mysql_object.end()
            process_state = 50.00
            response_message = {'isSuccess': 2, 'processState': process_state, 'orderNumber': str(order_number), 'statisticsDataIpAddress': '192.168.2.35', 'statisticsDataBaseName': 'weibo', 'statisticsDataBaseTable': 'fans_community_result'}
            return jsonify(response_message)
        else:
            mysql_object.end()
            response_message = {'message': results[0]['process_state']}
            return jsonify(response_message)
    else:
        # 插入MQL
        sql = '''insert into `fans_community_result` (`order_number`, `process_state`, `community_total`, `community_size_info`, `not_community_user_number`) values (%s, %s, %s, %s, %s)'''
        count = mysql_object.insert(sql, (order_number, 'waiting', 0, 'null', 0))
        if count > 0:
            mysql_object.end()
        else:
            mysql_object.end(option='rollback')
        # 插入Redis
        redis_str = str({'order_number': order_number, 'static_info_ip_address': static_info_ip_address, 'static_info_data_base_name': static_info_data_base_name, 'static_info_data_table_name': static_info_data_table_name, 'communtry_results_ip_address': communtry_results_ip_address, 'communtry_results_url': communtry_results_url})
        redis_object.sadd('weibo:get_community_result_statistics_user_fans_job:get_community_result_statistics_user_fans_param', redis_str)
        # 返回值
        response_message = {'isSuccess': 2, 'processState': 0.0, 'orderNumber': str(order_number), 'statisticsDataIpAddress': '', 'statisticsDataBaseName': '', 'statisticsDataBaseTable': ''}
        return jsonify(response_message)

@app.route('/marketingEventTrack/CommunityResultsStatistics/', methods=['POST'])
def stratGetCommunityResultsStatisticsByMarketingEventTrack():
    # 从request中获取表单请求的参数信息
    order_number = request.form['orderNumber']
    original_data_ip_address = request.form['originalDataIpAddress']
    original_data_base_name = request.form['originalDataBaseName']
    original_data_aittitude_table_name = request.form['originalDataBaseAttitudeTable']
    original_data_comment_table_name = request.form['originalDataBaseCommentTable']
    original_data_repost_table_name = request.form['originalDataBaseRepostTable']
    communtry_results_ip_address = request.form['communityResultsIpAddress']
    communtry_results_url = request.form['communityResultsUrl']
    if str(communtry_results_url).endswith('/'):
        pass
    else:
        communtry_results_url = communtry_results_url + '/'
    results = mysql_object.getAll(sql='''select * from mblog_track_community_result where order_number = %s''', param=str(order_number))
    if results != False:
        if results[0]['process_state'] == 'finish':
            mysql_object.end()
            response_message = {'isSuccess': 1, 'processState': 100.00, 'orderNumber': str(order_number), 'statisticsDataIpAddress': '192.168.2.35', 'statisticsDataBaseName': 'weibo', 'statisticsDataBaseTable': 'mblog_track_community_result'}
            return jsonify(response_message)
        elif results[0]['process_state'] == 'waiting':
            mysql_object.end()
            response_message = {'isSuccess': 2, 'processState': 0.0, 'orderNumber': str(order_number), 'statisticsDataIpAddress': '', 'statisticsDataBaseName': '', 'statisticsDataBaseTable': ''}
            return jsonify(response_message)
        elif results[0]['process_state'] == 'processing':
            mysql_object.end()
            process_state = 50.00
            response_message = {'isSuccess': 2, 'processState': process_state, 'orderNumber': str(order_number), 'statisticsDataIpAddress': '192.168.2.35', 'statisticsDataBaseName': 'weibo', 'statisticsDataBaseTable': 'mblog_track_community_result'}
            return jsonify(response_message)
        else:
            mysql_object.end()
            response_message = {'message': results[0]['process_state']}
            return jsonify(response_message)
    else:
        # 插入MQL
        sql = '''insert into `mblog_track_community_result` (`order_number`, `process_state`, `community_total`, `community_size_info`, `not_community_user_number`) values (%s, %s, %s, %s, %s)'''
        count = mysql_object.insert(sql, (order_number, 'waiting', 0, 'null', 0))
        if count > 0:
            mysql_object.end()
        else:
            mysql_object.end(option='rollback')
        # 插入Redis
        redis_str = str({'order_number': order_number, 'original_data_ip_address': original_data_ip_address,
                         'original_data_base_name': original_data_base_name,
                         'original_data_aittitude_table_name': original_data_aittitude_table_name,
                         'original_data_comment_table_name': original_data_comment_table_name,
                         'original_data_repost_table_name': original_data_repost_table_name,
                         'communtry_results_ip_address': communtry_results_ip_address,
                         'communtry_results_url': communtry_results_url})
        redis_object.sadd(
            'weibo:get_community_result_statistics_marketing_event_track_job:get_community_result_statistics_marketing_event_track_param',
            redis_str)
        # 返回值
        response_message = {'isSuccess': 2, 'processState': 0.0, 'orderNumber': str(order_number), 'statisticsDataIpAddress': '', 'statisticsDataBaseName': '', 'statisticsDataBaseTable': ''}
        return jsonify(response_message)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8081)