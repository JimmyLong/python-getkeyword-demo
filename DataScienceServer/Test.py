#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/8/20 15:39
# @Author : jimmy
# @File : Test.py
# @Software: PyCharm
import socket
hostname = socket.gethostname()
addrs = socket.getaddrinfo(hostname, None)
for item in addrs:
    if ':' not in item[4][0]:
        print('当前主机IPV4地址为:' + item[4][0])