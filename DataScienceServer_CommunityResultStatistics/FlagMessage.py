#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/7/29 17:12
# @Author : jimmy
# @File : FlagMessage.py
# @Software: PyCharm
# 标示量类
class FlagMessage(object):
    def __init__(self):
        self.success_number, self.fail_number = 0, 0

    def updataSuccessValue(self):
        self.success_number += 1

    def updataFailValue(self):
        self.fail_number += 1

    def getSuccessValue(self):
        return int(self.success_number)

    def getFailValue(self):
        return int(self.fail_number)

    def setSuccessValue(self, value_number):
        self.success_number = int(value_number)

    def setFailValue(self, value_number):
        self.fail_number = int(value_number)