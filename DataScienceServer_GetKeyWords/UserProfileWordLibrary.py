#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/7/29 17:19
# @Author : jimmy
# @File : UserProfileWordLibrary.py
# @Software: PyCharm
import threading
import os
import jieba
import jieba.posseg as pesg

# 用户画像词库
class UserprofileWordLibrary(object):
    _instance_lock = threading.Lock()

    def __init__(self, wordLibrary_path_list):
        self.wordLibrary_path_list = wordLibrary_path_list
        # 加载停用词
        self.stop_words = self.readDataFromTxtFile(inputPath=self.wordLibrary_path_list[0], dataTpye='set')
        # 加载词库
        self.word_library = {}
        for index in range(1, len(wordLibrary_path_list)):
            word_library_name = os.path.basename(wordLibrary_path_list[index]).replace('.txt', '')
            word_library_value = self.readDataFromTxtFile(inputPath=self.wordLibrary_path_list[index], dataTpye='set')
            if word_library_value != -1:
                self.word_library[word_library_name] = word_library_value
                jieba.load_userdict(wordLibrary_path_list[index])
        self.pesg = pesg

    @classmethod
    def instance(cls, *args, **kwargs):
        if not hasattr(UserprofileWordLibrary, "_instance"):
            with UserprofileWordLibrary._instance_lock:
                if not hasattr(UserprofileWordLibrary, "_instance"):
                    UserprofileWordLibrary._instance = UserprofileWordLibrary(*args, **kwargs)
        return UserprofileWordLibrary._instance

    # 读Txt文件内内容
    def readDataFromTxtFile(self, inputPath, dataTpye='List'):
        try:
            with open(inputPath, 'r', encoding='utf-8') as f:
                text = f.read()
            f.close()
            textList = text.split('\n')
            textList = list(filter(None, textList))
            if dataTpye.lower() == 'list':
                return textList
            elif dataTpye.lower() == 'set':
                return set(textList)
            else:
                return -1
        except:
            return -1