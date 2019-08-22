#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/7/29 19:03
# @Author : jimmy
# @File : MySqlHelper.py
# @Software: PyCharm
import pymysql
from pymysql.cursors import DictCursor
from DBUtils.PooledDB import PooledDB
from ConfigParam import ConfigParam

class BasePymysqlPool(object):
    def __init__(self, host, user, password, database, port):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = str(password)
        self.database = database
        self.conn = None
        self.cursor = None

class MyPymysqlPool(BasePymysqlPool):
    '''
    MySql数据库对象，负责产生数据库连接，此类中的连接采用连接池实现
    获取连接对象：conn = Mysql.getConn()
    释放连接对象：conn.close() 或 del conn
    '''
    # 连接池对象
    __pool = None

    def __init__(self, host, db_name, user_name, password, conf_name=None):
        self.config_object = ConfigParam()
        # 数据连接配置
        self.conf = {'host': host, 'user': str(user_name), 'password': str(password), 'database': db_name, 'port': self.config_object.getMySqlConnPort()}
        # 连接数据库
        super(MyPymysqlPool, self).__init__(**self.conf)
        # 数据库构造函数，从链接池中取出连接，并生成操作游标
        self._conn = self.__getConn()
        self._cursor = self._conn.cursor()

    def __getConn(self):
        '''
        静态方法，从连接池中取出连接
        :return:MySQLdb.connection
        '''
        if MyPymysqlPool.__pool is None:
            __pool = PooledDB(creator=pymysql,
                              mincached=1,
                              maxcached=20,
                              host=self.host,
                              port=self.port,
                              user=self.user,
                              password=self.password,
                              db=self.database,
                              use_unicode=None,
                              charset='utf8',
                              cursorclass=DictCursor)
            print('MySql Conn Success!')
        return __pool.connection()

    def getAll(self, sql, param=None):
        '''
        执行查询，并取出所有结果集
        :param sql: 查询SQL，如果有查询条件，请指定条件列表，并将条件值使用参数param传递进来
        :param param: 可选参数，条件列表值(元组/列表)
        :return: result list(字典对象)/boolean 查询到的结果集
        '''
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchall()
        else:
            result = False
        return result

    def getOne(self, sql, param=None):
        '''
        执行查询，并取出第一条
        :param sql: 查询SQL，如果有查询条件，请指定条件列表，并将条件值使用参数param传递进来
        :param param: 可选参数，条件列表值(元组/列表)
        :return: result list/boolean 查询到的结果集
        '''
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchone()
        else:
            result = False
        return result

    def getMany(self, sql, num, param=None):
        '''
        执行查询，并取出num条结果
        :param sql: 查询SQL，如果有查询条件，请指定条件列表，并将条件值使用参数param传递进来
        :param num: 取得的结果条数
        :param param: 可选参数，条件列表值(元组/列表)
        :return: result list/boolean 查询到的结果集
        '''
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchmany(num)
        else:
            result = False
        return result

    def __query(self, sql, param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        return count

    def update(self, sql, param=None):
        '''
        更新数据表记录
        :param sql: 更新SQL，格式及条件，使用(%s，%s)
        :param param: 要更新的值 tuple/list
        :return: count 受影响的行数
        '''
        return self.__query(sql, param)

    def insert(self, sql, param=None):
        '''
        插入数据表记录
        :param sql: 插入SQL，格式及条件，使用(%s，%s)
        :param param: 要插入的值 tuple/list
        :return: count 受影响的行数
        '''
        return self.__query(sql, param)

    def delete(self, sql, param=None):
        '''
        删除数据表记录
        :param sql: 删除SQL，格式及条件，使用(%s，%s)
        :param param: 要删除的值 tuple/list
        :return: count 受影响的行数
        '''
        return self.__query(sql, param)

    def begin(self):
        '''
        开启事务
        :return:
        '''
        self._conn.autocommit(0)

    def end(self, option='commit'):
        if option == 'commit':
            self._conn.commit()
        else:
            self._conn.rollback()

    def dispose(self, isEnd=1):
        if isEnd == 1:
            self.end('commit')
        else:
            self.end('rollback')
        self._cursor.close()
        self._conn.close()