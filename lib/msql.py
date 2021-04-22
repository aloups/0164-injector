#!/usr/bin/env python
"""msql a class for db mannipulation."""
# -*- coding: utf-8 -*-
#
#  msql.py
#
#  Copyright 2020 Ali Lahbabi - ali.lahbabi@gmail.com

import lib.settings as set_


class sql_engine(object):
    """Is base of the root."""

    def __init__(self, _conf_):
        """Is base of the root."""
        self.conf_ = _conf_
        self.HOST_ = set_.MYSQL[self.conf_]['HOST']
        self.PORT_ = set_.MYSQL[self.conf_]['PORT']
        self.DB_ = set_.MYSQL[self.conf_]['DB']
        self.USER_ = set_.MYSQL[self.conf_]['USER']
        self.PSW_ = set_.MYSQL[self.conf_]['PSW']
        self.PATH_LOG = set_.APPS[self.conf_]['PATH_LOG']
        self.log = set_.log_init('covid-msql.log', self.conf_)

    def connect_msql(self):
        """Is base of the root."""
        try:
            cnx = set_.mysql.connector.connect(user=self.USER_, password=self.PSW_,
                                               host=self.HOST_, port=self.PORT_,
                                               database=self.DB_)
        except set_.mysql.connector.Error as err:
            self.log.error(err)
            print(err)
            return False
        else:
            self.log.info("DB Connect " + self.DB_)
            return cnx

    def exec_msql(self, cur_, req_):
        """Is base of the root."""
        try:
            cur_.execute(req_)
        except set_.mysql.connector.Error as err:
            self.log.error(err)
            return False
        else:
            return True

    @staticmethod
    def com_sql(cur_):
        """Is base of the root."""
        cur_.execute("commit")

    def u_connect_msql(self, cnx_):
        """Is base of the root."""
        try:
            cnx_.close()
        except set_.mysql.connector.Error as err:
            self.log.error(err)
            return False
        else:
            self.log.info("DB Unconnected " + self.DB_)
            return True

    def kill_proc_mysql(self, bd_, status):
        """Killer Queen"""
        cnx_t = self.connect_msql()
        cnx_x = self.connect_msql()
        cursor_t = cnx_t.cursor()
        cursor_x = cnx_x.cursor()
        query_t = "SHOW FULL PROCESSLIST"
        cursor_t.execute(query_t)
        i_ = 0
        while 1:
            row_t = cursor_t.fetchone()
            if row_t is None:
                break
            id_process = row_t[0]
            if row_t[3] == bd_ and row_t[4] == status:
                query_x = "KILL " + str(id_process)
                try:
                    cursor_x.execute(query_x)
                except set_.mysql.connector.Error as err:
                    self.log.error("ERROR KILL SQL PROCESS => " + str(err.errno))
                    return False
                else:
                    i_ += 1
                    self.log.info("KILL SQL PROCESS OK")
                    return True

