#!/usr/bin/env python
"""load a json file from data_report NCBI."""
# -*- coding: utf-8 -*-
#
#  load_data_gen.py
#
#  Copyright 2020 Ali Lahbabi - ali.lahbabi@gmail.com

import lib.settings as set_
import lib.msql as msql_


class json_engine(object):
    """Is base of the root."""

    def __init__(self, _conf_):
        """Is base of the root."""
        self.conf_ = _conf_
        self.PATH_LOG = set_.APPS[self.conf_]['PATH_LOG']
        self.log = set_.log_init('covid-l_gen.log', self.conf_)
        self.PATH_DATA = set_.DATA[self.conf_]['PATH']
        self.DB_ = set_.MYSQL[self.conf_]['DB']
        pass

    def input_file(self, file_):
        """Is base of the root."""
        eng_ = msql_.sql_engine(self.conf_)
        cnx_ = eng_.connect_msql()
        cur_ = cnx_.cursor()
        with open(self.PATH_DATA + file_, 'r+') as f_:
            for line_ in f_:
                _accession = ''
                _nucleos = ''
                _codons = []
                try:
                    if line_[0] is '>':
                        pos_ = line_.index(' ')
                        _accession = line_[1:pos_]
                        _nucleos = ''
                    while 1:
                        read_ = next(f_)
                        if ord(read_[:1]) is 62:
                            pos_ = read_.index(' ')
                            _accession = read_[1:pos_]
                            break
                        else:
                            _nucleos += read_[:-1]
                except Exception as err:
                    self.log.error(err)
                else:
                    n_ = 3
                    _codons_00 = [_nucleos[i:i+n_] for i in range(0, len(_nucleos), n_)]
                    _codons_01 = [_nucleos[i:i + n_] for i in range(1, len(_nucleos), n_)]
                    _codons_02 = [_nucleos[i:i + n_] for i in range(2, len(_nucleos), n_)]
                    query_G = ("INSERT INTO " + self.DB_ + ".GENOME VALUES ('"
                               + str(_accession) + "','"
                               + str(_nucleos).replace("\n", "") + "','"
                               + str(_codons_00).replace("'", "") + "','"
                               + str(_codons_01).replace("'", "") + "','"
                               + str(_codons_02).replace("'", "") + "');")
                    eng_.exec_msql(cur_, query_G)
            eng_.com_sql(cur_)
            eng_.u_connect_msql(cnx_)
        f_.close()
