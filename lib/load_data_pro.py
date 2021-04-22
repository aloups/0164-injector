#!/usr/bin/env python
"""load a json file from data_report NCBI."""
# -*- coding: utf-8 -*-
#
#  load_data_gen.py
#
#  Copyright 2020 Ali Lahbabi - ali.lahbabi@gmail.com

import lib.settings as set_
import lib.msql as msql_


class gbff_engine(object):
    """Is base of the root."""

    def __init__(self, _conf_):
        """Is base of the root."""
        self.conf_ = _conf_
        self.PATH_LOG = set_.APPS[self.conf_]['PATH_LOG']
        self.log = set_.log_init('covid-l_pro.log', self.conf_)
        self.PATH_DATA = set_.DATA[self.conf_]['PATH']
        self.DB_ = set_.MYSQL[self.conf_]['DB']
        pass

    def retro_gen(self, aa_):
        """Is base of the root."""
        dict_ = {'A': 'GCU', 'R': 'CGU', 'N': 'AAU',
                 'D': 'GAU', 'C': 'UGU', 'E': 'GAA',
                 'Q': 'CAA', 'G': 'GGU', 'H': 'CAU',
                 'I': 'AUU', 'L': 'UUA', 'K': 'AAA',
                 'M': 'AUG', 'F': 'UUU', 'P': 'CCU',
                 'O': 'UAG', 'U': 'UGA', 'S': 'UCU',
                 'T': 'ACU', 'W': 'UGG', 'Y': 'UAU',
                 'V': 'GUU'}
        return dict_[aa_]

    def input_file(self, file_):
        """Is base of the root."""
        eng_ = msql_.sql_engine(self.conf_)
        cnx_ = eng_.connect_msql()
        cur_ = cnx_.cursor()
        with open(self.PATH_DATA + file_, 'r+') as f_:
            str_ = ''
            _accession = ''
            _gen_name = ''
            _proteinId = ''
            _translation = ''
            _codons = []
            _rank = 0
            for line_ in f_:
                str_ += line_
                if line_[0:2] == '//':
                    if str_.find('VERSION') != -1:
                        pos_s = str_.index('VERSION')+12
                        str_ = str_[pos_s:]
                        _accession = str_[0:10]
                    cds_ = ''
                    _rank = 0
                    for i in set_.re.finditer('CDS', str_):
                        _rank += 1
                        cds_ = str_[i.start():]

                        # gen
                        try:
                            pos_gen = cds_.index('/gene="')+7
                            cds_ = cds_[pos_gen:]
                            pos_end = cds_.index('\n')-1
                            _gen_name = cds_[0:pos_end]
                            cds_ = cds_[pos_end:]
                        except Exception as err:
                            self.log.error(err)
                            break

                        # Id Protein
                        try:
                            pos_gen = cds_.index('/protein_id="') + 13
                            cds_ = cds_[pos_gen:]
                            pos_end = cds_.index('\n') - 1
                            _proteinId = cds_[0:pos_end]
                            cds_ = cds_[pos_end:]
                        except Exception as err:
                            self.log.error(err)
                            break

                        # translation
                        try:
                            pos_gen = cds_.index('/translation="')+14
                            cds_ = cds_[pos_gen:]
                            pos_end = cds_.index('"')
                            _translation = cds_[0:pos_end].replace('\n', '').replace(' ', '')
                            cds_ = cds_[pos_end:]
                            cds_ = ''
                        except Exception as err:
                            self.log.error(err)
                            break

                        # retro translation
                        _codons = []
                        for c_ in _translation:
                            try:
                                _codons.append(self.retro_gen(c_))
                            except Exception as err:
                                self.log.error(err)
                                break
                        query_G = ("INSERT INTO " + self.DB_ + ".PROTEINS VALUES ('"
                                   + str(_accession).replace("'", "") + "','"
                                   + str(_gen_name).replace("'", "") + "','"
                                   + str(_proteinId).replace("'", "") + "','"
                                   + str(_translation).replace("'", "") + "','"
                                   + str(_codons).replace("'", "") + "','"
                                   + str(len(_codons)) + "','"
                                   + str(_rank).replace("'", "") + "');")
                        eng_.exec_msql(cur_, query_G)
                        eng_.com_sql(cur_)
                    str_ = ''
            eng_.u_connect_msql(cnx_)
        f_.close()

