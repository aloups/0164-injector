#!/usr/bin/env python
"""load a json file from data_report NCBI."""
# -*- coding: utf-8 -*-
#
#  load_data_report.py
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
        self.log = set_.log_init('covid-l_report.log', self.conf_)
        self.PATH_DATA = set_.DATA[self.conf_]['PATH']
        self.DB_ = set_.MYSQL[self.conf_]['DB']
        pass

    def clean_file(self, file_):
        """Is base of the root."""
        with open(self.PATH_DATA + file_, 'r') as f_:
            file_in = f_.read()
        file_in = file_in.replace('True', '\"True\"')
        with open(self.PATH_DATA + file_, 'w') as f_:
            f_.write(file_in)

    def input_file(self, file_):
        """Is base of the root."""
        eng_ = msql_.sql_engine(self.conf_)
        cnx_ = eng_.connect_msql()
        cur_ = cnx_.cursor()
        with open(self.PATH_DATA + file_, 'r') as f_:
            f_list = list(f_)
        for line_ in f_list:
            res_ = set_.json.loads(line_)
            # main corpus
            try:
                _accession = res_["accession"]
                _completeness = res_["completeness"]
                try:
                    _geneCount = res_["geneCount"]
                except Exception as err:
                    self.log.error(err)
                    _geneCount = ""
                try:
                    _isAnnotated = res_["isAnnotated"]
                except Exception as err:
                    self.log.error(err)
                    _isAnnotated = ""
                try:
                    _isolate_collectionDate = res_["isolate"]["collectionDate"]
                except Exception as err:
                    self.log.error(err)
                    _isolate_collectionDate = ""
                try:
                    _isolate_name = res_["isolate"]["name"]
                except Exception as err:
                    self.log.error(err)
                    _isolate_name = ""
                try:
                    _isolate_source = res_["isolate"]["source"]
                except Exception as err:
                    self.log.error(err)
                    _isolate_source = ""
                _length = res_["length"]
                try:
                    _location_geographicLocation = res_["location"]["geographicLocation"]
                except Exception as err:
                    self.log.error(err)
                    _location_geographicLocation = ""
                try:
                    _location_geographicRegion = res_["location"]["geographicRegion"]
                except Exception as err:
                    self.log.error(err)
                    _location_geographicRegion = ""
                _molType = res_["molType"]
                _nucleotide_accessionVersion = res_["nucleotide"]["accessionVersion"]
                _nucleotide_seqId = res_["nucleotide"]["seqId"]
                _nucleotide_sequenceHash = res_["nucleotide"]["sequenceHash"]
                _nucleotide_title = res_["nucleotide"]["title"]
                _nucleotideCompleteness = res_["nucleotideCompleteness"]
                try:
                    _proteinCount = res_["proteinCount"]
                except Exception as err:
                    self.log.error(err)
                    _proteinCount = ""
                _releaseDate = res_["releaseDate"]
                _sourceDatabase = res_["sourceDatabase"]
                _updateDate = res_["updateDate"]
                _host_sciName = res_["host"]["sciName"]
                _host_taxId = res_["host"]["taxId"]
                _virus_sciName = res_["virus"]["sciName"]
                _virus_taxId = res_["virus"]["taxId"]
            except Exception as err:
                self.log.error(err)
            else:
                query_M = ("INSERT INTO " + self.DB_ + ".MAINCATALOG VALUES ('"
                           + str(_accession).replace("'", "") + "','"
                           + str(_completeness).replace("'", "") + "','"
                           + str(_geneCount).replace("'", "") + "','"
                           + str(_isAnnotated).replace("'", "") + "','"
                           + str(_isolate_collectionDate).replace("'", "") + "','"
                           + str(_isolate_name).replace("'", "") + "','"
                           + str(_isolate_source).replace("'", "") + "','"
                           + str(_length).replace("'", "") + "','"
                           + str(_location_geographicLocation).replace("'", "") + "','"
                           + str(_location_geographicRegion).replace("'", "") + "','"
                           + str(_molType).replace("'", "") + "','"
                           + str(_nucleotide_accessionVersion).replace("'", "") + "','"
                           + str(_nucleotide_seqId).replace("'", "") + "','"
                           + str(_nucleotide_sequenceHash).replace("'", "") + "','"
                           + str(_nucleotide_title).replace("'", "") + "','"
                           + str(_nucleotideCompleteness).replace("'", "") + "','"
                           + str(_proteinCount).replace("'", "") + "','"
                           + str(_releaseDate).replace("'", "") + "','"
                           + str(_sourceDatabase).replace("'", "") + "','"
                           + str(_updateDate).replace("'", "") + "','"
                           + str(_host_sciName).replace("'", "") + "','"
                           + str(_host_taxId).replace("'", "") + "','"
                           + str(_virus_sciName).replace("'", "") + "','"
                           + str(_virus_taxId).replace("'", "") + "');")
                eng_.exec_msql(cur_, query_M)

            # cds corpus
            i_ = 0
            while 1:
                try:
                    _cds_name = res_['annotation']['genes'][i_]['cds'][0]['name']
                    _nucleo_accessionVersion = res_['annotation']['genes'][i_]['cds'][0]['nucleotide']['accessionVersion']
                    _nucleo_range = res_['annotation']['genes'][i_]['cds'][0]['nucleotide']['range']
                    _nucleo_seqId = res_['annotation']['genes'][i_]['cds'][0]['nucleotide']['seqId']
                    _nucleo_sequenceHash = res_['annotation']['genes'][i_]['cds'][0]['nucleotide']['sequenceHash']
                    _nucleo_title = res_['annotation']['genes'][i_]['cds'][0]['nucleotide']['title']
                    _otherNames = res_['annotation']['genes'][i_]['cds'][0]['otherNames']
                    _proteino_accessionVersion = res_['annotation']['genes'][i_]['cds'][0]['protein']['accessionVersion']
                    _proteino_range = res_['annotation']['genes'][i_]['cds'][0]['protein']['range']
                    _proteino_seqId = res_['annotation']['genes'][i_]['cds'][0]['protein']['seqId']
                    _proteino_sequenceHash = res_['annotation']['genes'][i_]['cds'][0]['protein']['sequenceHash']
                    _proteino_title = res_['annotation']['genes'][i_]['cds'][0]['protein']['title']
                    _name = res_['annotation']['genes'][i_]['name']
                except Exception as err:
                    self.log.error(err)
                    break
                else:
                    query_CDS = ("INSERT INTO " + self.DB_ + ".CDS VALUES ('"
                                 + str(_accession) + "','"
                                 + str(_cds_name).replace("'", "") + "','"
                                 + str(_nucleo_accessionVersion).replace("'", "") + "','"
                                 + str(_nucleo_range).replace("'", "") + "','"
                                 + str(_nucleo_seqId).replace("'", "") + "','"
                                 + str(_nucleo_sequenceHash).replace("'", "") + "','"
                                 + str(_nucleo_title).replace("'", "") + "','"
                                 + str(_otherNames).replace("'", "") + "','"
                                 + str(_proteino_accessionVersion).replace("'", "") + "','"
                                 + str(_proteino_range).replace("'", "") + "','"
                                 + str(_proteino_seqId).replace("'", "") + "','"
                                 + str(_proteino_sequenceHash).replace("'", "") + "','"
                                 + str(_proteino_title).replace("'", "") + "','"
                                 + str(_name).replace("'", "") + "');")
                    eng_.exec_msql(cur_, query_CDS)
                    i_ += 1

            # Host / lineage
            i_ = 0
            while 1:
                try:
                    _lineage_name = res_['host']['lineage'][i_]['name']
                    _lineage_taxId = res_['host']['lineage'][i_]['taxId']
                except Exception as err:
                    self.log.error(err)
                    break
                else:
                    query_HOST = ("INSERT INTO " + self.DB_ + ".HOST VALUES ('"
                                  + str(_accession) + "','"
                                  + str(_lineage_name).replace("'", "") + "','"
                                  + str(_lineage_taxId).replace("'", "") + "');")
                    eng_.exec_msql(cur_, query_HOST)
                    i_ += 1

            # Virus / lineage
            i_ = 0
            while 1:
                try:
                    _lineage_name = res_['virus']['lineage'][i_]['name']
                    _lineage_taxId = res_['virus']['lineage'][i_]['taxId']
                except Exception as err:
                    self.log.error(err)
                    break
                else:
                    query_VIRUS = ("INSERT INTO " + self.DB_ + ".VIRUS VALUES ('"
                                  + str(_accession) + "','"
                                  + str(_lineage_name).replace("'", "") + "','"
                                  + str(_lineage_taxId).replace("'", "") + "');")
                    eng_.exec_msql(cur_, query_VIRUS)
                    i_ += 1
            eng_.com_sql(cur_)
        eng_.u_connect_msql(cnx_)
        return True

