# -- coding: utf-8 --
from repository.RWjson import jsonToreIO as JIO
import logging
from utils.logger import json_log as log
import thread
import types
import copy
LOG = log(className = __name__, log_level=logging.DEBUG)
NumberTypes = (types.IntType, types.LongType, types.FloatType, types.ComplexType)

class kv_storage(object):

    def __init__(self):
        self.jio = JIO()
        self.mem_kv = self.jio.read_json_bootstrap()

    def create_index(self, index, data):
        LOG.info("start to write an index")
        if index in self.mem_kv:
            return False
        self.mem_kv[index] = data
        try:
            thread.start_new_thread(self.jio.write_json_index, (self.mem_kv, index,))
        except:
            LOG.error("unable to start a thread to backup")
        #self.jio.write_json_index(self.mem_kv, index)
        return True

    def get_index_which(self, filter):
        data = self.show_all_index()
        filtered_data = {}
        for key in data.keys():
            if (self._if_contains(data[key], filter)):
                filtered_data[key] = data[key]
        return filtered_data

    def get_index(self, index):
        LOG.info("start to retrieve index")
        if self.check_index(index):
            return self.mem_kv[index]
        else:
            return None

    def check_index(self, index):
        LOG.info("start to check an index")
        return (index in self.mem_kv)

    def update_index(self, index, data):
        LOG.info("start to update index")
        if not index in self.mem_kv:
            return False
        self.mem_kv[index] = data
        try:
            thread.start_new_thread(self.jio.write_json_index, (self.mem_kv, index,))
        except:
            LOG.error("unable to start a thread to backup")
        #self.jio.write_json_index(self.mem_kv, index)
        return True

    def delete_index(self, index):
        LOG.info("start to delete index")
        self.mem_kv.pop(index, None)
        self.jio.delete_json_index(index)
        return self.mem_kv

    def show_all_index(self):
        LOG.info("start to show all index")
        return self.mem_kv

    def size(self):
        LOG.info("start to get the size of the indices")
        return len(self.mem_kv)

    def write_to_file(self):
        LOG.info("start to dump database info")
        self.jio.write_json(self.mem_kv)

    def _if_contains(self, raw, filter):
        LOG.debug("start to validate if mem_kv has filter")
        for key in filter.keys():
            if (raw.get(key) == None):
                return False
            elif (not raw[key] == filter[key]):
                return False
        return True

    def get_index_where_and(self, filter):
        str_filter = {}
        num_filter = {}
        if (not filter.get("str") == None):
            str_filter = filter["str"]
        if (not filter.get("num") == None):
            num_filter = filter["num"]
        filtered_data = copy.deepcopy(self.mem_kv)
        filtered_data = self._str_filter(filtered_data, str_filter)
        filtered_data = self._num_filter(filtered_data, num_filter)
        return filtered_data


    def _str_filter(self, index_data, str_filter):
        # first check equals to eliminate most data
        if (not str_filter.get("equals") == None):
            for rule in str_filter["equals"]:
                for key in index_data.keys():
                    if index_data[key].get(rule["key"]) == None:
                        index_data.pop(key, None)
                        continue
                    if not isinstance(index_data[key].get(rule["key"]), basestring):
                        index_data.pop(key, None)
                        continue
                    elif not index_data[key][rule["key"]] == rule["value"]:
                        index_data.pop(key, None)
        # then check for _if_contains
        if (not str_filter.get("contains") == None):
            for rule in str_filter["contains"]:
                for key in index_data.keys():
                    if index_data[key].get(rule["key"]) == None:
                        index_data.pop(key, None)
                        continue
                    if not isinstance(index_data[key].get(rule["key"]), basestring):
                        index_data.pop(key, None)
                        continue
                    elif not rule["value"] in index_data[key][rule["key"]] :
                        index_data.pop(key, None)

        return index_data


    def _num_filter(self, index_data, num_filter):
        # ==
        if (not num_filter.get("==") == None):
            for rule in num_filter["=="]:
                for key in index_data.keys():
                    if index_data[key].get(rule["key"]) == None:
                        index_data.pop(key, None)
                        continue
                    if not isinstance(index_data[key].get(rule["key"]), NumberTypes):
                        index_data.pop(key, None)
                        continue
                    elif not index_data[key][rule["key"]] == rule["value"]:
                        index_data.pop(key, None)
        # >
        if (not num_filter.get(">") == None):
            for rule in num_filter[">"]:
                for key in index_data.keys():
                    if index_data[key].get(rule["key"]) == None:
                        index_data.pop(key, None)
                        continue
                    if not isinstance(index_data[key].get(rule["key"]), NumberTypes):
                        index_data.pop(key, None)
                        continue
                    elif not index_data[key][rule["key"]] > rule["value"]:
                        index_data.pop(key, None)
        # <
        if (not num_filter.get("<") == None):
            for rule in num_filter["<"]:
                for key in index_data.keys():
                    if index_data[key].get(rule["key"]) == None:
                        index_data.pop(key, None)
                        continue
                    if not isinstance(index_data[key].get(rule["key"]), NumberTypes):
                        index_data.pop(key, None)
                        continue
                    elif not index_data[key][rule["key"]] < rule["value"]:
                        index_data.pop(key, None)
        # >=
        if (not num_filter.get(">=") == None):
            for rule in num_filter[">="]:
                for key in index_data.keys():
                    if index_data[key].get(rule["key"]) == None:
                        index_data.pop(key, None)
                        continue
                    if not isinstance(index_data[key].get(rule["key"]), NumberTypes):
                        index_data.pop(key, None)
                        continue
                    elif not index_data[key][rule["key"]] >= rule["value"]:
                        index_data.pop(key, None)
        # <=
        if (not num_filter.get("<=") == None):
            for rule in num_filter["<="]:
                for key in index_data.keys():
                    if index_data[key].get(rule["key"]) == None:
                        index_data.pop(key, None)
                        continue
                    if not isinstance(index_data[key].get(rule["key"]), NumberTypes):
                        index_data.pop(key, None)
                        continue
                    elif not index_data[key][rule["key"]] <= rule["value"]:
                        index_data.pop(key, None)
        return index_data
