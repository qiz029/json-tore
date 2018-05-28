# -- coding: utf-8 --
from repository.RWjson import jsonToreIO as JIO
import logging
from utils.logger import json_log as log
import thread
LOG = log(className = __name__, log_level=logging.DEBUG)

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
