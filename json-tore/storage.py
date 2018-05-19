# -- coding: utf-8 --
from repository.RWjson import jsonToreIO as JIO

class kv_storage(object):

    def __init__(self):
        self.mem_kv = {}

    def create_index(self, index, data):
        if index in self.mem_kv:
            return False
        self.mem_kv[index] = data
        return True

    def get_index(self, index):
        if self.check_index(index):
            return self.mem_kv[index]
        else:
            return None

    def check_index(self, index):
        return (index in self.mem_kv)

    def update_index(self, index, data):
        if not index in self.mem_kv:
            return False
        self.mem_kv[index] = data
        return True

    def delete_index(self, index):
        return self.mem_kv.pop(index, None)

    def show_all_index(self):
        return self.mem_kv

    def size(self):
        return len(self.mem_kv)

    def write(self):
        JIO.write_json(self.mem_kv)
