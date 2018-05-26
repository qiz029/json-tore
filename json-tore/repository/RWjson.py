import datetime
import json
import logging
import os
from utils.logger import json_log as log

LOG = log(className = __name__, log_level=logging.INFO)

class jsonToreIO(object):

    def __init__(self, rootDir = "dataDir/"):
        self.rootDir = rootDir
        if not os.path.exists(self.rootDir):
            os.makedirs(self.rootDir)

    # write to a json file
    # timestamp embedded in filename
    # possible bug: duplicate filenames
    def write_json(self, dictionary):
        # generate json
        LOG.info("start to write to json file system")
        timestamp = self._generate_timestamp()
        entries = []
        LOG.info("there are {0} items".format(len(dictionary)))
        indices = self._read_json("indices.json")
        for entry in list(dictionary.keys()):
            with open(self.rootDir + entry + "-" + timestamp + ".json", 'w+') as fp:
                json.dump(dictionary[entry], fp, indent=4)
                fp.close()
            # modify indices.json
            if (indices.get(entry) == None):
                indices[entry] = {"ref": entry + "-" + timestamp + ".json"}
            else:
                indices[entry]["ref"] = entry + "-" + timestamp + ".json"
        outfile = open(self.rootDir + "indices.json", "w+")
        json.dump(indices, outfile, indent=4, sort_keys=True)
        outfile.close()

    def write_json_index(self, dictionary, index):
        # generate json
        LOG.info("start to write to json file system for index {0}".format(index))
        timestamp = self._generate_timestamp()
        indices = self._read_json("indices.json")
        with open(self.rootDir + index + "-" + timestamp + ".json", 'w+') as fp:
            json.dump(dictionary[index], fp, indent=4)
            fp.close()
        # modify indices.json
        if (indices.get(index) == None):
            indices[index] = {"ref": index + "-" + timestamp + ".json"}
        else:
            indices[index]["ref"] = index + "-" + timestamp + ".json"
        outfile = open(self.rootDir + "indices.json", "w+")
        json.dump(indices, outfile, indent=4, sort_keys=True)
        outfile.close()

    def delete_json_index(self, index):
        LOG.info("start to delete index {0} in json".format(index))
        indices = self._read_json("indices.json")
        if (indices.get(index) == None):
            return
        else:
            indices.pop(index)
        outfile = open(self.rootDir + "indices.json", "w+")
        json.dump(indices, outfile, indent=4, sort_keys=True)
        outfile.close()

    # read a json file
    # return a json object
    def _read_json(self, filename):
        data = ""
        try:
            file = open(self.rootDir + filename, 'rb')
        except IOError:
            return {}

        with file:
            filecontent = file.read()
            data = json.loads(filecontent)
        return data


    # generate timestamp
    # return string:YYMMDDHHMM (fixed length)
    def _generate_timestamp(self):
        timer = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        LOG.info("current timestamp is {0}".format(timer))
        return timer


    # # Test code
    # teststr = """ {
    #   "stuff1": "randomStuff1",
    #   "stuff2": "randomStuff2"
    # }"""
    # write_json(teststr)
