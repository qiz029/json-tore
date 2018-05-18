import datetime
import json


# write to a json file
# timestamp embedded in filename
# possible bug: duplicate filenames
def write_json(dictionary):
    # generate json
    timestamp = generate_timestamp()
    data = json.loads(dictionary)
    entries = []
    for item in data:
        entries.append(item)
    for entry in entries:
        file = open(entry + timestamp + ".json","w+")
        file.write(data[entry])
        file.close()
        # modify indices.json
        indices = read_json("indices.json")
        for x in range (0, len(indices["content"])):
            filename = indices["content"][x]["ref"]
            realname = ""
            for y in range(0, len(filename)-6-len(timestamp)):
                realname = realname + filename[y]
            if (realname == entry):
                indices["content"][x]["ref"] = realname + "-" + timestamp + ".json"
        outfile = open("indices.json", "w+")
        json.dump(indices, outfile)
        outfile.close()


# read a json file
# return a json object
def read_json(filename):
    data = ""
    with open(filename, 'r') as file:
        filecontent = file.read()
        data = json.loads(filecontent)
    return data


# generate timestamp
# return string:YYMMDDHHMM (fixed length)
def generate_timestamp():
    now = datetime.datetime.now()
    timestamp = str(now.year)
    month = ""
    if (len(str(now.month)) == 1):
        month = str(0) + str(now.month)
    timestamp = timestamp + month
    day = ""
    if (len(str(now.day)) == 1):
        day = str(0) + str(now.day)
    timestamp = timestamp + day
    hour = ""
    if (len(str(now.hour)) == 1):
        hour = str(0) + str(now.hour)
    timestamp = timestamp + hour
    minute = ""
    if (len(str(now.minute)) == 1):
        minute = str(0) + str(now.minute)
    timestamp = timestamp + minute
    return timestamp


# # Test code
# teststr = """ {
#   "stuff1": "randomStuff1",
#   "stuff2": "randomStuff2"
# }"""
# write_json(teststr)
