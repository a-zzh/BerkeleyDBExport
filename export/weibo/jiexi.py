# -*- coding: utf-8 -*-
import bsddb, cStringIO, gzip, json

# simplejson
fname = 'E:/fc.db'


def unzip(zipped):
    io = cStringIO.StringIO(zipped)
    fp = gzip.GzipFile(mode='r', fileobj=io)
    return fp.read()


db = bsddb.hashopen(fname)
i = 1
file = open("www.txt", "w+")  # d:/test/
for k, v in db.iteritems():
    try:
        # print k
        # formate indent=4
        jsonstr = json.dumps(json.loads(unzip(db[k])), sort_keys=True)
        data = json.loads(jsonstr)
        print k + ">" + jsonstr
        # print type(data)
        # print str(data)
        # file.write(jsonstr)
        # file.flush()
        # d = data[0]
        # print d["uid"]
        # print d.has_key("uid")
        # i = i + 1
        # if i > 5000:
        #     break
        file.write(k + ">" + jsonstr + "\n")
        file.flush()
        if str(k).endswith("_f"):
            break
    except Exception, e:
        print str(e)

file.close()
