# -*- coding: utf-8 -*-
import bsddb, cStringIO, gzip, json
# simplejson
fname = 'D:/test/dbEnv/05.db'


def unzip(zipped):
    io = cStringIO.StringIO(zipped)
    fp = gzip.GzipFile(mode='r', fileobj=io)
    return fp.read()


db = bsddb.hashopen(fname)
i = 1
file = open("www.txt","w+") #d:/test/
for k, v in db.iteritems():
    try:
        jsonstr = json.dumps(json.loads(unzip(db[k])), sort_keys=True)
        print k+">"+jsonstr
        if k.endswith("s"):
            break
    except Exception:
        print "导出错误"

file.close()