# -*- coding: utf-8 -*-

#############################################
#   Written By Qian_F                        #
#   2013-08-10                               #
#   获取文件路径列表，并写入到当前目录生成test.txt #
#############################################

import bsddb
import cStringIO
import gzip
import os
import simplejson


counter = 0
counterline=0
file = open("www.txt", "w+")

# 文件夹深度读取
def getfilelist(filepath):
    global counter
    filelist = os.listdir(filepath)
    for num in range(len(filelist)):
        filename = filelist[num]
        path = filepath + "/" + filename
        if os.path.isdir(path):
            print "dir: " + path
            getfilelist(path)
        else:
            counter = counter + 1
            print "file: " + path
            # 解析数据库
            if path.endswith(".db"):
                readDB(path)


# 解压
def unzip(zipped):
    io = cStringIO.StringIO(zipped)
    fp = gzip.GzipFile(mode='r', fileobj=io)
    return fp.read()


# 数据库读取
def readDB(path):
    global file,counter,counterline
    db = bsddb.hashopen(path)
    for k, v in db.iteritems():
        try:
            jsonstr = simplejson.dumps(simplejson.loads(unzip(db[k])), sort_keys=True)
            # write to txt
            file.write(k + "/x001" + jsonstr + "\r\n")
            file.flush()
            counterline=counterline+1
        except Exception:
            print k+"--数据导出错误!"
        else:
            print str(counter)+"--"+str(counterline)

if __name__ == "__main__":
    path = raw_input("请输入文件路径:")
    usefulpath = path.replace('\\', '/')
    if usefulpath.endswith("/"):
        usefulpath = usefulpath[:-1]
    if not os.path.exists(usefulpath):
        print "路径错误!"
    elif not os.path.isdir(usefulpath):
        print "输入的不是目录!"
    else:
        getfilelist(usefulpath)
        print counter
