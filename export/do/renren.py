# -*- coding: utf-8 -*-
import bsddb
import cStringIO
import gzip
import os
import json
import MySQLdb as mdb
import sys

'''
    2017-06-06 
    renren 数据 export to txt
'''

# jiexi renren data
# 数据库个数计数器
counter = 0
# 数据库内条数
counterline = 0

# connect to db
# conn = mdb.connect(host='192.168.1.109', port=3306, user='root', passwd='root', db='maimai', charset='utf8')
# cursor = conn.cursor()


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
    global file,file2, counter, counterline
    db = bsddb.hashopen(path)
    for k, v in db.iteritems():
        try:
            if k.endswith("_p"):
                # storage in mdb
                jsonstr = json.loads(unzip(db[k]))
                # print jsonstr[0]
                d = jsonstr[0]
                if d.has_key("uid"):

                    uid = d.get("uid")
                    name = d.get("name")
                    sex = d.get("sex")
                    birthday = d.get("birthday")
                    hometown_location = d.get("hometown_location")
                    star = d.get("star")
                    university_history = d.get("university_history")
                    vip = d.get("vip")
                    work_history = d.get("work_history")
                    zidou = d.get("zidou")
                    headurl = d.get("headurl")
                    mainurl = d.get("mainurl")
                    tinyurl = d.get("tinyurl")
                    resource = json.dumps(d).decode("unicode-escape")
                    #unicode to utf-8
                    if hometown_location!=None:
                        hometown_location=json.dumps(hometown_location).decode("unicode-escape")
                    if university_history != None:
                        university_history = json.dumps(university_history).decode("unicode-escape")
                    if work_history != None:
                        work_history = json.dumps(work_history).decode("unicode-escape")

                    value = []
                    value.append(uid)
                    value.append(name)
                    value.append(sex)
                    value.append(birthday)
                    value.append(hometown_location)
                    value.append(star)
                    value.append(university_history)
                    value.append(vip)
                    value.append(work_history)
                    value.append(zidou)
                    value.append(headurl)
                    value.append(mainurl)
                    value.append(tinyurl)
                    value.append(resource)
                    # formate value[] to str
                    line = ""
                    for s in value:
                        if s == None:
                            s = "\\N"
                        if type(s) == int:
                            s = str(s)
                        if type(s) == unicode:
                            s = s.encode("utf-8")
                        if type(s) == list:
                            # s = ",".join(s)
                            s = str(s)
                        if type(s) == dict:
                            s = str(s)

                        line = line + s + "/x001"
                        # print type(s)
                    # write to txt
                    file.write(line + "\r\n")
                    file.flush()
                else:
                    print "lose primary key!"
                counterline = counterline + 1
            elif k.endswith("_f"):
                jsonstr = json.loads(unzip(v))
                uid=k.split("_")[0]
                count=jsonstr.get("count")
                friend_list=jsonstr.get("friend_list")
                if friend_list!=None:
                    friend_list=json.dumps(friend_list).decode("unicode-escape")
                value = []
                value.append(uid)
                value.append(count)
                value.append(friend_list)
                line=""
                for s in value:
                    if s == None:
                        s = "\\N"
                    if type(s) == int:
                        s = str(s)
                    if type(s) == unicode:
                        s = s.encode("utf-8")
                    if type(s) == list:
                        # s = ",".join(s)
                        s = str(s)
                    if type(s) == dict:
                        s = str(s)

                    line = line + s + "/x001"
                    # print type(s)
                    # write to txt
                file2.write(line + "\r\n")
                file2.flush()

        except Exception, e:
            print k + "--数据导出错误!"+str(e)
            print unzip(v)
        else:
            print str(counter) + "--" + str(counterline) + "--" + k


if __name__ == "__main__":
    path = raw_input("请输入文件路径:")
    # path=sys.argv[1]
    file = open("renren.txt", "w+")
    file2 = open("renren_f.txt", "w+")
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
