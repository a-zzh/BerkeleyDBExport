# -*- coding: utf-8 -*-
import bsddb
import cStringIO
import gzip
import os
import json
import MySQLdb as mdb
import sys

'''
    renren 数据insert into db
'''

# jiexi renren data
#数据库个数计数器
counter = 0
#数据库内条数
counterline=0

# connect to db
conn = mdb.connect(host='192.168.1.109', port=3306, user='root', passwd='root', db='maimai', charset='utf8')
cursor = conn.cursor()

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
            if k.endswith("p"):
                # storage in mdb
                jsonstr = json.loads(unzip(db[k]))
                # print jsonstr[0]
                d = jsonstr[0]
                uid = None
                name = None
                sex = None
                birthday = None
                hometown_location = None
                star = None
                university_history = None
                vip = None
                work_history = None
                zidou = None
                headurl = None
                mainurl = None
                tinyurl = None
                resource = None
                if d.has_key("uid"):
                    uid = d["uid"]
                    if d.has_key("name"):
                        # name = d["name"]
                        name = d["name"]
                    if d.has_key("sex"):
                        sex = d["sex"]
                    if d.has_key("birthday"):
                        birthday = d["birthday"]
                    if d.has_key("hometown_location"):
                        hometown_location = d["hometown_location"]
                    if d.has_key("star"):
                        birthday = d["star"]
                    if d.has_key("university_history"):
                        university_history = d["university_history"]
                    if d.has_key("vip"):
                        vip = d["vip"]
                    if d.has_key("work_history"):
                        work_history = d["work_history"]
                    if d.has_key("zidou"):
                        zidou = d["zidou"]
                    if d.has_key("headurl"):
                        headurl = d["headurl"]
                    if d.has_key("mainurl"):
                        mainurl = d["mainurl"]
                    if d.has_key("tinyurl"):
                        tinyurl = d["tinyurl"]
                    resource = json.dumps(d).decode("unicode-escape")
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
                    # print value

                    cursor.execute(
                        "insert into renren(uid,name,sex,birthday,hometown_location,star,university_history,vip,work_history,zidou,headurl,mainurl,tinyurl,resource) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        value)
                    conn.commit()  # Commit the transaction
                else:
                    print "lose primary key!"
                counterline = counterline + 1
        except Exception:
            print k+"--数据导出错误!"
        else:
            print str(counter)+"--"+str(counterline)+"--"+k

if __name__ == "__main__":
    # path = raw_input("请输入文件路径:")
    path=sys.argv[1]
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
