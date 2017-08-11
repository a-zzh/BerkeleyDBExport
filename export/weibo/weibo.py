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
#写入文档
def write2txt(file,value):
    global counterline
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
        if type(s) == bool:
            s = str(s)
        if type(s) == long:
            s = str(s)
        line = line + s + "/x001"
    # print type(s)
    # write to txt
    file.write(line + "\r\n")
    file.flush()
    counterline = counterline + 1

# 数据库读取
def readDB(path):
    global counter, counterline, file_f, file_p, file_s, file_l, file_t
    db = bsddb.hashopen(path)
    for k, v in db.iteritems():
        try:
            if k.endswith("_p"):
                # storage in mdb
                jsonstr = json.loads(unzip(db[k]))
                # print jsonstr[0]
                d = jsonstr
                uid = d.get("id")
                name = d.get("name")
                allow_all_act_msg = d.get("allow_all_act_msg")
                allow_all_comment = d.get("allow_all_comment")
                avatar_hd = d.get("avatar_hd")
                avatar_large = d.get("avatar_large")
                bi_followers_count = d.get("bi_followers_count")
                block_word = d.get("block_word")
                city = d.get("city")
                Class = d.get("class")
                created_at = d.get("created_at")
                description = d.get("description")
                domain = d.get("domain")
                favourites_count = d.get("favourites_count")
                follow_me = d.get("follow_me")
                followers_count = d.get("followers_count")
                following = d.get("following")
                friends_count = d.get("friends_count")
                gender = d.get("gender")
                geo_enabled = d.get("geo_enabled")
                idstr = d.get("idstr")
                lang = d.get("lang")
                location = d.get("location")
                mbrank = d.get("mbrank")
                mbtype = d.get("mbtype")
                online_status = d.get("online_status")
                profile_image_url = d.get("profile_image_url")
                profile_url = d.get("profile_url")
                province = d.get("province")
                ptype = d.get("ptype")
                remark = d.get("remark")
                screen_name = d.get("screen_name")
                star = d.get("star")
                status = d.get("status")
                statuses_count = d.get("statuses_count")
                url = d.get("url")
                verified = d.get("verified")
                verified_reason = d.get("verified_reason")
                verified_type = d.get("verified_type")
                weihao = d.get("weihao")

                if status != None:
                    status = json.dumps(status).decode("unicode-escape")
                value = []
                value.append(uid)
                value.append(name)
                value.append(allow_all_act_msg)
                value.append(allow_all_comment)
                value.append(avatar_hd)
                value.append(avatar_large)
                value.append(bi_followers_count)
                value.append(block_word)
                value.append(city)
                value.append(Class)
                value.append(created_at)
                value.append(description)
                value.append(domain)
                value.append(favourites_count)
                value.append(follow_me)
                value.append(followers_count)
                value.append(following)
                value.append(friends_count)
                value.append(gender)
                value.append(geo_enabled)
                value.append(idstr)
                value.append(lang)
                value.append(location)
                value.append(mbrank)
                value.append(mbtype)
                value.append(online_status)
                value.append(profile_image_url)
                value.append(profile_url)
                value.append(province)
                value.append(ptype)
                value.append(remark)
                value.append(screen_name)
                value.append(star)
                value.append(status)
                value.append(statuses_count)
                value.append(url)
                value.append(verified)
                value.append(verified_reason)
                value.append(verified_type)
                value.append(weihao)
                # formate value[] to str
                # write to txt
                write2txt(file_p,value)

            elif k.endswith("_f"):
                jsonstr = json.loads(unzip(v))
                uid = k.split("_")[0]
                ids = jsonstr.get("ids")
                total_number = jsonstr.get("total_number")
                value = []
                value.append(uid)
                value.append(ids)
                value.append(total_number)

                    # write to txt
                write2txt(file_f,value)
            elif k.endswith("_l"):
                d = json.loads(unzip(v))
                uid = k.split("_")[0]
                ids = d.get("ids")
                next_cursor = d.get("next_cursor")
                previous_cursor = d.get("previous_cursor")
                total_number = d.get("total_number")
                value = []
                value.append(uid)
                value.append(ids)
                value.append(next_cursor)
                value.append(previous_cursor)
                value.append(total_number)
                #write to txt
                write2txt(file_l,value)


            elif k.endswith("_t"):
                uid = k.split("_")[0]
                tag = json.loads(unzip(v))
                if tag!=None:
                    tag=json.dumps(tag).decode("unicode-escape")
                value = []
                value.append(uid)
                value.append(tag)
                write2txt(file_t,value)

            elif k.endswith("_s") or k.endswith("_S"):
                d = json.loads(unzip(v))
                uid = k.split("_")[0]
                hasvisible = d.get("hasvisible")
                marks = d.get("marks")
                next_cursor = d.get("next_cursor")
                previous_cursor = d.get("previous_cursor")
                statuses = d.get("statuses")
                total_number = d.get("total_number")
                if statuses != None:
                    statuses = json.dumps(statuses).decode("unicode-escape")

                value = []
                value.append(uid)
                value.append(hasvisible)
                value.append(marks)
                value.append(next_cursor)
                value.append(previous_cursor)
                value.append(statuses)
                value.append(total_number)
                write2txt(file_s,value)
                print d
        except Exception, e:
            print k + "--数据导出错误!" + str(e)
            print unzip(v)
        else:
            print str(counter) + "--" + str(counterline) + "--" + k


if __name__ == "__main__":
    path = raw_input("请输入文件路径:")
    # path=sys.argv[1]
    file_p = open("weibo_p.txt", "w+")
    file_f = open("weibo_f.txt", "w+")
    file_l = open("weibo_l.txt", "w+")
    file_t = open("weibo_t.txt", "w+")
    file_s = open("weibo_s.txt", "w+")

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
