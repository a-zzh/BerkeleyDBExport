# -*- coding: utf-8 -*-
import bsddb
import cStringIO
import gzip
import os
import json
#import MySQLdb as mdb
import sys

'''
    2017-06-23
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
                try:
                    if counter > 1162:
                        readDB(path)
                except Exception,e:
                    print str(e)


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
    file.write(line + "/x002")
    file.flush()
    counterline = counterline + 1

# 数据库读取
def readDB(path):
    global counter, counterline, file_f, file_p
    db = bsddb.hashopen(path)
    for k, v in db.iteritems():
        try:
            jsonstr = json.loads(unzip(v))
            if k.endswith("_p"):
                # print jsonstr[0]
                data = jsonstr.get("data")
                if data!=None:
                    d=data
                    uid =k.split("_")[0]
                    birth_day = d.get("birth_day")
                    birth_month = d.get("birth_month")
                    birth_year = d.get("birth_year")
                    city_code = d.get("city_code")
                    comp = d.get("comp")
                    country_code = d.get("country_code")
                    edu = d.get("edu")
                    email = d.get("email")
                    exp = d.get("exp")
                    fansnum = d.get("fansnum")
                    favnum = d.get("favnum")
                    head = d.get("head")
                    homecity_code = d.get("homecity_code")
                    homecountry_code = d.get("homecountry_code")
                    homepage = d.get("homepage")
                    homeprovince_code = d.get("homeprovince_code")
                    hometown_code = d.get("hometown_code")
                    idolnum = d.get("idolnum")
                    industry_code = d.get("industry_code")
                    introduction = d.get("introduction")
                    isent = d.get("isent")
                    ismyblack = d.get("ismyblack")
                    ismyfans = d.get("ismyfans")
                    ismyidol = d.get("ismyidol")
                    isrealname = d.get("isrealname")
                    isvip = d.get("isvip")
                    level = d.get("level")
                    location = d.get("location")
                    mutual_fans_num = d.get("mutual_fans_num")
                    name = d.get("name")
                    nick = d.get("nick")
                    openid = d.get("openid")
                    province_code = d.get("province_code")
                    regtime = d.get("regtime")
                    send_private_flag = d.get("send_private_flag")
                    sex = d.get("sex")
                    tag = d.get("tag")
                    tweetinfo = d.get("tweetinfo")
                    tweetnum = d.get("tweetnum")
                    verifyinfo = d.get("verifyinfo")
                    if tweetinfo!=None:
                        tweetinfo = json.dumps(tweetinfo).decode("unicode-escape")
                    if comp !=None:
                        comp = json.dumps(comp).decode("unicode-escape")
                    if edu != None:
                        edu = json.dumps(edu).decode("unicode-escape")
                    if tag !=None:
                        tag = json.dumps(tag).decode("unicode-escape")

                    value = []
                    value.append(uid)
                    value.append(birth_day)
                    value.append(birth_month)
                    value.append(birth_year)
                    value.append(city_code)
                    value.append(comp)
                    value.append(country_code)
                    value.append(edu)
                    value.append(email)
                    value.append(exp)
                    value.append(fansnum)
                    value.append(favnum)
                    value.append(head)
                    value.append(homecity_code)
                    value.append(homecountry_code)
                    value.append(homepage)
                    value.append(homeprovince_code)
                    value.append(hometown_code)
                    value.append(idolnum)
                    value.append(industry_code)
                    value.append(introduction)
                    value.append(isent)
                    value.append(ismyblack)
                    value.append(ismyfans)
                    value.append(ismyidol)
                    value.append(isrealname)
                    value.append(isvip)
                    value.append(level)
                    value.append(location)
                    value.append(mutual_fans_num)
                    value.append(name)
                    value.append(nick)
                    value.append(openid)
                    value.append(province_code)
                    value.append(regtime)
                    value.append(send_private_flag)
                    value.append(sex)
                    value.append(tag)
                    value.append(tweetinfo)
                    value.append(tweetnum)
                    value.append(verifyinfo)
                    # formate value[] to str
                    # write to txt
                    write2txt(file_p, value)
            elif k.endswith("_f"):
                d=jsonstr.get("data")
                uid = k.split("_")[0]
                curnum = d.get("curnum")
                hasnext = d.get("hasnext")
                info = d.get("info")
                nextstartpos = d.get("nextstartpos")
                totalnum = d.get("totalnum")
                if info!=None:
                    info = json.dumps(info).decode("unicode-escape")
                value = []
                value.append(uid)
                value.append(curnum)
                value.append(hasnext)
                value.append(info)
                value.append(nextstartpos)
                value.append(totalnum)
                # write to txt
                write2txt(file_f,value)
        except Exception, e:
            print k + "--数据导出错误!" + str(e)
            # print unzip(v)
        else:
            print str(counter) + "--" + str(counterline) + "--" + k


if __name__ == "__main__":
    # path = raw_input("请输入文件路径:")
    path=sys.argv[1]
    file_p = open("qq_p.txt", "w+")
    file_f = open("qq_f.txt", "w+")

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
