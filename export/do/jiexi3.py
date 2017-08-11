import bsddb, cStringIO, gzip, json
import MySQLdb as mdb

# simplejson

# connect to db
conn = mdb.connect(host='123.57.35.10', port=3306, user='root', passwd='root', db='maimai', charset='utf8')
cursor = conn.cursor()

fname = 'D:/test/dbEnv/05.db'


def unzip(zipped):
    io = cStringIO.StringIO(zipped)
    fp = gzip.GzipFile(mode='r', fileobj=io)
    return fp.read()


db = bsddb.hashopen(fname)
i = 1
file = open("www.txt", "w+")  # d:/test/
for k, v in db.iteritems():
    # print k
    # formate indent=4
    # jsonstr = json.dumps(json.loads(unzip(db[k])), sort_keys=True).decode("unicode-escape")
    jsonstr = json.loads(unzip(db[k]))
    print k
    print jsonstr[0]
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
        print  d["name"]
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
        print value
        cursor.execute(
            "insert into renren(uid,name,sex,birthday,hometown_location,star,university_history,vip,work_history,zidou,headurl,mainurl,tinyurl,resource) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            value)
        conn.commit()  # Commit the transaction
    else:
        print "lose primary key!"
    i = i + 1
    if i > 1:
        break

file.close()
