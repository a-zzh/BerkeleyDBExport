# -*- coding: utf-8 -*-

#############################################
#   Written By Qian_F                        #
#   2013-08-10                               #
#   获取文件路径列表，并写入到当前目录生成test.txt #
#############################################

import os


def getfilelist(filepath, tabnum=1):
    simplepath = os.path.split(filepath)[1]
    returnstr = simplepath + "目录<>" + "\n"
    returndirstr = ""
    returnfilestr = ""
    filelist = os.listdir(filepath)
    for num in range(len(filelist)):
        filename = filelist[num]
        if os.path.isdir(filepath + "/" + filename):
            returndirstr += "\t" * tabnum + getfilelist(filepath + "/" + filename, tabnum + 1)
        else:
            returnfilestr += "\t" * tabnum + filename + "\n"
    returnstr += returnfilestr + returndirstr
    return returnstr + "\t" * tabnum + "</>\n"


path = raw_input("请输入文件路径:")
usefulpath = path.replace('\\', '/')
if usefulpath.endswith("/"):
    usefulpath = usefulpath[:-1]
if not os.path.exists(usefulpath):
    print "路径错误!"
elif not os.path.isdir(usefulpath):
    print "输入的不是目录!"
else:
    o = open("test.xml", "w+")
    o.writelines(getfilelist(usefulpath))
    o.close()
    print "成功！请查看test.xml文件"