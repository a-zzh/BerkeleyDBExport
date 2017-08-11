# -*- coding: utf-8 -*-

#############################################
#   Written By Qian_F                        #
#   2013-08-10                               #
#   获取文件路径列表，并写入到当前目录生成test.txt #
#############################################

import os
global counter
counter = 0
def  getfilelist(filepath):
    global counter
    simplepath = os.path.split(filepath)[1]
    filelist = os.listdir(filepath)
    for num in range(len(filelist)):
        filename = filelist[num]
        if os.path.isdir(filepath + "/" + filename):
            print "dir: "+filepath + "/" + filename
            getfilelist(filepath + "/" + filename)
        else:
            counter=counter+1
            print "file: "+filepath + "/" + filename


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