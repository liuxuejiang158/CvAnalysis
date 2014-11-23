# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import datetime
import time
import pymongo
from pymongo import MongoClient
'''提取出有性格测试结果、用户信息、微博数据的用户，将FinishedUser.txt和userinfo.json和weibo.json交起来'''

def userinfoANDcharacter():#获取用户的信息和性格测试结果写入数据库
    userIDmap={}#数字ID:性格测试结果
    for line in open('../sourcedata/weibo/TargetUser.txt','r'):
        line=line.strip()
        address,uid,char=line.split('\t')
        uid=int(uid)
        if uid not in userIDmap:
            userIDmap[uid]=char
        else:
            print("duplicated user",uid)
    finishedUserNum=len(userIDmap)#用户数目
    count=0#统计Finisheduser和userinfo的交集个数
    userinfo=MongoClient("localhost",27017)['shulianxunying']['weibo_userinfo'].find(timeout=False)
    write_userinfoMBTI=MongoClient("localhost",27017)['shulianxunying']["weibo_userinfo_MBTI"]
    for data in userinfo:
        uid=data['id']
        if uid in userIDmap:
            data['character']=userIDmap[uid]
            count+=1
            write_userinfoMBTI.insert(data)
        else:
            print("user from userinfo not found in FinishedUser",uid)
    print("finishedUserNum = ",finishedUserNum," found in userinfo = ",count)
    #('finishedUserNum = ', 1166, ' found in userinfo = ', 1153)


def computeintersect():#统计已有信息和性格测试的用户和微博数据的交集数目
    userinfo_MBTI=MongoClient("localhost",27017)['shulianxunying']['weibo_userinfo_MBTI']
    weibo_data=MongoClient("localhost",27017)['shulianxunying']['weibo_text'].find(timeout=False)

    #找出既有微博文本又有用户信息的用户ID
    if False:
        uidSet=set()#微博信息中的所有用户
        for data in weibo_data:
            uid=data['user']
            uidSet.add(uid)
        count=0
        #count_2=0
        fd_3=open("../result/3_intersect.txt",'w')#拥有性格测试、用户信息、用户微博的用户ID，即三个表的交集
        #fd_2=open("2_intersect.txt",'w')#用户信息userinfo和微博数据weibo的交集用户ID
        for uid in uidSet:
            if userinfo_MBTI.find_one({"id":uid}):
                count+=1
                fd_3.write(str(uid)+'\n')
            #if userinfo.find_one({"id":uid}):
            #    fd_2.write(str(uid)+'\n')
            #    count_2+=1
        print("weibo user num = ",len(uidSet)," found in userinfo_MBTI num = ",count)
        #('weibo user num = ', 916, ' found in userinfo_MBTI num = ', 905)

    #按照每个性格类型将微博写到不同的文件中
    for line in weibo_data:
        uid=line['user']
        data=userinfo_MBTI.find_one({"id":uid},{"character":1,"_id":0})
        if data:
            character=data['character']
            if "content" in line:
                content=line['content']
                fd=open("../result/"+character+".txt",'a')#每种性格的微博内容
                for key in content:
                    text=key['text']
                    text+=u'。\n'
                    fd.write(text.encode('utf-8'))
                fd.close()
            else:
                print("weibo_text has not content text")
        else:
            print("user from weibo not found in userinfo_MBTI", uid)


if __name__ == "__main__":
    if False:
        print("weibo userinfo MBTI")
        userinfoANDcharacter()
    if True:
        print("intersect start")
        computeintersect()
