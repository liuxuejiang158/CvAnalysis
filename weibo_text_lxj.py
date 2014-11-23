# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import datetime
import time
import pymongo
from pymongo import MongoClient
'''提取每个用户的微博内容，并且统计发微博总次数和转发次数'''

if __name__ == "__main__":
    fin = MongoClient("localhost",27017)['shulianxunying']['weibo'].find(timeout=False)#保持连接
    write_location=MongoClient("localhost",27017)['shulianxunying']['weibo_text']
    write_statics=MongoClient("localhost",27017)['shulianxunying']['weibo_statics']

    post={}#保存微博文本{"用户id":[{"微博发布时间":timestamp,"微博内容":text}...]}
    post_stat={}#统计微博数目
    for cv in fin:
    #for j in range(10):
        #cv=fin[j]
        if u'statuses' in cv:
            statuses=cv[u'statuses']
            for i in statuses:
                text='null'
                timestr='null'
                user='null'
                if u'text' in i:#微博文本
                    text=i[u'text']
                if u'created_at' in i:#微博发布时间
                    timestr=i[u'created_at']
                if 'uid' in i:#用户
                        user=i['uid']
                dt=datetime.datetime.strptime(timestr,'%a %b %d %H:%M:%S +0800 %Y')
                timestamp=int(time.mktime(dt.timetuple()))
                if user not in post:
                    post[user]=[{"timestamp":timestamp,"text":text}]
                else:
                    post[user].append({"timestamp":timestamp,"text":text})
                if user not in post_stat:
                    if 'retweeted_status' in i:
                        post_stat[user]={"total":1,"retweeted":1}
                    else:
                        post_stat[user]={"total":1,"retweeted":0}
                else:
                    if 'retweeted_status' in i:
                        post_stat[user]['retweeted']+=1
                    post_stat[user]['total']+=1
    for key in post:
        write_location.insert({"user":key,"content":post[key]})
    for key in post_stat:
        write_statics.insert({"user":key,"num":post_stat[key]})
