# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import jieba
import jieba.posseg as pseg
import pymongo
from pymongo import MongoClient
import sys
import numpy as np
import random
import time
import datetime
import os
'''新浪数据6维度刻画：期望工作地点、职业方向、专业能力、性格、行为特征、好友'''

skill_weight =set()
def read_skill_weight():#职业
    #skill_weight={job:{skill_1:weight_1,skill_2:weight_2,...},...}
    print("read_skill_weight")
    for line in open('../data/occupation_category.txt'):
        line = line.strip()
        skill_weight.add(line)

ingore_flag=['x','w']
def cut_word(string):#文本进行分词
    wordList=set()
    if string:
        words=pseg.cut(string)
        for w in words:
            if w.flag not in ingore_flag:
                wordList.add(w.word)
    return wordList

wordBag={}#{word:[tfidf,...],...}#特征词在16种性格下的权重
charList=[]#[性格,...]16种性格
def read_chacater_word():
    print("read_chacater_word")
    flag=True
    for line in open("../data/word_tfidf.txt",'r'):
        line=line.strip()
        if flag:
            charList.extend(line.split('\t')[1:17])
            flag=False
        else:
            List=line.split('\t')
            for i in range(1,17):
                List[i]=float(List[i])
            wordBag[List[0]]=np.array(List[1:17])#numpy数组可以直接相加

#计算性格
def cal_character(text):#计算文本的性格
    wordList=cut_word(text)
    charSum=np.zeros(16)
    for word in wordList:
        if word in wordBag:
            charSum+=wordBag[word]
    index=charSum.argmax()
    if (charSum==np.zeros(16)).all():
        index=random.randint(0,15)
    return charList[index]

#计算职业方向
def cal_job(name,description,tags):#,stat_text):
    name_des_tag=set()
    if name and name!="null":
        temp=cut_word(name)
        for key in temp:
            name_des_tag.add(key)
    if description and description!="null":
        temp=cut_word(description)
        for key in temp:
            name_des_tag.add(key)
    if tags:
        for key in tags:
            for i in key:
                if i!="flag" and i!="weight":
                    tag_=key[i]
                    name_des_tag.add(tag_)
    for key in name_des_tag:
        length=len(key)
        if key in skill_weight:
            return key,name_des_tag
        elif length%3==0:
            word=[]
            for i in range(0,length,3):
                word.append(key[i:i+3])
            for job in skill_weight:
                count=0
                for w in word:
                    if w in job.decode("utf-8"):
                        count+=1.
                if count/len(word)>0.8:
                    return job,name_des_tag
    return "null",name_des_tag

def cal_dimension():
    conn=MongoClient("192.168.4.249",27017)["SW_new_detail"]['user_info']
    conn_tag=MongoClient("192.168.4.249",27017)["SW_new_detail"]['tag']
    conn_text=MongoClient("192.168.4.249",27017)["SW_new_detail"]['text']
    write_location=MongoClient("192.168.4.249",27017)["shulianxunying"]['sina_dimension_8']#
    count=800000#
    fd=open("log_sina_8.txt",'w')#
    for cv in conn.find(timeout=False,skip=800000):#
        try:
            if count%200==0:
                fd.flush()
            count+=1
            fd.write(str(count)+"\n")
            id_=cv["id"]
            status=""
            if "status" in cv:
                status=cv['status']
            #计算职业方向(部分)
            tags=[]
            temp_tags=conn_tag.find_one({"id":id_},{"tags":1})
            if temp_tags:
                tags=temp_tags['tags']
            name=""
            if "name" in cv:
                name=cv["name"]
            description=""
            if "description" in cv:
                description=cv["description"]
            job="null"
            name_des_tag=[]
            job,name_des_tag=cal_job(name,description,tags)#,stat_text)
            #计算性格
            temp_text=conn_text.find_one({"id":id_},{"text":1})
            text=[]
            if temp_text:
                if "text" in temp_text:
                    text=temp_text['text']
            #else:
            #    print("id from user_info can't find in text")
            character="null"
            if text:#通过微博文本计算性格
                str1=""
                for key in text:
                    str1+=key
                character=cal_character(str1)
            elif name_des_tag:#没有微博文本通过name,description,tags,最新一条微博计算性格
                stat_text=''#最近一条微博
                if status:
                    if "text" in status:
                        stat_text=status['text']
                if stat_text and stat_text!="null":
                    temp=cut_word(stat_text)
                    for key in temp:
                        name_des_tag.add(key)
                charSum=np.zeros(16)
                for word in name_des_tag:
                    if word in wordBag:
                        charSum+=wordBag[word]
                index=charSum.argmax()
                if (charSum==np.zeros(16)).all():
                    index=random.randint(0,15)
                character=charList[index]
            '''
            #计算行为特征
            behavior=0.
            created_at=""
            start_year=0
            if "created_at" in status:
                created_at=status["created_at"]
            if created_at and created_at!="null":
                dt=datetime.datetime.strptime(created_at,'%a %b %d %H:%M:%S +0800 %Y')
                start_year=dt.year
                length=0.
                if(start_year<2014):
                    length=2014-start_year+1.0
                else:
                    length=1.
                behavior=len(text)/length#微博条数/年
            '''
            #地点
            location="null"
            if "location" in cv:
                location=cv["location"]
            #写入数据库
            post={}
            post['uid']=id_
            post['ability']=0.
            post['behavior']=behavior
            post['character']=character
            post['friends']=0.
            post['job']=job
            post['location']=location
            #PRINT(post)
            write_location.insert(post)
        except:
            print("some Exception in ",id,count)

#构建网络
def relationshipNetwork():#将微博关注列表整理为网络边数据:srcId dstID
    conn=MongoClient("192.168.4.249",27017)['SW_new_detail']['rel']
    conn_userInfo=MongoClient("192.168.4.249",27017)['SW_new_detail']['user_info']
    fd=open("relationshipNetwork.txt",'w')
    count=0
    for cv in conn.find(timeout=False):
        id_=""
        if count%200==0:
            fd.flush()
        count+=1
        if "id" in cv:
            id_=cv["id"]
            if id_:
                ids=""
                if "ids" in cv:
                    ids=cv["ids"]
                    if ids:
                        for key in ids:
                            temp=conn_userInfo.find_one({"id":key},{"_id":1})
                            if temp:
                                fd.write(str(id_)+" "+str(key)+"\n")

def PRINT(arg):
    for key in arg:
        print key,
        print arg[key]

if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args:cal_sina_dimension(计算维度),relationshipNetwork(构建网络)")
        sys.exit(1)
    elif sys.argv[1]=='cal_sina_dimension':
        print("cal_dimension")
        jieba.load_userdict("../data/user_dict.txt")
        read_skill_weight()
        read_chacater_word()
        cal_dimension()
    elif sys.argv[1]=="relationshipNetwork":
        print("relationshipNetwork")
        relationshipNetwork()
