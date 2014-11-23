# coding:utf-8
__author__ = "liuxuejiang"
import jieba
import os
import sys
import pymongo
from pymongo import MongoClient
import numpy as np
import random
'''
补全数据的6维度:job,ability,locaiton,behavior,friends,character,#uid
利用性格特征词库对人物进行性格刻画
'''

def cut_word(string):#文本进行分词
    wordList=set()
    words=jieba.cut(string)
    if string:
        for w in words:
            wordList.add(w)
    return wordList

wordBag={}#{word:[tfidf,...],...}#特征词在16种性格下的权重
charList=[]#[性格,...]16种性格
def read_chacater_word():
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
    '''
    count=0
    for key in wordBag:
        print key,wordBag[key]
        if count==0:
            break
    for i in range(16):
        print charList[i]
    '''

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

def cal_zhilian_character():#计算智联招聘的性格,将行为特征、好友 置为null
    zhilian_dimension=MongoClient("192.168.4.250",27017)['shulianxunying']['zhilian_dimension'].find(timeout=False)
    zhilian=MongoClient("192.168.4.250",27017)['zhilian']['zhilian_userinfo']
    write_location=MongoClient("192.168.4.250",27017)["shulianxunying"]['zhilian_dimension_six']
    for cv in zhilian_dimension:
        uid=cv['uid']
        selfEval=zhilian.find_one({"_id":uid},{"_id":0,"selfEval":1})
        character='null'
        if "selfEval"in selfEval:
            character=cal_character(selfEval['selfEval'])
        cv['character']=character
        cv['behavior']='null'
        cv['friends']='null'
        #PRINT(cv)
        write_location.insert(cv)

def cal_csdn_dimension():#将csdn的数据更正为六个维度,添加性格和好友为null
    csdn_dimension=MongoClient("192.168.4.250",27017)['shulianxunying']['csdn_dimension'].find(timeout=False)
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['csdn_dimension_six']
    for cv in csdn_dimension:
        cv['character']='null'
        cv['friends']='null'
        cv['location']='null'
        #PRINT(cv)
        write_location.insert(cv)

def cal_renren_dimension():#人人的6维度刻画,添加:行为特征,性格,好友 为null
    renren_dimension=MongoClient("192.168.4.250",27017)['shulianxunying']['renren_dimension'].find(timeout=False)
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['renren_dimension_six']
    for cv in renren_dimension:
        cv['behavior']="null"
        cv["character"]="null"
        cv['friends']='null'
        #PRINT(cv)
        write_location.insert(cv)

def cal_dajie_dimension():#大街6维度刻画，添加:行为特征,性格,好友 为null
    dajie_dimension=MongoClient("192.168.4.250",27017)['shulianxunying']['dajie_dimension'].find(timeout=False)
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['dajie_dimension_six']
    for cv in dajie_dimension:
        cv['behavior']='null'
        cv['character']='null'
        cv['friends']='null'
        #PRINT(cv)
        write_location.insert(cv)

def cal_yingcai_dimension():#英才6维度刻画,添加:行为特征,好友 为null;计算:性格
    yingcai_dimension=MongoClient("192.168.4.250",27017)['yingcai']['yingcai_dimension'].find(timeout=False)
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['yingcai_dimension_six']
    for cv in yingcai_dimension:
        self_eval=''
        if 'self_eval' in cv:
            self_eval=cv['self_eval']
        character="null"
        if self_eval:
            character=cal_character(self_eval)
        cv['character']=character
        cv['behavior']='null'
        cv['friends']='null'
        #PRINT(cv)
        write_location.insert(cv)

def cal_scholar_dimension():#期刊6维度刻画,更正字段名,添加性格为Null
    scholar_dimension=MongoClient("192.168.4.250",27017)['shulianxunying']['google_scholar_dimension'].find(timeout=False)
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['google_scholar_dimension_six']
    for cv in scholar_dimension:
        post={}
        post['uid']=cv['uid']
        post['location']=cv['country']
        post['ability']=cv['research_ability']
        post['job']=cv['interest']
        post['friends']=cv['friend_quality']
        post['behavior']=cv['behavior']
        post['character']='null'
        #PRINT(cv)
        write_location.insert(post)

#LinkedIn
def cal_linkedin_dimension():#添加:行为特征和好友 为null
    conn=MongoClient("192.168.4.250",27017)['shulianxunying']['linkedin_dimension'].find(timeout=False)
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['linkedin_dimension_six']
    for cv in conn:
        cv['behavior']="null"
        cv['friends']="null"
        write_location.insert(cv)

def PRINT(arg):#调试打印信息
    for key in arg:
        print key,
        print arg[key]

if __name__ == "__main__":
    if len(sys.argv)<2:
        print "Input args:cal_zhilian,cal_csdn,cal_renren,cal_renren,cal_dajie,cal_yingcai,cal_scholar,cal_linkedin"
        sys.exit(1)
    elif sys.argv[1]=="cal_zhilian":
        print("cal_zhilian_character")
        read_chacater_word()
        cal_zhilian_character()
    elif sys.argv[1]=="cal_csdn":
        print("cal_csdn_dimension")
        cal_csdn_dimension()
    elif sys.argv[1]=="cal_renren":
        print("cal_renren_dimension")
        cal_renren_dimension()
    elif sys.argv[1]=="cal_dajie":
        print("cal_dajie_dimension")
        cal_dajie_dimension()
    elif sys.argv[1]=="cal_yingcai":
        print("cal_yingcai_dimension")
        read_chacater_word()
        cal_yingcai_dimension()
    elif sys.argv[1]=="cal_scholar":
        print("cal_scholar_dimension")
        cal_scholar_dimension()
    elif sys.argv[1]=="cal_linkedin":
        print("cal_linkedin_dimension")
        cal_linkedin_dimension()


