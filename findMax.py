# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import pymongo
from pymongo import MongoClient
import sys
import time
import datetime
'''
找出各字段值的最大值
'''

def find():
    conn=MongoClient("192.168.4.249",27017)['shulianxunying']['combine_v4_dimension']#修改源数据库地址
    max_worktime=0
    max_changefreq=0
    max_experience=0
    max_ability=0
    min_refresh=10000
    count=0
    fd=open("log.txt",'w')
    fd.write("experience,changefreq,worktime,ability,refresh,uid\n")
    for cv in conn.find(timeout=False):
        fd.write(str(cv['experience'])+","+str(cv['changefreq'])+","+str(cv['worktime'])+","+str(cv['ability'])+","+str(cv['refresh'])+","+cv['uid']+"\n")
    '''
        if cv['experience']>max_experience:
            max_experience=cv['experience']
            if cv['experience']==23856:
                print "max_experience",cv['uid']
        if cv['changefreq']>max_changefreq:
            max_changefreq=cv['changefreq']
            if cv['changefreq']==17.1534701857:
                print "max_changefreq",cv['uid']
        if cv['worktime']>max_worktime:
            max_worktime=cv['worktime']
            if cv['worktime']==119280:
                print "worktime",cv['uid']
        if cv['ability']>max_ability:
            max_ability=cv['ability']
        if cv['refresh']>0 and cv['refresh']<min_refresh:
            min_refresh=cv['refresh']
            if cv['refresh']==59:
                print "min_refresh",cv['uid']
    print "max_worktime,max_changefreq,max_experience,max_ability,min_refresh"
    print max_worktime,max_changefreq,max_experience,max_ability,min_refresh
    '''
    fd.close()

#find()

def fun_1():
    experience=[]
    changefreq=[]
    worktime=[]
    refresh=[]
    flag=0
    for line in open("log.txt"):
        if flag==0:
            flag=1
        else:
            line=line.strip().split(",")
            experience.append(line[0])
            changefreq.append(line[1])
            worktime.append(line[2])
            refresh.append(line[4])
    experience.sort(reverse=True)
    changefreq.sort(reverse=True)
    worktime.sort(reverse=True)
    refresh.sort(reverse=True)
    print "experience,changefreq,worktime,refresh"
    for i in range(10):
        print experience[i],changefreq[i],worktime[i],refresh[i]

#fun_1()

def fun_2():
    conn=MongoClient("192.168.4.249",27017)['shulianxunying']['combine_v4_dimension']#修改源数据库地址
    fd1=open("ability.txt",'w')
    fd2=open("experience.txt",'w')
    fd3=open("worktime.txt",'w')
    fd4=open("changefreq.txt",'w')
    fd5=open("refresh.txt",'w')
    for cv in conn.find(timeout=False):
        fd1.write(str(cv['ability'])+"\n")
        fd2.write(str(cv['experience'])+"\n")
        fd3.write(str(cv['worktime'])+"\n")
        fd4.write(str(cv['changefreq'])+"\n")
        fd5.write(str(cv['refresh'])+"\n")
    fd1.close()
    fd2.close()
    fd3.close()
    fd4.close()

fun_2()
