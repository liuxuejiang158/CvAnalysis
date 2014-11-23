# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import math
import sys
import time
import pymongo
from pymongo import MongoClient
import jieba
import numpy as np
import random
'''前程无忧51job数据维度分析：职业方向null、工作地点、行为特征null、专业能力、性格null、好友null'''
college_loc = {}  #大学 城市表
skill_weight = {}  # 职业 技能 权值表
#job_exp = {}  # 职业 最高薪金表
academic_level = {u"初中":0.1,u"中技":0.1,u"MBA":0.8,u"高中":0.2,u"中专":0.2,u"大专":0.4,u"本科":0.6,u"硕士":0.8,u"博士":1.0}  # 学历  等级表
companyLoc=[]
job_word={}#{job:{word:weight,word2:weight2,...},...}24个职业的关键词语及权重
'''
def read_academic_level():
    for line in open('../data/academic_level.txt'):
        line = line.strip()
        academic, level = line.split('\t')
        academic_level[academic] = level


def read_job_exp():
    for line in open('../data/job_exp.txt'):
        line = line.strip()
        try:
            job, exp = line.split('\t')
            job_exp[job] = int(exp)
        except ValueError:
            continue
'''

def read_skill_weight():#职业技能表
    for line in open('../data/skill_weight.txt'):
        line = line.strip()
        job, skill, weight = line.split('\t')
        weight = float(weight)
        if job in skill_weight:
            skill_weight[job][skill] = weight
        else:
            skill_weight[job] = {skill: weight}

def college_location():#大学位置表
    for line in open('../data/college_location.txt'):
        line = line.strip()
        college, location = line.split('\t')
        college_loc[college] = location

def company_loc():#城市
    for line in open("../data/CityId.txt"):
        line=line.strip()
        temp=line.split("\t")[0]
        if temp:
            companyLoc.append(temp)
        else:
            print("read company location error")

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
def cal_job(exp_function,late_position,experience,h_major):
    if exp_function:
        return exp_function
    if late_position:
        return late_position
    if experience:
        for key in experience:
            if "job_name" in key:
                job_name=key['job_name']
                if job_name:
                    return job_name
    if h_major:
        return h_major
    return 'null'

#计算工作地点
def cal_location(exp_location,living,residence,h_college):#计算工作地点
    if exp_location:
        return exp_location
    if living:
        return living
    if residence:
        return residence
    if h_college:
        if h_college in college_loc:
            return college_loc[h_college]
    return 'null'

#计算专业能力
def major_ability(now_year_salary,academic_name,work_time,max_salary,job,IT_skill):
    #学历得分
    academic_ability = 0.
    if academic_name:
        if academic_name in academic_level:
            academic_ability += academic_level[academic_name]
        #else:
        #    print academic_name
    #工作经验得分
    time_ability=0.
    if work_time:
        str_num={u"一":1,u"二":2,u"三":3,u"四":4,u"五":1,u"六":6,u"七":7,u"八":8,u"九":9,u"十":10,u"十一":11,u"十二":12,u"十三":13,u"十四":14,u"十五":15,u"十六":16,u"十七":17,u"十八":18,u"十九":19,u"二十":20}
        if u"年" in work_time:
            numstr=work_time.split(u"年")[0]
            if numstr in str_num:
                temp=str_num[numstr]
                if temp>20:
                    temp=20
                time_ability+=str_num[numstr]/20.
    #目前年薪得分
    salary_ability=0.
    if now_year_salary:
        if u"万以下" in now_year_salary:#最大薪金以100万计算
            numstr=now_year_salary.split(u"万")[0]
            if numstr.isdigit():
                num=int(now_year_salary.split(u"万")[0])
                salary_ability+=num/max_salary
        elif u"万" in now_year_salary:
            if u"万" in now_year_salary:
                temp=now_year_salary.split(u"万")[0]
                if u"-" in temp:
                    temp=temp.split(u"-")
                    low=temp[0]
                    high=temp[1]
                    if low.isdigit() and high.isdigit():
                        low=int(low)
                        high=int(high)
                        mid=(low+high)/2.
                        salary_ability+=mid/max_salary
    #技能得分
    skill_ability=0.
    if job and IT_skill:
        if job in skill_weight:
            for s in IT_skill:
                if s in skill_weight[job]:
                    skill_ability+=skill_weight[job][s]
    return (skill_ability+time_ability+academic_ability+salary_ability)/4.

def cal_51job_dimension():#
    conn=MongoClient("192.168.4.249",27017)['qcwy51']['qcwy512_userinfo']
    write_location=MongoClient("192.168.4.249",27017)['shulianxunying']['qcwy512_dimension']
    max_salary=0
    '''
    for cv in conn.find():
        if "now_year_salary" in cv:
            now_year_salary=cv['now_year_salary']
            if now_year_salary:
                if u"万" in now_year_salary:
                    if u"以下" in now_year_salary:
                        temp=now_year_salary.split(u"万")[0]
                        if temp.isdigit():
                            if int(temp)>max_salary:
                                max_salary=int(temp)
                    elif u"-" in now_year_salary:
                        temp=now_year_salary.split(u"万")[0]
                        if u"-" in temp:
                            temp=temp.split(u"-")
                            low=temp[0]
                            high=temp[1]
                            if high.isdigit():
                                if int(high)>max_salary:
                                    max_salary=int(high)
    print("max now_year_salary=%d"%max_salary)
    '''
    max_salary+=150.#找出最大140万
    count=0
    for cv in conn.find(timeout=False):
        if count%1000==0:
            print count
        count+=1
        uid=cv["_id"]
        living=''
        if 'living' in cv:
            living=cv["living"]
        residence=''
        if 'residence' in cv:
            residence=cv['residence']
        work_time=''
        if 'work_time' in cv:
            work_time=cv['work_time']
        now_year_salary=''
        if 'now_year_salary' in cv:
            now_year_salary=cv['now_year_salary']
        self_assessment=""
        if "self_assessment" in cv:
            self_assessment=cv['self_assessment']
        exp_location=""
        if "exp_location" in cv:
            exp_location=cv['exp_location']
        exp_function=""
        if "exp_function" in cv:
            exp_function=cv['exp_function']
        late_position=""
        if "late_position" in cv:
            late_position=cv["late_position"]
        h_college=""
        if "h_college" in cv:
            h_college=cv['h_college']
        experience=None
        if "experience" in cv:
            experience=cv['experience']
        work_time=""
        if "work_time" in cv:
            work_time=cv["work_time"]
        academic_name=""
        if "academic_name" in cv:
            academic_name=cv["academic_name"]
        IT_skill=[]
        if "IT_skill" in cv:
            IT_skill=cv["IT_skill"]
        h_major=""
        if "h_major" in cv:
            h_major=cv["h_major"]
        #计算期望工作地点
        location='null'
        location=cal_location(exp_location,living,residence,h_college)
        #计算性格
        character="null"
        if self_assessment:
            character=cal_character(self_assessment)
        #计算职业方向
        job=cal_job(exp_function,late_position,experience,h_major)
        #计算专业能力:工作年限+薪金
        try:
            ability=major_ability(now_year_salary,academic_name,work_time,max_salary,job,IT_skill)
        except:
            print "throw exception"
            continue
        post={}
        post["ability"]=ability
        post['behavior']='null'
        post['character']=character
        post['friends']='null'
        post["job"]=job
        post['location']=location
        post["uid"]=uid
        #PRINT(post)
        write_location.insert(post)

def PRINT(arg):
    for key in arg:
        print key,
        print arg[key]

if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args:cal_51job")
        sys.exit(1)
    if sys.argv[1]=="cal_51job":
        print("cal 51job dimension")
        read_chacater_word()
        read_skill_weight()
        college_location()
        company_loc()
        cal_51job_dimension()
