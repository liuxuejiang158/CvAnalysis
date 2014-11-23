# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import pymongo
from pymongo import MongoClient
import sys
import numpy as np
import jieba
import random

college_loc = {}  #大学 城市表
skill_weight = {}  # 职业 技能 权值表
#job_exp = {}  # 职业 最高薪金表
academic_level = {u"中专":0.2,u"大专":0.4,u"本科":0.6,u"硕士":0.8,u"博士":1.0}  # 学历  等级表
companyLoc=[]
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
#计算专业能力
def major_ability(experience, exp_m_salary, pro_title, t_pre_skill, academic_name,max_salary):
    #技能得分
    skill_ability = 0.
    total_weight=0.000001
    if pro_title and t_pre_skill:
        for key in pro_title:
            if key in skill_weight:
                for i in skill_weight[key]:
                    total_weight+=skill_weight[key][i]
                if i.decode('utf-8') in t_pre_skill:
                    skill_ability+=skill_weight[key][i]
    skill_ability=skill_ability/total_weight
    #预期薪金得分
    job_exp_ability = 0.
    if exp_m_salary and exp_m_salary != "":
        if u"以上" in exp_m_salary:
            exp_m_salary = exp_m_salary.split(u'以上')[0]
            if exp_m_salary.isdigit():
                exp_m_salary=int(exp_m_salary)
                if exp_m_salary>100000:
                    exp_m_salary=100000.
                job_exp_ability += exp_m_salary/max_salary
        elif u"可面议":
            job_exp_ability = 1
    #工作经验得分
    time_ability = 0.
    if experience:
        timelines = []
        for exp in experience:
            if 'start_time' in exp:
                start_time=exp['start_time']
                if start_time:
                    if '-' in start_time:
                        s_time = start_time.split('-')[0]
                        if s_time.isdigit():
                            timelines.append(int(s_time))
            if "end_time" in exp:
                end_time=exp['end_time']
                if end_time:
                    if u"今" in end_time:
                        timelines.append(2014)
                    elif "-" in end_time:
                        e_time=end_time.split("-")[0]
                        if e_time.isdigit():
                            timelines.append(int(e_time))
        if len(timelines) > 0:
            temp=max(timelines)-min(timelines)+1
            if temp>20:
                temp=20
            time_ability += temp / 20.
    #学历得分
    academic_ability = 0.
    if academic_name:
        if academic_name in academic_level:
            academic_ability += academic_level[academic_name]
    return (skill_ability+job_exp_ability+time_ability+academic_ability)/4


def cal_location(exp_location, living, education,experience):
    if exp_location:
        return exp_location[0]
    if living:
        return living
    if experience:
        for key in experience:
            if "com_name" in key:
                com_name=key['com_name']
                if com_name:
                    for i in companyLoc:
                        if i.decode('utf-8') in com_name:
                            return i
    if education:
        for edu in education:
            college = edu['college']
            if college:
                if college in college_loc:
                    return college_loc[college]
    return 'null'


def cal_job(pro_title,experience,f_pre_skill):
    if pro_title:
        return pro_title[0]
    if experience:
        for key in experience:
            if "job_name" in key:
                job_name=key['job_name']
                if job_name:
                    return job_name
    if f_pre_skill:
        for job in skill_weight:
            for skill in skill_weight[job]:
                if skill.decode('utf-8') in f_pre_skill:
                    return job
    return 'null'


def cal_dimensions():
    conn=MongoClient("192.168.4.250",27017)['yingcai']['yingcai_userinfo']
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['yingcai_dimension_six']
    max_salary=100000.
    '''
    print("计算最大期望薪金")
    for cv in conn.find(timeout=False):
        if 'exp_m_salary' in cv:
            exp_m_salary=cv['exp_m_salary']
            if exp_m_salary:
                if u"以上" in exp_m_salary:
                    str1=exp_m_salary.split(u'以上')[0]
                    if str1.isdigit():
                        str1=int(str1)
                        if str1>max_salary:
                            max_salary=str1
    print("max_salary=%d"%(max_salary))#找出最大4000 0000
    '''
    max_salary+=1000.
    print("计算维度")
    count = 0
    for cv in conn.find(timeout=False):
        count += 1
        if count % 1000 == 0:
            print count
        post = {}
        uid = cv['_id']
        pro_title=''
        if 'pro_title' in cv:
            pro_title = cv['pro_title']
        exp_location=''
        if 'exp_location' in cv:
            exp_location = cv['exp_location']
        living=''
        if 'living' in cv:
            living = cv['living']
        education=''
        if 'education' in cv:
            education = cv['education']
        experience=''
        if 'experience' in cv:
            experience = cv['experience']
        exp_m_salary=''
        if 'exp_m_salary' in cv:
            exp_m_salary = cv['exp_m_salary']
        academic_name=''
        if 'academic_name' in cv:
            academic_name = cv['academic_name']
        f_pre_skill=''
        if 'f_pre_skill' in cv:
            f_pre_skill=cv['f_pre_skill']
        #计算工作地点
        location = cal_location(exp_location, living, education,experience)
        #计算职业方向
        job = cal_job(pro_title,experience,f_pre_skill)
        #计算专业能力
        ability = major_ability(experience, exp_m_salary, pro_title, f_pre_skill, academic_name,max_salary)
        #计算性格
        character='null'
        if 'self_eval' in cv:
            self_eval=cv['self_eval']
            if self_eval:
                character=cal_character(self_eval)
        post['ability'] = ability
        post['behavior']='null'
        post['character']=character
        post['friends']='null'
        post['job'] = job
        post['location'] = location
        post['uid'] = uid
        write_location.insert(post)


if __name__ =="__main__":
    if len(sys.argv)<2:
        print("Input args:cal_yingcai_dimension")
        sys.exit(1)
    elif sys.argv[1]=="cal_yingcai_dimension":
        #read_academic_level()
        #read_job_exp()
        read_chacater_word()
        read_skill_weight()
        college_location()
        company_loc()
        cal_dimensions()
