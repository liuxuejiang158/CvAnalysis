# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import jieba
import pymongo
from pymongo import MongoClient
import sys
import numpy as np
import random
import os
'''智联简历数据6维度刻画：期望工作地点、职业方向、专业能力、性格、行为特征null、好友null'''

college_loc = {}  #大学 城市表
skill_weight = {}  # 职业 技能 权值表
#job_exp = {}  # 职业 最高薪金表
academic_level = {u"中专":0.2,u"大专":0.4,u"本科":0.6,u"硕士":0.8,u"博士":1.0}  # 学历  等级表
companyLoc=[]
'''
def read_academic_level():#学历等级
    for line in open('./data/academic_level.txt'):
        line = line.strip()
        academic, level = line.split('\t')
        academic_level[academic] = int(level)

def read_job_exp():#职位薪金
    for line in open('./data/job_exp.txt'):
        line = line.strip()
        try:
            job, exp = line.split('\t')
            job_exp[job] = int(exp)
        except ValueError:
            continue
'''

def read_skill_weight():#职业 技能 权重
    #skill_weight={job:{skill_1:weight_1,skill_2:weight_2,...},...}
    for line in open('../data/skill_weight.txt'):
        line = line.strip()
        job, skill, weight = line.split('\t')
        weight=float(weight)
        if job in skill_weight:
            skill_weight[job][skill] = weight
        else:
            skill_weight[job] = {skill: weight}


def college_location():#教育地点
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

def major_ability(experience, exp_y_salary, pro_title, t_pre_skill, academic_name,max_salary):#专业能力
    #技能得分
    skill_ability = 0.
    total_weight=0.000001
    if pro_title and t_pre_skill:
        #pro_title=pro_title[0]
        for key in pro_title:#职业
            if key in skill_weight:
                for i in skill_weight[key]:
                    total_weight+=skill_weight[key][i]
                    if t_pre_skill:
                        #print(i,t_pre_skill)
                        if i.decode('utf-8') in t_pre_skill:
                            skill_ability+=skill_weight[key][i]
    skill_ability=skill_ability/total_weight
    #计算期望薪金得分
    job_exp_ability = 0.#S_exp
    if exp_y_salary:
        if u"元/月以上" in exp_y_salary:
            str1=exp_y_salary.split(u"元/月")[0]
            if str1.isdigit():
                job_exp_ability+=int(str1)/max_salary
        elif u"元/月" in exp_y_salary:
            str1=exp_y_salary.split(u"元/月")
            if "-" in str1:
                low,high=str1.split("-")
                if low.isdigit() and high.isdigit():
                    low=int(low)
                    high=int(high)
                    mid=(low+high)/2.
                    job_exp_ability+=mid/max_salary
    #计算工作经验得分
    time_ability = 0.#T_experience
    if experience:#工作经验
        timelines = []
        for exp in experience:
            if 'start_time' in exp:
                if "." in exp['start_time']:
                    s_time = exp['start_time'].split('.')[0]
                    e_time=2014
                    if 'end_time' in exp:
                        if "." in exp['end_time']:
                            e_time=exp['end_time'].split('.')[0]
                        timelines.append(int(s_time))
                        timelines.append(int(e_time))
        if len(timelines)>1:
            temp=max(timelines)-min(timelines)
            if temp>20:
                temp=20
            time_ability+=temp/20.
    #计算学历得分
    academic_ability = 0.
    if academic_name:#学历
        if academic_name in academic_level:
            academic_ability += academic_level[academic_name]
    return (skill_ability + job_exp_ability + time_ability+academic_ability)/4. #?


def cal_location(exp_location, living, education):#期望工作地点
    if exp_location:
        return exp_location[0]
    elif living:
        return living#[0]
    elif education:
        for edu in education:
            if 'college' in edu:
                college = edu['college']
                if college in college_loc:
                    return college_loc[college]
        return 'null'
    else:
        return 'null'


def cal_job(pro_title, work_experience):#职业方向
    if pro_title:
        return pro_title[0]
    elif work_experience:
        for work in work_experience:
            return work['job_name']
        return 'null'
    else:
        return 'null'


def cal_dimensions():
    conn=MongoClient("192.168.4.250",27017)['zhilian']['zhilian_userinfo']
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['zhilian_dimension_six']
    print("找出最大薪金start...")
    max_salary=25000
    '''
    for cv in conn.find(timeout=False):
        exp_m_salary=''
        if 'exp_m_salary' in cv:
            exp_m_salary=cv['exp_m_salary']
        if exp_m_salary:
            if u"元/月以上" in exp_m_salary:
                temp=exp_m_salary.split(u"元/月")[0]
                if temp.isdigit():
                    temp=int(temp)
                    if temp>max_salary:
                        max_salary=temp
            elif u"元/月" in exp_m_salary:
                temp=exp_m_salary.split(u"元/月")[0]
                if u"-" in temp:
                    str1=temp.split("-")[1]
                    if str1.isdigit():
                        str1=int(str1)
                        if str1>max_salary:
                            max_salary=str1
    print("max_salary=%d"%(max_salary))
    '''
    max_salary+=1000
    print("6维度刻画start")
    count=0
    for cv in conn.find(timeout=False):
        if count%1000==0:
            print count
        count+=1
        post = {}
        uid = cv['_id']#
        pro_title=''
        if 'pro_title' in cv:
            pro_title = cv['pro_title']#职位
        exp_location=''
        if 'exp_location' in cv:
            exp_location = cv['exp_location']#期望工作地点
        living=''
        if 'living' in cv:
            living = cv['living']#居住地
        education=''
        if 'education' in cv:
            education = cv['education']#学历
        work_experience=''
        if 'experience' in cv:
            work_experience = cv['experience']#工作经验
        exp_y_salary=''
        if 'exp_m_salary' in cv:
            exp_y_salary = cv['exp_m_salary']#期望薪资
        t_pre_skill=''
        if 't_pre_skill' in cv:
            t_pre_skill = cv['t_pre_skill']#技能
        academic_name=''
        if 'academic_name' in cv:
            academic_name = cv['academic_name']#学历
        #计算工作地点
        location = cal_location(exp_location, living, education)
        #计算职业方向
        job = cal_job(pro_title, work_experience)
        #计算专业能力
        ability = major_ability(work_experience, exp_y_salary, pro_title, t_pre_skill, academic_name,max_salary)
        #计算性格
        character='null'
        if "selfEval" in cv:
            selfEval=cv['selfEval']
            if selfEval:
                character=cal_character(selfEval)
        #插入数据库
        post['ability'] = ability
        post['behavior']='null'
        post['character']=character
        post['friends']='null'
        post['job'] = job
        post['location'] = location
        post['uid'] = uid
        write_location.insert(post)
        '''
        print(i)
        print(location)
        print(job)
        print(ability)
        '''

if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args:cal_dimension(计算维度)")
        sys.exit(1)
    elif sys.argv[1]=='cal_dimension':
        #read_academic_level()
        #read_job_exp()
        read_chacater_word()
        read_skill_weight()
        college_location()
        company_loc()
        cal_dimensions()
