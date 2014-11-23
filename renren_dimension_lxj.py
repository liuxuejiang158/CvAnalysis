# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import time
import pymongo
from pymongo import MongoClient
import sys
'''人人网简历数据6维度刻画：职业方向、工作地点、专业能力、行为null、性格null、好友null'''
college_loc = {}  #大学 城市表
companyLoc = [] #公司 城市表
skill_weight = {}  # 职业 技能 权值表
#job_exp = {}  # 职业 最高薪金表
academic_level = {}  # 学历  等级表

'''
def read_academic_level():#学历等级
    for line in open('./data/academic_level.txt'):
        line = line.strip()
        try:
            academic, level = line.split('\t')
            academic_level[academic] = level
        except ValueError:
            continue

def read_job_exp():#职位薪金
    for line in open("./data/job_exp.txt"):#open('./data/skill_weight.txt'):
        line = line.strip()
        try:
            job, exp = line.split('\t')
            job_exp[job] = exp
        except ValueError:
            continue
'''

def read_skill_weight():
    for line in open('../data/skill_weight.txt'):
        line = line.strip()
        job, skill, weight = line.split('\t')
        if job in skill_weight:
            skill_weight[job][skill] = weight
        else:
            skill_weight[job] = {skill: weight}

def college_location():#大学 地点
    for line in open('../data/college_location.txt'):
        line = line.strip()
        college, location = line.split('\t')
        college_loc[college] = location

def company_loc():#公司  地点
    for line in open("../data/CityId.txt"):
        line=line.strip()
        temp=line.split("\t")[0]
        if temp:
            companyLoc.append(temp)
        else:
            print("read company location error")

#计算专业能力
def major_ability(work,education):#专业能力
    #专业技能无法计算
    skill_ability = 0.
    #工作经验得分
    time_ability = 0.
    if work:
        timelines=[]
        for key in work:
            if "time" in key:
                time=key['time']
                if '-' in time:
                    timelines.append(int(time.split('-')[0]))
        if len(timelines)>1:
            temp=max(timelines)-min(timelines)+1.
            if temp>20:
                temp=20
            time_ability+=(max(timelines)-min(timelines))/20.
    #学历得分
    academic_ability = 0.
    if education:
        college=education[0]
        if "educationBackground" in college:
            educationBackground=college['educationBackground']
            if educationBackground == "COLLEGE":
                academic_ability+=0.1
            elif educationBackground == "MASTER":
                academic_ability+=0.15
            elif educationBackground=="DOCTOR":
                academic_ability+0.25
            else:
                print(educationBackground)
    return (skill_ability + time_ability)/4.+academic_ability

#计算工作地点
def cal_location(work,basicInfo):#工作地点
    if work:
        for key in work:
            if "name" in key:
                name=key['name']
                for city in companyLoc:
                    if city.decode('utf-8') in name:
                        return city
    if basicInfo:
        if "homeTown" in basicInfo:
            homeTown=basicInfo['homeTown']
            if homeTown:
                if 'city' in homeTown:
                    return homeTown['city']
    return 'null'

#计算职业方向
def cal_job(work):#职业方向
    if work:
        for key in work:
            if "job" in key:
                job=key['job']
                if job:
                    if "jobDetail" in job:
                        jobDetail=job['jobDetail']
                        if jobDetail and jobDetail!="null":
                            return jobDetail
    return 'null'

#6维度刻画
def cal_dimensions():
    conn=MongoClient("192.168.4.250",27017)['renren']['user']
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['renren_dimension']
    count = 0
    for cv in conn.find(timeout=False):
        post = {}
        if count%10000==0:
            print(count)
        count=count+1
        uid = cv['_id']#
        work=[]
        if "work" in cv:
            work=cv['work']
        basicInformation={}
        if "basicInformation" in cv:#包含homeTown
            basicInfo=cv['basicInformation']
        education=[]
        if "education" in cv:#包含educationBackground
            education=cv['education']
        #计算工作地点
        location = cal_location(work,basicInfo)#期望工作地点
        #计算职业方向
        job=cal_job(work)
        #计算专业能力
        ability=0.
        if job!='null':
            ability = major_ability(work,education)#专业能力
        post['ability'] = ability
        post['behavior']='null'
        post['character']='null'
        post['job'] = job
        post['location'] = location
        post['friends']='null'
        post['uid'] = uid
        write_location.insert(post)

if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args:cal_renren_dimension")
        sys.exit(1)
    elif sys.argv[1]=="cal_renren_dimension":
        print("cal_renren_dimension")
        #read_academic_level()
        #read_job_exp()
        read_skill_weight()
        college_location()
        company_loc()
        cal_dimensions()
