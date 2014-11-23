# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import datetime
import pymongo
from pymongo import MongoClient
import sys
'''csdn数据维度分析：职业方向、工作地点、专业能力、行为特征、性格null、好友null'''

skill_weight = {}  # 职业 技能 权值表
job_skill={}

def read_skill_weight():#skill_weight={后台开发:{c:10,c++:8,linux:15},...}
    for line in open('./data/skill_weight.txt'):
        line = line.strip()
        job, skill, weight = line.split('\t')
        if job in skill_weight:
            skill_weight[job][skill] = weight
        else:
            skill_weight[job] = {skill: weight}

def read_job_skill():#job_skill={后台开发:[c,c++,linux],...}
    for key in skill_weight:
        temp=skill_weight[key]
        for j in temp:
            if key not in job_skill:
                job_skill[key]=[j]
            else:
                job_skill[key].append(j)

def cal_behavior(csdnInfo):#行为特征
    end_time=datetime.date(2014,4,1)
    start_time=''
    if u"个人信息" in csdnInfo:
        if u"注册日期" in csdnInfo[u'个人信息']:
            start_time=csdnInfo[u'个人信息'][u'注册日期']
    year=2014
    month=4
    day=1
    y=''
    m=''
    d=''
    if u'年' in start_time:
        y,start_time=start_time.split(u'年')
        if u'月' in start_time:
            m,start_time=start_time.split(u'月')
            if u'日' in start_time:
                d=start_time.split(u'日')[0]
    if y:
        year=int(y)
    if m:
        month=int(m)
    if d:
        day=int(d)
    register_time=datetime.date(year,month,day)
    days=(end_time-register_time).days+1.0

    score=0.
    if u'博客' in csdnInfo:
        if u'积分' in csdnInfo[u"博客"]:
            score=int(csdnInfo[u"博客"][u"积分"])

    result=score/days
    if result>=1.4:
        return "博客达人"
    elif result>=0.7:
        return "活跃分子"
    elif result>=0.1:
        return "博客平民"
    else:
        return "博客懒人"

def cal_job(csdnInfo):#职业方向
    category=[]#找出所有的过技能
    if u"论坛" in csdnInfo:
        if u"论坛分数详情" in csdnInfo[u"论坛"]:
            temp=csdnInfo[u"论坛"][u"论坛分数详情"]
            for key in temp:
                if u"类别" in key:
                    category.append(key[u"类别"])
    if category:#找出job_skill下某个职业的技能库和category交集最多的职业作为职业方向
        category=set(category)
        job_skill_count={}#统计职业技能库中某职业下技能和category的交集数,{job:count}
        for key in job_skill:
            skills=job_skill[key]
            skills=set(skills)
            job_skill_count[key]=len(category.intersection(skills))
        job=max(job_skill_count.items(),key=lambda x:x[1])[0]
        skills=category.intersection(set(job_skill[job]))
        return job,skills
    return None,None

def major_ability(csdnInfo,job,skills):#专业能力
    expertScore=0.
    if u"CSDN信息:" in csdnInfo:
        if u"论坛" in csdnInfo[u"CSDN信息:"]:
            if u"专家分" in csdnInfo[u"CSDN信息:"][u"论坛"]:
                expertScore=csdnInfo[u"CSDN信息:"][u"论坛"][u"专家分"]
    skill_ability=0.
    total_skill_ability=0.
    if job:
        if skills:
            temp=skill_weight[job]
            for key in temp:
                total_skill_ability+=float(temp[key])
                if key in skills:
                    skill_ability+=float(temp[key])
    if total_skill_ability>0.:
        return (skill_ability/total_skill_ability+expertScore/1000000.)/2.#最大专家分为910413
    else:
        return expertScore/1000000./2.

def cal_dimensions():
    conn=MongoClient("192.168.4.250",27017)["csdn"]['csdn_userinfo2']
    write_location=MongoClient("192.168.2.1",27017)['shulianxunying']['csdn_dimension']
    count = 0
    fd=open('log_csdn.txt','w')
    for cv in conn.find(timeout=False):
        post = {}
        if count%10000==0:
            print(count)
        count=count+1
        uid=cv['_id']
        csdnInfo={}
        if u"CSDN信息:" in cv:
            csdnInfo=cv[u"CSDN信息:"]
        behavior=cal_behavior(csdnInfo)
        job,skills=cal_job(csdnInfo)
        ability=major_ability(csdnInfo,job,skills)
        if job:
            post['job']=job
        else:
            post['job']='null'
        post['ability']=ability
        post['behavior']=behavior
        post['character']='null'
        post['location']='null'
        post['friends']='null'
        post['uid']=uid
        write_location.insert(post)

if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args:cal_csdn_dimension")
        sys.exit(1)
    elif sys.argv[1]=="cal_csdn_dimension":
        print("cal_csdn_dimension")
        read_skill_weight()
        read_job_skill()
        cal_dimensions()
