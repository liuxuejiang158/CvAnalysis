# coding=utf-8
__author__ = 'liuxuejiang'
import collections
#import jieba
import pymongo
from pymongo import MongoClient
import sys
'''大街网简历数据6维度刻画：职业方向、工作地点、专业能力、性格null、行为特征null、好友null'''

college_loc = {}  #大学 城市表
companyLoc = [] #公司 城市表
skill_weight = {}  # 职业 技能 权值表
#job_exp = {}  # 职业 最高薪金表
academic_level = {u"中专":0.2,u"大专":0.4,u"本科":0.6,u"硕士研究生":0.8,u"博士研究生":1.0}  # 学历  等级表
'''
def read_academic_level():#学历等级
    for line in open('./data/academic_level.txt'):
        line = line.strip()
        academic, level = line.split('\t')
        academic_level[academic] = level

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
        weight=float(weight)
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
def major_ability(experience, exp_y_salary, pro_title, t_pre_skill,
                  degree_name,max_salary):#专业能力
    #技能得分
    skill_ability = 0.
    total_weight=0.000001
    if pro_title and pro_title!="null" and t_pre_skill and t_pre_skill!="null":
        if pro_title in skill_weight:#职业技能
            for i in skill_weight[pro_title]:
                total_weight+=skill_weight[pro_title][i]
                if i.decode('utf-8') in t_pre_skill:
                    skill_ability+=skill_ability[pro_title][i]
    skill_ability=skill_ability/total_weight
    #期望薪金得分，这里期望薪金全为null无需计算
    job_exp_ability = 0.
    #工作经验得分
    time_ability = 0.
    if experience:
        timelines = []
        for exp in experience:
            if "start_time" in exp:
                s_time=exp['start_time']
                if s_time and s_time!="null":
                    if u'年' in s_time:
                        s_time=s_time.split(u'年')[0]
                        if s_time.isdigit():
                            timelines.append(int(s_time))
            if "end_time" in exp:
                e_time=exp['end_time']
                if e_time and e_time!='null':
                    if u'年' in e_time:
                        e_time=e_time.split(u'年')[0]
                        if e_time.isdigit():
                            timelines.append(int(e_time))
        if(len(timelines)>1):#必须要有最小时间和最大时间
            temp=max(timelines)-min(timelines)+1.
            if temp>50:
                temp=50
            time_ability +=temp/50.
    #学历得分
    academic_ability = 0.
    if degree_name and degree_name!="null":#学历
        if degree_name in academic_level:
            academic_ability += academic_level[degree_name]
    return (skill_ability + job_exp_ability + time_ability+academic_ability)/4.

#计算工作地点
def cal_location(exp_location, living, work_now,education):#工作地点
    #期望工作地点:exp_location,living,工作地，大学所在地
    if exp_location and exp_location!='null':#期望工作地
        return exp_location
    if living and living!='null':#居住地
        return living
    if work_now and work_now!="null":#现在工作地,字符串
        for key in companyLoc:
            if key.decode('utf-8') in work_now:
                return key
    if education:#学校所在地，列表
        edu=education[0]#仅大学
        if "college" in edu:
            college = edu['college']
            if college:
                if college in college_loc:
                    return college_loc[college]
    return 'null'

#计算职业方向
def cal_job(status, experience):#职业方向
    #职业方向:status字段，工作经验中的职业名称
    if status and status!="null":
        return status
    if experience:#一个列表
        for work in experience:
            if "job_name" in work:
                job_name=work['job_name']
                if job_name:
                    return work['job_name']
    return 'null'


def cal_dimensions():
    conn=MongoClient("192.168.4.250",27017)['dajieweb']['dajieweb_userinfo']
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['dajieweb_dimension_six']
    max_salary=25000.
    '''
    print("计算最大期望薪金")
    for cv in conn.find(timeout=False):
        #似乎全是null
    print("max_salary=%d"%max_salary)
    print("计算维度")
    '''
    count = 0
    for cv in conn.find(timeout=False):
        post = {}
        if count%10000==0:
            print(count)
        count=count+1
        uid = cv['_id']#
        #pro_title = cv['pro_title']#职位
        status=''
        if 'status' in cv:
            status=cv['status']
        exp_location=''
        if 'exp_location' in cv:
            exp_location = cv['exp_location']#期望工作地点
        living=''
        if 'living' in cv:
            living = cv['living']#居住地
        work_now=''
        if 'work_now' in cv:
            work_now=cv['work_now']#现在工作单位
        education=''
        if 'education' in cv:
            education = cv['education']#学历
        experience=''
        if 'experience' in cv:
            experience = cv['experience']#工作经验
        exp_m_salary=''
        if 'exp_m_salary' in cv:
            exp_m_salary=cv['exp_m_salary']
        t_pre_skill=''
        if 't_pre_skill' in cv:
            t_pre_skill = cv['t_pre_skill']#技能
        degree_name=''
        if education:
            if 'degree_name' in education[0]:
                degree_name = education[0]['degree_name']#学历
        #计算工作地点
        location = cal_location(exp_location,living,work_now,education)#期望工作地点
        #计算职业方向
        job=cal_job(status,experience)
        #计算专业能力
        ability = major_ability(experience,exp_m_salary,status,
                                t_pre_skill,degree_name,max_salary)#专业能力
        #写入数据库
        post['ability'] = ability
        post['behavior']='null'
        post['character']='null'
        post['friends']='null'
        post['job'] = job
        post['location'] = location
        post['uid'] = uid
        write_location.insert(post)

if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args:cal_dajie_dimension(计算维度)")
        sys.exit(1)
    elif sys.argv[1]=="cal_dajie_dimension":
        print("cal_dajie_dimension")
        #read_academic_level()
        #read_job_exp()
        read_skill_weight()
        college_location()
        company_loc()
        cal_dimensions()
