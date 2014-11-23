# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import pymongo
from pymongo import MongoClient
import sys
'''领英6维度刻画:职业方向、专业能力、工作地、性格null、行为特征null、好友null'''

college_loc = {}  #大学 城市表
skill_weight = {}  # 职业 技能 权值表
academic_level = {}  # 学历  等级表
skill_maxweight_job={}#职业技能表中某个技能权重最大时对应的职业

def read_academic_level():#读取学位表
    for line in open("./data/bachelor.txt"):#学士学位
        line=line.strip()
        full,abbr=line.split('\t')
        if ',' in abbr:
            abbrList=abbr.split(',')
            for key in abbrList:
                academic_level[key.strip()]=0.4
        else:
            academic_level[abbr]=0.4
        academic_level[full]=0.4
    for line in open("./data/master.txt"):#硕士学位
        line=line.strip()
        full,abbr=line.split('\t')
        academic_level[full]=0.8
        if ',' in abbr:
            abbrList=abbr.split(',')
            for key in abbrList:
                academic_level[key.strip()]=0.4
        else:
            academic_level[abbr]=0.4
    for line in open("./data/phd.txt"):#博士学位
        line=line.strip()
        full,abbr=line.split('\t')
        academic_level[full]=1.0
        if ',' in abbr:
            abbrList=abbr.split(',')
            for key in abbrList:
                academic_level[key.strip()]=0.4
        else:
            academic_level[abbr]=0.4

def read_skill_weight():#读取职业技能库，并找出技能在权值最大时对应的职业
    #skill_weight={job:{skill_1:weight_1,skill_2:weight_2,...},...}
    for line in open('./data/skill_weight.txt'):
        line = line.strip()
        job, skill, weight = line.split('\t')
        weight = float(weight)
        if job in skill_weight:#职业技能权重表
            skill_weight[job][skill] = weight
        else:
            skill_weight[job] = {skill: weight}

        if skill not in skill_maxweight_job:#找出某技能最大权重的职业{skill:[weight,job],...}
            skill_maxweight_job[skill]=[weight,job]
        else:
            if weight>skill_maxweight_job[skill][0]:
                skill_maxweight_job[skill]=[weight,job]

def read_college_location():#读取大学城市表
    for line in open('./data/college_location.txt'):
        line = line.strip()
        college, location = line.split('\t')
        college_loc[college] = location


def major_ability(job,skillsMpr,positionsMpr,educationsMpr,honorsMpr,projectsMpr,patentsMpr):#计算职业能力
    #技能得分
    skill_ability=0.
    total_skill_weight=0.0001#防止除0
    if job:#计算技能能力
        if job in skill_weight:#职业方向在职业技能表中
            if skillsMpr:
                skills=set()
                for key in skillsMpr:#擅长技能
                    if "fmt__skill_name" in key:
                        skills.add(key['fmt__skill_name'])
                for key in skill_weight[job]:
                    total_skill_weight+=skill_weight[job][key]
                    if key in skills:#当擅长技能在职业技能库中
                        skill_ability+=skill_weight[job][key]
    skill_ability=skill_ability/total_skill_weight
    #工作经验得分
    timeslines=[]
    if positionsMpr:#工作经验
        for key in positionsMpr:
            if "startdate_my" in key:
                if u"年" in key['startdate_my']:#获取公元纪年
                    timeslines.append(int(key['startdate_my'].split(u"年")[0]))
            if "enddate_my" in key:
                if u"年" in key['enddate_my']:
                    timeslines.append(int(key['enddate_my'].split(u"年")[0]))
    if projectsMpr:
        for key in projectsMpr:
            if 'single_date_my_long' in key:
                if u"年" in key['single_date_my_long']:
                    timeslines.append(int(key['single_date_my_long'].split(u"年")[0]))
    time_ability=0.
    if timeslines:
        time_ability+=(max(timeslines)-min(timeslines))/20.
    #获奖情况得分
    award_ability=0.
    if honorsMpr:#所获荣誉
        award_ability+=len(honorsMpr)
    if patentsMpr:#专利情况
        award_ability+=len(patentsMpr)
    award_ability=min(award_ability,10)
    award_ability=award_ability/10.
    #学历得分
    academic_ability=0.
    if educationsMpr:
        degree=''
        if 'degree' in educationsMpr[0]:#获取学位
            degree=educationsMpr[0]['degree']
        if degree in academic_level:#学位在学历表中
            academic_ability+=academic_level[degree]
        elif "Master" in degree:
            academic_ability+=0.6
        elif "Bachelor" in degree:
            academic_ability+=0.4
        elif "PhD" in degree:
            academic_ability+=1.0
        elif "MBA" in degree:
            academic_ability+=0.8
    return (skill_ability+time_ability+award_ability+academic_ability)/4.0


def cal_location(BasicInfo_location_highlight,positionsMpr, educationsMpr):#计算工作地点
    if BasicInfo_location_highlight:#基本信息中有地点
        return BasicInfo_location_highlight
    elif positionsMpr:#从工作经验的公司所在地中找地点
        for key in positionsMpr:
            if 'locationName' in key:
                return key['locationName']
    if educationsMpr:#从教育经历中找学校地点
        for key in educationsMpr:
            if 'schoolname' in key:
                schoolname=key['schoolname']
                if shoolname in college_loc:
                    return college_loc[schoolname]
    return 'null'#无法判定地点


def cal_job(positionsMpr,skillsMpr,BasicInfo_industry_highlight):#职业方向
    if positionsMpr:#从工作经验中提取职业
        for key in positionsMpr:
            if "title_highlight" in key:
                return key['title_highlight']
    if skillsMpr:#从擅长技能中提取职业
        for key in skillsMpr:
            if "fmt__skill_name" in key:
                skill=key['fmt__skill_name']
                if skill in skill_maxweight_job:
                    return skill_maxweight_job[skill][1]#返回技能对应最大权重的职业
    if BasicInfo_industry_highlight:#从基本信息中的行业获取职业
        return BasicInfo_industry_highlight#可能返回''
    return 'null'

def cal_dimensions():#计算各个维度
    conn=MongoClient("192.168.4.250",27017)['linkedin']['linkedin_userinfo']
    write_location = MongoClient("192.168.2.254", 27017)['shulianxunying']['linkedin_dimension']
    count = 0
    logfd=open("log_linkedin.txt",'w')
    for cv in conn.find(timeout=False):
    	post={}
        count += 1
        if count % 1000 == 0:
            print count
        BasicInfo_industry_highlight=''
        BasicInfo_industry_highlight=''
        if 'BasicInfo' in cv:
            if "location_highlight" in cv['BasicInfo']:
                BasicInfo_location_highlight=cv['BasicInfo']['location_highlight']
            if "industry_highlight" in cv["BasicInfo"]:
                BasicInfo_industry_highlight=cv['BasicInfo']['industry_highlight']
        positionsMpr=[]
        if 'positionsMpr' in cv:
            positionsMpr=cv['positionsMpr']
        educationsMpr=[]
        if 'educationsMpr' in cv:
            educationsMpr=cv['educationsMpr']
        skillsMpr=[]
        if 'skillsMpr' in cv:
            skillsMpr=cv['skillsMpr']
        honorsMpr=[]
        if 'honorsMpr' in cv:
            honorsMpr=cv['honorsMpr']
        projectsMpr=[]
        if 'projectsMpr' in cv:
            projectsMpr=cv['projectsMpr']
        patentsMpr=[]
        if 'patentsMpr' in cv:
            patentsMpr=cv['patentsMpr']
        #计算工作地点
        location=''
        location=cal_location(BasicInfo_location_highlight,positionsMpr,educationsMpr)
        if location:
            post['location']=location
        else:
            post['location']='null'
        #计算职业方向
        job=''
        job=cal_job(positionsMpr,skillsMpr,BasicInfo_industry_highlight)
        if job:
            post['job']=job
        else:
            post['job']='null'
        #计算专业能力
        ability=0.
        ability=major_ability(job,skillsMpr,positionsMpr,educationsMpr,honorsMpr,projectsMpr,patentsMpr)
        if ability:
            post['ability']=ability
        else:
            post['ability']=0.
        #职业性格(暂时无法刻画)
        post['character']='null'
        post['uid']=cv['_id']
        write_location.insert(post)

if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args: cal_linkedin_dimension")
        sys.exit(1)
    elif sys.argv[1]=="cal_linkedin_dimension":
        read_college_location()#读取大学城市表
        read_academic_level()#读取学历得分表
        read_skill_weight()#读取职业技能权重表
        cal_dimensions()#计算：职业方向，工作地点，职业性格，专业能力
