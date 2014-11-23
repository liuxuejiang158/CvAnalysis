# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import pymongo
from pymongo import MongoClient
import sys
import numpy as np
import jieba
import random
'''
跨域关联数据维度刻画:职业方向,性格,工作地点,专业能力,好友(null),行为特征(null)
'''

college_loc = {}  #大学 城市表
skill_weight = {}  # 职业 技能 权值表
#job_exp = {}  # 职业 最高薪金表
academic_level = {u"中专":0.2,u"大专":0.4,u"本科":0.6,u"硕士":0.8,u"博士":1.0,"Bachelor":0.6,"MBA":0.8,"Master":0.8,
                  "Junior college":0.4,"Doctor":1.0}  # 学历  等级表
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
def read_skill_weight(fileName):#职业技能表
    for line in open(fileName):
        line = line.strip()
        job, skill, weight = line.split('\t')
        weight = float(weight)
        if job in skill_weight:
            skill_weight[job][skill] = weight
        else:
            skill_weight[job] = {skill: weight}

def college_location(fileName):#大学位置表
    for line in open(fileName):
        line = line.strip()
        college, location = line.split('\t')
        college_loc[college] = location

def company_loc(fileName):#城市
    for line in open(fileName):
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
def read_chacater_word(fileName):
    flag=True
    for line in open(fileName):
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
def major_ability(work_time, exp_m_salary, pro_title, f_pre_skill,s_pre_skill,t_pre_skill, academic_name,max_salary):
    #技能得分
    max_skill_ability=0.
    max_total_weight=0.000001
    if pro_title:
        for key in pro_title:
            skill_ability = 0.
            total_weight=0.000001
            if key in skill_weight:
                for i in skill_weight[key]:
                    total_weight+=skill_weight[key][i]
                if f_pre_skill:
                    if i.decode('utf-8') in f_pre_skill:
                        skill_ability+=skill_weight[key][i]
                if s_pre_skill:
                    if i.decode('utf-8') in s_pre_skill:
                        skill_ability+=skill_weight[key][i]
                if t_pre_skill:
                    if i.decode('utf-8') in t_pre_skill:
                        skill_ability+=skill_weight[key][i]
            if skill_ability>max_skill_ability:
                max_skill_ability=skill_ability
                max_total_weight=total_weight
    skill_ability=max_skill_ability/max_total_weight
    #预期薪金得分
    job_exp_ability = 0.
    salary=0.
    if exp_m_salary:
        if u"以上" in exp_m_salary:
            str1=exp_m_salary.split(u'以上')[0]
            if str1.isdigit():
                salary=float(str1)
        elif "-" in exp_m_salary:
            if u'/月' in exp_m_salary:
                str1=exp_m_salary.split(u"/月")[0]
                if '-' in str1:
                    temp=str1.split('-')
                    if len(temp)==2:
                        low=temp[0]
                        high=temp[1]
                        if low.isdigit() and high.isdigit():
                            salary=(int(low)+int(high))/2.
        elif ' negotiation' in exp_m_salary:
            str1=exp_m_salary.split(' ')[0]
            if str1.isdigit():
                salary=float(str1)
        elif " or more" in exp_m_salary:
            str1=exp_m_salary.split(' ')[0]
            if str1.isdigit():
                salary=float(str1)
        elif u"面议" in exp_m_salary:
            salary=max_salary
    if salary>=max_salary:
        job_exp_ability=0.5
    else:
        job_exp_ability=salary/max_salary
    #工作经验得分
    time_ability = 0.
    if work_time and work_time.isdigit():
        temp=float(work_time)
        if temp>30:
            time_ability=0.
        elif temp>10 and temp<20:
            time_ability=1.
        else:
            time_ability+=int(work_time)/30.
    #学历得分
    academic_ability = 0.
    if academic_name:
        if academic_name in academic_level:
            academic_ability += academic_level[academic_name]
    return (skill_ability+job_exp_ability+time_ability+academic_ability)/4.


def cal_location(exp_location, living, education_yingcai,education_qcwy51,education_zhilian,experience_yingcai,experience_qcwy51,experience_zhilian):
    if exp_location:
        return exp_location[0]
    if living:
        return living
    if experience_yingcai:
        for key in experience_yingcai:
            if "com_name" in key:
                com_name=key['com_name']
                if com_name:
                    for i in companyLoc:
                        if i.decode('utf-8') in com_name:
                            return i
    if experience_qcwy51:
        for key in experience_qcwy51:
            if "com_name" in key:
                com_name=key['com_name']
                if com_name:
                    for i in companyLoc:
                        if i.decode('utf-8') in com_name:
                            return i
    if experience_zhilian:
        for key in experience_zhilian:
            if "com_name" in key:
                com_name=key['com_name']
                if com_name:
                    for i in companyLoc:
                        if i.decode('utf-8') in com_name:
                            return i
    if education_yingcai:
        for edu in education_yingcai:
            college = edu['college']
            if college:
                if college in college_loc:
                    return college_loc[college]
    if education_qcwy51:
        for edu in education_qcwy51:
            college = edu['college']
            if college:
                if college in college_loc:
                    return college_loc[college]
    if education_zhilian:
        for edu in education_zhilian:
            college = edu['college']
            if college:
                if college in college_loc:
                    return college_loc[college]
    return 'null'


def cal_job(pro_title,experience_yingcai,experience_zhilian,experience_qcwy51):
    if pro_title:
        return pro_title[0]
    if experience_yingcai:
        for key in experience_yingcai:
            if "job_name" in key:
                job_name=key['job_name']
                if job_name:
                    return job_name
    if experience_zhilian:
        for key in experience_zhilian:
            if "job_name" in key:
                job_name=key['job_name']
                if job_name:
                    return job_name
    if experience_qcwy51:
        for key in experience_qcwy51:
            if "job_name" in key:
                job_name=key['job_name']
                if job_name:
                    return job_name
    return 'null'

allKey=['cv_id','cv_origin','demande_id','comapre_dual','exp_location','exp_m_salary','exp_y_salary','f_lang','f_lang_skill','f_pre_skill',
's_lang','s_lang_skill','s_pre_skill','t_pre_skill','gender','identity_id','last_refresh','living','marry','nation','natives','other_info',
'politics','residence','self_eval','similarity','take_up_date','academic_name','birthday','user_img','work_now','work_status','work_time']


def cal_converage(cv):
    valid=0.
    if 'cv_id' in cv:
        if cv['cv_id']:
            valid+=1
    if 'cv_origin' in cv:
        if cv['cv_origin']:
            valid+=1
    if 'demande_id' in cv:
        if cv['demande_id']:
            valid+=1
    if 'comapre_dual' in cv:
        if cv['comapre_dual'] in cv:
            valid+=1
    if 'exp_location' in cv:
        if cv['exp_location']:
            valid+=1
    if 'exp_m_salary' in cv:
        if cv['exp_m_salary']:
            valid+=1
    if 'exp_y_salary' in cv:
        if cv['exp_y_salary']:
            valid+=1
    if 'f_lang' in cv:
        if cv['f_lang']:
            valid+=1
    if 'f_lang_skill' in cv:
        if cv['f_lang_skill']:
            valid+=1
    if 'f_pre_skill' in cv:
        if cv['f_pre_skill']:
            valid+=1
    if 's_lang' in cv:
        if cv['s_lang']:
            valid+=1
    if 's_lang_skill' in cv:
        if cv['s_lang_skill']:
            valid+=1
    if 's_pre_skill' in cv:
        if cv['s_pre_skill']:
            valid+=1
    if 't_pre_skill' in cv:
        if cv['t_pre_skill']:
            valid+=1
    if 'gender' in cv:
        if cv['gender']:
            valid+=1
    if 'identity_id' in cv:
        if cv['identity_id']:
            valid+=1
    if 'last_refresh' in cv:
        if cv['last_refresh']:
            valid+=1
    if 'living' in cv:
        if cv['living']:
            valid+=1
    if 'marry' in cv:
        if cv['marry']:
            valid+=1
    if 'nation' in cv:
        if cv['nation']:
            valid+=1
    if 'natives' in cv:
        if cv['natives']:
            valid+=1
    if 'other_info' in cv:
        if cv['other_info']:
            valid+=1
    if 'politics' in cv:
        if cv['politics']:
            valid+=1
    if 'pro_title' in cv:
        if cv['pro_title']:
            valid+=1
    if 'residence' in cv:
        if cv['residence']:
            valid+=1
    if 'self_eval' in cv:
        if cv['self_eval']:
            valid+=1
    if 'similarity' in cv:
        if cv['similarity']:
            valid+=1
    if 'take_up_date' in cv:
        if cv['take_up_date']:
            valid+=1
    if 'academic_name' in cv:
        if cv['academic_name']:
            valid+=1
    if 'birthday' in cv:
        if cv['birthday']:
            valid+=1
    if 'user_img' in cv:
        if cv['user_img']:
            valid+=1
    if 'work_now' in cv:
        if cv['work_now']:
            valid+=1
    if 'work_status' in cv:
        if cv['work_status']:
            valid+=1
    if 'work_time' in cv:
        if cv['work_time']:
            valid+=1
    if 'comapre_dual' in cv:#35
        if cv['comapre_dual']:
            valid+=1
    educationKey={'college':0,'start_time':0,'end_time':0,'major':0,'form_study':0,'academic_name':0,'edu_desc':0}#7
    education=[]
    if 'education_yingcai' in cv:
        education=cv['education_yingcai']
        for item in education:
            for key in educationKey:
                if key in item:
                    if item[key]:
                        educationKey[key]+=1
    education=[]
    if 'education_qcwy51' in cv:
        education=cv['education_qcwy51']
        for item in education:
            for key in educationKey:
                if key in item:
                    if item[key]:
                        educationKey[key]+=1
    education=[]
    if 'education_zhilian' in cv:
        education=cv['education_zhilian']
        for item in education:
            for key in educationKey:
                if key in item:
                    if item[key]:
                        educationKey[key]+=1
    for key in educationKey:
        if educationKey[key]>0:
            valid+=1
    experienceKey={'com_name':0,'com_type':0,'com_address':0,'job_name':0,'start_time':0,'end_time':0,'job_type':0,
                   'work_desc':0,'rea_leave':0,'reference':0,'reference_name':0}#11
    experience=[]
    if 'experience_yingcai' in cv:
        experience=cv['experience_yingcai']
        for item in experience:
            for key in experienceKey:
                if key in item:
                    if item[key]:
                        experienceKey[key]+=1
    experience=[]
    if 'experience_qcwy51' in cv:
        experience=cv['experience_qcwy51']
        for item in experience:
            for key in experienceKey:
                if key in item:
                    if item[key]:
                        experienceKey[key]+=1
    experience=[]
    if 'experience_zhilian' in cv:
        experience=cv['experience_zhilian']
        for item in experience:
            for key in experienceKey:
                if key in item:
                    if item[key]:
                        experienceKey[key]+=1
    for key in experienceKey:
        if experienceKey[key]>0:
            valid+=1
    projectsKey={'project_desc':0,'project_end':0,'project_name':0,'project_resp':0,'project_start':0}#5
    projects=[]
    if 'projects_yingcai' in cv:
        projects=cv['projects_yingcai']
        for item in projects:
            for key in projectsKey:
                if key in item:
                    if item[key]:
                        projectsKey[key]+=1
    projects=[]
    if 'projects_qcwy51' in cv:
        projects=cv['projects_qcwy51']
        for item in projects:
            for key in projectsKey:
                if key in item:
                    if item[key]:
                        projectsKey[key]+=1
    projects=[]
    if 'projects_zhilian' in cv:
        projects=cv['projects_zhilian']
        for item in projects:
            for key in projectsKey:
                if key in item:
                    if item[key]:
                        projectsKey[key]+=1
    for key in projectsKey:
        if projectsKey[key]>0:
            valid+=1
    trainingKey={'school':0,'start_time':0,'end_time':0,'train_content':0,'achieve':0}#5
    training=[]
    if 'training_yingcai' in cv:
        training=cv['training_yingcai']
        for item in training:
            for key in trainingKey:
                if key in item:
                    if item[key]:
                        trainingKey[key]+=1
    training=[]
    if 'training_qcwy51' in cv:
        training=cv['training_qcwy51']
        for item in training:
            for key in trainingKey:
                if key in item:
                    if item[key]:
                        trainingKey[key]+=1
    training=[]
    if 'training_zhilian' in cv:
        training=cv['training_zhilian']
        for item in training:
            for key in trainingKey:
                if key in item:
                    if item[key]:
                        trainingKey[key]+=1
    for key in trainingKey:
        if trainingKey[key]>0:
            valid+=1
    return valid/63.#(35.0+len(educationKey)+len(trainingKey)+len(experienceKey)+len(projectsKey))

def cal_dimensions(srcIP,srcDB,srcCollection,dstIP,dstDB,dstCollection):
    conn=MongoClient(srcIP,27017)[srcDB][srcCollection]#修改源数据库地址
    write_location=MongoClient(dstIP,27017)[dstDB][dstCollection]#修改目的数据库地址
    max_salary=300000.
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
                elif "-" in exp_m_salary:
                    if u'/月' in exp_m_salary:
                        str1=exp_m_salary.split(u"/月")[0]
                        if '-' in str1:
                            temp=str1.split('-')
                            if len(temp)==2:
                                low=temp[0]
                                high=temp[1]
                                if high.isdigit() and int(high)>max_salary:
                                    max_salary=int(high)
                elif ' negotiation' in exp_m_salary:
                    str1=exp_m_salary.split(' ')[0]
                    if str1.isdigit() and int(str1)>max_salary:
                        max_salary=int(str1)
                elif " or more" in exp_m_salary:
                    str1=exp_m_salary.split(' ')[0]
                    if str1.isdigit() and int(str1)>max_salary:
                        max_salary=int(str1)
    print("max_salary=%d"%(max_salary))#找出最大4000 0000
    '''
    max_salary+=1000.
    print("计算维度")
    count = 0
    for cv in conn.find(timeout=False):
        count += 1
        if count % 10000 == 0:
            print count
        post = {}
        uid = cv['_id']
        pro_title=''
        if 'pro_title' in cv:
            pro_title = cv['pro_title']
        exp_location=[]
        if 'exp_location' in cv:
            exp_location = cv['exp_location']
        living=''
        if 'living' in cv:
            living=cv['living']
        education_yingcai=[]
        if "education_yingcai" in cv:
            education_yingcai=cv['education_yingcai']
        education_qcwy51=[]
        if "education_qcwy51" in cv:
            education_qcwy51=cv['education_qcwy51']
        education_zhilian=[]
        if "education_zhilian" in cv:
            education_zhilian=cv['education_zhilian']
        experience_yingcai=[]
        if "experience_yingcai" in cv:
            experience_yingcai=cv["experience_yingcai"]
        experience_zhilian=[]
        if "experience_zhilian" in cv:
            experience_zhilian=cv["experience_zhilian"]
        experience_qcwy51=[]
        if "experience_qcwy51" in cv:
            experience_qcwy51=cv["experience_qcwy51"]
        exp_m_salary=''
        if 'exp_m_salary' in cv:
            exp_m_salary = cv['exp_m_salary']
        academic_name=''
        if 'academic_name' in cv:
            academic_name = cv['academic_name']
        f_pre_skill=''
        if 'f_pre_skill' in cv:
            f_pre_skill=cv['f_pre_skill']
        s_pre_skill=''
        if 's_pre_skill' in cv:
            s_pre_skill=cv['s_pre_skill']
        t_pre_skill=''
        if 't_pre_skill' in cv:
            t_pre_skill=cv['t_pre_skill']
        work_time=''
        if 'work_time' in cv:
            work_time=cv['work_time']
        academic_name=''
        if 'academic_name' in cv:
            academic_name=cv['academic_name']
        #计算工作地点
        location = cal_location(exp_location,living,education_yingcai,education_qcwy51,education_zhilian,experience_yingcai,experience_qcwy51,experience_zhilian)
        #计算职业方向
        job = cal_job(pro_title,experience_yingcai,experience_zhilian,experience_qcwy51)
        #计算专业能力
        ability = major_ability(work_time,exp_m_salary, pro_title, f_pre_skill,s_pre_skill,t_pre_skill, academic_name,max_salary)
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
        #post['uid'] = uid
        post['uid']=cv['cv_id']
        post['coverage']=cal_converage(cv)
        write_location.insert(post)


if __name__ =="__main__":
    #read_academic_level()
    #read_job_exp()
    print("输入参数:源数据库名称(srcDB)、源数据集合名称(srcCollection)、结果数据库名称(dstDB)、结果数据集合名称(dstCollection)")
    if len(sys.argv)<4:
        print("输入参数错误")
        sys.exit(1)
    read_chacater_word("../data/word_tfidf.txt")
    read_skill_weight("../data/skill_weight.txt")
    college_location("../data/college_location.txt")
    company_loc("../data/CityId.txt")
    #print(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])
    cal_dimensions("192.168.4.249",sys.argv[1],sys.argv[2],"192.168.4.249",sys.argv[3],sys.argv[4])
    '''
    cal_dimensions(srcIP,srcDB,srcCollection,dstIP,dstDB,dstCollection)
    #参数依次表示:  源数据存放的服务器地址srcIP、
                    源数据所在的数据库名称srcDB
                    源数据的集合名称srcCollection
                    结果数据存放的机器地址dstIP
                    结果数据存放的数据库名称dstDB
                    结果数据集合名称dstCollection
    #调用实例:
        cal_dimensions("192.168.4.249","combine","combine_info","192.168.4.249","shulianxunying","combine_info_dimension")
    #表示计算服务器192.168.4.249上数据库combine下的combine_info数据集合
    #并将结果保存至192.168.4.249上的数据库shulianxunying下的combine_info_dimension数据集合
    
    #脚本执行命令shell
    python 
    '''

