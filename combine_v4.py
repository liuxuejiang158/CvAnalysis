# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import pymongo
from pymongo import MongoClient
import sys
import time
import datetime
'''
跨域关联数据维度刻画:学历,专业能力,工作经验，工作年限，跳槽频率，简历更新度
'''

skill_weight = {}  # 职业 技能 权值表
#job_exp = {}  # 职业 最高薪金表
academic_level = {u"中专":0.2,u'高中':0.2,u'专科':0.4,u"大专":0.4,u"本科":0.6,u"硕士":0.8,u"博士":1.0,"Bachelor":0.6,"MBA":0.8,"Master":0.8,
                  "Junior college":0.4,"Doctor":1.0}  # 学历  等级表
def read_skill_weight(fileName):#职业技能表
    for line in open(fileName):
        line = line.strip()
        job, skill, weight = line.split('\t')
        weight = float(weight)
        if job in skill_weight:
            skill_weight[job][skill] = weight
        else:
            skill_weight[job] = {skill: weight}

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

def cal_experience(experience_max,projects_max,last_refresh):
    ei=0#计算工作经验中的累计时间(月为单位)
    pi=0#计算项目经验中的累计时间(月为单位)
    ei_1=0.
    last_year=datetime.datetime.now().year
    last_month=datetime.datetime.now().month
    now_year=datetime.datetime.now().year
    if '-' in last_refresh:
        temp=last_refresh.split('-')
        if len(temp)==3:
            if temp[0].isdigit():
                last_year=int(temp[0])
            if temp[1].isdigit():
                last_month=int(temp[1])
    for item in experience_max:#工作经验时间累计
        start_time=item['start_time']
        end_time=item['end_time']
        temp=[]
        if '-' in start_time:
            temp=start_time.split('-')
        elif '.' in start_time:
            temp=start_time.split('.')
        elif '/' in start_time:
            temp=start_time.split('/')
        if len(temp)==2:
            if temp[0].isdigit():
                start_year=int(temp[0])
                if start_year>=1960 and start_year<=now_year:
                    if temp[1].isdigit():
                        start_month=int(temp[1])
                        if start_month>=1 and start_month<=12:
                            if u'今' in end_time:
                                if last_year<=now_year and last_year>=start_year:
                                    num=(last_year-start_year-1)*12+12-start_month+last_month
                                    if num>0:
                                        ei+=num
                                        ei_1+=1.0/num
                            else:
                                temp=[]
                                if '-' in end_time:
                                    temp=end_time.split('-')
                                elif '.' in end_time:
                                    temp=end_time.split('.')
                                elif '/' in end_time:
                                    temp=end_time.split('/')
                                if len(temp)==2:
                                    if temp[0].isdigit():
                                            end_year=int(temp[0])
                                            if end_year>=start_year and end_year<=now_year:
                                                if temp[1].isdigit():
                                                        end_month=int(temp[1])
                                                        if end_month>=1 and end_month<=12:
                                                            num=(end_year-start_year-1)*12+(12-start_month)+end_month
                                                            if num>0:
                                                                ei+=num
                                                                ei_1+=1./num
    for item in projects_max:#项目经验时间累计
        start_time=item['project_start']
        end_time=item['project_end']
        temp=[]
        if '-' in start_time:
            temp=start_time.split('-')
        elif '.' in start_time:
            temp=start_time.split('.')
        elif '/' in start_time:
            temp=start_time.split('/')
        if len(temp)==2:
            if temp[0].isdigit():
                start_year=int(temp[0])
                if start_year>=1960 and start_year<=now_year:
                    if temp[1].isdigit():
                        start_month=int(temp[1])
                        if start_month>=1 and start_month<=12:
                            if u'今' in end_time:
                                if last_year<=now_year and last_year>=start_year:
                                    pi+=(last_year-start_year-1)*12+12-start_month+last_month
                            else:
                                temp=[]
                                if '-' in end_time:
                                    temp=end_time.split('-')
                                elif '.' in end_time:
                                    temp=end_time.split('.')
                                elif '/' in end_time:
                                    temp=end_time.split('/')
                                if len(temp)==2:
                                    if temp[0].isdigit():
                                        end_year=int(temp[0])
                                        if end_year>=start_year and end_year<=now_year:
                                            if temp[1].isdigit():
                                                end_month=int(temp[1])
                                                if end_month>=1 and end_month<=12:
                                                    pi+=(end_year-start_year-1)*12+(12-start_month)+end_month
    return ei,pi,ei_1

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
        projects_yingcai=[]
        if "projects_yingcai" in cv:
            projects_yingcai=cv['projects_yingcai']
        projects_qcwy51=[]
        if 'projects_qcwy51' in cv:
            projects_qcwy51=cv['projects_qcwy51']
        projects_zhilian=[]
        if 'projects_zhilian' in cv:
            projects_zhilian=cv['projects_zhilian']
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
        last_refresh=''
        if 'last_refresh' in cv:
            last_refresh=cv['last_refresh']
        #学历
        academic=0.2
        if academic_name!="":
            if academic_name in academic_level:
                academic=academic_level[academic_name]
        #计算专业能力
        ability = major_ability(work_time,exp_m_salary, pro_title, f_pre_skill,s_pre_skill,t_pre_skill, academic_name,max_salary)
        if ability>=0.6:
            ability=0.99
        else:
            ability=ability/0.6
        #工作经验
        experience_max=experience_yingcai
        if len(experience_yingcai)<len(experience_zhilian):
            experience_max=experience_zhilian
            if len(experience_zhilian)<len(experience_qcwy51):
                experience_max=experience_qcwy51
        elif len(experience_yingcai)<len(experience_qcwy51):
            experience_max=experience_qcwy51
        projects_max=projects_yingcai
        if len(projects_yingcai)<len(projects_zhilian):
            projects_max=projects_zhilian
            if len(projects_zhilian)<len(projects_qcwy51):
                projects_max=projects_qcwy51
        elif len(projects_yingcai)<len(projects_qcwy51):
            projects_max=projects_qcwy51
        ei,pi,ei_1=cal_experience(experience_max,projects_max,last_refresh)
        experience=ei+pi
        if experience>=1200:
            experience=0.99
        else:
            experience=experience/1200.
        #计算工作年限
        worktime=1
        num=1
        if work_time.isdigit():
            num=int(work_time)
            if num<1:
                num=1
            worktime=num*ei
        if worktime>=10000:
            worktime=0.99
        else:
            worktime=worktime/10000.
        #计算跳槽频率
        changefreq=(1./num)*ei_1
        if changefreq>=0.7:
            changefreq=0.99
        else:
            changefreq=changefreq/0.7
        #计算简历更新时间
        refresh=1000
        if '-' in last_refresh:
            temp=last_refresh.split('-')
            if len(temp)==3:
                if temp[0].isdigit() and temp[1].isdigit() and temp[2].isdigit():
                    refresh=(datetime.datetime.now()-datetime.datetime(int(temp[0]),int(temp[1]),int(temp[2]))).days
        if refresh>=1000:
            refresh=0.99
        else:
            refresh=refresh/1000.
        refresh=1-refresh
        post['academic']=academic
        post['ability'] = ability
        post['experience']=experience
        post['worktime']=worktime
        post['changefreq']=changefreq
        post['refresh']=refresh
        post['uid']=cv['cv_id']
        post['coverage']=cal_converage(cv)
        write_location.insert(post)

#脚本执行方法：python combine_v4.py combine combine2_info shulianxunying combine2_info_dimension
#参数意义分别是:从combine数据库中取出combine2_info集合进行计算，结果存至shulianxunying数据库的combine2_info_dimension集合中
if __name__ =="__main__":
    print("输入参数:源数据库名称(srcDB)、源数据集合名称(srcCollection)、结果数据库名称(dstDB)、结果数据集合名称(dstCollection)")
    if len(sys.argv)<4:
        print("输入参数错误")
        sys.exit(1)
    #print(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])
    read_skill_weight("../data/skill_weight.txt")
    cal_dimensions("192.168.4.249",sys.argv[1],sys.argv[2],"192.168.4.249",sys.argv[3],sys.argv[4])
    '''
    函数cal_dimensions说明:
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
    '''

