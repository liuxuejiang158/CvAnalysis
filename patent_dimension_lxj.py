# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import math
import sys
import time
import pymongo
from pymongo import MongoClient
'''patent数据6维度刻画：职业方向、工作地点、行为特征、专业能力、性格null、好友null'''
def cal_first():#以专利人为中心重建数据库
    #finishedRecord=[]
    data=MongoClient("192.168.4.250",27017)['wanfang']['patent'].find(timeout=False,skip=610300)
    write_location=MongoClient("192.168.4.250",27017)['wanfang']['userinfo_patent_applicator']
    write_location_null=MongoClient("192.168.4.250",27017)['wanfang']['userinfo_patent_null']
    count=610300
    fd=open('log_patent_610300.txt','w')
    for cv in data:
    #for i in range(10):
    #   cv=data[i]
        count+=1
        fd.write(str(count)+'\n')
        uid=cv['_id']
        designer=''#发明人
        applicator=''#申请人
        if u"发明(设计)人" in cv:
            designer=cv[u"发明(设计)人"]
            if u"申请(专利权)人" in cv:
                applicator=cv[u"申请(专利权)人"]
        if designer:
            app_list=[]
            if ',' in applicator:
                app_list=applicator.split(',')
            else:
                app_list.append(applicator)
            if app_list:
                flag=0
                for key in app_list:
                    if key in designer:
                        flag=1#申请人和发明者有交集则说明申请人不是公司
                        break
                if flag==0:#申请人为公司
                    des_list=[]
                    if ',' in designer:
                        des_list=designer.split(',')
                    else:
                        des_list.append(designer)
                    for key in des_list:
                        for it in app_list:
                            #if (key,it) in finishedRecord:
                            temp=write_location.find_one({"designer":key,"applicator":it})
                            if temp:
                                write_location.update({"designer":key,"applicator":it},{"$push":{"patent":{"patent_id":uid,"patent_user_num":len(des_list)}}})
                            else:
                                #finishedRecord.append((key,it))
                                write_location.insert({"designer":key,"applicator":it,"patent":[{"patent_id":uid,"patent_user_num":len(des_list)}]})
                else:#申请人中有发明人
                    des_list=[]
                    if ',' in designer:
                        des_list=designer.split(',')
                    else:
                        des_list.append(designer)
                    for key in des_list:
                        write_location_null.insert({"designer":key,"applicator":'null',"patent":[{"patent_id":uid,"patent_user_num":len(des_list)}]})

city=[]#城市名称
def read_city():
    for line in open("../data/CityId.txt",'r'):
        line=line.strip()
        city.append(line.split('\t')[0])

college_location={}#大学所在地
def read_college_location():
    for line in open("../data/college_location.txt",'r'):
        line=line.strip()
        college,location=line.split('\t')
        college_location[college]=location

def cal_location(applicator):#通过专利申请人所属机构计算工作地点
    if applicator and applicator!="null":
        if u"公司" in applicator:
            for key in city:
                if key.decode("utf-8") in applicator:
                    return key
            return "null"
        if u"大学" in applicator:
            for key in college_location:
                if key.decode("utf-8") in applicator:
                    return college_location[key]
            return "null"
        for key in city:
            if key.decode("utf-8") in applicator:
                return key
        for key in college_location:
            if key.decode("utf-8") in applicator:
                return college_location[key]
        return 'null'
    return 'null'

patent_class={'A':"生活必需",'B':"运输作业",'C':"化学冶金",'D':"纺织制造",'E':"建筑采矿",'F':"机械工程",'G':"物理",'H':"电学"}
patentData=MongoClient("192.168.4.250",27017)['wanfang']['patent']
def cal_dimension_1():#计算专利的维度:职业方向,工作地点,行为特征,专业能力
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['wanfang_dimension_1']
    userData_1=MongoClient("192.168.4.250",27017)['wanfang']['userinfo_patent_applicator'].find(timeout=False)
    print("applicator is not null")
    count=0
    for cv in userData_1:
        if count%10000==0:
            print count
        count+=1
        post={}
        applicator=cv['applicator']#申请机构
        patent=cv['patent']#[{'patent_id':id,'patent_user_num':num},...]该用户拥有的所有专利需要通过patent_id从源数据库中查询
        info_patent=[]#找出用户的每个专利信息
        patent_num=[]#找出用户每个专利发明人数
        patent_id=[]#找出用户每个专利在源数据库中的ID
        for key in patent:
            data=patentData.find_one({"_id":key['patent_id']})
            if data:
                info_patent.append(data)
                patent_num.append(key['patent_user_num'])
                patent_id.append(data["_id"])
            else:
                print('error')#用户为中心的专利在源数据库未找到
        #计算职业方向
        jobDict={}
        job='null'
        for key in info_patent:
            main_class=''
            if u"主分类号" in key:
                main_class=key[u'主分类号']
            if main_class and main_class!="null":
                str1=main_class[0]
                if str1 not in jobDict:
                    jobDict[str1]=1
                else:
                    jobDict[str1]+=1
        if jobDict:
            str1=max(jobDict.items(),key=lambda x:x[1])[0]
            if str1 in patent_class:
                job=patent_class[str1]
        #工作地点
        location=cal_location(applicator)
        #行为特征
        timeList=[]
        avg=0.
        for key in info_patent:
            if u'申请日期' in key:
                if u'年' in key[u'申请日期']:
                    str1=key[u"申请日期"].split(u"年")[0]
                    if str1.isdigit():
                        timeList.append(int(str1))
        if timeList:
            temp=max(timeList)-min(timeList)+1.
            if temp>20:
                temp=20.
            avg=len(info_patent)/(temp)
        behavior='null'
        if avg>1.0:
            behavior=u'专利狂人'
        elif avg>0.5:
            behavior=u'专利达人'
        else:
            behavior=u'专利平民'
        #计算专业能力
        ability=0.0
        for i in range(len(info_patent)):
            p=0.
            category=""
            if u"专利类型" in info_patent[i]:
                category=info_patent[i][u"专利类型"]
            if not category:
                ability+=0.2
            elif category==u"发明专利":
                ability+=0.3/patent_num[i]
            elif category==u"实用新型":
                ability+=0.2/patent_num[i]
            elif category==u"外观设计专利":
                ability+=0.1/patent_num[i]
        post['ability']=ability
        post['behavior']=behavior
        post['character']='null'
        post['friends']='null'
        post['job']=job
        post['location']=location
        post['uid']=patent_id
        #PRINT(post)
        write_location.insert(post)

def cal_dimension_2():#计算applicator是null的数据
    print("applicator is null")
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['wanfang_dimension_2']
    userData_2=MongoClient("192.168.4.250",27017)['wanfang']['userinfo_patent_null'].find(timeout=False)
    count=0
    for cv in userData_2:
        if count%10000==0:
            print count
        count+=1
        post={}
        patent_id=cv['patent'][0]['patent_id']
        data=patentData.find_one({"_id":patent_id},{u"专利类型":1,u"主分类号":1})
        #计算职业方向
        job='null'
        main_class=''
        if u"主分类号" in data:#主分类号可能为空
            main_class=data[u"主分类号"]
            if main_class and main_class!="null":
                str1=main_class[0]
                if str1 in patent_class:
                    job=patent_class[str1]
        #计算工作地点
        location="null"
        #行为特征
        behavior=u"专利平民"
        #计算专业能力
        ability=0.
        category=""
        if u"专利类型" in data:#专利类型可能为空
            category=data[u"专利类型"]
        if not category:
            ability+=0.2
        elif category==u"发明专利":
            ability+=0.3
        elif category==u"实用新型":
            ability+=0.2
        elif category==u"外观设计专利":
            ability+=0.1
        #专利在源数据库中的ID
        patent_id=data["_id"]
        post['ability']=ability
        post['behavior']=behavior
        post['character']='null'
        post['friends']='null'
        post['job']=job
        post['location']=location
        post['uid']=patent_id
        #PRINT(post)
        write_location.insert(post)

def combine_1():#合并结果数据库
    conn_1=MongoClient("192.168.4.250",27017)['shulianxunying']['wanfang_dimension_1'].find(timeout=False)
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['wanfang_dimension']
    normalize={u"生活必需":200.,u"运输作业":5.0,u"化学冶金":75.,u"纺织制造":3.,u"建筑采矿":2.,u"机械工程":6.,u"物理":3.,u"电学":2.5}
    for cv in conn_1:
        job=cv['job']
        if job and job!="null":
            if cv["job"] in normalize:
                cv["ability"]/=normalize[cv['job']]
                write_location.insert(cv)
            else:
                print(cv['job'])
def combine_2():
    conn_2=MongoClient("192.168.4.250",27017)['shulianxunying']['wanfang_dimension_2'].find(timeout=False)
    write_location=MongoClient("192.168.4.250",27017)['shulianxunying']['wanfang_dimension']
    for cv in conn_2:
        write_location.insert(cv)


def PRINT(arg):
    for key in arg:
        print key,
        print arg[key]

if __name__ == "__main__":
    if len(sys.argv)<2:
        print "args:cal_first(辅助计算),cal_dimension_1,cal_dimension_2(维度计算),combine_1,combine_2(合并结果数据库)"
        sys.exit(1)
    if sys.argv[1]=="cal_first":
        print "cal_first"
        cal_first()
    elif sys.argv[1]=='cal_dimension_1':
        print("cal_dimension_1")
        read_city()
        read_college_location()
        cal_dimension_1()
    elif sys.argv[1]=="cal_dimension_2":
        print("cal_dimension_2")
        read_city()
        read_college_location()
        cal_dimension_2()
    elif sys.argv[1]=="combine_1":
        print("combine_1")
        combine_1()
    elif sys.argv[1]=="combine_2":
        print("combine_2")
        combine_2()
