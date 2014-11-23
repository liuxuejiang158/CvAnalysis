# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import math
import time
import pymongo
import sys
from pymongo import MongoClient
'''google-scholar数据维度分析：职业方向、工作地点、专业能力、行为特征、好友、性格null'''

country_lowercase=[]#每个元素是国家英文名称
def read_country():#读取国家地区的英文名称并转为小写
    for line in open("./data/country.txt",'r'):
        line=line.strip().split('\t')[0].lower()
        country_lowercase.append(line)

def cal_first():#第一次计算出字段:uid,GoogleID,country,interest,h_level,ave,co_author_GoogleID
    conn_1=MongoClient("192.168.4.250",27017)["google_scholar"]['userinfo'].find(timeout=False)
    write_conn_1=MongoClient("192.168.2.254",27017)['shulianxunying']['temp_google_scholar']
    count=0
    for cv in conn_1:
        if count%1000==0:
            print count;
        count+=1
        post={}
        uid=cv['_id']
        info={}
        if u"信息:" in cv:
            info=cv[u"信息:"]
        GoogleID=''
        if 'GoogleID' in cv:
            GoogleID=cv['GoogleID']
        interest='null'#科研方向
        country='null'#国家
        research_ability=0.0#科研能力
        behavior='null'#活跃程度
        co_author_GoogleID=[]#合作者的GoogleID
        h_level=0#H因子
        if info:
            #计算科研方向:信息->兴趣方向
            if u"兴趣方向" in info:
                tempstr=info[u"兴趣方向"]
                if tempstr:
                    if tempstr!=u"兴趣未知":
                        interest=tempstr
            #计算国家:信息->所属机构
            if u"所属机构" in info:
                tempstr=info[u"所属机构"]
                if tempstr:
                    tempstr=tempstr.lower()
                    for key in country_lowercase:
                        if key in tempstr:
                            country=key
                            break
            #计算科研能力:信息->引用计数->h指数->总计
            if u"引用指数" in info:
                if u"h指数" in info[u"引用指数"]:
                    if u"总计" in info[u"引用指数"][u"h指数"]:
                        tempint=info[u"引用指数"][u"h指数"][u"总计"]
                        if tempint:
                            research_ability=1-math.pow(math.e,-tempint/10.)
                            h_level=tempint
            #计算活跃程度:信息->全部文章
            if u"全部文章" in info:
                total_article=info[u"全部文章"]
                if total_article:
                    timelist=[]
                    for key in total_article:
                        if u"发表年份" in total_article[key]:
                            tempstr=total_article[key][u"发表年份"]
                            if tempstr:
                                timelist.append(int(tempstr))
                    if timelist:
                        minyear=min(timelist)
                        ave=len(timelist)/(2014-minyear+0.0001)
                        activity=ave*math.pow(2014-minyear,0.5)
                        if activity>=20.:
                            behavior=u'超级活跃学者'
                        elif activity>=5.0:
                            behavior=u'活跃学者'
                        elif behavior>=1.:
                            behavior=u'普通学者'
                        else:
                            behavior=u'沉默学者'
            #找出所有合作者的GoogleID
            if u"合著作者" in info:
                co_author=info[u"合著作者"]
                if co_author:
                    for key in co_author:
                        tempstr=co_author[key]
                        start=tempstr.find('=')
                        end=tempstr.find('&')
                        if start>0 and end>0:
                            co_author_GoogleID.append(tempstr[start+1:end])
        post['uid']=uid
        post['GoogleID']=GoogleID
        post['interest']=interest
        post['country']=country
        post['research_ability']=research_ability
        post['behavior']=behavior
        post['co_author_GoogleID']=co_author_GoogleID
        post['h_level']=h_level
        write_conn_1.insert(post)
        '''
        print(interest)
        print(country)
        print(research_ability)
        print(behavior)
        '''

def cal_second():#计算好友质量
    conn_2=MongoClient("192.168.2.254",27017)["shulianxunying"]["temp_google_scholar"]
    conn_3=MongoClient("192.168.2.254",27017)["shulianxunying"]["temp_google_scholar"].find(timeout=False)
    write_conn_2=MongoClient("192.168.2.254",27017)['shulianxunying']['google_scholar_dimension']
    count=0
    for cv in conn_3:
    #for i in range(10):
    #    cv=conn_3[i]
        if count%1000==0:
            print count
        count+=1
        co_author_GoogleID=cv['co_author_GoogleID']
        co_author_interest=[]
        co_author_country=[]
        co_author_hlevel=[]
        for authorID in co_author_GoogleID:
            data=conn_2.find_one({"GoogleID":authorID},{"interest":1,"h_level":1,"country":1,"_id":0})#查找合著者的情况
            if data:
                if data['interest']!='null':
                    co_author_interest.append(data['interest'])
                if data['country']!='null':
                    co_author_country.append(data['country'])
                if data['h_level']>0:
                    co_author_hlevel.append(data['h_level'])
        #统计合著者的研究方向分布
        interest_set=set(co_author_interest)
        interest_count=[]
        for key in interest_set:#计算相同研究方向的合著者
            interest_count.append(co_author_interest.count(key))
        #统计合著者的地区分布
        country_set=set(co_author_country)
        country_count=[]
        for key in country_set:#计算相同地区的合著者
            country_count.append(co_author_country.count(key))
        #计算合著者的平均H因子
        sum_hlevel=0.
        for key in co_author_hlevel:#计算合著者的H因子
            sum_hlevel+=key
        avg_hlevel=sum_hlevel/(len(co_author_hlevel)+0.000001)#计算合著者的平均H因子
        #计算合著者的研究方向墒
        f_interest=0.0
        sum_interest=sum(interest_count)+0.000001
        for key in interest_count:
            p=key/sum_interest
            f_interest+=p*math.log(p)
        #计算合著者的地区墒
        sum_country=sum(country_count)+0.000001
        f_country=0.
        for key in country_count:
            p=key/sum_country
            f_country+=p*math.log(p)
        f=-1/3.*f_interest-1/3.*f_country+0.03*avg_hlevel
        #print(-1/3*f_interest,-1/3*f_country,0.03*avg_hlevel,f)
        if f>1:
            f=1
        post={}
        post['ability']=cv['research_ability']
        post['behavior']=cv['behavior']
        post['character']='null'
        post['friends']=f
        post['job']=cv['interest']
        post['location']=cv['country']
        post['uid']=cv['uid']
        write_conn_2.insert(post)



if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args:cal_first(辅助计算),cal_second(计算维度)")
        sys.exit(1)
    elif sys.argv[1]=="cal_first":#用于第一次辅助计算,主要计算出字段:uid,国家,科研方向,科研能力,活跃程度,好友ID
        print("cal first")
        read_country()
        cal_first()
    elif sys.argv[2]=="cal_second":#在第一次辅助的结果上计算好友质量
        print("cal second")
        cal_second()
