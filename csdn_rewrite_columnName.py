# coding=utf-8
__author__ = 'liuxuejiang'
import collections
import pymongo
from pymongo import MongoClient
'''将csdn源数据的中文字段转为英文字段'''
dest = "192.168.2.1"
dbname = "csdn"
client = MongoClient(dest, 27017)
db = client[dbname]
yingcai = db['csdn_userinfo2']
fin = yingcai.find(timeout=False)#保持连接

write_location=MongoClient("192.168.2.1",27017)['shulianxunying']['csdn_data']

if __name__ == "__main__":
    post={}
    #for cv in fin:
    for i in range(5):
        cv=fin[i]
        for key in cv:
            if key == "username":
                post['username']=cv[key]

            if key == "_id":
                post['uid']=cv[key]

            if "CSDN" in key:
                temp = cv[key]
                csdnInfo={}
                for i in temp:
                    if i == u"个人信息":
                        profile={}
                        temp1 = temp[i]
                        for j in temp1:
                            if j == u"注册日期":
                                profile['start_time']=temp1[j]
                            if j == u"性别":
                                profile['sex']=temp1[j]
                        csdnInfo['profile']=profile

                    if i == u"下载":
                        download={}
                        temp1=temp[i]
                        for j in temp1:
                            if j == u"上传资源":
                                download['upload']=temp1[j]
                            if j == u'排名':
                                download['rankDownload']=temp1[j]
                            if j == u"积分":
                                download['scoreDownload']=temp1[j]
                        csdnInfo['download']=download

                    if i == u"博客":
                        blog={}
                        temp1=temp[i]
                        for j in temp1:
                            if j == u"排名":
                                blog['rankBlog']=temp1[j]
                            if j == u"访问量":
                                blog['visitvolume']=temp1[j]
                            if j == u"积分":
                                blog['scoreBlog']=temp1[j]
                        csdnInfo['blog']=blog

                    if i == u"论坛":
                        bbs={}
                        temp1=temp[i]
                        for j in temp1:
                            if j == u'可用分':
                                bbs['availableScore']=temp1[j]
                            if j == u'专家分':
                                bbs['expertScore']=temp1[j]
                            if j == u"所获勋章":
                                bbs['medal']=temp1[j]

                            if j == u"论坛分数详情":
                                bbsScoreDetail=[]
                                bbsScoreElement=temp1[j]
                                for k in bbsScoreElement:
                                    element={}
                                    categoryScore=k
                                    for m in categoryScore:
                                        if m == u"类别":
                                            element['category']=categoryScore[m]
                                        if m == u"分数":
                                            element['scorebbs']=categoryScore[m]
                                    bbsScoreDetail.append(element)
                                bbs['bbsScoreDetail']=bbsScoreDetail
                        post['bbs']=bbs
            write_location.insert(post)


