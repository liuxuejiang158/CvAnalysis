# coding=utf-8
__author__ = 'liuxuejiang'
import pymongo
from pymongo import MongoClient
import sys
import datetime
'''新浪数据：构建网络'''


def relationshipNetwork():#将微博关注列表整理为网络边数据:srcId dstID
    conn=MongoClient("192.168.4.249",27017)['SW_new_detail']['rel']
    conn_userInfo=MongoClient("192.168.4.249",27017)['SW_new_detail']['user_info']
    fd=open("relationshipNetwork.txt",'w')
    fd_1=open("relationship2.txt",'w')
    count=0
    for cv in conn.find(timeout=False):
        id_=""
        if count%1000==0:
            print(count)
            fd.flush()
        count+=1
        if "id" in cv:
            id_=cv["id"]
            if id_:
                ids=""
                if "ids" in cv:
                    ids=cv["ids"]
                    if ids:
                        flag=False
                        for key in ids:
                            temp=conn_userInfo.find_one({"id":key},{"id":1})
                            if temp:
                                fd.write(str(id_)+" "+str(key)+"\n")
                                if flag==False:
                                    fd_1.write(str(id_)+","+str(key))
                                    flag=True
                                else:
                                    fd_1.write(","+str(key))
                        if flag:
                            fd_1.write("\n")

def cal_behavior(collection):
    conn=MongoClient("192.168.4.249",27017)['shulianxunying']
    conn_userInfo=MongoClient("192.168.4.249",27017)['SW_new_detail']["user_info"]
    write_location=MongoClient("192.168.4.249",27017)['shulianxunying']['sina_dimension']
    count=0
    for cv in conn[collection].find(timeout=False):
        if count%1000==0:
            print(count)
        count+=1
        uid=cv['uid']
        #计算行为特征
        behavior=0.
        temp=conn_userInfo.find_one({"id":uid},{"statuses_count":1,"status":1,"verified":1})
        if temp:
            statuses_count=temp['statuses_count']
            start_year=0
            status=""
            if "status" in temp:
                status=temp["status"]
            created_at=""
            if status:
                if "created_at" in status:
                    created_at=status["created_at"]
                if created_at and created_at!="null":
                    dt=datetime.datetime.strptime(created_at,'%a %b %d %H:%M:%S +0800 %Y')
                    start_year=dt.year
                    length=0.
                    if(start_year<2014):
                        length=2014-start_year+1.0
                    else:
                        length=1.
                    behavior=statuses_count/length#微博条数/年
        verified=""
        if "verified" in temp:
            cv["verified"]=temp["verified"]
        cv['behavior']=behavior
        write_location.insert(cv)

#职业补全
def cal_job():
    conn_dimension=MongoClient("192.168.4.249",27017)['shulianxunying']['sina_dimension']
    #write_location=MongoClient("192.168.4.249",27017)['shulianxunying']['sinaDimension']
    count=0
    fd=open("log_job.txt",'w')
    total=0
    success=0
    for line in open("relationship2.txt"):
        line=line.strip()
        ids=line.split(",")
        srcId=ids[0]
        srcData=conn_dimension.find_one({"uid":long(srcId)},{"job":1})
        if count%500==0:
            fd.flush()
            print(count)
        fd.write(str(count)+"\n")
        count+=1
        if srcData:
            srcJob=srcData['job']
            if srcJob=="null":
                jobMap={}
                for i in range(1,len(ids)):
                    dstData=conn_dimension.find_one({"uid":long(ids[i])},{"job":1,"verified":1})
                    if dstData:
                        verified=""
                        if "verified" in dstData:
                            verified=dstData['verified']
                            if not verified:
                                job=dstData['job']
                                if job!="null":
                                    if job not in jobMap:
                                        jobMap[job]=1
                                    else:
                                        jobMap[job]+=1
                    else:
                        print("some error 2")
                if jobMap:
                    maxJob=sorted(jobMap.iteritems(), key=lambda d:d[1], reverse = True)
                    temp=maxJob[0][0]
                    #print(temp)
                    conn_dimension.update({"_id":srcData["_id"]},{"$set":{"job":temp}})
                    success+=1
            total+=1
        else:
            print("some error 1")
    print("job为空的数目=%d, 补全job成功的数目=%d" %(total,success))



#将新浪网络整理进数据库
def cal_relationship():
    conn=MongoClient("192.168.4.249",27017)['shulianxunying']['sina_rel']
    count=0
    for line in open("relationship2.txt"):
        if count%1000==0:
            print(count)
        count+=1
        post={}
        line=line.strip()
        ids=line.split(",")
        srcId=long(ids[0])
        post["id"]=srcId
        dstId=[]
        for i in range(1,len(ids)):
            dstId.append(long(ids[i]))
        post['ids']=dstId
        conn.insert(post)

#查看location的分布
def location_distribution():
    conn=MongoClient("192.168.4.249",27017)['shulianxunying']['sina_dimension']
    location=set()
    for cv in conn.find(timeout=False):
        location.add(cv["location"])
    fd=open("location.txt",'w')
    for key in location:
        fd.write(key.encode('utf-8')+"\n")
    fd.close()


#将职业、地点、行为特征、性格、ID、verified存入文件
def save():
    conn=MongoClient("192.168.4.249",27017)['shulianxunying']['sina_dimension']
    fd=open("sina_dimension.txt",'w')
    for cv in conn.find(timeout=False):
        uid=cv['uid']
        location=cv['location']
        job=cv['job']
        verified=cv['verified']
        behavior=cv['behavior']
        character=cv['character']
        fd.write(str(uid)+","+location.encode('utf-8')+","+job.encode("utf-8")+","+str(verified)+","+str(behavior)+","+character.encode("utf-8")+"\n")
    fd.close()

if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args:relationship(构建网络),cal_behavior(计算行为特征),cal_job(补全职业),cal_rel(网络数据库),cal_loc(补全地点),save(存文件)")
        sys.exit(1)
    elif sys.argv[1]=="relationship":
        print("relationshipNetwork")
        relationshipNetwork()
    elif sys.argv[1]=="cal_behavior":
        print("cal_behavior sina_dimension_0")
        cal_behavior("sina_dimension_0")
        print("cal_behavior sina_dimension_2")
        cal_behavior("sina_dimension_2")
        print("cal_behavior sina_dimension_4")
        cal_behavior("sina_dimension_4")
        print("cal_behavior sina_dimension_6")
        cal_behavior("sina_dimension_6")
        print("cal_behavior sina_dimension_8")
        cal_behavior("sina_dimension_8")
    elif sys.argv[1]=="cal_job":
        print("cal_job")
        cal_job()
    elif sys.argv[1]=="cal_rel":
        print("cal_relationship")
        cal_relationship()
    elif sys.argv[1]=="cal_loc":
        print("cal_loc")
        location_distribution()
    elif sys.argv[1]=="save":
        print("save")
        save()
