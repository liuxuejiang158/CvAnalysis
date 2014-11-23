# coding:utf-8
import sys
import pymongo
from pymongo import MongoClient
'''
    找出各个职业的最大ability的前5名
job=set()
for line in open("../data/24.txt"):
    line=line.strip()
    line=line.split("\t")
    job.add(line[0])
    print line[0]
'''
def fun(collection,fileName):
    conn=MongoClient("192.168.4.249",27017)["shulianxunying"]
    fd=open(fileName,'w')
    for i in job:
        data=conn[collection].find({"job":i}).sort({"ability":-1}).limit(5)
        for cv in data:
            fd.write(data)

def job_max_ability(collection):
    conn=MongoClient("192.168.4.249",27017)["shulianxunying"]
    job=set()
    for cv in conn[collection].find(timeout=False):
        if "job" in cv:
            job.add(cv['job'])
    print("%s job num=%d"%(collection,len(job)))
    fd=open(collection+"_jobMaxAbility.txt",'w')
    for i in job:
        fd.write(i.encode('utf-8')+"\t")
        for cv in conn[collection].find({"job":i}).sort("ability",pymongo.DESCENDING).limit(5):
            for key in cv:
                if key=="ability":
                    fd.write("ability"+"\t"+str(cv[key])+"\t")
                if key=="character":
                    fd.write("character"+"\t"+cv[key].encode("utf-8")+"\t")
                #if key=="job":
                #    fd.write("job"+"\t"+cv[key].encode("utf-8")+"\t")
                if key=="uid":
                    fd.write("uid"+'\t'+str(cv[key]).encode("utf-8")+'\t')
                if key=="location":
                    fd.write("location"+"\t"+cv[key].encode("utf-8")+'\t')
        fd.write("\n")
    fd.close()

def pre_process(jobList):
    jobList=jobList.strip()
    jobList=jobList.replace('\\','，')
    jobList=jobList.replace('、','，')
    jobList=jobList.replace('/','，')
    jobList=jobList.replace(';','，')
    jobList=jobList.replace('；','，')
    jobList=jobList.replace(',','，')
    if not jobList.isalpha():
        jobList=jobList.replace(" ",'，')
    if '（' in jobList and '）' in jobList:
        index1=jobList.find('（')
        index2=jobList.find('）')
        jobList=jobList.replace(jobList[index1:index2+1],'')
    if '(' in jobList and ')' in jobList:
        index1=jobList.find('(')
        index2=jobList.find(')')
        jobList=jobList.replace(jobList[index1:index2+1],'')
    if '(' in jobList and '）' in jobList:
        index1=jobList.find('(')
        index2=jobList.find('）')
        jobList=jobList.replace(jobList[index1:index2+1],'')
    if "，" in jobList:
        jobList=jobList.split("，")
    else:
        jobList=[jobList]
    return jobList

jobAll={}
def parse_file(string):
    for line in open("../result/maxability/"+string):
        line=line.strip()
        temp=line.split("\t")
        jobList=pre_process(temp[0])
        if (len(temp)-1)%8!=0:
            print("len(temp)=%d"%(len(temp)))
        else:
            for i in range(1,len(temp),8):
                if temp[i]=="ability" and temp[i+2]=="character" and temp[i+4]=="location" and temp[i+6]=="uid":
                    ability=float(temp[i+1])
                    character=temp[i+3]
                    location=temp[i+5]
                    if "，" in location:
                        location=location.split("，")
                    elif location=="null":
                        location=[]
                    else:
                        location=[location]
                    temploc={}
                    for item in location:
                        if item not in temploc:
                            temploc[item]=1
                        else:
                            temploc[item]+=1
                    location=temploc
                    if character=="null":
                        character=[]
                    else:
                        character=[character]
                    for job in jobList:
                        if job not in jobAll:
                            jobAll[job]={"ability":ability,"character":character,"location":location}
                        else:
                            jobAll[job]['ability']=max(jobAll[job]['ability'],ability)
                            jobAll[job]['character'].extend(character)
                            for loc in location:
                                if loc in jobAll[job]['location']:
                                    jobAll[job]['location'][loc]+=1
                                else:
                                    jobAll[job]['location'][loc]=1


def merge():
    print("qcwy51_dimension_jobMaxAbility")
    parse_file('qcwy51_dimension_jobMaxAbility.txt')
    print("yingcai_dimension_jobMaxAbility")
    parse_file('yingcai_dimension_jobMaxAbility.txt')
    print("zhilian_max_ability")
    parse_file('zhilian_dimension_jobMaxAbility.txt')
    print("yingcai2_maxability")
    parse_file('yingcai2_dimension_jobMaxAbility.txt')
    print("len(jobAll)=%d"%(len(jobAll)))
    fd=open("../result/maxability/maxability_result.txt",'w')
    for job in jobAll:
        character=jobAll[job]['character']
        location=jobAll[job]['location']
        charSet=set(character)
        tempChar="null"
        count=0
        for char in charSet:
            temp=character.count(char)
            if temp>count:
                tempChar=char
                count=temp
        tempLoc="null"
        count=0
        for loc in location:
            temp=location[loc]
            if temp>count:
                tempLoc=loc
                count=temp
        fd.write(job+"\t"+str(jobAll[job]['ability'])+"\t"+tempChar+"\t"+tempLoc+"\n")
    fd.close()
    #PRINT(jobAll)

def stat():
    jobAll={}
    for line in open("../result/maxability/yingcai_dimension_jobMaxAbility.txt"):
        line=line.strip()
        job=line.split("\t")[0]
        if job not in jobAll:
            jobAll[job]=1
        else:
            jobAll[job]+=1
    for line in open("../result/maxability/yingcai2_dimension_jobMaxAbility.txt"):
        line=line.strip()
        job=line.split("\t")[0]
        if job not in jobAll:
            jobAll[job]=1
        else:
            jobAll[job]+=1
    for line in open("../result/maxability/qcwy51_dimension_jobMaxAbility.txt"):
        line=line.strip()
        job=line.split("\t")[0]
        if job not in jobAll:
            jobAll[job]=1
        else:
            jobAll[job]+=1
    for line in open("../result/maxability/zhilian_dimension_jobMaxAbility.txt"):
        line=line.strip()
        job=line.split("\t")[0]
        if job not in jobAll:
            jobAll[job]=1
        else:
            jobAll[job]+=1
    jobFreq= sorted(jobAll.iteritems(), key=lambda d:d[1], reverse = True)
    fd=open("../result/maxability/jobFreq.txt",'w')
    for job in jobFreq:
        fd.write(job[0]+"\t"+str(job[1])+'\n')

jobCount={}
def help_stat_2(string):
    for line in open("../result/maxability/"+string):
        line=line.strip()
        job=line.split("\t")[0]
        for item in jobCount:
            if item in job:
                jobCount[item]+=1
def stat_2():
    print("maxability_result")
    for line in open("../result/maxability/maxability_result.txt"):
        line=line.strip()
        temp=line.split("\t")[0]
        if temp not in jobCount:
            jobCount[temp]=0
    print("len(jobCount)=%d"%len(jobCount))
    print("qcwy51_dimension_jobMaxAbility")
    help_stat_2("qcwy51_dimension_jobMaxAbility.txt")
    print("yingcai_dimension_jobMaxAbility")
    help_stat_2("yingcai_dimension_jobMaxAbility.txt")
    print("yingcai2_dimension_jobMaxAbility")
    help_stat_2("yingcai2_dimension_jobMaxAbility.txt")
    print("zhilian_dimension_jobMaxAbility")
    help_stat_2("zhilian_dimension_jobMaxAbility.txt")
    print("write")
    fd=open("../result/maxability/new_job_freq.txt",'w')
    jobFreq= sorted(jobCount.iteritems(), key=lambda d:d[1], reverse = True)
    for item in jobFreq:
        fd.write(item[0]+"\t"+str(item[1])+"\n")
    fd.close()


def PRINT(temp):
    count=0
    for key in temp:
        print key,
        print temp[key]
        if count==10:
            break
        count+=1



if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args:24_job(找出24个职业最大的ability),job_max_ability(找出每个职业的标杆人物),merge(整理结果),stat(统计职业次数)")
    elif sys.argv[1]=="24_job":
        print("yingcai start")
        fun("yingcai_dimension","../yingcai_max_ability.txt")
        print("zhilian start")
        fun("zhilian_dimension","../zhilian_max_ability.txt")
        print("51job start")
        fun("51job_dimension","../51job_max_ability.txt")
    elif sys.argv[1]=="job_max_ability":
        print("找出各个职业ability最大的前五个标杆人物")
        #print("yingcai_dimension")
        #job_max_ability("yingcai_dimension")
        print("yingcai2_dimension")
        job_max_ability("yingcai2_dimension")
        #print("zhilian_dimension")
        #job_max_ability("zhilian_dimension")
        #print("qcwy51_dimension")
        #job_max_ability("qcwy51_dimension")
    elif sys.argv[1]=="merge":
        merge()
    elif sys.argv[1]=="stat":
        #stat()
        stat_2()

