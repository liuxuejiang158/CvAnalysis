# coding=utf-8
__author__ ="liuxuejiang"
from pymongo import MongoClient
import sys
'''
    针对csdn_dimension,google_scholar_dimension,sina_dimension进行每个职业job下行为特征behavior频数统计
    针对sina_dimension,yingcai_dimension,zhilian_dimension进行每个职业job下性格character频数统计
'''
IP="192.168.4.249"
conn=MongoClient(IP,27017)['shulianxunying']

#统计每个职业下频数最多的行为特征
def stat_behavior(collection,resultFile):
    job_bev={}
    fd=open(resultFile,'w')
    for cv in conn[collection].find(timeout=False):
        job=cv['job']
        if job!="null":
            behavior=cv['behavior']
            if behavior!="null":
                if job not in job_bev:
                    job_bev[job]={behavior:1}
                else:
                    if behavior not in job_bev[job]:
                        job_bev[job][behavior]=1
                    else:
                        job_bev[job][behavior]=job_bev[job][behavior]+1
    for job in job_bev:
        behavior=max(job_bev[job].items(),key=lambda x:x[1])[0]
        fd.write(job.encode("utf-8")+"\t"+behavior.encode("utf-8")+"\n")
    print("job num=%d" %len(job_bev))
    fd.close()

#统计每个职业下的出现次数最多的性格
def stat_character():
    job_char={}
    #sina
    print("sina character")
    for cv in conn['sina_dimension'].find(timeout=False):
        job=cv['job']
        if job!="null":
            character=cv['character']
            if character!="null":
                if job not in job_char:
                    job_char[job]={character:1}
                else:
                    if character not in job_char[job]:
                        job_char[job][character]=1
                    else:
                        job_char[job][character]=job_char[job][character]+1
    #yingcai
    print("yingcai character")
    for cv in conn['yingcai_dimension'].find(timeout=False):
        job=cv['job']
        if job!="null":
            character=cv['character']
            if character!="null":
                if job not in job_char:
                    job_char[job]={character:1}
                else:
                    if character not in job_char[job]:
                        job_char[job][character]=1
                    else:
                        job_char[job][character]=job_char[job][character]+1
    #zhilian
    print("zhilian character")
    for cv in conn['zhilian_dimension'].find(timeout=False):
        job=cv['job']
        if job!="null":
            character=cv['character']
            if character!="null":
                if job not in job_char:
                    job_char[job]={character:1}
                else:
                    if character not in job_char[job]:
                        job_char[job][character]=1
                    else:
                        job_char[job][character]=job_char[job][character]+1
    fd=open("job_character.txt",'w')
    for job in job_char:
        character=max(job_char[job].items(),key=lambda x:x[1])[0]
        fd.write(job.encode("utf-8")+"\t"+character.encode("utf-8")+"\n")
    print("job num=%d" %len(job_char))
    fd.close()

if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args: stat(统计每个职业下频数最多的性格和行为特征)")
        sys.exit(1)
    else:
        '''
        print("sina behavior")
        stat_behavior("sina_dimension","sina_behavior.txt")
        print("google_scholar behavior")
        stat_behavior("google_scholar_dimension","google_scholar_behavior.txt")
        print("csdn behavior")
        stat_behavior("csdn_dimension","csdn_behavior.txt")
        '''
        print("wanfang behavior")
        stat_behavior("wanfang_dimension","wanfang_behavior.txt")
        #print("character")
        #stat_character()
