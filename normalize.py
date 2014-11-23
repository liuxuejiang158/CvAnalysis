# coding:utf-8
import sys
import os
job_word={}#{job:{word:weight,word2:weight2,...},...}
def read_job_word():
    #从文件中读取24个职业的词语权重
    rootDir="../data/Standard_keyword24/"
    for lists in os.listdir(rootDir):
        path = os.path.join(rootDir,lists)
        if not os.path.isdir(path):
            job=lists.split("_")[0]
            for line in open(path):
                line=line.strip()
                word,weight=line.split("\t")
                if job not in job_word:
                    job_word[job]={word:float(weight)}
                else:
                    job_word[job][word]=float(weight)
    #归一化
    for job in job_word:
        sum=0.
        for word in job_word[job]:
            sum+=job_word[job][word]
        temp=0.
        for word in job_word[job]:
            job_word[job][word]=job_word[job][word]/sum
            temp+=job_word[job][word]
        print("%s,%f"%(job,temp))

read_job_word()
