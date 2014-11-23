# coding:utf-8
import jieba
import random
from pymongo import MongoClient
import sys
'''
    云图算法相关
'''
conn=MongoClient("192.168.4.249",27017)
skill=set()
for line in open("../data/cloud/skill.txt"):
    line=line.strip()
    skill.add(line)
Character_lexicon=set()
for line in open("../data/cloud/Character_lexicon.txt"):
    line=line.strip()
    Character_lexicon.add(line)
job_skill_weight={}
for line in open("../data/cloud/job_skill_weight.txt"):
    line=line.strip()
    temp=line.split("\t")
    if temp[0] not in job_skill_weight:
        job_skill_weight[temp[0]]={temp[1]:temp[2]}
    else:
        job_skill_weight[temp[0]][temp[1]]=temp[2]

def yingcai():
    print("len(skill)=%d,len(Character_lexicon)=%d,len(job_skill_weight)=%d"%(len(skill),len(Character_lexicon),len(job_skill_weight)))
    num=conn['yingcai']['yingcai_userinfo'].count()
    index=random.randint(0,num)%100000
    print("index=%d"%index)
    data=conn['yingcai']['yingcai_userinfo'].find({"self_eval":{"$nin":[None]}}).limit(100000)
    data=data[index]
    if data:
        string=""
        if 't_pre_skill' in data:
            if data['t_pre_skill']:
                string+=data['t_pre_skill']+","
        if "f_pre_skill" in data:
            if data['f_pre_skill']:
                string+=data['f_pre_skill']+","
        if "education" in data:
            education=data["education"]
            if education:
                for edu in education:
                    if "major" in edu:
                        major=edu['major']
                        if major:
                            string+=major+","
        self_eval=""
        if "self_eval" in data:
            if data['self_eval']:
                self_eval=data['self_eval']
                string+=data['self_eval']+","
        if "training" in data:
            training=data['training']
        if "experience" in data:
            experience=data['experience']
            if experience:
                for exp in experience:
                    if "work_desc" in exp:
                        if exp['work_desc']:
                            string+=exp['work_desc']+","
        if "s_pre_skill" in data:
            if data['s_pre_skill']:
                string+=data['s_pre_skill']
        print("string=%s"%string)
        '''
        words=jieba.cut(string)
        intersectionSkills=set()
        for w in words:
            if w in skill:
                intersectionSkills.add(w)
        print("intersectionSkills.num=%d"%len(intersectionSkills))
        '''
        dimension=conn['shulianxunying']['yingcai_dimension'].find_one({"uid":data['_id']})
        fd_skill=open(str(data['_id'])+"_skill.txt",'w')
        if dimension:
            '''
            job=dimension['job']
            if job in job_skill_weight:
                for i in intersectionSkills:
                    if i in job_skill_weight[job]:
                        fd_skill.write(i+"\t"+str(job_skill_weight[job][i])+"\n")
                    else:
                        print("skill not found in job_skill_weight")
                        fd_skill.write(i+"\t"+str(0.2)+"\n")
            else:
                for i in intersectionSkills:
                    fd_skill.write(i+"\t"+str(0.2)+"\n")
                print("job from dimension not in job_skill_weight")
            '''
            for i in skill:
                if i in string.encode("utf-8"):
                    job=dimension['job']
                    if job in job_skill_weight:
                        if i in job_skill_weight[job]:
                            print("skill (%s) in job_skill_weight"%i)
                            fd_skill.write(i+"\t"+str(job_skill_weight[job][i])+'\n')
                        else:
                            print("skill (%s) not in job_skill_weight,but job in"%i)
                            fd_skill.write(i+"\t"+str(0.2)+"\n")
                    else:
                        print("skill (%s) not in job_skill_weight,and job not in"%i)
                        fd_skill.write(i+"\t"+str(0.2)+"\n")
        fd_char=open(str(data["_id"])+"_character.txt",'w')
        print("self_eval=%s"%self_eval)
        for char in Character_lexicon:
            if char in self_eval.encode("utf-8"):
                print("find char=%s"%char)
                fd_char.write(char+"\t"+str(0.2)+"\n")
        '''
        words=jieba.cut(self_eval)
        for w in words:
            if w in Character_lexicon:
                print("char=%s"%w)
                fd_char.write(w+"\t"+str(0.2)+"\n")
        '''
    else:
        print("not found _id")

yingcai()
