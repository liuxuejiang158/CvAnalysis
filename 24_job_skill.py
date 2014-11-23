# coding:utf-8
'''
    找出指定的24个职业的职业技能权重
'''
job=set()
for line in open("../data/24.txt"):
    line=line.strip()
    line=line.split("\t")
    print line[0]
    job.add(line[0])
print "............collect......."
fd=open("../data/24_job_skill.txt",'w')
for line in open("../data/skill_weight.txt",'r'):
    line_1=line.strip()
    line_2=line_1.split("\t")
    if line_2[0] in job:
        print line_2[0];
        fd.write(line)

