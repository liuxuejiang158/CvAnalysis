# coding=utf-8
import sys
__author__ = "liuxuejiang"
'''
    将职业技能词库skill_weight.txt中的职业job提取出来作为jieba分词的自定义词库
'''
def fun():
    jobSet=set()
    for line in open("../data/occupation_category.txt"):
        line=line.strip()
        jobSet.add(line)
    fd=open("../data/user_dict.txt",'w')
    for key in jobSet:
        if " " not in key:
            fd.write(key+" "+str(3)+" "+"nz"+'\n')

if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args:user_dict")
        sys.exit(1)
    elif sys.argv[1]=="user_dict":
        print("生成用户自定义词典")
        fun()
