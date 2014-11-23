# coding:utf-8
__author__ ="liuxeujiang"
'''
http://baike.baidu.com/view/3407132.htm找出国家英文名称，首先去掉第一列简称后，再用本脚本将格式转换为:英文名称\t中文名称
'''
fd=open('country.txt','w')
for line in open('country','r'):
    line=line.strip()
    temp=line.split()
    num=len(temp)
    en=temp[0]
    for i in range(1,num-1):
        en+=' '+temp[i]
    fd.write(en+'\t'+temp[num-1]+'\n')
