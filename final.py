# coding=utf-8
fd=open("sina_dimension_final.txt","w")
count=0
for line in open("sina_dimension_final_1.txt"):
    line=line.strip()
    fields=line.split(",")
    if len(fields)!=8:
        print("%d rows error" %count)
    fd.write(fields[0]+","+fields[1]+","+fields[2]+","+fields[3]+","+fields[4]+","+fields[6]+","+fields[7]+"\n")
    count+=1
