# coding=utf-8
import sys
import os
if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Input args:resultFileName(整理结果文件)\n,intersect(合并结果)、inputPath(part-0000*的路径/)、resultFile(结果文件)、oldFile(上次迭代结果)\n")
        print("check(新结果与旧结果比较)、newFile(新结果)、oldFile(上次迭代结果)\n")
        print("tidy_location(整理地区)、resultFile(结果文件)、inputFile(输入文件)\n")
        print("check_loc(检查地点为空数目),inputFile(输入文件)")
        print("locIntersect(合并地点)、inputPath(part-0000*路径/)、resultFile(输出文件)、oldFile(上次迭代结果)")
        print("friends(加入好友质量)、inputPath(part-0000*路径/)、resultFile(输出文件)、oldFile(上次迭代结果)")
        print("prePageRank(拆分网络同职业子网络)、vertexFile(定点数据)、edgeFile(网络数据)")
        print("behavior(统计behavior的分布情况)、inputFile(输入文件)")
        print("pageRankInter(将pageRank添加到维度中)、inputPath(各职业网络pageRank结果目录/)、resultFile(输出结果文件sina_dimension_final.txt)、oldFile(上次迭代结果sina_dimension_friends.txt)")
        sys.exit(1)
    elif sys.argv[1]=="intersect":
        job={}
        print("part-00000")
        for line in open(sys.argv[2]+"part-00000",'r'):#
            line=line.strip()
            line=line.split("(")[1]
            line=line.split(",")
            x=line[0]
            y=line[1].split(")")[0]
            if x in job:
                print("some error")
            else:
                job[x]=y
        print("part-00001")
        for line in open(sys.argv[2]+"part-00001",'r'):#
            line=line.strip()
            line=line.split("(")[1]
            line=line.split(",")
            x=line[0]
            y=line[1].split(")")[0]
            if x in job:
                print("some error")
            else:
                job[x]=y
        print("intersect")
        fd=open("../data/"+sys.argv[3],'w')#
        for line in open(sys.argv[4],'r'):
            line=line.strip()
            line=line.split(",")
            if len(line)!=6:
                print("some error")
            if line[0] in job:
                line[2]=job[line[0]]
            fd.write(line[0]+","+line[1]+","+line[2]+","+line[3]+","+line[4]+","+line[5]+"\n")
    elif sys.argv[1]=="check":
        count=0
        job={}
        for line in open(sys.argv[3],'r'):#旧结果
            line=line.strip()
            fields=line.split(",")
            if fields[0] not in job:
                job[fields[0]]=fields[2]
            else:
                count+=1
                print("重复ID",fields[0])
        print("重复ID=%d" %count)
        count=0
        total=0
        for line in open(sys.argv[2],'r'):#新结果
            line=line.strip()
            fields=line.split(",")
            if len(fields)!=6:
                print("bad sina_dimension data %d" %(count))
            if fields[2]!="null":
                count+=1
            if fields[2]!=job[fields[0]]:
                if job[fields[0]]!="null" and fields[2]=="null":
                    print("补没了%s"%fields[0])
            total+=1
        print("total=%d,job is not null=%d" %(total,count))
    elif sys.argv[1]=="tidy_location":
        province={u"河北":u"石家庄",u"山西":u"太原",u"内蒙古":u"呼和浩特",u"辽宁":u"沈阳",u"吉林":u"长春",
                  u"黑龙江":u"哈尔滨",u"江苏":u"南京",u"浙江":u"杭州",u"安徽":u"合肥",u"福建":u"福州",
                  u"江西":u"南昌",u"山东":u"济南",u"河南":u"郑州",u"湖北":u"武汉",u"湖南":u"长沙",
                  u"广东":u"广州",u"广西":u"南宁",u"海南":u"海口",u"四川":u"成都",u"贵州":u"贵阳",u"云南":u"昆明",
                  u"西藏":u"拉萨",u"陕西":u"西安",u"甘肃":u"兰州",u"青海":u"西宁",u"宁夏":u"银川",u"新疆":u"乌鲁木齐"}
        city=["北京","上海","天津","重庆","台湾","香港","澳门"]
        fd=open(sys.argv[2],'w')
        count=0
        for line in open(sys.argv[3],'r'):
            line=line.strip()
            fields=line.split(",")
            location=fields[1].decode("utf-8")
            if location==u"其他":
                location="null"
            elif u"北京" in location:
                location=u"北京"
            elif u"上海" in location:
                location=u"上海"
            elif u"天津" in location:
                location=u"天津"
            elif u"重庆" in location:
                location=u"重庆"
            elif u"台湾" in location:
                location=u"台湾"
            elif u"香港" in location:
                location=u"香港"
            elif u"澳门" in location:
                location=u"澳门"
            elif u"海外" in location:
                location=u"海外"
            elif " " in location:
                temp=location.split(" ")
                if u"其他" in location:
                    location=province[temp[0]]
                else:
                    location=temp[1]
            else:
                if location in province:
                    location=province[location]
                else:
                    print("%d,%s" %(count,location.encode("utf-8")))
            #print("%d,%s,%s" %(count,fields[1],location.encode('utf-8')))
            fd.write(fields[0]+","+location.encode("utf-8")+","+fields[2]+","+fields[3]+","+fields[4]+","+fields[5]+"\n")
            count+=1
    elif sys.argv[1]=="check_loc":
        count=0
        for line in open(sys.argv[2],'r'):
            line=line.strip()
            fields=line.split(",")
            if fields[1]=="null":
                count+=1
        print("loc is null=%d" %count)
    elif sys.argv[1]=="locIntersect":
        loc={}
        count=0
        print("part-00000")
        for line in open(sys.argv[2]+"part-00000",'r'):#
            line=line.strip()
            line=line.split("(")[1]
            line=line.split(",")
            x=line[0]
            y=line[1].split(")")[0]
            if x in loc:
                print("some error")
            else:
                loc[x]=y
            count+=1
        print("part-00001")
        for line in open(sys.argv[2]+"part-00001",'r'):#
            line=line.strip()
            line=line.split("(")[1]
            line=line.split(",")
            x=line[0]
            y=line[1].split(")")[0]
            if x in loc:
                print("some error")
            else:
                loc[x]=y
            count+=1
        print("intersect %d" %count)
        fd=open("../data/"+sys.argv[3],'w')#
        for line in open(sys.argv[4],'r'):
            line=line.strip()
            line=line.split(",")
            if len(line)!=6:
                print("some error")
            if line[0] in loc:
                line[1]=loc[line[0]]
                if line[1]!="null" and loc[line[0]]=="null":
                    print("补没了")
            fd.write(line[0]+","+line[1]+","+line[2]+","+line[3]+","+line[4]+","+line[5]+"\n")
    elif sys.argv[1]=="friends":
        friends={}
        count=0
        print("part-00000")
        for line in open(sys.argv[2]+"part-00000",'r'):#
            line=line.strip()
            line=line.split("(")[1]
            line=line.split(",")
            x=line[0]
            y=line[1].split(")")[0]
            if x in friends:
                print("some error 1")
            else:
                friends[x]=y
            count+=1
        print("part-00001")
        for line in open(sys.argv[2]+"part-00001",'r'):#
            line=line.strip()
            line=line.split("(")[1]
            line=line.split(",")
            x=line[0]
            y=line[1].split(")")[0]
            if x in friends:
                print("some error 2")
            else:
                friends[x]=y
            count+=1
        print("intersect %d" %count)
        fd=open(sys.argv[3],'w')
        for line in open(sys.argv[4],'r'):
            line=line.strip()
            line=line.split(",")
            fri=0.
            if len(line)!=6:
                print("some error 3")
            if line[0] in friends:
                fri=friends[line[0]]
            #id,location,job,verified,behavior,character,friends
            fd.write(line[0]+","+line[1]+","+line[2]+","+line[3]+","+line[4]+","+line[5]+","+str(fri)+"\n")
    elif sys.argv[1]=="prePageRank":
        job={}
        for line in open(sys.argv[2]):#提取顶点数据中的ID和job
            line=line.strip()
            line=line.split(",")
            if len(line)!=7:
                print("some error 1")
            if line[0] not in job:
                job[line[0]]=line[2]
            else:
                print("duplicate ID %s" %line[0])
        jobId={}
        count=0
        for key in job:#每个job对应一个编号
            if job[key] not in jobId:
                jobId[job[key]]=count
                count+=1
        fd_1=open("jobId.txt",'w')
        for key in jobId:
            fd_1.write(key+"\t"+str(jobId[key])+"\n")
        fd_1.close()
        existFile=set()
        print("开始构建各职业网络")
        count=0
        for line in open(sys.argv[3]):#边
            line=line.strip()
            srcId,dstId=line.split(" ")
            if srcId in job:
                srcJob=job[srcId]
                if dstId in job:
                    dstJob=job[dstId]
                    if srcJob==dstJob:#若边上两个顶点的job相同就将其写入同一文件
                        if srcJob in existFile:
                            fd=open("./job/"+str(jobId[srcJob])+".txt",'a')
                            fd.write(srcId+" "+dstId+"\n")
                            fd.close()
                        else:
                            fd=open("./job/"+str(jobId[srcJob])+".txt",'w')
                            fd.write(srcId+" "+dstId+"\n")
                            existFile.add(srcJob)
                            fd.close()
                else:
                    print("%s #%s" %(srcId,dstId))
                    count+=1
            else:
                print("#%s %s" %(srcId,dstId))
                count+=1
        print("边中共有%d个顶点在顶点数据中未找到" %count)
    elif sys.argv[1]=="pageRankInter":
        pageRank={}
        count=0
        for lists in os.listdir(sys.argv[2]):
            path=os.path.join(sys.argv[2],lists)
            path=os.path.join(path,"part-00000")
            print path
            for line in open(path):
                line=line.strip()
                line=line.split("(")[1]
                line=line.split(",")
                x=line[0]
                y=line[1].split(")")[0]
                if x in pageRank:
                    print("some error")
                else:
                    pageRank[x]=y
                count+=1
        print("pageRank num=%d" %count)
        fd=open(sys.argv[3],'w')
        count=0
        for line in open(sys.argv[4]):
            line=line.strip()
            fields=line.split(",")#id,location,job,verified,behavior,character,friends
            if len(fields)!=7:
                print("row %d error" %count)
            PRvalue=0.
            if fields[0] in pageRank:
                PRvalue=pageRank[fields[0]]
            behavior=""
            behaviorValue=float(fields[4])
            if behaviorValue>=10000:
                behavior="微博达人"
            elif behaviorValue>=1000:
                behavior="微博控"
            elif behavior>=100:
                behavior="微博常客"
            elif behaviorValue>=10:
                behavior="微博稀客"
            elif behaviorValue>0:
                behaviorValue="微博懒人"
            else:
                behavior="微博僵尸"
            #id,location,job,behavior,character,friends,ability
            fd.write(fields[0]+","+fields[1]+","+fields[2]+","+behavior+","+fields[5]+","+fields[6]+","+str(PRvalue)+"\n")
    elif sys.argv[1]=="final":
        friends={}
        count=0
        print("part-00000")
        for line in open("./friends/part-00000",'r'):#
            line=line.strip()
            line=line.split("(")[1]
            line=line.split(",")
            x=line[0]
            y=line[1].split(")")[0]
            if x in friends:
                print("some error 1")
            else:
                friends[x]=y
            count+=1
        print("part-00001")
        for line in open("./friends/part-00001",'r'):#
            line=line.strip()
            line=line.split("(")[1]
            line=line.split(",")
            x=line[0]
            y=line[1].split(")")[0]
            if x in friends:
                print("some error 2")
            else:
                friends[x]=y
            count+=1
        print("intersect %d" %count)
        fd=open("sina_dimension_final_1.txt",'w')
        count=0
        for line in open("./sina_dimension_final.txt",'r'):
            line=line.strip()
            line=line.split(",")
            fri=0.
            if len(line)!=7:
                print("some error 3")
            if line[0] in friends:
                fri=friends[line[0]]
            if line[6]>0:
                count+=1
            #id,location,job,behavior,character,friends,pagerank
            fd.write(line[0]+","+line[1]+","+line[2]+","+line[3]+","+line[4]+","+str(fri)+","+line[6]+"\n")
        print("pageRank>0=%d" %count)
