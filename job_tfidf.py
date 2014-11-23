# coding:utf-8
__author__ = "liuxuejiang"
import jieba
import jieba.posseg as pseg
import os
import sys
import math
'''
  use jieba cut job' demands words,calculate these words' tfidf
'''
def new_tf_idf(power_num,thresh):#对idf平方改进tf-idf计算公式
    corpus=[]#every job'词语是一个list,[{word:num,...},...]
    name=[]#job顺序
    wordBag=set()#词库
    total=[]#每个文档的词总数
    print('collect word start')
    for line in open("../data/24.txt",'r'):
        line=line.strip()
        line=line.split("\t")
        name.append(line[0])
        word=pseg.cut(line[1])
        temp={}
        for w in word:
            if w.flag[0]!="x" and w.flag[0]!="m":
                if w.word not in temp:
                    temp[w.word]=1
                else:
                    temp[w.word]=temp[w.word]+1
        corpus.append(temp)
    times=int(thresh)
    #过滤掉频数小于指定阈值的词语
    '''
    for doc in corpus:
        for key in doc.keys():#迭代器做一份拷贝
            if doc[key]<=times:
                doc.pop(key)
    '''
    #统计每个文档的词数
    for doc in corpus:
        count=0.
        for key in doc:
            count+=doc[key]
            wordBag.add(key)
        total.append(count)
    #统计每个词在多少文档中出现
    word_doc={}#{词语:出现的文档数目,...}
    for word in wordBag:
        count=0.
        for doc in corpus:
            if word in doc:
                count+=1
        word_doc[word]=count
    #计算new_tf_idf
    num=0
    print '文档名 词语 文档数 文档词数 tf idf tfidf'
    for doc in corpus:#计算每个文档中词语的权重
        #fd=open("./new_tf_idf/"+str(num)+"_newTfidf.txt",'w')
        #print ("file::%s,word num::%d,unique word num::%d"%(name[num],total[num],len(doc)))
        for word in doc:
            tf=doc[word]/total[num]
            idf=math.pow(math.log(len(name)/word_doc[word]),float(power_num))#idf改进的地方
            tf_idf=tf*idf
            #print name[num],word,doc[word],total[num],tf,idf,tf_idf
            doc[word]=tf_idf
            #if tf_idf>0.0:
                #fd.write(word.encode("utf-8")+'\t'+str(tf_idf)+'\n')
        #fd.close()
        num+=1
    #sorted tf-idf
    num=0
    for doc in corpus:
        temp= sorted(doc.iteritems(), key=lambda d:d[1], reverse = True)
        fd=open("../result/new_tf_idf/"+str(name[num])+"_newTfidf_2.txt",'w')
        for word in temp:
            fd.write(word[0].encode("utf-8")+"\t"+str(word[1])+"\n")
        fd.close()
        num+=1

if __name__ == "__main__":
    print "compute new tf-idf"
    if len(sys.argv)<3:
        print "Input args: new_tf_idf,idf power num,thresh(word frequency)"
        sys.exit(1)
    new_tf_idf(sys.argv[2],sys.argv[3])#第一个参数是分词文件的输入目录，第二个结果输出目录,第三个参数是idf的指数
'''
POS = {#词性
	"n": {  #1.	名词  (1个一类，7个二类，5个三类)
		"n":"名词",
		"nr":"人名",
		"nr1":"汉语姓氏",
		"nr2":"汉语名字",
		"nrj":"日语人名",
		"nrf":"音译人名",
		"ns":"地名",
		"nsf":"音译地名",
		"nt":"机构团体名",
		"nz":"其它专名",
		"nl":"名词性惯用语",
		"ng":"名词性语素"
	},
	"t": {  #2.	时间词(1个一类，1个二类)
		"t":"时间词",
		"tg":"时间词性语素"
	},
	"s": {  #3.	处所词(1个一类)
		"s":"处所词"
	},
	"f": {  #4.	方位词(1个一类)
		"f":"方位词"
	},
	"v": {  #5.	动词(1个一类，9个二类)
		"v":"动词",
		"vd":"副动词",
		"vn":"名动词",
		"vshi":"动词“是”",
		"vyou":"动词“有”",
		"vf":"趋向动词",
		"vx":"形式动词",
		"vi":"不及物动词（内动词）",
		"vl":"动词性惯用语",
		"vg":"动词性语素"
	},
	"a": {  #6.	形容词(1个一类，4个二类)
		"a":"形容词",
		"ad":"副形词",
		"an":"名形词",
		"ag":"形容词性语素",
		"al":"形容词性惯用语"
	},
	"b": {  #7.	区别词(1个一类，2个二类)
		"b":"区别词",
		"bl":"区别词性惯用语"
	},
	"z": {  #8.	状态词(1个一类)
		"z":"状态词"
	},
	"r": {  #9.	代词(1个一类，4个二类，6个三类)
		"r":"代词",
		"rr":"人称代词",
		"rz":"指示代词",
		"rzt":"时间指示代词",
		"rzs":"处所指示代词",
		"rzv":"谓词性指示代词",
		"ry":"疑问代词",
		"ryt":"时间疑问代词",
		"rys":"处所疑问代词",
		"ryv":"谓词性疑问代词",
		"rg":"代词性语素"
	},
	"m": {  #10.	数词(1个一类，1个二类)
		"m":"数词",
		"mq":"数量词"
	},
	"q": {  #11.	量词(1个一类，2个二类)
		"q":"量词",
		"qv":"动量词",
		"qt":"时量词"
	},
	"d": {  #12.	副词(1个一类)
		"d":"副词"
	},
	"p": {  #13.	介词(1个一类，2个二类)
		"p":"介词",
		"pba":"介词“把”",
		"pbei":"介词“被”"
	},
	"c": {  #14.	连词(1个一类，1个二类)
		"c":"连词",
		"cc":"并列连词"
	},
	"u": {  #15.	助词(1个一类，15个二类)
		"u":"助词",
		"uzhe":"着",
		"ule":"了 喽",
		"uguo":"过",
		"ude1":"的 底",
		"ude2":"地",
		"ude3":"得",
		"usuo":"所",
		"udeng":"等 等等 云云",
		"uyy":"一样 一般 似的 般",
		"udh":"的话",
		"uls":"来讲 来说 而言 说来",
		"uzhi":"之",
		"ulian":"连 " #（“连小学生都会”）
	},
	"e": {  #16.	叹词(1个一类)
		"e":"叹词"
	},
	"y": {  #17.	语气词(1个一类)
		"y":"语气词(delete yg)"
	},
	"o": {  #18.	拟声词(1个一类)
		"o":"拟声词"
	},
	"h": {  #19.	前缀(1个一类)
		"h":"前缀"
	},
	"k": {  #20.	后缀(1个一类)
		"k":"后缀"
	},
	"x": {  #21.	字符串(1个一类，2个二类)
		"x":"字符串",
		"xx":"非语素字",
		"xu":"网址URL",
		"xm":"Unknown",
		"xs":"Unknown"
	},
	"w":{   #22.	标点符号(1个一类，16个二类)
		"w":"标点符号",
		"wkz":"左括号", 	#（ 〔  ［  ｛  《 【  〖 〈   半角：( [ { <
		"wky":"右括号", 	#） 〕  ］ ｝ 》  】 〗 〉 半角： ) ] { >
		"wyz":"全角左引号", 	#“ ‘ 『
		"wyy":"全角右引号", 	#” ’ 』
		"wj":"全角句号",	#。
		"ww":"问号",	#全角：？ 半角：?
		"wt":"叹号",	#全角：！ 半角：!
		"wd":"逗号",	#全角：， 半角：,
		"wf":"分号",	#全角：； 半角： ;
		"wn":"顿号",	#全角：、
		"wm":"冒号",	#全角：： 半角： :
		"ws":"省略号",	#全角：……  …
		"wp":"破折号",	#全角：——   －－   ——－   半角：---  ----
		"wb":"百分号千分号",	#全角：％ ‰   半角：%
		"wh":"单位符号"	#全角：￥ ＄ ￡  °  ℃  半角：$
	}
}
'''
