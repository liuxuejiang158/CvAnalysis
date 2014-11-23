# coding:utf-8
__author__ = "liuxuejiang"
import jieba
import jieba.posseg as pseg
import os
import sys
from sklearn import feature_extraction
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
import math
import re
'''
    采用jieba分词对每种性格下的微博文本进行分词写入不同的文件
    计算每种性格下微博文本中词语的tf-idf权重
'''
def cut_word(resultDir):#针对16种性格的微博文本进行分词
    #jieba.load_userdict("../data/userdict.txt")
    rootDir="../result/weibo/data"
    lexical=['x','w']#字符串 符号
    for lists in os.listdir(rootDir):
        path=os.path.join(rootDir,lists)
        print path
        if not os.path.isdir(path):
            print("cut text:",path)
            resFd=open(resultDir+lists.split('.')[0]+"_cut.txt",'w')
            for line in open(path,'r'):
                line=line.strip()
                line=re.sub('//@.*?:','',line)#过滤转发@
                line=re.sub('@.*?(\s|。)','',line)#过滤@
                line=re.sub('#.*?#','',line)#过滤话题
                line=re.sub('转发微博','',line)
                line=re.sub('微信.*?。','',line)
                line=re.sub('&.*?;','',line)
                #过滤网址
                line=re.sub('(http|ftp|https):\/\/[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?','',line)
                if '@' in line:
                    print line
                words=pseg.cut(line)
                for w in words:
                    if w.flag[0] not in lexical:
                        #print w.word
                        if line.count(w.word.encode('utf-8'))<1/3.*len(line):
                            resFd.write(w.word.encode('utf-8')+'\t'+w.flag+'\n')
            resFd.close()

def tf_idf():#计算每种性格下微博文本中词语的tf-idf权重
    rootDir="../result/weibo/cutword/"
    corpus=[]
    name=[]#16种性格的顺序
    print('collect word start')
    for lists in os.listdir(rootDir):#16中性格构成一个大的list
        path=os.path.join(rootDir,lists)
        if not os.path.isdir(path):
            temp=''
            print(path)
            for line in open(path,'r'):
                line=line.strip().split('\t')[0]
                temp+=line+' '
            corpus.append(temp)
            name.append(lists.split('_')[0])#从文件名中获取性格
    #计算每种性格下词语的tf-idf权重
    print("compute tf-idf start")
    vectorizer=CountVectorizer(max_df=5)
    transformer=TfidfTransformer()
    tfidf=transformer.fit_transform(vectorizer.fit_transform(corpus))
    word=vectorizer.get_feature_names()
    weight=tfidf.toarray()
    #将每种性格下的词语tf-idf权重写入各自性格的文件:word tf-idf
    print("write to file start")
    if len(name)!=len(weight) and len(name)!=16:#16种性格
        print("character num is not equal tf-idf array rows ",len(name),len(weight))
    else:
        count=0#从0~15表示每种性格的下标
        for key in name:#16种性格的tf-idf写入各自的文件
            fd=open("../result/weibo/tfidf/tfidf/"+key+'_tfidf.txt','w')
            if len(word)!=len(weight[count]):#error
                print("word num is not equal tf-idf weight columns ",len(word),len(weight[count]))
            else:
                for i in range(len(word)):#每个词语和其tf-idf值写入文件的一行
                    if weight[count][i]>0.0:
                        fd.write(word[i].encode('utf-8')+'\t'+str(weight[count][i])+'\n')
            count+=1
            fd.close()

def new_tf_idf(sourceDir,directory,power_num,thresh):#对idf平方改进tf-idf计算公式
    rootDir=sourceDir
    corpus=[]#每种性格的词语是一个list,[{word:num,...},...]
    name=[]#16种性格的顺序
    wordBag=set()#词库
    total=[]#每个文档的词总数
    print('collect word start')
    for lists in os.listdir(rootDir):#16中性格构成一个大的list
        path=os.path.join(rootDir,lists)
        if not os.path.isdir(path):
            temp={}#统计某一性格下某个词语出现的次数，{词语:频数,....}
            print(path)
            count=0.#统计每个文档出现的词数
            for line in open(path,'r'):
                line,flag=line.strip().split('\t')
                if flag[0]!='n':
                    if line not in temp:
                        temp[line]=1
                    else:
                        temp[line]+=1
                    #wordBag.add(line)
                    count+=1
            #total.append(count)#每个文档的词总数是total一个元素
            corpus.append(temp)#每个文档的词频统计是dict,该dict是corpus的一个元素
            name.append(lists.split('_')[0])#从文件名中获取性格,文件名格式INFJ_cut.txt
    #输出未过滤低词频前各个文档的不重复词语数目
    print("they are must be equal number of documents(there is 16)::len(corpus)=%d,len(name)=%d,len(total)=%d"%(len(corpus),len(name),len(total)));
    for doc in corpus:
        print("unique word num::%d"%len(doc))
    times=int(thresh)
    #过滤掉频数小于指定阈值的词语
    for doc in corpus:
        for key in doc.keys():#迭代器做一份拷贝
            if doc[key]<=times:
                doc.pop(key)
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
        #fd=open("../result/weibo/new_tf_idf_3/"+name[num]+"_newTfidf.txt",'w')
        print ("file::%s,word num::%d,unique word num::%d"%(name[num],total[num],len(doc)))
        for word in doc:
            tf=doc[word]/total[num]
            idf=math.pow(math.log(len(name)/word_doc[word]),float(power_num))#idf改进的地方
            tf_idf=tf*idf
            #print name[num],word,doc[word],total[num],tf,idf,tf_idf
            doc[word]=tf_idf
            if word.decode('utf-8')==u"睡觉":
                print("word=%s,tf=%f,idf=%f,tfidf=%f,出现文档数目=%d"%(word,tf,idf,tf_idf,word_doc[word]))
            #if tf_idf>0.0:
                #fd.write(word+'\t'+str(tf_idf)+'\n')
        #fd.close()
        num+=1
    #将权重前10000的写入文件
    num=0
    finished_doc_count=[]#统计每个文档最后tfidf值大于0的词数
    finished_wordBag=set()#统计所有文档tfidf大于0的不重复词语数目
    for doc in corpus:
        temp=sorted(doc.iteritems(),key=lambda d:d[1],reverse=True)
        wordSet=set()
        fd=open(directory+'/'+name[num]+"_"+str(power_num)+"_tfidf.txt",'w')
        if len(temp)>10000:
            i=0
            for key in temp:
                if key[1]>0.0:
                    fd.write(key[0]+'\t'+str(key[1])+'\n')
                    wordSet.add(key[0])
                    finished_wordBag.add(key[0])
                if i>10000:
                    break
                i+=1
        else:
            for key in temp:
                if key[1]>0.0:
                    fd.write(key[0]+'\t'+str(key[1])+'\n')
                    wordSet.add(key[0])
                    finished_wordBag.add(key[0])
        finished_doc_count.append(len(wordSet))
        fd.close()
        num+=1
    #输出每个文档tfidf值大于0的不重复词语数目及所有文档tfidf大于0的不重复词数目
    for i in range(len(finished_doc_count)):
        print("file=%s,unque word(tfidf>0) num=%d"%(name[i],finished_doc_count[i]))
    print("unique word(tfidf>0) count::%d"%len(finished_wordBag))


def word_character_weight(resultFile):#单词在每种性格下的权重写入文件
    rootDir="../result/weibo/tfidf/tfidf_1/"
    charList=['ENFJ','ENFP','ENTJ','ENTP','ESFJ','ESFP','ESTJ','ESTP','INFJ','INFP','INTJ','INTP','ISFJ','ISFP','ISTJ','ISTP']
    word_tfidf={}
    for lists in os.listdir(rootDir):
        path=os.path.join(rootDir,lists)
        if not os.path.isdir(path):
            print path
            charName=lists.split('_')[0]
            index=charList.index(charName)
            for line in open(path,'r'):
                line=line.strip()
                word,tfidf=line.split('\t')
                if word not in word_tfidf:
                    word_tfidf[word]=[0.,0.,0.,0.,0.,0.,0.,0.,0.,0.,0.,0.,0.,0.,0.,0.]
                    word_tfidf[word][index]=tfidf
                else:
                    word_tfidf[word][index]=tfidf
    fd=open(resultFile,'w')
    fd.write("word")
    for key in charList:
        fd.write("\t"+key)
    fd.write('\n')
    for key in word_tfidf:
        fd.write(key)
        for weight in word_tfidf[key]:
            fd.write('\t'+str(weight))
        fd.write('\n')
    fd.close()




if __name__ == "__main__":
    print('sys.argv[1] must be one of these:cut_word(分词),tf_idf(scikit-learn API计算),new_tf_idf(改进的tfidf计算),write_result(词在16种性格的tfidf权重写文件)')
    if sys.argv[1]=='cut_word':
        print "cut word"
        if len(sys.argv)<3:
            print "Input args:result directory(include /)"
            sys.exit(1)
        if sys.argv[2][len(sys.argv[2])-1] != '/':
            print("must include / for directory")
            sys.exit(1)
        cut_word(sys.argv[2])
    elif sys.argv[1]=='tf_idf':
        print "compute tf-idf"
        tf_idf()
    elif sys.argv[1]=='new_tf_idf':
        print "compute new tf-idf"
        if len(sys.argv)<6:
            print "Input args: new_tf_idf,source directory(include /),result directory(include /),idf power num"
            sys.exit(1)
        if sys.argv[2][len(sys.argv[2])-1] != '/' or sys.argv[3][len(sys.argv[3])-1]!='/':
            print("must include / for directory")
            sys.exit(1)
        new_tf_idf(sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])#第一个参数是分词文件的输入目录，第二个结果输出目录,第三个参数是idf的指数
    elif sys.argv[1]=='write_result':
        if len(sys.argv)<3:
            print("must specify:write_result,the result file path")
            sys.exit(1)
        word_character_weight(sys.argv[2])
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
