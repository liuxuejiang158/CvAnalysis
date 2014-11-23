import re
fd=open("../data/cloud/skill.txt_1",'w')
for line in open("../data/cloud/skill.txt"):
    line=line.strip()
    if not re.match("^[A-Za-z0-9]+$",line):
        fd.write(line+"\n")
    else:
        print line
