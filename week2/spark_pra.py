from pyspark import SparkConf,SparkContext

def loadJob():
    Job_id={}
    Job_list={}
    cnt=0
    with open("~~") as f : # open 주소!!
        for line in f:
            fields=line.split(",")
            if fields[4] not in Job_list.values():
                cnt+=1
                Job_list[cnt]=fields[4]
                Job_id[fields[4]]=cnt
    return Job_list

def parseInput(line):
    fields=line.split(",")
    return ()


