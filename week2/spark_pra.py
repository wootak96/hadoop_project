from pyspark import SparkConf,SparkContext
Job_id={}
def loadJob():

    Job_list={}
    cnt=0
    with open("ds_salaries.text") as f :
        for line in f:
            fields=line.split(",")
            if fields[4] not in Job_list.values():
                cnt+=1
                Job_list[cnt]=fields[4]
                Job_id[fields[4]]=cnt

    return Job_list

def parseInput(line):
    fields=line.split(",")
    return (Job_id[fields[4]],(float(fields[5]),1.0))

if __name__=="__main__":

    conf = SparkConf().setAppName("lucrative job")
    sc = SparkContext(conf = conf)

    Job_list = loadJob()


    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/ds_salaries.text")

    job_salary = lines.map(parseInput)


    ratingTotalsAndCount = job_salary.reduceByKey(lambda job1,job2 : (job1[0]+job2[0],job1[1]+job2[1]) )

    averageSalary = ratingTotalsAndCount.mapValues(lambda totalCount : totalCount[0]/totalCount[1])

    sortedJob = averageSalary.sortBy(lambda x: -x[1])

    results = sortedJob.take(len(Job_list))

    for result in results:
        print(Job_list[result[0]],result[1])

