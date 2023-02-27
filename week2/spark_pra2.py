from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
Job_id={}

def loadJob():
    cnt=0
    Job_list = {}
    with open("ds_salaries.text") as f:
        for line in f:
            fields = line.split(',')
            if fields[4] not in Job_list.values():
                cnt+=1
                Job_list[cnt] = fields[4]
                Job_id[fields[4]]=cnt

    return Job_list

def parseInput(line):
    fields = line.split(',')
    return Row(JOBID = (fields[4]), salary = float(fields[5]))

if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    spark = SparkSession.builder.appName("ds_best_salary").getOrCreate()

    # Load up our movie ID -> name dictionary
    JOBNames = loadJob()

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/ds_salaries.text")
    # Convert it to a RDD of Row objects with (movieID, rating)
    jobs = lines.map(parseInput)
    # Convert that to a DataFrame
    jobsDataset = spark.createDataFrame(jobs)

    # Compute average rating for each movieID
    averageRatings = jobsDataset.groupBy("JOBID").avg("salary")

    # Compute count of ratings for each movieID
    counts = jobsDataset.groupBy("JOBID").count()

    # Join the two together (We now have movieID, avg(rating), and count columns)
    averagesAndCounts = counts.join(averageRatings, "JOBID")

    # Pull the top 10 results
    top = averagesAndCounts.orderBy("avg(salary)").take(len(JOBNames))

    # Print them out, converting movie ID's to names as we go.
    for job in top:
        print (Job_id[job[0]], job[1], job[2])

    # Stop the session
    spark.stop()
