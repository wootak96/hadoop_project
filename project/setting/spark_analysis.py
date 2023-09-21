from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql.functions import *



def loadPlayer():
    Players={}
    with open("player.txt") as f:
        for info in f:
            player = info.split(",")
            Players[int(player[0])]=[player[1],player[2][0]]

    return Players

def loadMap():
    Maps={}
    with open("map.txt") as f:
        for info in f:
            map = info.split(",")
            Maps[int(map[0])]=str(map[1][:-1])

    return Maps


def parseInput(line):
    fields=line.split(",")
    if int(fields[1]) in player.keys():
        if fields[0]=="win":
            if fields[2]=="Z" and fields[3]=="P":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]),  superiority_win = int(1),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(0))

            elif fields[2]=="Z" and fields[3]=="T":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(1), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(0))

            elif fields[2]=="Z" and fields[3]=="Z":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(1), sametribe_lose=int(0))

            elif fields[2]=="T" and fields[3]=="Z":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(1),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(0))

            elif fields[2]=="T" and fields[3]=="P":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(1), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(0))

            elif fields[2]=="T" and fields[3]=="T":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(1), sametribe_lose=int(0))

            elif fields[2]=="P" and fields[3]=="T":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(1),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(0))

            elif fields[2]=="P" and fields[3]=="Z":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(1), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(0))

            elif fields[2]=="P" and fields[3]=="P":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(1), sametribe_lose=int(0))



        else: 
            if fields[2]=="Z" and fields[3]=="P":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(1),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(0))

            elif fields[2]=="Z" and fields[3]=="T":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(1), sametribe_win=int(0), sametribe_lose=int(0))

            elif fields[2]=="Z" and fields[3]=="Z":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(1))

            elif fields[2]=="T" and fields[3]=="Z":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(1),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(0))

            elif fields[2]=="T" and fields[3]=="P":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(1), sametribe_win=int(0), sametribe_lose=int(0))

            elif fields[2]=="T" and fields[3]=="T":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(1))

            elif fields[2]=="P" and fields[3]=="T":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(1),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(0))

            elif fields[2]=="P" and fields[3]=="Z":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(1), sametribe_win=int(0), sametribe_lose=int(0))

            elif fields[2]=="P" and fields[3]=="P":
                return Row(player_name = str(player[int(fields[1])][0])+str(player[int(fields[1])][1]), superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(1))
    else:
        return Row(player_name = "none",superiority_win = int(0),superiority_lose = int(0),inferiority_win=int(0), inferiority_lose = int(0), sametribe_win=int(0), sametribe_lose=int(0))



def parseInput2(line):
    fields=line.split(",")
    if int(fields[4]) in maps.keys():
        if fields[0]=="win":
            if fields[2]=="Z" and fields[3]=="P":
                return Row(map_name = str(maps[int(fields[4])]),ZvsP=int(1),ZvsT=int(0),TvsZ=int(0),TvsP=int(0),PvsT=int(0),PvsZ=int(0))

            elif fields[2]=="Z" and fields[3]=="T":
                return Row(map_name = str(maps[int(fields[4])]),ZvsP=int(0),ZvsT=int(1),TvsZ=int(0),TvsP=int(0),PvsT=int(0),PvsZ=int(0))

            elif fields[2]=="T" and fields[3]=="Z":
                return Row(map_name = str(maps[int(fields[4])]),ZvsP=int(0),ZvsT=int(0),TvsZ=int(1),TvsP=int(0),PvsT=int(0),PvsZ=int(0))

            elif fields[2]=="T" and fields[3]=="P":
                return Row(map_name = str(maps[int(fields[4])]),ZvsP=int(0),ZvsT=int(0),TvsZ=int(0),TvsP=int(1),PvsT=int(0),PvsZ=int(0))

            elif fields[2]=="P" and fields[3]=="T":
                return Row(map_name = str(maps[int(fields[4])]),ZvsP=int(0),ZvsT=int(0),TvsZ=int(0),TvsP=int(0),PvsT=int(1),PvsZ=int(0))

            elif fields[2]=="P" and fields[3]=="Z":
                return Row(map_name = str(maps[int(fields[4])]),ZvsP=int(0),ZvsT=int(0),TvsZ=int(0),TvsP=int(0),PvsT=int(0),PvsZ=int(1))

            else:
                return Row(map_name = "none",ZvsP=int(0),ZvsT=int(0),TvsZ=int(0),TvsP=int(0),PvsT=int(0),PvsZ=int(0))
        else:
            return Row(map_name = "none",ZvsP=int(0),ZvsT=int(0),TvsZ=int(0),TvsP=int(0),PvsT=int(0),PvsZ=int(0))

    else:
        return Row(map_name = "none",ZvsP=int(0),ZvsT=int(0),TvsZ=int(0),TvsP=int(0),PvsT=int(0),PvsZ=int(0))



if __name__ == "__main__":
    spark = SparkSession.builder.appName("all about starcraft1").getOrCreate()

    player=loadPlayer()
    lines=spark.sparkContext.textFile("match.txt")
    matches=lines.map(parseInput)
    match_dataset = spark.createDataFrame(matches)
    player_winning = match_dataset.groupBy("player_name").sum()
    
    player_winning=player_winning.withColumn("win",col("sum(superiority_win)")+col("sum(inferiority_win)")+col("sum(sametribe_win)"))
    player_winning=player_winning.withColumn("lose",col("sum(superiority_lose)")+col("sum(inferiority_lose)")+col("sum(sametribe_lose)"))
    player_winning=player_winning.withColumn("총 경기수",col("win")+col("lose"))
    player_winning=player_winning.withColumn("winning_rate",round(col("win")/(col("win")+col("lose")),3))
    player_winning=player_winning.orderBy(col("winning_rate").desc())

    player_winning=player_winning.withColumnRenamed("player_name","선수명")
    player_winning=player_winning.withColumnRenamed("sum(superiority_win)","상성전 승")
    player_winning=player_winning.withColumnRenamed("sum(superiority_lose)","상성전 패")
    player_winning=player_winning.withColumnRenamed("sum(inferiority_win)","역상성전 승")
    player_winning=player_winning.withColumnRenamed("sum(inferiority_lose)","역상성전 패")
    player_winning=player_winning.withColumnRenamed("sum(sametribe_win)","동족전 승")
    player_winning=player_winning.withColumnRenamed("sum(sametribe_lose)","동족전 패")    
    player_winning=player_winning.filter(col("총 경기수")>=100)
    player_winning.show()


   
    maps=loadMap() 
    lines2=spark.sparkContext.textFile("match.txt")
    matches2=lines2.map(parseInput2)
    match_dataset2 = spark.createDataFrame(matches2)
    map_winning = match_dataset2.groupBy("map_name").sum()
    
    map_winning=map_winning.withColumn("TvsZ",when(col("sum(TvsZ)")>=col("sum(ZvsT)"),round(col("sum(TvsZ)")/(col("sum(TvsZ)")+col("sum(ZvsT)")),3)).otherwise(round(col("sum(ZvsT)")/(col("sum(TvsZ)")+col("sum(ZvsT)")),3)))
    map_winning=map_winning.withColumn("PvsT",when(col("sum(PvsT)")>=col("sum(TvsP)"),round(col("sum(PvsT)")/(col("sum(PvsT)")+col("sum(TvsP)")),3)).otherwise(round(col("sum(TvsP)")/(col("sum(PvsT)")+col("sum(TvsP)")),3)))
    map_winning=map_winning.withColumn("ZvsP",when(col("sum(ZvsP)")>=col("sum(PvsZ)"),round(col("sum(ZvsP)")/(col("sum(ZvsP)")+col("sum(PvsZ)")),3)).otherwise(round(col("sum(PvsZ)")/(col("sum(ZvsP)")+col("sum(PvsZ)")),3)))

    map_winning=map_winning.withColumn("map_balance",round((((col("TvsZ")-0.5)**2+(col("PvsT")-0.5)**2+(col("ZvsP")-0.5)**2)/3),5))
    map_winning=map_winning.withColumn("총 경기수",col("sum(PvsT)")+col("sum(PvsZ)")+col("sum(TvsZ)")+col("sum(TvsP)")+col("sum(ZvsT)")+col("sum(ZvsP)"))
    map_winning=map_winning.withColumnRenamed("sum(ZvsT)","Z win versus T")
    map_winning=map_winning.withColumnRenamed("sum(ZvsP)","Z win versus p")
    map_winning=map_winning.withColumnRenamed("sum(TvsZ)","T win versus Z") 
    map_winning=map_winning.withColumnRenamed("sum(TvsP)","T win versus P")
    map_winning=map_winning.withColumnRenamed("sum(PvsT)","P win versus T")
    map_winning=map_winning.withColumnRenamed("sum(PvsZ)","P win versus Z")


    map_winning=map_winning.orderBy(col("map_balance"))
    map_winning=map_winning.filter(col("총 경기수")>=200)
    map_winning.show()
    print(type(player_winning))
    print(type(map_winning))
    print(player_winning)

    '''
    n=datetime.now()
    player_winning.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/home/wootag/player_winning{}.csv".format(n))
    map_winning.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("/home/wootag/map_winning{}.csv".format(n))
    '''
    print("@@@@@@@@@@@@@완료@@@@@@@@@@@@@")
    spark.stop()




