from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadPlayer():
    Players={}
    with open("player.txt") as f:
        for info in f:
            player = info.split("\t")
            Players[player[0]]=[player[1],player[2]]
            # Plyers[인덱스]=[이름,종족,상성전 승,상성전 패,역상성전 승,역상성전 패,동족전 승,동족전 패]
    return Players

def loadMap():
    Maps={}
    with open("map.txt") as f:
        for info in f:
            map = info.split("\t")
            Maps[map[0]]=map[1]
            # Map[인덱스]=[맵이름]
    return Maps

def parseInput(line):
    fields=line.split("\t")
    if fields[0]=="win":
        #이겼으면
        if fields[2]=="Z" and fields[3]=="P":
            return Row(player_num = (fields[1]), superiority_win = float(1),superiority_lose = float(0),inferiority_win=float(0), inferiority_lose = float(0), sametribe_win=float(0), sametribe_lose=float(0))
        
        elif fields[2]=="Z" and fields[3]=="T":
            return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(0),inferiority_win=float(1), inferiority_lose = float(0), sametribe_win=float(0), sametribe_lose=float(0))

        elif fields[2]=="Z" and fields[3]=="Z":
            return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(0),inferiority_win=float(0), inferiority_lose = float(0), sametribe_win=float(1), sametribe_lose=float(0))

        elif fields[2]=="T" and fields[3]=="Z": 
            return Row(player_num = (fields[1]), superiority_win = float(1),superiority_lose = float(0),inferiority_win=float(0), inferiority_lose = float(0), sametribe_win=float(0), sametribe_lose=float(0))

        elif fields[2]=="T" and fields[3]=="P":
            return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(0),inferiority_win=float(1), inferiority_lose = float(0), sametribe_win=float(0), sametribe_lose=float(0))

        elif fields[2]=="T" and fields[3]=="T":
           return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(0),inferiority_win=float(0), inferiority_lose = float(0), sametribe_win=float(1), sametribe_lose=float(0))
           
        elif fields[2]=="P" and fields[3]=="T":
            return Row(player_num = (fields[1]), superiority_win = float(1),superiority_lose = float(0),inferiority_win=float(0), inferiority_lose = float(0), sametribe_win=float(0), sametribe_lose=float(0))
            
        elif fields[2]=="P" and fields[3]=="Z":
            return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(0),inferiority_win=float(1), inferiority_lose = float(0), sametribe_win=float(0), sametribe_lose=float(0))
            
        elif fields[2]=="P" and fields[3]=="P":
           return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(0),inferiority_win=float(0), inferiority_lose = float(0), sametribe_win=float(1), sametribe_lose=float(0))
           


    else: # 졌으면
        if fields[2]=="Z" and fields[3]=="P":
            return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(1),inferiority_win=float(0), inferiority_lose = float(0), sametribe_win=float(0), sametribe_lose=float(0))
        
        elif fields[2]=="Z" and fields[3]=="T":
            return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(0),inferiority_win=float(0), inferiority_lose = float(1), sametribe_win=float(0), sametribe_lose=float(0))

        elif fields[2]=="Z" and fields[3]=="Z":
            return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(0),inferiority_win=float(0), inferiority_lose = float(0), sametribe_win=float(0), sametribe_lose=float(1))

        elif fields[2]=="T" and fields[3]=="Z": 
            return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(1),inferiority_win=float(0), inferiority_lose = float(0), sametribe_win=float(0), sametribe_lose=float(0))

        elif fields[2]=="T" and fields[3]=="P":
            return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(0),inferiority_win=float(0), inferiority_lose = float(1), sametribe_win=float(0), sametribe_lose=float(0))

        elif fields[2]=="T" and fields[3]=="T":
           return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(0),inferiority_win=float(0), inferiority_lose = float(0), sametribe_win=float(0), sametribe_lose=float(1))
           
        elif fields[2]=="P" and fields[3]=="T":
            return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(1),inferiority_win=float(0), inferiority_lose = float(0), sametribe_win=float(0), sametribe_lose=float(0))
            
        elif fields[2]=="P" and fields[3]=="Z":
            return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(0),inferiority_win=float(0), inferiority_lose = float(1), sametribe_win=float(0), sametribe_lose=float(0))
            
        elif fields[2]=="P" and fields[3]=="P":
           return Row(player_num = (fields[1]), superiority_win = float(0),superiority_lose = float(0),inferiority_win=float(0), inferiority_lose = float(0), sametribe_win=float(0), sametribe_lose=float(1))
           

if __name__ == "__main__":
    spark = SparkSession.builder.appName("all about starcraft1").getOrCreate()

    # 플레이어 이름이 들어있는 사전
    player_name = loadPlayer()

    lines=spark.sparkContext.textFile("match2.txt")
    
    matches=lines.map(parseInput)

    match_dataset = spark.createDataFrame(matches)

    player_winning = match_dataset.groupBy("player_num")

    top = player_winning.take(len(10))

    for i in top:
        print(i)

    spark.stop()