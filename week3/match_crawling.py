import requests
from bs4 import BeautifulSoup

with open("C:/Users/신현준/Desktop/crawling/match.txt","w",encoding="utf8") as f:
    for i in range(1,1488): 
        url="https://ygosu.com/reports/?m2=result&s_type=oneonone&s_type2=all&search=&searcht=&page={}".format(i)
        headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"}
        res=requests.get(url,headers=headers)
        res.raise_for_status()

        soup=BeautifulSoup(res.text,"lxml")
        matchs=soup.find_all("tr")
        for match in matchs:
            winner=match.find_all("td",attrs={"class":"win"})
            loser=match.find_all("td",attrs={"class":"lose"})
            match_map=match.find_all("td",attrs={"class":"map"})

            if winner and loser and match_map:
                temp_winner=str(winner)
                temp_winner=temp_winner.split(",")
                winner_name=temp_winner[1][6:-1]
                s=str(temp_winner[-1])
                winner_tribe=s[-8]

                temp_loser=str(loser)
                temp_loser=temp_loser.split(",")
                loser_name=temp_loser[1][6:-1] 
                s=str(temp_loser[-1])
                loser_tribe=s[-8]
                
                match_map=str(match_map)
                match_map=match_map.split(",")
                match_map=match_map[1][4:-1]
                                
                f.write(winner_name+"   "+winner_tribe+"   "+loser_name+"   "+loser_tribe+"   "+match_map+"\n")
