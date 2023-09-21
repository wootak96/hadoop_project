import requests
from bs4 import BeautifulSoup

with open("C:/Users/신현준/Desktop/new/match.txt","w",encoding="utf8") as f:
    for i in range(1,1547): 
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
                temp_winner=temp_winner.split('"')
                winner_name=temp_winner[3].split("=")[2]
                temp_winner2=temp_winner[6].split(">")
                winner_tribe=temp_winner2[3][1]
                

                temp_loser=str(loser)
                temp_loser=temp_loser.split('"')
                loser_name=temp_loser[3].split("=")[2]
                temp_loser2=temp_loser[6].split(">")
                loser_tribe=temp_loser2[3][1]
                
                match_map=str(match_map)
                match_map=match_map.split('"')
                match_map=match_map[3].split("=")
                match_map=match_map[3]
                
                 
                #f.write(winner_name+"   "+winner_tribe+"   "+loser_name+"   "+loser_tribe+"   "+match_map+"\n")
                f.write("win"+","+winner_name+","+winner_tribe+","+loser_tribe+","+match_map+"\n")
                f.write("lose"+","+loser_name+","+loser_tribe+","+winner_tribe+","+match_map+"\n")

