import requests
from bs4 import BeautifulSoup
with open("C:/Users/신현준/Desktop/crawling/player.txt","w",encoding="utf8") as f:
    for i in range(1534,1562):
        url="https://ygosu.com/reports/?m2=person&idx={}&newwindow=&search=YToxOntzOjExOiJzX2tleXdvcmRfdCI7czo0OiJuYW1lIjt9".format(i)
        headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"}
        res=requests.get(url,headers=headers)
        res.raise_for_status()

        soup=BeautifulSoup(res.text,"lxml")
     
        player=soup.find_all("div",attrs={"class":"info"})
        
        name=str(player).split(" ")
        
      
        if len(name)>=14:
            f.write(str(i)+","+name[8]+","+name[13][0]+"\n") 
