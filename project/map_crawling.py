import requests
from bs4 import BeautifulSoup
with open("C:/Users/신현준/Desktop/crawling/map.txt","w",encoding="utf8") as f:
    for i in range(1,405):
        url="https://ygosu.com/reports/?m2=map&assort=league&page=1&idx={}".format(i)
        headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"}
        res=requests.get(url,headers=headers)
        res.raise_for_status()
        soup=BeautifulSoup(res.text,"lxml")
        map=soup.find("div",attrs={"class":"top"})
        if map:
            map=str(map)
            map=map.split(">")
            map=map[2][:-4]
           
            f.write(str(i)+","+map+"\n") 
