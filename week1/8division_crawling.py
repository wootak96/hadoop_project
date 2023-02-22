import requests
from bs4 import BeautifulSoup
with open("C:/Users/신현준/Desktop/crawling/8division_pac.text","w",encoding="utf8") as f:
    for i in range(1,11):
        url="https://www.8division.com/product/list.html?cate_no=220&page={}".format(i)
        headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"}
        res=requests.get(url,headers=headers)
        res.raise_for_status()

        soup=BeautifulSoup(res.text,"lxml")


        items=soup.find_all("div",attrs={"class":"thumbnail-info"})
        cnt=0
        for item in items:
            cnt+=1
            if cnt>=24:
                brand=item.find("p",attrs={"class":"brand"}).get_text().strip()
                name=item.find("p",attrs={"class":"name"}).get_text().strip()
                price=item.find("p",attrs={"class":"price"})
                if price.find('span'):
                    price.find('span').decompose()
                price=item.find("p",attrs={"class":"price"}).get_text().strip()
                n_price=""
                for i in range(len(price)):
                    if price[i]!="W" and price[i]!=",":
                        n_price+=price[i]

                f.write(brand+"   "+name+"   "+n_price+"\n") 
