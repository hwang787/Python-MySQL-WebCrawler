from bs4 import BeautifulSoup
from urllib import request
import chardet

from mysqlHandler import mysqlHandler

url = "https://www.huxiu.com"
response = request.urlopen(url)
html = response.read()
charset = chardet.detect(html)
# set encoding for retrieved html
html = html.decode(str(charset["encoding"]))

# use python's html.parser, other options like lxml, xml, etc
soup = BeautifulSoup(html, 'html.parser')
# retrive class with tag = hot-article-img
allList = soup.select('.hot-article-img')

# connect to db
mysqlHandler = mysqlHandler()
mysqlHandler.connectMysql()
mysqlHandler.createDbAndTable()
#increment id by one after each row insertion
dataCount = mysqlHandler.getLastId() + 1
for news in allList:  
    aaa = news.select('a')
    # only select res longer than 0 (valid result)
    if len(aaa) > 0:
        # article link
        try:  
            href = url + aaa[0]['href']
        except Exception:
            href = ''
        # pic url
        try:
            imgUrl = aaa[0].select('img')[0]['src']
        except Exception:
            imgUrl = ""
        # news title
        try:
            title = aaa[0]['title']
        except Exception:
            title = ""

        #combine all the results to a dictionary
        news_dict = {
            "id": str(dataCount),
            "title": title,
            "url": href,
            "img_path": imgUrl
        }
        try:
            # insert to table and fail if find duplicates
            res = mysqlHandler.insertData(news_dict)
            if res:
                dataCount+=1
        except Exception as e:
            print("insertion failed", str(e))
# close mysql connection
mysqlHandler.closeMysql()
dataCount=0