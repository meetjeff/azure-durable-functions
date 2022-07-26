# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.

import logging
from lxml import html
from bs4 import BeautifulSoup as bs
import requests
import pymysql
import time,random
import nums_from_string as nfs
import json
import os
from dotenv import load_dotenv

load_dotenv()

def SalNum(i):
    ex = i
    ex1 = ex.replace(",", "")
    number = nfs.get_nums(ex1)
    Number=[]
    for i in number:
        num = i*14
        Number.append(round(num))
    return Number

def ave(i):
    if len(i) == 2:
        num = 0
        for a in i:
            num = num+a
            Sal = round(num/2)
        return Sal
    else:
        try:
            Sal = round(i[0])
            return Sal
        except:
            #except: "面議(薪資達四萬以上)" = 40000以上
            return "560000" 


def main(ser:str) -> str:

    jdata=json.loads(ser)
    sp=jdata['search'].replace('、',' ')
    rp=jdata['page']

    logging.info('518 Start Search')

    result = []
    for page in range(1,rp):
        link = f"https://www.518.com.tw/job-index-P-{page}.html?ad={sp}"
        res = requests.get(link)
        soup = bs(res.text,"lxml")
        job = soup.find_all('ul', class_ = "all_job_hover") 
        if job == []:
            break
        for i in job:
            JOB = {}
            JOB["職缺"] = i.find("h2").text
            JOB["地區"] = i.find("li", class_="area").text
            JOB["薪資"] = i.find("p").text
            JOB["薪資統一格式"] = ave(SalNum(i.find("p").text))
            JOB["公司名稱"] = i.find("li", class_="company").text.replace("\n","")
            JOB["內容"] = i.find_all("p", class_="jobdesc")[1].text.replace("\n" , "")
            JOB["更新時間"] = soup.find("li", class_="date").text.strip()
            
            url = i.find("a").get("href")
            req = requests.get(url)
            tree = html.fromstring(req.text)
            
            JOB["id"] = url.split("/")[3].split(".")[0].split("-")[1]
            JOB["學歷"] = tree.xpath('//*[@id="content"]/div[2]/div/ul/li[6]/span')[0].text.replace(' ', '').replace('\n', '').replace('\r', '')
            JOB["科系"] = tree.xpath('//*[@id="content"]/div[2]/div/ul/li[7]/span')[0].text
            JOB["要求技能"] = tree.xpath('//*[@id="content"]/div[2]/div/ul/li[8]/span')[0].text
            
            soup2 = bs(req.text, "lxml")
            jobB = soup2.find_all("div", class_ = "text-box")
            try:
                job_benefits = jobB[0].text.replace("\n", "").replace(" ", "").replace("\xa0","").replace("\r","").replace("\u3000","")
            except:
                job_benefits = "不公告"
            
        
            JOB["公司福利"] = job_benefits
            result.append(JOB)
        time.sleep(random.uniform(3, 5))
    
    con = pymysql.connect(
        host = os.getenv("dbip"),
        user = os.getenv("dbuser"),
        password = os.getenv("dbpassword"),
        port = int(os.getenv("dbport")),
        database='career')

    cur = con.cursor()

    for i in range(len(result)):

        try:
            cur.execute("""
                    INSERT INTO `career`.`newjob` (`id`,`job`,`location`,`salary`,`annualsalary`,`company`,`education`,`skill`,`description`,`benefits`,`lastupdate`,`website`)
                    VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','518') on duplicate key update job=values(job),location=values(location),salary=values(salary),annualsalary=values(annualsalary),company=values(company),education=values(education),skill=values(skill),description=values(description),benefits=values(benefits),lastupdate=values(lastupdate),website=values(website);
            """.format(result[i]['id'], result[i]['職缺'],result[i]['地區'],result[i]['薪資'],result[i]['薪資統一格式'], result[i]['公司名稱'], result[i]['學歷'], result[i]['要求技能'], result[i]['內容'], result[i]['公司福利'], result[i]['更新時間']))
            con.commit()
        except Exception as m:
            logging.info(str(result[i]['id'])+result[i]['職缺']+str(m))
    cur.close()
    con.commit()
    con.close()

    logging.info(f"已新增 {jdata['search']} 職缺查詢至 518 ({jdata['page']} 頁) !")

    return f"已新增 {jdata['search']} 職缺查詢至 518 ({jdata['page']} 頁) !"
