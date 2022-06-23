import datetime
import logging
import azure.functions as func
from lxml import html
from bs4 import BeautifulSoup as bs
import requests
import pymysql
import time,random
import nums_from_string as nfs

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

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    logging.info('518 Start Search(Time)')

    pstop = 0
    result = []
    for page in range(1,10):
        link = f"https://www.518.com.tw/job-index-P-{page}.html?&ab=2032001022%2C2032001017%2C2032001018%2C2032002003%2C2032001011"
        res = requests.get(link)
        soup = bs(res.text,"lxml")
        job = soup.find_all('ul', class_ = "all_job_hover") 
        if job == []:
            pstop = page - 1
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
            time.sleep(random.uniform(2, 4))
    
    con = pymysql.connect(
        host= 'azsqltop.mysql.database.azure.com',
        port= 3306,
        user= "jeff",
        password='@a0987399832',
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

    logging.info('已更新'+str(pstop)+'頁查詢至518')

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('518 timer trigger function ran at %s', utc_timestamp)
