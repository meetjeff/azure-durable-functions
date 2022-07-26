# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.

from bs4 import  BeautifulSoup as bs
import requests
import json
import pymysql
import time,random
import logging
import os
from dotenv import load_dotenv

load_dotenv()

def main(ser:str) -> str:

    jdata=json.loads(ser)
    sp=jdata['search'].replace('、',' ')
    rp=jdata['page']

    logging.info('104 Start Search')
    
    url1=[]
    for page in range(1,rp):
        url=f"https://www.104.com.tw/jobs/search/?ro=0&jobcat=2007002002%2C2007001012&kwop=11&keyword={sp}&expansionType=area%2Cspec%2Ccom%2Cjob%2Cwf%2Cwktm&order=14&asc=0&page={page}&mode=l&jobsource=2018indexpoc&langFlag=0&langStatus=0&recommendJob=1&hotJob=1"
        url1.append(url)

    headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Safari/537.36',
                'Referer': 'https://www.104.com.tw/job/'
                }
    soup1=[]
    for i in url1:
        try:
            resp = requests.get(i, headers=headers)
            soup = bs(resp.text,"html.parser")
            soup1.append(soup)
            time.sleep(random.uniform(1, 3))
        except Exception as m:
            logging.info('第'+str(i)+'頁'+str(m))

    result=[]
    for i in soup1:
        try:
            for n in range(len(i.find_all('a',class_='js-job-link'))):
                url=i.find_all('a',class_='js-job-link')[n].get('href').replace('//','')
                result.append(url)
        except Exception as m:
            logging.info('第'+str(i)+'頁'+str(m))

    jobid=[]
    for i in result:
        job_id=i.split('/')[2][0:5]
        jobid.append(job_id)

    newjobid = []
    
    for element in jobid:
        if element not in newjobid:
            newjobid.append(element)

    all=[]
    for i in newjobid:
        url = f'https://www.104.com.tw/job/ajax/content/{i}'
        headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Safari/537.36',
                'Referer': 'https://www.104.com.tw/job/'
                }
        r = requests.get(url, headers=headers)
        data = r.json()
        all.append(data)
        time.sleep(random.uniform(1, 3))

    newresult=[]
    for i in all:
        try:
            job={
                "id":i['data']['header']['analysisUrl'].split('/')[-1],
                "jobname":i['data']['header']['jobName'].replace("'",""),
                "appearDate":i['data']['header']['appearDate'].replace("'",""),
                "edu":i['data']['condition']['edu'].replace("'",""),
                "welfare":i['data']['welfare']['welfare'].replace('\n','').replace('\r','').replace('\u3000','').replace('\t','').replace("'",""),
                "custName":i['data']['header']['custName'].replace("'",""),
                "salary":i['data']['jobDetail']['salary'],
                "addressRegion":i['data']['jobDetail']['addressRegion'][0:3],
                "jobDescription":i['data']['jobDetail']['jobDescription'].replace('\n','').replace('\n1','').replace('\n2','').replace('\n3','').replace('\n4','').replace("'","").replace('\r4','').replace('\r2',''),
                "skill":','.join([b['description'] for b in i['data']['condition']['specialty']]),
                "department":','.join(i['data']['condition']['major'])
                }
            salary=i['data']['jobDetail']['salaryMin']
            
            if salary > 200000:
                salaryannual = salary
            if salary >= 24000 and salary <= 200000:
                salaryannual = salary*14
            if salary > 0 and salary < 24000:
                salaryannual = 0
            if salary == 0:
                salaryannual = 560000
            
            job['salaryannual']=salaryannual
            newresult.append(job)
        except Exception as m:
            logging.info('第'+str(i)+'筆'+str(m))

    con = pymysql.connect(
        host = os.getenv("dbip"),
        user = os.getenv("dbuser"),
        password = os.getenv("dbpassword"),
        port = int(os.getenv("dbport")),
        database = "career")
    cur = con.cursor()

    for i in range(len(newresult)):
        try:
            cur.execute("""
                INSERT INTO `career`.`newjob` (`id`,`job`,`location`,`salary`,`annualsalary`,`company`,`education`,`skill`,`description`,`benefits`,`lastupdate`,`website`)
                VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','104') on duplicate key update job=values(job),location=values(location),salary=values(salary),annualsalary=values(annualsalary),company=values(company),education=values(education),skill=values(skill),description=values(description),benefits=values(benefits),lastupdate=values(lastupdate),website=values(website);
                """.format(newresult[i]['id'], newresult[i]['jobname'], newresult[i]['addressRegion'], newresult[i]['salary'], newresult[i]['salaryannual'], newresult[i]['custName'],newresult[i]['edu'], newresult[i]['skill'], newresult[i]['jobDescription'], newresult[i]['welfare'], newresult[i]['appearDate']))
        except Exception as m:
            logging.info(str(newresult[i]['id'])+','+newresult[i]['jobname']+','+str(m))
            
    con.commit()
    cur.close()
    con.close()
    logging.info(f"已新增 {jdata['search']} 職缺查詢至 104 ({jdata['page']} 頁) !")

    return f"已新增 {jdata['search']} 職缺查詢至 104 ({jdata['page']} 頁) !"
