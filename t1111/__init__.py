import datetime
import logging
import azure.functions as func
from lxml import html
from bs4 import BeautifulSoup as bs
import requests
import pymysql
import cn2an
import nums_from_string as nfs

def SalNum(i):
    ex = i
    ex1 = ex.replace(",", ".")
    ex2 = cn2an.transform(ex1)
    if "萬" in ex2:
        number = nfs.get_nums(ex2)
        Number=[]
        for i in number:
            num = i*10000
            Number.append(round(num))
        return Number
    else:
        Number=[]
        number = nfs.get_nums(ex2.replace(".", ""))
        for i in number:
            num = i
            Number.append(round(num))
        return Number

def ave(i):
    if len(i) == 2:
        num = 0
        for a in i:
            num = num+a
            Sal = round(num/2)
        return Sal
    elif len(i) == 1:
        Sal = round(i[0])
        return Sal
    else:
        return Sal

def annual(i):
    if i < 500000:
        return i*14
    else:
        return i

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    logging.info('1111 Start Search(Time)')
    
    jobS = []
    time = []
    jobL = []
    jobA = []
    jobN = []
    urls = []
    result = []

    for p in range(1,10):
        try:        
            title = requests.get(f"https://www.1111.com.tw/search/job?d0=140500&ks=數據&page={p}")
            soup = bs(title.text, 'lxml')
            job = soup.find_all('div', attrs={'class':'job_item_info'})
            
            SAL = soup.find_all('div', attrs={'job_item_detail_salary ml-3 font-weight-style digit_6'})
            for i in SAL:
                jobS.append(i.text)
            Tim = soup.find_all('small', attrs={'text-muted job_item_date'})
            for i in Tim:
                time.append(i.text)
            cop = soup.find_all('h6', attrs={'job_item_company mb-1 digit_5 body_3'})
            for i in cop:
                jobL.append(i.text)
            are = soup.find_all('a', attrs={'job_item_detail_location mr-3 position-relative'})
            for i in are:
                jobA.append(i.text[0:3])
            for i in job:
                jobN.append(i.a.text)            
            for i in range(len(job)):
                urls.append(job[i].a.get("href"))

        except Exception as m:
            logging.info(str(p)+','+str(m))

    num = 0

    while True:
        if num <= len(urls):
            for i in range(len(urls)):
                try:
                    req = requests.get(urls[i])
                    tree = html.fromstring(req.text)
                    
                    system = []
                    for a in tree.xpath("//li[3]/div/span/a"):
                        system.append(a.text.replace(' ', '').replace('\n', '').replace('\r', ''))

                    if len(system) == 0:
                        system.append("不拘")

                    systemS = ",".join(system)            
                        
                    skill = []
                    for b in tree.xpath("//li[4]/div/span[2]/a"):
                        skill.append(b.text.replace(' ', '').replace('\n', '').replace('\r', ''))

                    if len(skill) == 0:
                        skill.append("不拘")

                    skillS = ",".join(skill)
                                    
                    experience = tree.xpath("//div[5]/div/div[5]/div/ul/li[1]/div/span[2]")[0].text.replace(' ', '').replace('\n', '').replace('\r', '')            
                    
                    education = tree.xpath("//div[5]/div/ul/li[2]/div/span[2]")[0].text.replace(' ', '').replace('\n', '').replace('\r', '')    
                        
                    soup2 = bs(req.text, 'lxml')
                    
                    jobC = soup2.find_all('div', attrs={'content_items job_description'})

                    job_contemt = jobC[0].text.replace("\n", '').replace("\xa0", "")                                      
                    
                    jobB = soup2.find_all('div', attrs={'content_items job_benefits'})

                    try:
                        job_benefits = jobB[0].text.replace("\n", '').replace(" ", '').replace("\r", '')
                    except:
                        job_benefits = "不公告"
                    ID = nfs.get_nums(urls[i])[1]


                    JOB = {}
                    JOB["id"] = ID
                    JOB["職缺"] = jobN[i]
                    JOB["地區"] = jobA[i]
                    JOB["小薪水"] = SalNum(jobS[i])
                    JOB["薪資"] = annual(ave(SalNum(jobS[i])))
                    JOB["公司"] = jobL[i]
                    JOB["學歷"] = education
                    JOB["科系"] = systemS
                    JOB["要求技能"] = skillS
                    JOB["內容"] = job_contemt
                    JOB["福利"] = job_benefits
                    JOB["更新時間"] = time[i]
                    result.append(JOB)
                    num = num+1

                except Exception as m:
                    logging.info(str(i)+','+str(m))
                
        else:
            break        

    con = pymysql.connect(
        host= 'azsqltop.mysql.database.azure.com',
        port= 3306,
        user= "jeff",
        password= "@a0987399832",
        db= "career")

    cur = con.cursor()

    for i in range(len(result)):
        try:
            cur.execute("""
                    INSERT INTO `career`.`newjob` (`id`,`job`,`location`,`salary`,`annualsalary`,`company`,`education`,`skill`,`description`,`benefits`,`lastupdate`,`website`)
                    VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','1111') on duplicate key update job=values(job),location=values(location),salary=values(salary),annualsalary=values(annualsalary),company=values(company),education=values(education),skill=values(skill),description=values(description),benefits=values(benefits),lastupdate=values(lastupdate),website=values(website);
            """.format(result[i]['id'], result[i]['職缺'], result[i]['地區'], result[i]['小薪水'], result[i]['薪資'], result[i]['公司'], result[i]['學歷'], result[i]['要求技能'], result[i]['內容'], result[i]['福利'], result[i]['更新時間']))
        except Exception as m:
            logging.info(str(result[i]['id'])+','+result[i]['職缺']+','+str(m))
    cur.close()
    con.commit()
    con.close()
    logging.info('已更新查詢至1111')

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('1111 timer trigger function ran at %s', utc_timestamp)
