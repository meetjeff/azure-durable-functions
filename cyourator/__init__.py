# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.

import logging
import time,random
import requests
import json
import pymysql
from bs4 import  BeautifulSoup as bs
import cn2an
import nums_from_string as nfs
import os
from dotenv import load_dotenv

load_dotenv()

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
    else:
        Sal = round(i[0])
        return Sal

def annual(i):
    if i < 400000:
        return i*14
    else:
        return i

def main(ser:str) -> str:
    jdata=json.loads(ser)
    sp=jdata['search'].replace('、','&term[]=')
    rp=jdata['page']

    logging.info('Yourator Start Search')

    db_settings = {
        "host": os.getenv("dbip"),
        "port": int(os.getenv("dbport")),
        "user": os.getenv("dbuser"),
        "password": os.getenv("dbpassword")
    }
    conn = pymysql.connect(**db_settings)
    cursor = conn.cursor()
    
    for p in range(1,rp):
        res = requests.get("https://www.yourator.co/api/v2/jobs?term[]={}&sort=most_related&page={}".format(sp,p)).json()["jobs"]
        if res == []:
            break
        for n in range(len(res)):
            try:
                cursor.execute("insert career.newjob(id,job,location,lastupdate,company,annualsalary,salary,education,website) values('{}','{}','{}','{}','{}','{}','{}','不拘','yourator') on duplicate key update job=values(job),location=values(location),lastupdate=values(lastupdate),company=values(company),annualsalary=values(annualsalary),salary=values(salary),education=values(education),website=values(website);".format(res[n]["id"],res[n]["name"],res[n]["city"],res[n]["category"]["updated_at"],res[n]["company"]["brand"],annual(ave(SalNum(res[n]["salary"]))),res[n]["salary"]))
                conn.commit()
            except Exception:
                logging.info(str(p)+','+str(n))
            soup = bs(requests.get("https://www.yourator.co"+res[n]["path"]).text,'lxml')

            for i,e in enumerate(soup.select("h2.job-heading")):

                if e.text == "工作內容":
                    try:
                        l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("li")]
                        if l == []:
                           l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("p")]
                        j=json.dumps(l,ensure_ascii=False)
                        cursor.execute("update career.newjob set description='{}' where id='{}';".format(j,res[n]["id"]))
                        conn.commit()
                    except Exception as m:
                        logging.info(str(p)+','+str(n)+','+str(res[n]["id"])+','+res[n]["name"]+','+e.text+','+str(m))

                if e.text == "條件要求":
                    try:
                        l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("li")]
                        if l == []:
                            l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("p")]
                        j=json.dumps(l,ensure_ascii=False)
                        cursor.execute("update career.newjob set skill='{}' where id='{}';".format(j,res[n]["id"]))
                        conn.commit()
                    except Exception as m:
                        logging.info(str(p)+','+str(n)+','+str(res[n]["id"])+','+res[n]["name"]+','+e.text+','+str(m))

                # if e.text == "加分條件":
                #     try:
                #         l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("li")]
                #         if l == []:
                #             l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("p")]
                #         j=json.dumps(l,ensure_ascii=False)
                #         cursor.execute("update career.yourator set skilloption='{}' where id={};".format(j,res[n]["id"]))
                #         conn.commit()
                #     except Exception as m:
                #         logging.info(str(p)+','+str(n)+','+str(res[n]["id"])+','+res[n]["name"]+','+e.text+','+str(m))

                if e.text == "員工福利":
                    try:
                        l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("li")]
                        if l == []:
                            l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("p")]
                        j=json.dumps(l,ensure_ascii=False)
                        cursor.execute("update career.newjob set benefits='{}' where id='{}';".format(j,res[n]["id"]))
                        conn.commit()
                    except Exception as m:
                        logging.info(str(p)+','+str(n)+','+str(res[n]["id"])+','+res[n]["name"]+','+e.text+','+str(m))
            time.sleep(random.uniform(2, 4))
        time.sleep(random.uniform(5, 8))
    cursor.close()
    conn.close()
    logging.info(f"已新增 {jdata['search']} 職缺查詢至 Yourator ({jdata['page']} 頁) !")

    return f"已新增 {jdata['search']} 職缺查詢至 Yourator ({jdata['page']} 頁) !"
