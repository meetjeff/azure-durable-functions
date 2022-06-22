# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import time,random
import requests
import json
import pymysql
from bs4 import  BeautifulSoup as bs

def main(ser :str) -> str:
    jdata=json.loads(ser)
    sp=jdata['search'].replace('、','&term[]=')
    rp=jdata['page']

    logging.info('Yourator Start Search')

    db_settings = {
        "host": "azsqltop.mysql.database.azure.com",
        "port": 3306,
        "user": "jeff",
        "password": "@a0987399832",
        "charset": "utf8"
    }
    conn = pymysql.connect(**db_settings)
    cursor = conn.cursor()
    
    for p in range(1,rp):
        res = requests.get("https://www.yourator.co/api/v2/jobs?term[]={}&sort=most_related&page={}".format(sp,p)).json()["jobs"]
        if res == []:
            break
        for n in range(len(res)):
            try:
                cursor.execute("insert career.yourator(id,job,location,lastupdate,company,salary) values({},'{}','{}','{}','{}','{}') on duplicate key update job=values(job),location=values(location),lastupdate=values(lastupdate),company=values(company),salary=values(salary);".format(res[n]["id"],res[n]["name"],res[n]["city"],res[n]["category"]["updated_at"],res[n]["company"]["brand"],res[n]["salary"]))
                conn.commit()
            except Exception:
                logging.info(str(p)+str(n))
            soup = bs(requests.get("https://www.yourator.co"+res[n]["path"]).text,'lxml')

            for i,e in enumerate(soup.select("h2.job-heading")):

                if e.text == "工作內容":
                    try:
                        l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("li")]
                        if l == []:
                           l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("p")]
                        j=json.dumps(l,ensure_ascii=False)
                        cursor.execute("update career.yourator set description='{}' where id={};".format(j,res[n]["id"]))
                        conn.commit()
                    except Exception as m:
                        logging.info(str(p)+str(n)+str(res[n]["id"])+res[n]["name"]+e.text+str(m))

                if e.text == "條件要求":
                    try:
                        l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("li")]
                        if l == []:
                            l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("p")]
                        j=json.dumps(l,ensure_ascii=False)
                        cursor.execute("update career.yourator set skill='{}' where id={};".format(j,res[n]["id"]))
                        conn.commit()
                    except Exception as m:
                        logging.info(str(p)+str(n)+str(res[n]["id"])+res[n]["name"]+e.text+str(m))

                if e.text == "加分條件":
                    try:
                        l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("li")]
                        if l == []:
                            l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("p")]
                        j=json.dumps(l,ensure_ascii=False)
                        cursor.execute("update career.yourator set skilloption='{}' where id={};".format(j,res[n]["id"]))
                        conn.commit()
                    except Exception as m:
                        logging.info(str(p)+str(n)+str(res[n]["id"])+res[n]["name"]+e.text+str(m))

                if e.text == "員工福利":
                    try:
                        l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("li")]
                        if l == []:
                            l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("p")]
                        j=json.dumps(l,ensure_ascii=False)
                        cursor.execute("update career.yourator set benefits='{}' where id={};".format(j,res[n]["id"]))
                        conn.commit()
                    except Exception as m:
                        logging.info(str(p)+str(n)+str(res[n]["id"])+res[n]["name"]+e.text+str(m))
            time.sleep(random.uniform(2, 4))
        time.sleep(random.uniform(5, 8))
    cursor.close()
    conn.close()
    logging.info(f"已新增 {jdata['search']} 職缺查詢至 Yourator ({jdata['page']} 頁) !")

    return f"已新增 {jdata['search']} 職缺查詢至 Yourator ({jdata['page']} 頁) !"
