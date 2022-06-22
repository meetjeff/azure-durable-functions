import datetime
import logging

import azure.functions as func
import time,random
import requests
import json
import pymysql
from bs4 import  BeautifulSoup as bs


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    db_settings = {
        "host": "azsqltop.mysql.database.azure.com",
        "port": 3306,
        "user": "jeff",
        "password": "@a0987399832",
        "charset": "utf8"
    }
    conn = pymysql.connect(**db_settings)
    cursor = conn.cursor()

    for p in range(1,10):
        res = requests.get("https://www.yourator.co/api/v2/jobs?category[]={}%20%2F%20{}&sort=recent_updated&page={}".format('資料工程','機器學習',p)).json()["jobs"]
        if res == []:
            pstop = p
            break
        for n in range(len(res)):
            try:
                cursor.execute("insert career.yourator(id,job,location,lastupdate,company,salary) values({},'{}','{}','{}','{}','{}') on duplicate key update job=values(job),location=values(location),lastupdate=values(lastupdate),company=values(company),salary=values(salary);".format(res[n]["id"],res[n]["name"],res[n]["city"],res[n]["category"]["updated_at"],res[n]["company"]["brand"],res[n]["salary"]))
                conn.commit()
            except Exception:
                logging.info("1")
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
                        logging.info(str(m))

                if e.text == "條件要求":
                    try:
                        l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("li")]
                        if l == []:
                            l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("p")]
                        j=json.dumps(l,ensure_ascii=False)
                        cursor.execute("update career.yourator set skill='{}' where id={};".format(j,res[n]["id"]))
                        conn.commit()
                    except Exception as m:
                        logging.info(str(m))

                if e.text == "條件要求":
                    try:
                        l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("li")]
                        if l == []:
                            l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("p")]
                        j=json.dumps(l,ensure_ascii=False)
                        cursor.execute("update career.yourator set skill='{}' where id={};".format(j,res[n]["id"]))
                        conn.commit()
                    except Exception as m:
                        logging.info(str(m))

                if e.text == "加分條件":
                    try:
                        l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("li")]
                        if l == []:
                            l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("p")]
                        j=json.dumps(l,ensure_ascii=False)
                        cursor.execute("update career.yourator set skilloption='{}' where id={};".format(j,res[n]["id"]))
                        conn.commit()
                    except Exception as m:
                        logging.info(str(m))

                if e.text == "員工福利":
                    try:
                        l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("li")]
                        if l == []:
                            l=[t.text.replace("'","").replace("\n","。") for t in soup.select("section.content__area")[i].select("p")]
                        j=json.dumps(l,ensure_ascii=False)
                        cursor.execute("update career.yourator set benefits='{}' where id={};".format(j,res[n]["id"]))
                        conn.commit()
                    except Exception as m:
                        logging.info(str(m))
            time.sleep(random.uniform(2, 4))
        time.sleep(random.uniform(5, 8))
    cursor.close()
    conn.close()
    logging.info('已更新'+str(pstop)+'頁查詢至Yourator')


    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
