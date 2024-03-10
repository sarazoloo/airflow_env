from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

baseurl = 'http://zangia.mn/'

args = {
    'owner': 'Sara',
    'depends_on_past': False,
    'email': ['sarangoo@scs.mn'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def job_listing_scrape():
    def scrape_links(a):
        links = []
        for i in range(1, a):
            url = requests.get(f"http://zangia.mn/job/list/pg.{i}")
            soup = BeautifulSoup(url.text, 'html.parser')
            rows = soup.find('div', class_='list')
            row = rows.find_all('div', class_='ad')
            for item in row:
                for link in item.find_all('a', href=True):
                    full_link = baseurl + link['href']
                    links.append(full_link)
        return links

    def scrape_listing(joblinks):
        job_result = []
        for b in joblinks:
            time.sleep(20)
            url1 = requests.get(b)
            soup1 = BeautifulSoup(url1.text, 'html.parser')
            jobs = soup1.find('div', class_ = 'job-holder')
    
            if jobs is not None: # some of the jobs don't has a none type so I made an if statement
            
                if jobs.find('div', class_='name') is not None:
                    company = jobs.find('div', class_='name').get_text()
                else:
                    company = "Not included"
            
                for item in jobs.find_all('div', class_ = 'job-body'):
                    title = item.find('h3').text.split("/",1)[0] # this is splitting the string by "/" and getting the first element
                    divs = item.find_all('div', class_ = 'section') # there are also a few divs with the same class so this makes it easier to choose which div to get
                    main_responsibility = divs[0].get_text().strip().replace('Гүйцэтгэх үндсэн үүрэг','').replace('\n',' ')
                    requirements = divs[1].get_text().replace('Ажлын байранд тавигдах шаардлага', '').replace('\n',' ').replace("\xa0", " ").strip()[:2000]

            
                    details = item.find('div', class_ = 'details')
                    spans = details.find_all('span')
                
                    salary_range = jobs.find('div', class_ = 'salary')# this chose the last span
                    salary_min = salary_range.get_text().replace(",", '').replace("₮", '').split('-')[0].strip()
                    salary_max = salary_range.get_text().replace(",", '').replace("₮", '').split('-')[0].strip()
                
                    if (len(spans) >= 1):
                        location = spans[0].get_text().strip()
                    else:
                        location = "Not Included"
                    
                    if (len(spans) >= 2):
                        sector = spans[1].get_text().strip()
                    else:
                        sector = "Not included"
                    
                    if (len(spans) >= 3):
                        level = spans[2].get_text().strip()
                    else:
                        level = "Not included"
                
                    if (len(spans) >= 4):
                        hour_type = spans[3].get_text().strip()
                    else:
                        hour_type = "Not included"
                    
                    if (len(divs) >= 5):
                        phone_num = divs[4].find('span').get_text().split(',')[0].strip()
                    else:
                        phone_num = "Not included"
                
                    if (len(divs) >= 6):
                        uploaded_date = divs[5].find('span').get_text().split(' ')[0].strip()
                    elif (len(divs) >= 5):
                        uploaded_date = divs[4].find('span').get_text().split(',')[0].strip()
                    else:
                        uploaded_date = "Not included"
                
                
                    job_dict = {'Job_Title':title, 'Job_Description':main_responsibility,
                                'Job_Requirements':requirements,'Job_Sector':sector, 
                                'Salary_Min': salary_min, 'Salary_Max': salary_max, 'Location':location, 'Sector':sector, 
                                'Hour_type':hour_type, 'Phone_number':phone_num, 'Uploaded_date':uploaded_date} 
                    job_result.append(job_dict)
        
                    data = pd.DataFrame(job_result)
        return data

    
    links = scrape_links(200)
    df_links = pd.DataFrame(links)
    df_links.columns = ['links']
    df_links.to_csv("links.csv", encoding="utf-8")

    df_links = df_links.fillna(0)

    for i in range(0, len(df_links), 100):
        batch = df_links.iloc[i:100 + i,]
        print(f"""batch number {i} loading""")
        scraped_df = scrape_listing(batch["links"])
        print(f"""batch number {i} loaded""")
        now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        scraped_df.to_csv(f'Zangia listing - {now}.csv', index=False)

dag = DAG(
    dag_id='Scrape_zangai',
    default_args=args,
    catchup=False,
    schedule_interval="0 12 * * 1",
    start_date=pendulum.datetime(2024, 1, 25, 1, 0, 0, tz='Asia/Ulaanbaatar')
)

airflow_job = PythonOperator(task_id='Scrape_zangai', python_callable=job_listing_scrape, dag=dag)