import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

def extract(**kwargs):
    
    url = kwargs['url']
    ti = kwargs['ti']
    response = requests.get("https://monark.com.pk/products/mnk24-07wt?variant=40177525784705")
    ti.xcom_push(value=response.text , key="html_response" )
    ti.log.info("html response : " ,response.text)

def transform(**kwargs):
    
    ti = kwargs['ti']
    html_reponse = ti.xcom_pull(key="html_response")
    ti.log.info("html response : " , len(html_reponse))
    
    html = BeautifulSoup(html_reponse)
    price_div = html.find('.ts4-price')
    ti.log.info("price div : " , price_div)
    
    
with DAG(
    'price_wice_dag',
    default_args={ 'retries' : 2 },
    schedule_interval="* * * * *",
    start_date= pendulum.datetime(2025, 2, 19),
    catchup=False,
    tags=['example' , 'price-wice']
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
        op_kwargs={'url' : 'https://monark.com.pk/products/mnk24-07wt?variant=40177525719169'}
    )
    
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
    )
    
    extract_task >> transform_task
    