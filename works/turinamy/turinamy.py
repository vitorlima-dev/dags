import os
import pendulum
import pandas as pd
from os.path import join
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

local_tz = pendulum.timezone("America/Fortaleza")

@dag(
    schedule = None,
    start_date =  pendulum.datetime(2023, 6, 2, tz=local_tz),
    catchup = False )
def turinamy():

    @task
    def start():
         return True

    @task
    def extract(start: bool) -> str:
            data_inicio = datetime.now()
            data_fim = data_inicio + timedelta(days=7)

            data_inicio = data_inicio.strftime('%Y-%m-%d')
            data_fim = data_fim.strftime('%Y-%m-%d')
            
            city = 'Boston'
            key = 'T7N4KZPJLVV39QMHQK7Z6Y7Y4'
            URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                        f'{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv')            
            
            filename = f'/mnt/datatemp/turinamy_{datetime.today().strftime("%Y-%m-%d")}.csv'
            
            df = pd.read_csv(URL)
            df.to_csv(filename, index = False)
            return filename

    @task
    def load(filename: str) -> str:
        hook = S3Hook('aws_conn') 
        hook.load_file ( bucket_name = 'turinamy', filename = filename, key = f'dataset_{datetime.today().strftime("%Y-%m-%d")}')
        return filename

    @task 
    def end(filename: str):
        os.remove(filename)

    transform = end(load(extract(start())))

turinamy()