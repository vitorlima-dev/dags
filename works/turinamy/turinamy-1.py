from airflow.models import DAG
import pandas as pd
from os.path import join
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pendulum


local_tz = pendulum.timezone("America/Fortaleza")

with DAG (
    dag_id = 'turinamy-1',
    start_date =  pendulum.datetime(2023, 4, 24, tz=local_tz),
    schedule_interval = '50 18 * * *'
) as dag:
        

        t1 = DummyOperator(
            task_id = "start"
        )

        t2 = BashOperator(
            task_id = "create_folder",
            bash_command = f'mkdir -p /mnt/datalake/turinamy/dataset_{datetime.today().strftime("%Y-%m-%d")}'      
        )
                            
        def saveWeatherData():
            # intervalo de datas
            data_inicio = datetime.now()
            data_fim = data_inicio + timedelta(days=7)

            # formatando as datas
            data_inicio = data_inicio.strftime('%Y-%m-%d')
            data_fim = data_fim.strftime('%Y-%m-%d')

            city = 'Boston'
            key = 'T7N4KZPJLVV39QMHQK7Z6Y7Y4'

            URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                        f'{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv')

            dados = pd.read_csv(URL)

            file_path = f'/mnt/datalake/turinamy/dataset_{datetime.today().strftime("%Y-%m-%d")}/'

            dados.to_csv(file_path + 'dados_brutos.csv')
            dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
            dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')

        t3 = PythonOperator(
            task_id = 'executer',
            python_callable = saveWeatherData
        )

        t4 = DummyOperator(
            task_id = "stop"
        )

        t1 >> t2 >> t3 >> t4