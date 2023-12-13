'''
Сергей Черных

Прошу прощния, я пропустил звонок из-за невнимательности,
самостоятельно не разобрался с файловой системой airflow,
как задать пути и залить на сервер localhost airflow файлы.csv,
чтобы посмотреть на http://localhost:8080/ как всё красиво работает

буду благодарен, если порекомендуете статьи, где это просто и
быстро описанно

здесь я использовал глоблаьные переменные, чтобы не открывать всё по новой и код выглядел логичнее
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
from datetime import timedelta

table1 = None
table2 = None
merged_table = None

'''
1) Загружает данные из таблицы public.flight_delays (или файла flight_delays.csv)
'''
def load_fights(flight_path:str='flight_delays.csv') -> None:
    fight_delays = pd.read_csv(flight_path, encoding='cp1251')
    table1 =  fight_delays


'''
2) Загружает данные из файла Month.csv
'''
def load_moths(months_path:str='Month.csv') -> None:
    Month_table = pd.read_csv(months_path, header=None, encoding='cp1251', sep=';', names = ['code', 'Month'])
    table2 =  Month_table


'''
3) Выполняет merge по столбцам Month и month_code
'''
def merge_data_by_months() -> None:
    fight_delays = table1
    Month_table = table2
    merged_df = fight_delays.merge(Month_table, left_on='Month', right_on='code', how='left')
    merged_df.Month_x = merged_df.Month_y
    merged_df.drop(['code', 'Month_y'], axis=1, inplace=True)
    merged_df.rename(columns={'Month_x': 'Month'}, inplace=True)
    merged_table = merged_df


'''
4) Вывести результат функцией print
'''
def print_table() -> None:
    print(merged_table)


def convert_merged_to_xml() -> None:
    merged_table.to_xml('merged_df.xml') ## сохранится там же, где и скрипт .py так как я не разобрался с файловой системой airflow

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 12),
    'retries': 1
}

dag = DAG('flight_delay_analysis', default_args=default_args, schedule=timedelta(days=1))

load_flight_delays_task = PythonOperator(
    task_id='load_flight_delays_data',
    python_callable=load_fights,
    dag=dag
)


load_month_data_task = PythonOperator(
    task_id='load_month_data',
    python_callable=load_moths,
    dag=dag
)

merge_data_task = PythonOperator(
    task_id='merge_flight_delays_with_month',
    python_callable=merge_data_by_months,
    dag=dag
)

print_result_task = PythonOperator(
    task_id='print_result_df',
    python_callable=print_table,
    dag=dag
)

convert_xml_task = PythonOperator(
    task_id='save_xml_result',
    python_callable=convert_merged_to_xml,
    dag=dag
)


'''
описываю поток
'''
load_flight_delays_task >> merge_data_task
load_month_data_task >> merge_data_task
merge_data_task >> print_result_task
print_result_task >> convert_xml_task