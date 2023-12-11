'''
Сергей Черных

Прошу прощния, я пропустил звонок из-за невнимательности,
самостоятельно не разобрался с файловой системой airflow,
как задать пути и залить на сервер localhost airflow файлы.csv,
чтобы посмотреть на http://localhost:8080/ как всё красиво работает

буду благодарен, если порекомендуете статьи, где это просто и
быстро описанно 
'''
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
from datetime import timedelta


'''
1) Загружает данные из таблицы public.flight_delays (или файла flight_delays.csv)
'''
def load_fights(flight_path:str='flight_delays.csv') -> pd.DataFrame:
    fight_delays = pd.read_csv(flight_path, encoding='cp1251')
    return fight_delays


'''
2) Загружает данные из файла Month.csv
'''
def load_moths(months_path:str='Month.csv') -> pd.DataFrame:
    Month_table = pd.read_csv(months_path, header=None, encoding='cp1251', sep=';', names = ['code', 'Month'])
    return Month_table


'''
3) Выполняет merge по столбцам Month и month_code
'''
def merge_data_by_months(flight_path:str='flight_delays.csv', months_path:str='Month.csv') -> pd.DataFrame:
    fight_delays = load_fights(flight_path)
    Month_table = load_moths(months_path)
    merged_df = fight_delays.merge(Month_table, left_on='Month', right_on='code', how='left')
    merged_df.Month_x = merged_df.Month_y
    merged_df.drop(['code', 'Month_y'], axis=1, inplace=True)
    merged_df.rename(columns={'Month_x': 'Month'}, inplace=True)
    return merged_df


'''
4) Вывести результат функцией print
'''
def print_table(flight_path:str='flight_delays.csv', months_path:str='Month.csv') -> None:
    print(merge_data_by_months(flight_path, months_path))


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


'''
описываю поток
'''
load_flight_delays_task >> merge_data_task
load_month_data_task >> merge_data_task
merge_data_task >> print_result_task

