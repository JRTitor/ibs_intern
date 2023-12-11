from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime

def load_fights(flight_path:str='flight_delays.csv') -> pd.DataFrame:
    fight_delays = pd.read_csv(flight_path, encoding='cp1251')
    return fight_delays

def load_moths(months_path:str='Month.csv') -> pd.DataFrame:
    Month_table = pd.read_csv(months_path, header=None, encoding='cp1251', sep=';', names = ['code', 'Month'])
    return Month_table

def merge_data_by_months(fight_delays:pd.DataFrame, Month_table:pd.DataFrame) -> pd.DataFrame:
    # fight_delays = load_fights(flight_path)
    # Month_table = load_moths(months_path)

    merged_df = fight_delays.merge(Month_table, left_on='Month', right_on='code', how='left')
    merged_df.Month_x = merged_df.Month_y
    merged_df.drop(['code', 'Month_y'], axis=1, inplace=True)
    merged_df.rename(columns={'Month_x': 'Month'}, inplace=True)

    return merged_df

def print_table(merged_df:pd.DataFrame) -> None:
    print(merged_df)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 11),
    'retries': 1
}

dag = DAG('flight_delay_analysis', default_args=default_args, schedule=None)

load_flight_delays_task = PythonOperator(
    task_id='load_flight_delays_data',
    python_callable=load_moths,
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
    # provide_context=True, provide_context is deprecated as of 2.0 and is no longer required
    dag=dag
)

print_result_task = PythonOperator(
    task_id='print_result_df',
    python_callable=print_table,
    # provide_context=True, provide_context is deprecated as of 2.0 and is no longer required
    dag=dag
)

load_flight_delays_task >> merge_data_task
load_month_data_task >> merge_data_task
merge_data_task >> print_result_task
