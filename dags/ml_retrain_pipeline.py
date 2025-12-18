import os
import requests
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

def train_model():
    print("Модель обучена")

def evaluate_model() -> bool:
    # Проверяем качество работы "модели"
    metric = float(os.getenv("METRIC", 0.9))
    ok = (metric > 0.85)
    
    print("Модель оценена, метрики", "в норме" * ok + "ниже" * (not ok))
    return ok

def choose_path(**_):
    # в случае низкого качества, высылаем сообщение об ошибке и прекращаем пайплайн
    return "deploy_model" if evaluate_model() else "notify_fail"

def deploy_model():
    print("Модель выведена в продакшен")
    
def notify_success():
    MODEL_VERSION = os.getenv("MODEL_VERSION", "<placeholder>")
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    
    message = f"Новая модель <b>{MODEL_VERSION}</b> успешно развернута"
    requests.get(f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}")
    
def notify_fail():
    MODEL_VERSION = os.getenv("MODEL_VERSION", "")
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    
    message = f"Новая модель <b>{MODEL_VERSION}</b> не развернута. Метрики ниже порога"
    requests.get(f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}")

with DAG(
    dag_id="ml_retrain_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    train = PythonOperator(task_id="train_model", python_callable=train_model)
    
    gate = BranchPythonOperator(
        task_id="evaluate_and_branch",
        python_callable=choose_path,
    )
    
    deploy = PythonOperator(task_id="deploy_model", python_callable=deploy_model)
    notify1 = PythonOperator(task_id="notify_success",python_callable=notify_success)
    notify2 = PythonOperator(task_id="notify_fail",python_callable=notify_fail)

    # используем ветвление для случаев прохода порога и нет 
    train >> gate 
    gate >> deploy >> notify1
    gate >> notify2