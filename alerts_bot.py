Отчет по ленте
Итак, пришло время автоматизировать базовую отчетность нашего приложения.  Давайте наладим автоматическую отправку аналитической сводки в телеграм каждое утро! Что нам для этого понадобится:

Создайте своего телеграм-бота с помощью @BotFather
Чтобы получить chat_id, воспользуйтесь ссылкой https://api.telegram.org/bot<токен_вашего_бота>/getUpdates  или методом bot.getUpdates()

Напишите скрипт для сборки отчета по ленте новостей. Отчет должен состоять из двух частей:

текст с информацией о значениях ключевых метрик за предыдущий день
график с значениями метрик за предыдущие 7 дней
Отобразите в отчете следующие ключевые метрики: 

DAU 
Просмотры
Лайки
CTR
Автоматизируйте отправку отчета с помощью Airflow. Код для сборки отчета разместите в GitLab, для этого: 

Клонируете репозиторий
В локальной копии внутри папки dags создаёте свою папку — она должна совпадать по названию с вашим именем пользователя, которое через @ в профиле GitLab
Создаёте там DAG — он должен быть в файле с форматом .py
Запушиваете результат
Включаете DAG, когда он появится в Airflow
Отчет должен приходить ежедневно в 11:00 в чат. 

import telegram
import pandas as pd
import pandahouse as ph
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import os
import sys
from datetime import date, datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 't-pozharskaja',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 12),
}

# Интервал запуска DAG (каждый день в 11:00)
schedule_interval = '0 11 * * *'

connection = {'host': 'https://clickhouse.lab.karpov.courses',                 
               'database':'simulator_20230520',
                'user':'student',
                'password':'dpo_python_2020'
              }

chat_id = -938659451 # id канала   
#chat_id = 797978378   
my_token = '5981322866:AAE-nxcLJKljsaeR5WC00bYHTdmMGeg8duA' # мой токен 
bot = telegram.Bot(token=my_token) # получаем доступ

        

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def bot_pozharskaya_1():
    @task()
    # посчитаем ключевые метрики за предыдущий день
    def extract_1_day():
        query_1 = ''' 
          select toDate(time) as date,
                 count(distinct user_id) as DAU, 
                 sum(action = 'view') as views,
                 sum(action = 'like') as likes,
                 countIf(action='like') / countIf(action='view') as CTR
          from simulator_20230520.feed_actions
          where toDate(time) = yesterday()
          group by toDate(time)
          '''
        df = ph.read_clickhouse(query_1, connection=connection)
        return df
        
        
    @task() 
    # соберем данные для отчета 
    def report_text(df, chat_id=None):
        dau = df['DAU'].sum()
        views = df['views'].sum()
        likes = df['likes'].sum()
        ctr = df['CTR'].sum()
        
        msg = '-' * 20 + '\n\n' + f'Статистика ленты новостей за вчера:\n\nDAU: {dau}\nПросмотры: {views}\nЛайки: {likes}\nCTR: {ctr:.2f}\n' + '-' * 20 + '\n'
        return msg
    
        chat_id = chat_id or 797978378
        bot.sendMessage(chat_id=chat_id, text=msg)
    
    @task()   
    # посчитаем ключевые метрики за предыдущие 7 дней
    def extract_7_day():
        query_7 = ''' 
          select toDate(time) as date,
                 count(distinct user_id) as DAU, 
                 sum(action = 'view') as views,
                 sum(action = 'like') as likes,
                 countIf(action='like') / countIf(action='view') as CTR
          from simulator_20230520.feed_actions
          where toDate(time) between today() - 7 and yesterday()
          group by toDate(time)
          '''
        df_week = ph.read_clickhouse(query_7, connection=connection)
        return df_week
    
    @task()
    # сформируем график
    def report_chart(df_week, chat_id=None):
        
        fig, axes = plt.subplots(2, 2, figsize=(20, 14))
        fig.suptitle('Динамика показателей за последние 7 дней', fontsize=30)
        sns.lineplot(ax = axes[0, 0], data = df_week, x = 'date', y = 'DAU')
        axes[0, 0].set_title('DAU')
        axes[0, 0].grid()
        sns.lineplot(ax = axes[0, 1], data = df_week, x = 'date', y = 'CTR')
        axes[0, 1].set_title('CTR')
        axes[0, 1].grid()
        sns.lineplot(ax = axes[1, 0], data = df_week, x = 'date', y = 'views')
        axes[1, 0].set_title('Просмотры')
        axes[1, 0].grid()
        sns.lineplot(ax = axes[1, 1], data = df_week, x = 'date', y = 'likes')
        axes[1, 1].set_title('Лайки')
        axes[1, 1].grid()
                       
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Metrics for last 7 days.png'
        plt.close()
        return plot_object
    
        chat_id = chat_id or 797978378
        bot.sendPhoto(chat_id=chat_id, photo=photo)   
        
   
        
        
    df = extract_1_day()
    df_week = extract_7_day()
    msg = report_text(df)
    report_text(df, chat_id)
    report_chart(df_week, chat_id)
    
    
bot_pozharskaya_1 = bot_pozharskaya_1()



