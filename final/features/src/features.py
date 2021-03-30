#!/usr/bin/python3
# coding: utf-8

import pika
import json
import numpy as np
import time
from sklearn.datasets import load_diabetes
import uuid


X, y = load_diabetes(return_X_y=True)

# сгенерируем уникальный код пары признаки-правильный ответ
un_id = str(uuid.uuid4())

while True:
    try:
        random_row = np.random.randint(0, X.shape[0]-1)

        # Подключение к серверу:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()

        # Создание очередей, с которыми будем работать:
        channel.queue_declare(queue='Features')
        channel.queue_declare(queue='y_true')


        # Опубликуем сообщение c признаками
        channel.basic_publish(exchange='',
                                routing_key='Features',
                                body=json.dumps([list(X[random_row]), un_id]))
        print('Сообщение с вектором признаков, отправлено в очередь')

        # Опубликуем сообщение c правильным ответом
        channel.basic_publish(exchange='',
                                routing_key='y_true',
                                body=json.dumps([y[random_row], un_id]))
        print('Сообщение с правильным ответом, отправлено в очередь')

        # Закроем подключение 
        connection.close()
        
        # иммитация задержки
        time.sleep(2)
    except:
        print(f"Не удалось подключиться к очереди")