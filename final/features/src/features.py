#!/usr/bin/python3
# coding: utf-8

import pika
import json
import numpy as np
import time
from sklearn.datasets import load_diabetes


X, y = load_diabetes(return_X_y=True)
# print("Shape X: ", X.shape)

ind = 0

while ind < 100:
    ind += 1
    try:
        random_row = np.random.randint(0, X.shape[0]-1)

        # Подключение к серверу на локальном хосте:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Создадим очереди, с которыми будем работать:
        channel.queue_declare(queue='Features')
        channel.queue_declare(queue='y_true')


        # Опубликуем сообщение c признаками
        channel.basic_publish(exchange='',
                              routing_key='Features',
                              body=json.dumps(list(X[random_row]))
        print('Сообщение с вектором признаков, отправлено в очередь')

        # Опубликуем сообщение c правильным ответом
        channel.basic_publish(exchange='',
                              routing_key='y_true',
                              body=json.dumps(list(y[random_row]))
        
        print('Сообщение с правильным ответом, отправлено в очередь')

        # Закроем подключение 
        connection.close()
        
        # иммитация задержки
        time.sleep(2)
    except:
        print(f"Не удалось подключиться к очереди")