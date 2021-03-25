#!/usr/bin/python3
# coding: utf-8

import pika
import numpy as np
from sklearn.datasets import load_diabetes
import json


X, y = load_diabetes(return_X_y=True)
print("Shape X: ", X.shape)
random_row = np.random.randint(0, X.shape[0]-1)

# Подключение к серверу на локальном хосте:
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Создадим очередь, с которой будем работать:
channel.queue_declare(queue='y_true')

# сериализуем y[random_row]
data = json.dumps(y[random_row])

# Опубликуем сообщение 
channel.basic_publish(exchange='',
                      routing_key='y_true',
                      body=data)
print('Сообщение с правильным ответом, отправлено в очередь')

# ==================
# Создадим очередь, с которой будем работать:
channel.queue_declare(queue='Features')

print('Shape X after used random_row:', X[random_row].shape)

# сериализуем X[random_row].
X_data = json.dumps(list(X[random_row]))

# Опубликуем сообщение
channel.basic_publish(exchange='',
                      routing_key='Features',
                      body=X_data)
print('Сообщение с признаками, отправлено в очередь')

# Закроем подключение 
connection.close()