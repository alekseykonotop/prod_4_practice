#!/usr/bin/python3
# coding: utf-8

import pika
import pickle
import numpy as np
import requests

from sklearn import linear_model

# Скачаем сначала нашу обученную модель
# PKL_URL = 'https://lms.skillfactory.ru/assets/courseware/v1/d4d921964d6f845735270713f07ab4fe/asset-v1:Skillfactory+DST-12+11MAR2020+type@asset+block/myfile.pkl'
# res = requests.get(PKL_URL, stream=True)
# res.raise_for_status()
# with open('myfile.pkl', 'wb') as pkl_file:
#     for chunk in res.iter_content(1024):
#         pkl_file.write(chunk)


# Прочтем скачанный файл с обученной моделью из модуля 1 
with open('myfile.pkl', 'rb') as pkl_file:
    regressor = pickle.load(pkl_file)

# Подключимся к серверу:
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Укажем, с какой очередью будем работать:
channel.queue_declare(queue='Features')


# Напишем функцию, определяющую отправку предсказанных значений в очередь y_true.
def sending_data_to_queue(data, queue_name):
    """
    Отправим данные в нужную очередь, а перед этим
    сериализуем их в объект JSON.
    """
    # Укажем, с какой очередью будем работать:
    channel.queue_declare(queue=queue_name)
    # сериализуем y[random_row]
    data = json.dumps(data)

    # Опубликуем сообщение
    channel.basic_publish(exchange='',
                        routing_key=queue_name,
                        body=data)
    print('Сообщение с предсказанным ответом, отправлено в очередь y_pred')


# Напишем функцию, определяющую, как работать с полученным сообщением:
def callback(ch, method, properties, body):
    print(f'Получен вектор признаков {body}')
    X = json.loads(body)
    y_pred = regressor.predict(X)
    print("Получили предсказание модели.")
    sending_data_to_queue(y_pred, 'y_pred')



# Зададим правила чтения из очереди, указанной в параметре queue:
# on_message_callback показывает какую функцию вызвать при получении сообщения
channel.basic_consume(
    queue='Features', on_message_callback=callback, auto_ack=True)
print('...Ожидание сообщений, для выхода нажмите CTRL+C')

# Запустим чтение очереди.
channel.start_consuming()