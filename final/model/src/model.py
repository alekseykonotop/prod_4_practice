#!/usr/bin/python3
# coding: utf-8

import pika
import json
import pickle
import numpy as np


# Чтение файла с обученной моделью
with open('myfile.pkl', 'rb') as pkl_file:
    regressor = pickle.load(pkl_file)

try:
    # Подключимся к серверу:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    # Укажем, с какими очередями будем работать:
    channel.queue_declare(queue='Features')
    channel.queue_declare(queue='y_predict')

    # Напишем функцию, определяющую, как работать с полученным сообщением:
    def callback(ch, method, properties, body):
        features, un_id = json.loads(body)
        print(f"{un_id}: Получен вектор признаков {body}")
        pred = regressor.predict(np.array(features).reshape(1, -1))

        # Опубликуем сообщение
        channel.basic_publish(exchange='',
                            routing_key='y_predict',
                            body=json.dumps([pred[0], un_id]))
        print(f"{un_id}: Предсказание с {pred[0]}, отправлено в очередь y_predict")

    # Зададим правила чтения из очереди, указанной в параметре queue:
    # on_message_callback показывает какую функцию вызвать при получении сообщения
    channel.basic_consume(
        queue='Features', on_message_callback=callback, auto_ack=True)
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')

    # Запустим чтение очереди.
    channel.start_consuming()

except:
    print(f"Не удалось подключиться к очереди")
