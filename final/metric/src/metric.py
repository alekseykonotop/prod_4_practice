#!/usr/bin/python3
# coding: utf-8

import pika
import json
import os
from sklearn.metrics import mean_squared_error as mse
import numpy as np

y_true_lst = []
y_prerdict_lst = []
y_true_dict = {}
y_prerdict_dict = {}

try:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='y_true')
    channel.queue_declare(queue='y_predict')

    def calc_rmse():
        mse_metric = mse(y_true_lst, y_prerdict_lst)
        rmse = np.round(math.sqrt(mse), 4)

        # Запись метрики в файл
        with open('./logs/metrics_log.txt', 'a') as log:
            log.write(f"RMSE метрика= {rmse}" +'\n')
        print(f"RMSE метрика записана в metrics_log.txt")

    
    def callback(ch, method, properties, body):
        y_value, un_id = json.loads(body)
        
        if method.routing_key == 'y_true':
            if un_id not in y_true_dict:
                y_true_dict[un_id] = y_value

        if method.routing_key == 'y_predict':
            if un_id not in y_predict_dict:
                y_predict_dict[un_id] = y_value

        message = f'Из очереди {method.routing_key} получено значение {y_value}'


        # Запись сообщения в файл
        with open('./logs/labels_log.txt', 'a') as log:
            log.write(message +'\n')
        print(f"Сообщение от {method.routing_key} записано в labels_log.txt")

        # Добавление значений в y_true_lst и y_prerdict_lst если получено оба значения
        if un_id in y_true_dict and un_id in y_prerdict_dict:
            y_true_lst.append(y_true_dict.pop(un_id))
            y_prerdict_lst.append(y_prerdict_dict.pop(un_id))

            # пересчет метрики RMSE
            calc_rmse()

    channel.basic_consume(
        queue='y_predict', on_message_callback=callback, auto_ack=True)

    channel.basic_consume(
        queue='y_true', on_message_callback=callback, auto_ack=True)

    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()

except:
    print(f"Не удалось подключиться к очереди")
