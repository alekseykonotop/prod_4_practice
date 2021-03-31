#!/usr/bin/python3
# coding: utf-8

import pika
import json
import os
from sklearn.metrics import mean_squared_error as mse
import numpy as np
import math

y_true_lst = []
y_pred_lst = []
y_true_dict = {}
y_pred_dict = {}

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='y_true')
channel.queue_declare(queue='y_predict')

def calc_rmse(id_data):
    mse_metric = mse(y_true_lst, y_pred_lst)
    rmse = np.round(math.sqrt(mse_metric), 1)

    # Запись метрики в файл
    with open('./logs/metrics_log.txt', 'a') as log:
        log.write(f"{id_data}: RMSE метрика обновлена= {rmse}\n")
    print(f"{id_data}: RMSE метрика записана в metrics_log.txt")


def callback(ch, method, properties, body):
    y_value, un_id = json.loads(body)
    
    if method.routing_key == 'y_true':
        if un_id not in y_true_dict:
            y_true_dict[un_id] = y_value

    if method.routing_key == 'y_predict':
        if un_id not in y_pred_dict:
            y_pred_dict[un_id] = y_value

    message = f"{un_id}: Из очереди {method.routing_key}, получено значение: [ {np.round(y_value, 1)} ]\n'


    # Запись сообщения в файл
    with open('./logs/labels_log.txt', 'a') as log:
        log.write(message +'\n')
    print(f"{un_id}: Сообщение от {method.routing_key} записано в labels_log.txt")

    # Добавление значений в y_true_lst и y_prerdict_lst если получено оба значения
    if un_id in y_true_dict and un_id in y_pred_dict:
        y_true_lst.append(y_true_dict.pop(un_id))
        y_pred_lst.append(y_pred_dict.pop(un_id))

        # пересчет метрики RMSE
        calc_rmse(un_id)

channel.basic_consume(
    queue='y_predict', on_message_callback=callback, auto_ack=True)

channel.basic_consume(
    queue='y_true', on_message_callback=callback, auto_ack=True)

print('...Ожидание сообщений, для выхода нажмите CTRL+C')
channel.start_consuming()

