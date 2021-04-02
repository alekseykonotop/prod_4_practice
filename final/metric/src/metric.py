#!/usr/bin/python3
# coding: utf-8

import pika
import json
import os
import numpy as np
import math

from sklearn.metrics import mean_squared_error as mse

y_true_lst = []
y_pred_lst = []
y_true_dict = {}
y_pred_dict = {}

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='y_true')
channel.queue_declare(queue='y_predict')

def calc_rmse():
    mse_metric = mse(y_true_lst, y_pred_lst)
    rmse = np.round(math.sqrt(mse_metric), 1)
    
    # Запись метрики в файл
    message = f"RMSE метрика обновлена: [ {rmse} ]"
    with open('./logs/metrics_log.txt', 'a') as log:
        log.write(message + '\n')
    print(f"RMSE метрика записана в metrics_log.txt")


def callback(ch, method, properties, body):
    y_value, pair_id = json.loads(body)
    
    if method.routing_key == 'y_true':
        if pair_id not in y_true_dict:
            y_true_dict[pair_id] = y_value

    if method.routing_key == 'y_predict':
        if pair_id not in y_pred_dict:
            y_pred_dict[pair_id] = y_value

    # Запись сообщения в файл
    message = f"{pair_id}: Из очереди {method.routing_key}, получено значение: [ {np.round(y_value, 1)} ]'
    with open('./logs/labels_log.txt', 'a') as log:
        log.write(message +'\n')
    print(f"{pair_id}: Сообщение от {method.routing_key} записано в labels_log.txt")

    # Добавим y_true и y_predict в списки значений, если уже имеем оба значения с одинаковым id пары   
    if pair_id in y_true_dict and pair_id in y_pred_dict:
        y_true_lst.append(y_true_dict.pop(pair_id))
        y_pred_lst.append(y_pred_dict.pop(pair_id))

        #  обновим метрику RMSE на новых данных
        calc_rmse()

channel.basic_consume(
    queue='y_predict', on_message_callback=callback, auto_ack=True)

channel.basic_consume(
    queue='y_true', on_message_callback=callback, auto_ack=True)

print('...Ожидание сообщений, для выхода нажмите CTRL+C')
channel.start_consuming()

