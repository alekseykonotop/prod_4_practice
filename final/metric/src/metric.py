#!/usr/bin/python3
# coding: utf-8

import pika
import json
import os
import math
import csv
import numpy as np
import pandas as pd

from sklearn.metrics import mean_squared_error as mse


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='y_true')
channel.queue_declare(queue='y_predict')

def calc_rmse():
    # Read csv-files
    pred_df = pd.read_csv('./results/y_predict.csv', delimiter=';', header=None)
    true_df = pd.read_csv('./results/y_true.csv', delimiter=';', header=None)
    pred_df.columns = ['uid', 'value']
    true_df.columns = ['uid', 'value']

    # Concatenate dataframes
    df_calc = pd.merge(true_df, pred_df, on='uid', suffixes=['_true', '_pred'])
    
    # Calculate rmse
    rmse = np.round(mse(df_calc['value_true'], df_calc['value_pred'], squared=False), 2)
    print(f"RMSE: {rmse}")

    # Write metrics value in file
    message = f"RMSE метрика обновлена: [ {rmse} ]"
    with open('./results/metrics.txt', 'a') as txt_file:
        txt_file.write(message + '\n')
    print(f"RMSE метрика записана в metrics.txt")


def callback(ch, method, properties, body):
    y_value, pair_id = json.loads(body)

    # write data to csv file
    with open(f"./results/{method.routing_key}.csv", mode='a') as csv_file:
        writer = csv.writer(csv_file, delimiter=';')
        writer.writerow([pair_id, y_value])

    # write log to file
    message = f"{pair_id}: Из очереди {method.routing_key}, получено знач.: [ {np.round(y_value, 1)} ]"
    with open('./logs/labels_log.txt', 'a') as log_file:
        log_file.write(message +'\n')
    print(f"{pair_id}: Сообщение от {method.routing_key} записано в labels_log.txt")

    # обновим метрику RMSE на новых данных
    calc_rmse()

channel.basic_consume(
    queue='y_predict', on_message_callback=callback, auto_ack=True)

channel.basic_consume(
    queue='y_true', on_message_callback=callback, auto_ack=True)

print('...Ожидание сообщений, для выхода нажмите CTRL+C')
channel.start_consuming()

