#!/bin/bash
# Запуск Flink-джобы внутри jobmanager контейнера
set -e

echo "[1/3] Проверяем JAR-коннекторы..."
ls /opt/flink/lib/flink-sql-connector-kafka*.jar
ls /opt/flink/lib/flink-connector-jdbc*.jar
ls /opt/flink/lib/postgresql*.jar

echo "[2/3] Ждем 30 сек пока всё поднимется..."
sleep 30

echo "[3/3] Запускаем джобу..."
cd /opt/flink-job
python3 flink_star_schema.py
