import csv
import json
import time
import os
import glob
import re
from kafka import KafkaProducer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "pet_store_sales")
DATA_DIR = os.getenv("DATA_DIR", "/data")

DELAY = float(os.getenv("SEND_DELAY", "0.01"))


def wait_for_kafka(retries: int = 30, interval: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            )
            print(f"[producer] Подключились к Kafka ({KAFKA_BOOTSTRAP})")
            return producer
        except Exception as e:
            print(f"[producer] Попытка {attempt}/{retries}: Kafka недоступна — {e}")
            time.sleep(interval)
    raise RuntimeError("Не удалось подключиться к Kafka")


def get_file_index(filename: str) -> int:
    match = re.search(r'\((\d+)\)', filename)
    if match:
        return int(match.group(1))
    return 0


def collect_csv_files(data_dir: str) -> list[str]:
    files = glob.glob(os.path.join(data_dir, "*.csv"))
    result = sorted(files)
    print(f"[producer] Найдено CSV-файлов: {len(result)}")
    for f in result:
        print(f"  - {os.path.basename(f)}")
    return result


def send_csv_to_kafka(producer: KafkaProducer, csv_path: str, topic: str, file_index: int) -> int:
    offset = file_index * 1000
    count = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            message = {k: v for k, v in row.items() if k}

            try:
                original_id = int(message.get("id", 0))
                message["id"] = str(original_id + offset)
                message["sale_customer_id"] = str(int(message.get("sale_customer_id", 0)) + offset)
                message["sale_seller_id"] = str(int(message.get("sale_seller_id", 0)) + offset)
                message["sale_product_id"] = str(int(message.get("sale_product_id", 0)) + offset)
            except (ValueError, TypeError) as e:
                print(f"[producer] Предупреждение: ошибка конвертации ID: {e}")

            producer.send(topic, value=message)
            count += 1
            if DELAY > 0:
                time.sleep(DELAY)

    producer.flush()
    return count


def main():
    print(f"[producer] Запуск. Kafka={KAFKA_BOOTSTRAP}, Topic={TOPIC}, Data={DATA_DIR}")
    producer = wait_for_kafka()

    csv_files = collect_csv_files(DATA_DIR)
    if not csv_files:
        print("[producer] CSV-файлы не найдены! Проверьте директорию /data")
        return

    total = 0
    for csv_path in csv_files:
        filename = os.path.basename(csv_path)
        file_index = get_file_index(filename)
        count = send_csv_to_kafka(producer, csv_path, TOPIC, file_index)
        total += count
        print(f"[producer] {filename} (offset={file_index * 1000}): отправлено {count} сообщений")

    print(f"[producer] Готово! Всего отправлено {total} сообщений в топик '{TOPIC}'")
    producer.close()


if __name__ == "__main__":
    main()
