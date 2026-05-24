import csv
import json
import time
import glob
from kafka import KafkaProducer


def get_producer():
    for _ in range(15):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Успешное подключение к Kafka!")
            return producer
        except Exception as e:
            print("Ожидание Kafka...")
            time.sleep(5)
    raise Exception("Не удалось подключиться к Kafka")


if __name__ == "__main__":
    producer = get_producer()
    TOPIC_NAME = 'sales_topic'

    csv_files = glob.glob('/app/data/MOCK_DATA*.csv')

    if not csv_files:
        print("ВНИМАНИЕ: Файлы MOCK_DATA*.csv не найдены в /app/data!")
    else:
        print(f"Найдено файлов для отправки: {len(csv_files)}")

        global_sale_id = 1

        for file in csv_files:
            print(f"Чтение файла: {file}")
            with open(file, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row['id'] = global_sale_id
                    global_sale_id += 1

                    producer.send(TOPIC_NAME, row)
                    time.sleep(0.01)

        producer.flush()
        print("Все данные успешно отправлены в Kafka!")
