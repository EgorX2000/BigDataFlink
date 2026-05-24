# Инструкция по запуску и проверке стримингового пайплайна

### 1. Подготовка инфраструктуры
Разверните стек (PostgreSQL, Kafka, Flink JobManager/TaskManager, Producer) с помощью Docker Compose:
```bash
docker-compose up -d --build
```
Инициализация схем в PostgreSQL происходит автоматически (скрипты в `./sql`). Сырые данные загружаются в Kafka-топик `sales_topic` в формате JSON автоматически при старте контейнера `producer`, который эмулирует источник данных, последовательно считывая 10 CSV-файлов из папки `./исходные данные`.

### 2. Запуск Flink-джоб
Запуск стриминговой джобы автоматизирован и производится через контейнер-сабмиттер, который отправляет код на выполнение в Flink-кластер.

**Логика работы компонентов:**
    *   **`flink.py`**: Считывает JSON-сообщения из топика Kafka, трансформирует их на лету в модель «Звезда» и записывает в СУБД PostgreSQL.
    *   **`producer.py`**: Эмулирует поток данных, считывая 10 файлов по 1000 строк и отправляя их в Kafka. Ход отправки можно отслеживать в логах контейнера:
        ```bash
        docker-compose logs -f producer
        ```

### 3. Верификация данных
Для проверки результатов используйте DBeaver:
*   **PostgreSQL:** `localhost:5445` (Database: `bigdata_flink`, User: `user`, Password: `password`) — проверка таблиц фактов и измерений.
*   **Контрольные показатели:**
    *   Таблица фактов `fact_sales` должна содержать ровно `10000` записей (сгенерированных автоматически через последовательность `SERIAL`).
    *   Таблицы измерений `dim_customers`, `dim_sellers`, `dim_products` и др. должны содержать около `1000` уникальных записей (за счет логики дедупликации UPSERT).
*   **Наложение связей (Схема «Звезда»):** После завершения загрузки выполните скрипт (продублирован в `./sql/foreign_keys.sql`) в DBeaver для создания физических связей и построения ER-диаграммы (изначально внешние ключи не создаются, чтобы исключить deadlockи и ошибки при асинхронной запииси в таблицы):
    ```sql
    ALTER TABLE fact_sales ADD CONSTRAINT fk_customer FOREIGN KEY(customer_id) REFERENCES dim_customers(customer_id);
    ALTER TABLE fact_sales ADD CONSTRAINT fk_seller FOREIGN KEY(seller_id) REFERENCES dim_sellers(seller_id);
    ALTER TABLE fact_sales ADD CONSTRAINT fk_product FOREIGN KEY(product_id) REFERENCES dim_products(product_id);
    ALTER TABLE fact_sales ADD CONSTRAINT fk_store FOREIGN KEY(store_name) REFERENCES dim_stores(store_name);
    ALTER TABLE fact_sales ADD CONSTRAINT fk_supplier FOREIGN KEY(supplier_name) REFERENCES dim_suppliers(supplier_name);
    ```