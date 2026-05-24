from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

kafka_source_ddl = """
create table kafka_sales (
    id int,
    customer_first_name string, customer_last_name string, customer_age int,
    customer_email string, customer_country string, customer_postal_code string,
    customer_pet_type string, customer_pet_name string, customer_pet_breed string,
    seller_first_name string, seller_last_name string, seller_email string,
    seller_country string, seller_postal_code string,
    product_name string, product_category string, product_price decimal, product_quantity int,
    sale_date string, sale_customer_id int, sale_seller_id int, sale_product_id int,
    sale_quantity int, sale_total_price decimal,
    store_name string, store_location string, store_city string, store_state string,
    store_country string, store_phone string, store_email string,
    pet_category string, product_weight decimal, product_color string, product_size string,
    product_brand string, product_material string, product_description string,
    product_rating decimal, product_reviews int, product_release_date string,
    product_expiry_date string, supplier_name string, supplier_contact string,
    supplier_email string, supplier_phone string, supplier_address string,
    supplier_city string, supplier_country string
) with (
    'connector' = 'kafka',
    'topic' = 'sales_topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-group',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'scan.startup.mode' = 'earliest-offset'
)
"""
t_env.execute_sql(kafka_source_ddl)


def create_jdbc_sink_dim(table_name, schema, pk):
    return f"""
    create table {table_name} (
        {schema},
        primary key ({pk}) not enforced
    ) with (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
        'table-name' = '{table_name}',
        'username' = 'user',
        'password' = 'password',
        'sink.buffer-flush.max-rows' = '1000',
        'sink.buffer-flush.interval' = '1s'
    )
    """


fact_sales_ddl = """
create table fact_sales (
    sale_date string,
    customer_id int,
    seller_id int,
    product_id int,
    store_name string,
    supplier_name string,
    sale_quantity int,
    sale_total_price decimal
) with (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'fact_sales',
    'username' = 'user',
    'password' = 'password',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '1s'
)
"""

t_env.execute_sql(create_jdbc_sink_dim(
    "dim_customers", "customer_id int, first_name string, last_name string, age int, email string, country string, postal_code string, pet_type string, pet_name string, pet_breed string", "customer_id"))
t_env.execute_sql(create_jdbc_sink_dim(
    "dim_sellers", "seller_id int, first_name string, last_name string, email string, country string, postal_code string", "seller_id"))
t_env.execute_sql(create_jdbc_sink_dim("dim_products", "product_id int, product_name string, product_category string, pet_category string, product_price decimal, product_weight decimal, product_color string, product_size string, product_brand string, product_material string, product_rating decimal, product_reviews int", "product_id"))
t_env.execute_sql(create_jdbc_sink_dim(
    "dim_stores", "store_name string, store_location string, store_city string, store_state string, store_country string, store_phone string, store_email string", "store_name"))
t_env.execute_sql(create_jdbc_sink_dim(
    "dim_suppliers", "supplier_name string, supplier_contact string, supplier_email string, supplier_phone string, supplier_address string, supplier_city string, supplier_country string", "supplier_name"))
t_env.execute_sql(fact_sales_ddl)

statement_set = t_env.create_statement_set()
statement_set.add_insert_sql(
    "insert into dim_customers select sale_customer_id, customer_first_name, customer_last_name, customer_age, customer_email, customer_country, customer_postal_code, customer_pet_type, customer_pet_name, customer_pet_breed from kafka_sales")
statement_set.add_insert_sql(
    "insert into dim_sellers select sale_seller_id, seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code from kafka_sales")
statement_set.add_insert_sql(
    "insert into dim_products select sale_product_id, product_name, product_category, pet_category, product_price, product_weight, product_color, product_size, product_brand, product_material, product_rating, product_reviews from kafka_sales")
statement_set.add_insert_sql(
    "insert into dim_stores select store_name, store_location, store_city, store_state, store_country, store_phone, store_email from kafka_sales")
statement_set.add_insert_sql(
    "insert into dim_suppliers select supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country from kafka_sales")
statement_set.add_insert_sql(
    "insert into fact_sales select sale_date, sale_customer_id, sale_seller_id, sale_product_id, store_name, supplier_name, sale_quantity, sale_total_price from kafka_sales")

statement_set.execute().wait()
