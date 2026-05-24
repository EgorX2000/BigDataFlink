create table fact_sales (
    sale_id serial primary key,
    sale_date varchar(255),
    customer_id int,
    seller_id int,
    product_id int,
    store_name varchar(255),
    supplier_name varchar(255),
    sale_quantity int,
    sale_total_price decimal
);