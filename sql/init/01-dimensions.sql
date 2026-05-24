create table dim_customers (
    customer_id int primary key,
    first_name varchar(255),
    last_name varchar(255),
    age int,
    email varchar(255),
    country varchar(255),
    postal_code varchar(255),
    pet_type varchar(255),
    pet_name varchar(255),
    pet_breed varchar(255)
);

create table dim_sellers (
    seller_id int primary key,
    first_name varchar(255),
    last_name varchar(255),
    email varchar(255),
    country varchar(255),
    postal_code varchar(255)
);

create table dim_products (
    product_id int primary key,
    product_name varchar(255),
    product_category varchar(255),
    pet_category varchar(255),
    product_price decimal,
    product_weight decimal,
    product_color varchar(255),
    product_size varchar(255),
    product_brand varchar(255),
    product_material varchar(255),
    product_rating decimal,
    product_reviews int
);

create table dim_stores (
    store_name varchar(255) primary key,
    store_location varchar(255),
    store_city varchar(255),
    store_state varchar(255),
    store_country varchar(255),
    store_phone varchar(255),
    store_email varchar(255)
);

create table dim_suppliers (
    supplier_name varchar(255) primary key,
    supplier_contact varchar(255),
    supplier_email varchar(255),
    supplier_phone varchar(255),
    supplier_address varchar(255),
    supplier_city varchar(255),
    supplier_country varchar(255)
);