alter table fact_sales add constraint fk_customer foreign key(customer_id) references dim_customers(customer_id);
alter table fact_sales add constraint fk_seller foreign key(seller_id) references dim_sellers(seller_id);
alter table fact_sales add constraint fk_product foreign key(product_id) references dim_products(product_id);
alter table fact_sales add constraint fk_store foreign key(store_name) references dim_stores(store_name);
alter table fact_sales add constraint fk_supplier foreign key(supplier_name) references dim_suppliers(supplier_name);