-- Create the reviews_table
CREATE TABLE staging.reviews (
  "review" INT NOT NULL PRIMARY KEY,
  "product_id" INT NOT NULL
);

-- Create the orders_table
CREATE TABLE staging.orders (
  "order_id" INT NOT NULL PRIMARY KEY,
  "customer_id" INT NOT NULL,
  "order_date" date NOT NULL,
  "product_id" INT NOT NULL,
  "unit_price" INT NOT NULL,
  "quantity" INT NOT NULL,
  "amount" INT NOT NULL,
  FOREIGN KEY ("order_id") REFERENCES staging.shipment_deliveries("order_id")
);

-- Create shipments_deliveries
CREATE TABLE staging.shipment_deliveries (
  "shipment_id" INT NOT NULL PRIMARY KEY,
  "order_id" INT NOT NULL,
  "shipment_date" date NULL,
  "delivery_date" date NULL,
  FOREIGN KEY ("order_id") REFERENCES staging.orders("order_id")
);
