# Databricks notebook source
# DBTITLE 1,Set Catalog and Schema to Deploy Tables + Data
catalog = "hive_metastore"
schema = "default"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# DBTITLE 1,Create Baggage Table + Data
spark.sql(""" 
CREATE TABLE baggage (
    baggage_id INT,
    ticket_id INT,
    weight FLOAT,
    baggage_type STRING
);          
""")

spark.sql("""
INSERT INTO baggage (baggage_id, ticket_id, weight, baggage_type)
VALUES
    (1, 1, 23.50, 'Checked'),
    (3, 3, 18.00, 'Checked'),
    (4, 4, 5.50, 'Carry-on'),
    (2, 2, 7.00, 'Checked on different flight');
""")

# COMMAND ----------

# DBTITLE 1,Create the Flights Table + Data
spark.sql("""
CREATE TABLE flights (
    flight_id INT,
    flight_number STRING,
    airline STRING,
    origin STRING,
    destination STRING,
    departure_time TIMESTAMP,
    arrival_time TIMESTAMP,
    status STRING,
    ingestion_datetime TIMESTAMP
)
""")

# Insert data into the flights table
spark.sql("""
INSERT INTO flights (flight_id, flight_number, airline, origin, destination, departure_time, arrival_time, status, ingestion_datetime)
VALUES
    (1, 'AA101', 'American Airlines', 'JFK', 'LAX', '2025-04-01T08:00:00.000+00:00', '2025-04-01T11:30:00.000+00:00', 'Scheduled', '2025-03-29T00:36:21.245+00:00'),
    (2, 'DL202', 'Delta Airlines', 'ATL', 'ORD', '2025-04-02T14:00:00.000+00:00', '2025-04-02T16:15:00.000+00:00', 'Scheduled', '2025-03-29T00:36:21.245+00:00'),
    (3, 'UA303', 'United Airlines', 'SFO', 'SEA', '2025-04-03T18:00:00.000+00:00', '2025-04-03T19:45:00.000+00:00', 'Delayed', '2025-03-29T00:36:21.245+00:00'),
    (4, 'SW404', 'Southwest', 'LAS', 'DEN', '2025-04-04T10:00:00.000+00:00', '2025-04-04T12:00:00.000+00:00', 'Cancelled', '2025-03-29T00:36:21.245+00:00')
""")

# COMMAND ----------

# DBTITLE 1,Create the Passengers Table + Data
spark.sql("""
CREATE TABLE passengers (
    passenger_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    dob DATE,
    nationality STRING
)
""")

# Insert data into the passengers table
spark.sql("""
INSERT INTO passengers (passenger_id, first_name, last_name, email, phone, dob, nationality)
VALUES
    (1, 'John', 'Doe', 'john.doe@example.com', '123456', '1985-06-15', 'USA'),
    (2, 'Alice', 'Smith', 'alice.smith@example.com', '987-654-3210', '1992-11-20', 'UK'),
    (3, 'Bob', 'Johnson', 'bob.j@example.com', '456-789-0123', '1978-03-05', 'Canada'),
    (4, 'Emily', 'Davis', 'emily.d@example.com', '321-654-0987', '2000-01-10', 'Australia')
""")

# COMMAND ----------

# DBTITLE 1,Create the Tickets Table + Data
# Create the tickets table
spark.sql("""
CREATE TABLE tickets (
    ticket_id INT,
    passenger_id INT,
    flight_id INT,
    seat_number STRING,
    ticket_price FLOAT,
    booking_time TIMESTAMP,
    status STRING
)
""")

# Insert data into the tickets table
spark.sql("""
INSERT INTO tickets (ticket_id, passenger_id, flight_id, seat_number, ticket_price, booking_time, status)
VALUES
    (1, 1, 1, '12A', 300.00, '2025-03-15T10:30:00.000+00:00', 'Confirmed'),
    (2, 2, 2, '15B', 250.50, '2025-03-20T09:45:00.000+00:00', 'Checked-in'),
    (3, 3, 3, '9C', 150.00, '2025-03-22T14:00:00.000+00:00', 'Confirmed'),
    (4, 4, 4, '22D', 180.00, '2025-03-25T16:15:00.000+00:00', 'Cancelled')
""")
