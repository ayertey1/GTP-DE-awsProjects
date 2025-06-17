CREATE SCHEMA IF NOT EXISTS presentation;

CREATE TABLE IF NOT EXISTS presentation.weekly_avg_price (
    week DATE,
    avg_price DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS presentation.repeat_customers (
    user_id INT,
    total_bookings INT,
    is_repeat_customer INT 
);

CREATE TABLE IF NOT EXISTS presentation.popular_locations (
    cityname VARCHAR(100),
    num_bookings INT
);

CREATE TABLE IF NOT EXISTS presentation.occupancy_rate (
    month DATE,
    total_booked_nights INT,
    num_bookings INT,
    occupancy_rate DECIMAL(5,4)
);

CREATE TABLE IF NOT EXISTS presentation.avg_booking_duration (
    avg_booking_duration DECIMAL(5,2)
);

CREATE TABLE IF NOT EXISTS select * from presentation.top_performing_listings (
    apartment_id INT,
    total_revenue DECIMAL(12,2)
);