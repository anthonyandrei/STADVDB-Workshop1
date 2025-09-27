/*
Group members:
Ana Victoria R. Angat
Ramon John L. Dela Cruz
Nelson Darwin A. Lii
Anthony Andrei C. Tan
*/

describe taxi_payment_details;
describe taxi_route_details;
describe zone_lookup;

select * from taxi_payment_details limit 20;
select * from taxi_route_details limit 20;
select * from zone_lookup limit 20;

-- Create indices for trip ids
create index trip_index_payment on taxi_payment_details(tripID);
create index trip_route_index on taxi_route_details(tripID);

-- Make tripID column in taxi_route_details to type INT
ALTER TABLE taxi_route_details MODIFY COLUMN tripID INT;


CREATE TABLE denormalized_taxi (
	tripID int primary key,
	RatecodeID bigint,
	store_and_fwd_flag varchar(45),
	payment_type bigint,
	fare_amount float,
	extra float,
	mta_tax float,
	tip_amount float,
	tolls_amount float,
	improvement_surcharge float,
	total_amount float,
	congestion_surcharge float,
	Airport_fee float,
	cbd_congestion_fee float,
    VendorID int,
	tpep_pickup_datetime varchar(45),
	tpep_dropoff_datetime varchar(45),
	passenger_count	bigint,
	trip_distance float,
	PUborough varchar(45),
	PUzone varchar(45),
	PUservice_zone varchar(45),
	DOborough varchar(45),
	DOzone varchar(45),
	DOservice_zone varchar(45)
);

insert into denormalized_taxi
select 
	tpd.tripID, 
    tpd.RatecodeID, 
    tpd.store_and_fwd_flag, 
    tpd.payment_type, 
    tpd.fare_amount, 
    tpd.extra,
    tpd.mta_tax,
    tpd.tip_amount,
    tpd.tolls_amount,
    tpd.improvement_surcharge,
    tpd.total_amount,
    tpd.congestion_surcharge,
    tpd.Airport_fee,
    tpd.cbd_congestion_fee,
    trd.VendorID,
    trd.tpep_pickup_datetime,
    trd.tpep_dropoff_datetime,
    trd.passenger_count,
    trd.trip_distance,
    zl1.borough as 'PUborough',
    zl1.zone as 'PUzone',
    zl1.service_zone as 'PUservice_zone',
    zl2.borough as 'DOborough',
    zl2.zone as 'DOzone',
    zl2.service_zone as 'DOservice_zone'
from taxi_payment_details tpd
join taxi_route_details trd on tpd.tripID = trd.tripID
join zone_lookup zl1 on zl1.location_id = PULocationID
join zone_lookup zl2 on zl2.location_id = DOLocationID;

describe denormalized_taxi;

-- Which vendor got the most trips per month?


-- Are taxis earning more if they have more passengers?
/*
We can determine this using pearson correlation.
*/
select 
	sum((passenger_count - psgr_avg) * (total_amount - amt_avg))
    /
    sqrt(sum(pow(passenger_count - psgr_avg, 2)) * sum(pow(total_amount - amt_avg, 2)))
    as pearson_corr
from denormalized_taxi, 
(select avg(passenger_count) as psgr_avg from denormalized_taxi) x,
(select avg(total_amount) as amt_avg from denormalized_taxi) y; 
/*
Given that the result is of the pearson correlation is 0.05852764063427896, we can say that there is *no* correlation between the amount taxis earn and their passenger count.
*/

-- Count the number of trips per vendor per month per pickup location?


-- What are the peak hours per vendor per month?


-- What is the top mode of payment per pickup location?


-- QUESTIONS PER MEMBER:
-- Ana: What is the least common pickup and dropoff borough for each vendor?
-- Create indices for faster query
create index vendor_pu on denormalized_taxi(vendorid, puborough);
create index vendor_do on denormalized_taxi(vendorid, doborough);
-- drop index vendor_pu on denormalized_taxi;
-- drop index vendor_do on denormalized_taxi;

with pickup as (
	select VendorID, PUborough, count(*) as pu_count, rank() over (partition by vendorid order by count(*) asc) as pu_rank
    from denormalized_taxi
    group by VendorID, PUborough
),
dropoff as (
	select vendorid, DOborough, count(*) as do_count, rank() over (partition by vendorid order by count(*) asc) as do_rank
    from denormalized_taxi
    group by vendorid, DOborough
)
select p.vendorid, puborough, pu_count, doborough, do_count
from pickup p
join dropoff d on p.vendorid = d.vendorid
where pu_rank = 1 and do_rank = 1;

-- Ramon:


-- Nelson:


-- Andrei:

