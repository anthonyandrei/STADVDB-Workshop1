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
