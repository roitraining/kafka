create user 'python'@'localhost' identified by 'python';
grant all privileges on *.* to 'python'@'localhost' with grant option;

create database stocks;
use stocks;
create table trades
(
kafka_key binary(16)
,kafka_timestamp bigint(32)
,event_time datetime
,symbol varchar(5)
,price decimal(13,2)
,quantity int
);
grant all on trades to 'python'@'localhost';
create view trades2 as
 select LOWER(CONCAT(
     SUBSTR(HEX(kafka_key), 1, 8), '-',
     SUBSTR(HEX(kafka_key), 9, 4), '-',
     SUBSTR(HEX(kafka_key), 13, 4), '-',
     SUBSTR(HEX(kafka_key), 17, 4), '-',
     SUBSTR(HEX(kafka_key), 21)
   )) as kafka_key, kafka_timestamp, event_time, symbol, price, quantity from trades;
grant all on trades2 to 'python'@'localhost';

