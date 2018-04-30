create database nytimes;

CREATE TABLE customers(
`customer_id` int,
`name` varchar(255));

CREATE TABLE orders(
`order_id` int,
`quantity` int,
`order_date` DATE,
`customer_id` int);


insert into customers values(1,"COMPANY_A");
insert into customers values(2,'COMPANY_B');

insert into orders values
(1002,12,'2015-01-01',2),
(1003,8,'2015-01-01',1),
(1003,9,'2015-01-02',1),
(1003,62,'2015-01-02',2)
(1004,5,'2015-01-01',1);
