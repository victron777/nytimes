select c.name, DATE_FORMAT(max(o.order_date), 'Y-MM-dd') order_date  from customers c inner join orders o ON c.customer_id = o.customer_id
group by c.name;
