select b.customer, b.order_date, max(b.total_difference) total_difference
from
    (select a.name customer, DATE_FORMAT(a.order_date, 'Y-MM-dd') order_date, 
    lag((sum(a.quantity) over (partition by a.cust_date_key order by a.order_date)),1) over (partition by a.name order by a.customer_id, a.order_date) - sum(a.quantity) over (partition by a.cust_date_key order by a.order_date) as total_difference
    from( 
        select c.customer_id customer_id, c.name name, o.order_id, DATE_FORMAT(o.order_date, 'Y-MM-dd') order_date, 
        concat(c.customer_id,'-',order_date) cust_date_key,
        o.quantity
        from customers c inner join orders o ON
        c.customer_id = o.customer_id
        ) a
    )b
where b.total_difference is not null and b.total_difference <> 0
group by b.customer, b.order_date
