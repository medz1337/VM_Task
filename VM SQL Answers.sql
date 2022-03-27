
--1. How many total orders were there in the dataset?
SELECT 
count(distinct o.uuid) totalorders
FROM 
orders o;

--2. How many orders were from each channel (requestedFrom column is what shows the channel)
SELECT 
o.requestedfrom channel,
count(distinct o.uuid) totalorders
FROM 
orders o
group by 1
order by 2 desc;

--3. How many items were sold for each hour of the day for each tenant?
SELECT 
o.exttenantuuid tenant,
to_char(o.createdat, 'hh24') hour24,
count(oi.item_uuid) totalitems

FROM 
orders o
join order_items oi on oi.order_uuid=o.uuid

group by 1,2
order by 1,2 desc;

--4. What were the top 5 items sold for each tenant?
SELECT * FROM (
	SELECT 
 	o.exttenantuuid tenant,
	i.item_uuid,
	i.name,
	count(i.item_uuid) totalsolditems,
	rank() over (partition by o.exttenantuuid order by count(o.uuid) desc) rank
	
	
	FROM 
	orders o
	join order_items oi on oi.order_uuid=o.uuid
	join items i on i.item_uuid = oi.item_uuid
	
	
	group by 1,2,3
	order by 1, rank asc
	) z
WHERE z.rank <=5;


--5. What were the items for each tenant that were sold more than 5 of?
SELECT 
o.exttenantuuid tenant,
i.item_uuid,
i.name,
count(i.item_uuid) totalsolditems

FROM 
orders o
join order_items oi on oi.order_uuid=o.uuid
join items i on i.item_uuid = oi.item_uuid

group by 1,2,3

HAVING
count(i.item_uuid) > 5
order by 1,4 desc;

--6. Which order UUIDs had multiples of the same bundle?
SELECT 
z.uuid,
count(distinct z.item_uuid) NmbrItemsSoldMultipleTimes

from (
	SELECT 
	o.uuid,
	i.item_uuid,
	count(i.item_uuid) totalsolditems

	FROM 
	orders o
	join order_items oi on oi.order_uuid=o.uuid
	join items i on i.item_uuid = oi.item_uuid
	group by 1,2
	having count(i.item_uuid)>1
	) z
group by 1