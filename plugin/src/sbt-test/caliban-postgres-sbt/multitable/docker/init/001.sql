\connect product

insert into public.product(upc, name, price, weight) values ('1', 'Table', 899, 100);
insert into public.product(upc, name, price, weight) values ('2', 'Couch', 1299, 1000);
insert into public.product(upc, name, price, weight) values ('3', 'Chair', 54, 50);

insert into public.orders(id, buyer, created_at) values ('71a4419d-14ad-44ef-8194-4bece18c116f', 'Rune', '2021-10-26T11:03:07+0000');

insert into public.lineitems(order_id, product_upc, amount) VALUES ('71a4419d-14ad-44ef-8194-4bece18c116f', '1', 1);
insert into public.lineitems(order_id, product_upc, amount) VALUES ('71a4419d-14ad-44ef-8194-4bece18c116f', '2', 3);