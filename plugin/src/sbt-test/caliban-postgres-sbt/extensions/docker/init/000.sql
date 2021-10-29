\connect product

create table public.product(
                               upc text primary key,
                               name text,
                               price integer,
                               weight integer
);

create table public.orders(
                              id uuid primary key,
                              created_at timestamptz not null,
                              buyer text not null
);

create table public.lineitems(
    order_id uuid not null references public.orders(id),
    product_upc text not null references public.product(upc),
    amount int not null
);

