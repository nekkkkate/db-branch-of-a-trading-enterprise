create extension if not exists postgres_fdw;


create server if not exists west foreign data wrapper postgres_fdw
    options (host 'postgres', dbname 'filial_west', port '5436');
create user  mapping if not exists for postgres server west
    options(user 'postgres', password 'password');

create server if not exists east foreign data wrapper postgres_fdw
    options (host 'postgres', dbname 'filial_east', port '5436');
create user mapping if not exists for postgres server east
    options(user 'postgres', password 'password');

create schema if not exists filial_west;
create schema if not exists filial_east;

import foreign schema public from server west into filial_west;
import foreign schema public from server east into filial_east;
