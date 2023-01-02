create table if not exists users ( id uuid primary key, name text, email text, age int);
create or replace view names as select distinct(name) from users;
create or replace view emails as select distinct(email) from users;
