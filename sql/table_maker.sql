create table container (
    code varchar(32) primary key,
    type varchar(32) not null
);
create table ship (
    name varchar(32) primary key,
    company varchar(32) not null
);
create table import_information (
  id bigserial primary key,
  city varchar(32) not null,
  time date,
  tax numeric(20, 7) not null
);
create table export_information (
  id bigserial primary key,
  city varchar(32) not null,
  time date,
  tax numeric(20, 7) not null
);
create table delivery_courier (
  phone_number varchar(32) primary key,
  name varchar(32) not null,
  gender char(1) not null,
  age int not null,
  company varchar(32) not null
);
create table delivery_information (
    id bigserial primary key,
    city varchar(32) not null,
    finish_time date,
    courier_phone_number varchar(32) references delivery_courier(phone_number)
);
create table retrieval_courier (
  phone_number varchar(32) primary key,
  name varchar(32) not null,
  gender char(1) not null,
  age int not null,
  company varchar(32) not null
);
create table retrieval_information (
    id bigserial primary key,
    city varchar(32) not null,
    start_time date not null,
    courier_phone_number varchar(32) references retrieval_courier(phone_number) not null
);
create table item (
    name varchar(32) primary key,
    type varchar(32) not null,
    price int not null,
    container_code varchar(32) references container(code),
    ship_name varchar(32) references ship(name),
    import_information_id int references import_information(id) not null,
    export_information_id int references export_information(id) not null,
    delivery_information_id int4 references delivery_information(id) not null,
    retrieval_information_id int4 references retrieval_information(id) not null,
    log_time timestamp not null
);