create table if not exists container (
    code varchar(32) primary key,
    type varchar(32) not null
);
create table if not exists ship (
    name varchar(32) primary key,
    company varchar(32) not null
);
create table if not exists item (
    name varchar(32) primary key,
    type varchar(32) not null,
    price int not null,
    container_code varchar(32) references container(code),
    ship_name varchar(32) references ship(name),
    log_time timestamp not null
);
create table if not exists import_information (
  item varchar(32) primary key references item(name),
  city varchar(32) not null,
  time date,
  tax numeric(20, 7) not null
);
create table if not exists export_information (
  item varchar(32) primary key references item(name),
  city varchar(32) not null,
  time date,
  tax numeric(20, 7) not null
);
create table if not exists delivery_courier (
  phone_number varchar(32) primary key,
  name varchar(32) not null,
  gender char(1) not null,
  birth_year int not null,
  company varchar(32) not null
);
create table if not exists delivery_information (
    item varchar(32) primary key references item(name),
    city varchar(32) not null,
    finish_time date,
    courier_phone_number varchar(32) references delivery_courier(phone_number)
);
create table if not exists retrieval_courier (
  phone_number varchar(32) primary key,
  name varchar(32) not null,
  gender char(1) not null,
  birth_year int not null,
  company varchar(32) not null
);
create table if not exists retrieval_information (
    item varchar(32) primary key references item(name),
    city varchar(32) not null,
    start_time date not null,
    courier_phone_number varchar(32) references retrieval_courier(phone_number) not null
);