drop table if exists house;
drop table if exists door;

create table if not exists house (
    id integer primary key autoincrement,
    name text
);

create table if not exists door (
    id integer primary key autoincrement,
    id_house integer not null,
    hinges integer not null,
    foreign key (id_house) references house(id)
);