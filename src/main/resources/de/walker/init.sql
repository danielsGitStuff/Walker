create table if not exists walk(
    id integer primary key autoincrement,
    dir text not null
);

create table if not exists walkfiles(
    id integer primary key autoincrement,
    walkid integer not null,
    path text,
    name text not null,
    ext text not null,
    hash text not null,
    foreign key (walkid) references walk (id)
);

create index ientry on walkfiles(hash, walkid, ext);
