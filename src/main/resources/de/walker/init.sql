create table if not exists walk
(
    id  integer primary key autoincrement,
    dir text not null
);

create table if not exists walkfiles
(
    id       integer primary key autoincrement,
    walkid   integer not null,
    path     text,
    name     text    not null,
    size     integer,
    ext      text,
    hash     text,
    modified int     not null,
    created  int     not null,
    foreign key (walkid) references walk (id)
);

create index if not exists ientry on walkfiles (hash, walkid, ext);

create table if not exists walkfile_exceptions
(
    id            integer primary key autoincrement,
    id_file_entry integer not null,
    clazz         text    not null,
    message       text,
    stacktrace    text,
    foreign key (id_file_entry) references walkfiles (id) on delete cascade
)