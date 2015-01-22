drop table if exists workers;
create table workers(
    name text primary key,
    maxNumberJobs integer,
    minEFT integer not null,
    maxEFT integer not null
);

insert into workers values("M1", 1, 20, 20);
insert into workers values("M2", 2, 30, 30);
insert into workers values("M3", 3, 35, 35);

insert into workers values("testnode1", 2, 0, 100);
insert into workers values("testnode2", 4, 0, 100);
