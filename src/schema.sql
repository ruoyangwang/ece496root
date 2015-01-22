drop table if exists workers;
create table workers(
    name text primary key,
    maxNumberJobs integer,
    minEFT integer not null,
    maxEFT integer not null
);
