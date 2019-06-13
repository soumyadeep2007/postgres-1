--
-- parallel grouping sets
--

-- test data sources
create table gstest(c1 int, c2 int, c3 int) with (parallel_workers = 4);

insert into gstest select 1,10,100 from generate_series(1,10)i;
insert into gstest select 1,10,200 from generate_series(1,10)i;
insert into gstest select 1,20,30 from generate_series(1,10)i;
insert into gstest select 2,30,40 from generate_series(1,10)i;
insert into gstest select 2,40,50 from generate_series(1,10)i;
insert into gstest select 3,50,60 from generate_series(1,10)i;
insert into gstest select 1,NULL,0 from generate_series(1,10)i;
analyze gstest;

SET parallel_tuple_cost=0;
SET parallel_setup_cost=0;
SET max_parallel_workers_per_gather=4;

-- test for hashagg
set enable_hashagg to on;
explain (costs off, verbose)
select c1, c2, avg(c3) from gstest group by grouping sets((c1,c2),(c1));
select c1, c2, avg(c3) from gstest group by grouping sets((c1,c2),(c1)) order by 1,2,3;

explain (costs off, verbose)
select c1, c2, c3, avg(c3) from gstest group by grouping sets((c1,c2),(c1),(c2,c3));
select c1, c2, c3, avg(c3) from gstest group by grouping sets((c1,c2),(c1),(c2,c3)) order by 1,2,3,4;


-- test for groupagg
set enable_hashagg to off;
explain (costs off, verbose)
select c1, c2, avg(c3) from gstest group by grouping sets((c1,c2),(c1));
select c1, c2, avg(c3) from gstest group by grouping sets((c1,c2),(c1)) order by 1,2,3;

explain (costs off, verbose)
select c1, c2, c3, avg(c3) from gstest group by grouping sets((c1,c2),(c1),(c2,c3));
select c1, c2, c3, avg(c3) from gstest group by grouping sets((c1,c2),(c1),(c2,c3)) order by 1,2,3,4;


drop table gstest;
