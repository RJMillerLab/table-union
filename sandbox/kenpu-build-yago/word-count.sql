drop table if exists words_count;
create table words_count as
select entity, count(*) as words_count
from words
group by entity;
