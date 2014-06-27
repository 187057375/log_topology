

create table goods_pv (
goods_id int(32) NOT NULL,
pv int(11),
uv int(11),
store_pv int(11),
store_uv int(11),
UNIQUE KEY `UK_goods_id` (`goods_id`)
)engine=innodb DEFAULT CHARSET=utf8mb4;


create table goods_pv_total_tmp (
goods_id int(32) NOT NULL,
pv int(11),
uv int(11),
store_pv int(11),
store_uv int(11),
UNIQUE KEY `UK_goods_id` (`goods_id`)
)engine=innodb DEFAULT CHARSET=utf8mb4;


create table goods_pv_total (
goods_id int(32) NOT NULL,
pv int(11),
uv int(11),
store_pv int(11),
store_uv int(11),
UNIQUE KEY `UK_goods_id` (`goods_id`)
)engine=innodb DEFAULT CHARSET=utf8mb4;


DELETE FROM goods_pv_total WHERE 1;


INSERT INTO goods_pv_total (goods_id, pv, uv, store_pv, store_uv)
select 
goods_id,
sum(pv),
sum(uv),
sum(store_pv),
sum(store_uv)
from
(
select *
from goods_pv_total_tmp
union all 
select *
from goods_pv
) b
group by goods_id;


DELETE FROM goods_pv_total_tmp WHERE 1;


INSERT INTO goods_pv_total_tmp (goods_id, pv, uv, store_pv, store_uv)
select * from goods_pv_total;
