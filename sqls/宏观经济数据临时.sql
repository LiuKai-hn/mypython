select f_level_one_code, f_level_one_name, f_level_two_code, f_level_two_name, f_level_thr_code, f_level_thr_name, f_code, f_name, f_code_from_ld_code
from ods.hgjj_mic_index_con_df
where dt='2023-07-06' and f_level_two_name = '人口与就业';



show partitions ods.hgjj_mic_index_con_df;
-- 2292833
select f_level_one_name
from ods.hgjj_mic_index_con_df
where dt='2023-07-07' and f_level_one_name='民营经济发展指标'
group by f_level_one_name
;

select count(1) from ads.hgjj_macro_eco_index_do;


insert overwrite table ads.hgjj_macro_eco_index_do
select f_typecode,
       f_type,
       f_code,
       f_name,
       f_time,
       f_data,
       f_area,
       current_date() insert_time,
       current_date() change_time,
       '1' as status,
       null as id
from ods.hgjj_macro_eco_index_df
where dt='2023-09'
union all  -- 手动添加的数据
select f_typecode,
       f_type,
       f_code,
       f_name,
       f_time,
       f_data,
       f_area,
       current_date() insert_time,
       current_date() change_time,
       '1' as status,
       null as id
from ods.hgjj_macro_eco_index_shoudong
union all -- 第一版去掉的数据，但现在要保留
select f_typecode,
       f_type,
       f_code,
       f_name,
       f_time,
       f_data,
       f_area,
       current_date() insert_time,
       current_date() change_time,
       '1' as status,
       null as id
from dwd.hgjj_macro_eco_index_do
;










select count(1) from ads.hgjj_macro_eco_index_do;




select a.* from ads.hgjj_macro_eco_index_do a
join ods.hgjj_mic_index_con_df b on a.f_code=b.f_code
where b.dt='2023-07-06' and b.f_level_one_name="民营经济发展指标"  limit 1;

insert overwrite table ads.hgjj_macro_eco_index_do
select a.f_typecode,
             a.f_type,
             a.f_code,
             a.f_name,
             a.f_time,
             a.f_data,
             a.f_area,
             a.insert_time,
             a.change_time,
             a.status,
             a.id
      from ads.hgjj_macro_eco_index_do a
               join ads.hgjj_mic_index_con_do b on a.f_code = b.f_code
;


select
       a.f_name,
       concat(nvl(min(a.f_time),'null'),'-',nvl(max(a.f_time),'null')) as time_scope,
       concat(nvl(min(a.f_data),'null'),'-',nvl(max(a.f_data),'null')) as data_scope,
       concat_ws(',',collect_set(if(length(a.f_area)=0,null,a.f_area))) as areas,
       count(distinct if(length(a.f_area)=0,null,a.f_area)) as area_cnt
from ads.hgjj_macro_eco_index_do a
where a.f_name like "%私营%"
group by a.f_name
;


select  a.f_area
from ads.hgjj_macro_eco_index_do a join ads.hgjj_mic_index_con_do b on a.f_code=b.f_code
where f_level_one_name<>"世界经济数据"
group by a.f_area;



select * from ods.hgjj_macro_eco_index_df where dt='2023-09' and flag='1';




select * from ads.hgjj_macro_eco_index_do where f_name='私营工业企业亏损企业数_上年同期';


select count(1),count(f_code)
from (select a.f_typecode,
             a.f_type,
             b.f_code,
             a.f_name,
             a.f_time,
             a.f_data,
             a.f_area,
             a.insert_time,
             a.change_time,
             a.status,
             a.id
      from ads.hgjj_macro_eco_index_do a
               left join ads.hgjj_mic_index_con_do b on a.f_code = b.f_code
      where b.f_code is null
      ) t




select dt,count(1) from ods.hgjj_mic_index_con_df group by dt;

show partitions ods.hgjj_macro_eco_index_df;

show create table ads.hgjj_macro_eco_index_do;


select * from ods.hgjj_macro_eco_index_df where dt='2023-09' and flag='3'

select f_level_one_name from ads.hgjj_mic_index_con_do group by f_level_one_name

select f_level_one_name from ods.hgjj_mic_index_con_df group by f_level_one_name


select * from dwd.hgjj_macro_eco_index_do;




insert overwrite table ads.hgjj_macro_eco_index_do
select f_typecode,
       f_type,
       f_code,
       f_name,
       f_time,
       f_data,
       f_area,
       current_date() insert_time,
       current_date() change_time,
       '1' as status,
       null as id
from ods.hgjj_macro_eco_index_df
where dt='2023-09'
union all  -- 手动添加的数据
select f_typecode,
       f_type,
       f_code,
       f_name,
       f_time,
       f_data,
       f_area,
       current_date() insert_time,
       current_date() change_time,
       '1' as status,
       null as id
from ods.hgjj_macro_eco_index_shoudong
union all -- 第一版去掉的数据，但现在要保留
select f_typecode,
       f_type,
       f_code,
       f_name,
       f_time,
       f_data,
       f_area,
       current_date() insert_time,
       current_date() change_time,
       '1' as status,
       null as id
from dwd.hgjj_macro_eco_index_do


select f_code,count(*)
from ads.hgjj_macro_eco_index_do
group by f_code having count(1)>1;




show partitions ods.hgjj_macro_eco_index_df;

insert overwrite table ads.hgjj_mic_index_con_do
select case when f_level_one_name='世界经济数据' then replace(f_level_one_code,"A","B")
            when f_level_one_name='民营经济发展指标' then replace(f_level_one_code,"A","C")
                else f_level_one_code end as f_level_one_code,
       f_level_one_name,
       case when f_level_one_name='世界经济数据' then replace(f_level_two_code,"A","B")
            when f_level_one_name='民营经济发展指标' then replace(f_level_two_code,"A","C")
            else f_level_two_code end as f_level_two_code,
       f_level_two_name,
       case when f_level_one_name='世界经济数据' then replace(f_level_thr_code,"A","B")
            when f_level_one_name='民营经济发展指标' then replace(f_level_thr_code,"A","C")
            else f_level_thr_code end as f_level_thr_code,
       f_level_thr_name,
       case when f_level_one_name='世界经济数据' then replace(f_code,"A","B")
            when f_level_one_name='民营经济发展指标' then replace(f_code,"A","C")
            else f_code end as f_code,
       f_name,
       current_date() insert_time,
       '1' as f_status,
       current_date() change_time,
       null as id
from ods.hgjj_mic_index_con_df
where dt='2023-07-06';



insert overwrite table ods.hgjj_mic_index_con_df partition (dt='2023-07-07')
select case when f_level_one_name='世界经济数据' then replace(f_level_one_code,"A","B")
            when f_level_one_name='民营经济发展指标' then replace(f_level_one_code,"A","C")
            else f_level_one_code end as f_level_one_code,
       f_level_one_name,
       case when f_level_one_name='世界经济数据' then replace(f_level_two_code,"A","B")
            when f_level_one_name='民营经济发展指标' then replace(f_level_two_code,"A","C")
            else f_level_two_code end as f_level_two_code,
       f_level_two_name,
       case when f_level_one_name='世界经济数据' then replace(f_level_thr_code,"A","B")
            when f_level_one_name='民营经济发展指标' then replace(f_level_thr_code,"A","C")
            else f_level_thr_code end as f_level_thr_code,
       f_level_thr_name,
       case when f_level_one_name='世界经济数据' then replace(f_code,"A","B")
            when f_level_one_name='民营经济发展指标' then replace(f_code,"A","C")
            else f_code end as f_code,
       f_name,
       f_code_from_ld_code
from  ods.hgjj_mic_index_con_df b
 where b.dt='2023-07-06';



select * from ods.hgjj_mic_index_con_df where dt='2023-07-06' -- and f_level_one_name='min'


select * from ods_jcsj.ods_jcsj_sat_news_info_mi



select b.f_level_one_name,
       b.f_level_two_name,
       b.f_level_thr_name,
       a.f_name,
       a.f_time,
       case when a.f_area='' then '全国' else a.f_area end as f_area,
       a.f_data
from ads.hgjj_macro_eco_index_do a
join ads.hgjj_mic_index_con_do b on a.f_code=b.f_code
where a.f_time>='2020' and f_time<'2023'
  and a.f_name in(
               '私营企业固定资产投资额_累计增长',
               '私营工业企业亏损企业数_上年同期',
               '私营企业增加值_同比增长',
               '私营工业企业单位数_本月末',
               '私营工业企业利润总额_累计增长',
               '私营企业增加值_累计增长',
               '私营工业企业亏损企业数_本月末',
               '私营工业企业利润总额_上年同期累计值',
               '私营工业企业利润总额_累计值',
               '私营工业企业亏损企业数_增减')
;





select b.f_level_one_name,
       b.f_level_two_name,
       b.f_level_thr_name,
       a.f_name,
       a.f_time,
       case when a.f_area='' then '全国' else a.f_area end as f_area,
       a.f_data
from ads.hgjj_macro_eco_index_do a
join ads.hgjj_mic_index_con_do b on a.f_code=b.f_code
where a.f_time>='2020' and a.f_time<'2023' and a.f_name='私营企业增加值_累计增长'
;


select *
from ods.hgjj_macro_eco_index_df
where dt='2023-09' and flag='2' and  f_name='私营企业增加值_累计增长'
;

select * from ods.hgjj_mic_index_con_df where dt='2023-09' and f_code in ('A01120103','A02020204');


select * from ads.hgjj_mic_index_con_do where f_code in ('A01120103','A02020204');

show partitions ods.hgjj_mic_index_con_df;









show partitions ods.hgjj_macro_eco_index_df;




select *
from ads.hgjj_macro_eco_index_do
where f_time>='2020' and f_time<'2023' and f_name='私营企业增加值_累计增长'
;


select
    b.f_level_one_name,
    b.f_level_two_name,
    b.f_level_thr_name,
    a.f_name,
    concat(nvl(min(a.f_time),'null'),'-',nvl(max(a.f_time),'null')) as time_scope,
    concat(nvl(min(a.f_data),'null'),'-',nvl(max(a.f_data),'null')) as data_scope,
    concat_ws(',',collect_set(if(length(a.f_area)=0,null,a.f_area))) as areas,
    count(distinct if(length(a.f_area)=0,null,a.f_area)) as area_cnt
from ads.hgjj_macro_eco_index_do  a
join ads.hgjj_mic_index_con_do b on a.f_code=b.f_code
where a.f_time>='2020' and a.f_time<'2023'
  and a.f_name in( '私营企业固定资产投资额_累计增长',
                 '私营工业企业亏损企业数_上年同期',
                 '私营企业增加值_同比增长',
                 '私营工业企业单位数_本月末',
                 '私营工业企业利润总额_累计增长',
                 '私营企业增加值_累计增长',
                 '私营工业企业亏损企业数_本月末',
                 '私营工业企业利润总额_上年同期累计值',
                 '私营工业企业利润总额_累计值',
                 '私营工业企业亏损企业数_增减')
group by
    b.f_level_one_name,
    b.f_level_two_name,
    b.f_level_thr_name,
    a.f_name;




insert overwrite table ads.hgjj_mic_index_con_do
select f_level_one_code,
       f_level_one_name,
       f_level_two_code,
       f_level_two_name,
       f_level_thr_code,
       f_level_thr_name,
       f_code,
       f_name,
       current_date() insert_time,
       '1' as f_status,
       current_date() change_time,
       null as id
from ods.hgjj_mic_index_con_df
where dt='2023-09'
;

use szsm_pt;


create table szsm_pt.zhjc_hot_words_do (
                                       id string comment 'id',
                                       enterprise_hkey string comment 'hash主键',
                                       words string comment '词条名',
                                       popularity string comment '热度',
                                       insert_time string comment '插入时间',
                                       update_time string comment '更新时间',
                                       change_type  string comment '数据更新类型',
                                       status  string comment '数据状态',
                                       begin_time string comment '开始时间',
                                       finish_time string comment '结束时间'
) comment '热词信息表'
    stored as orc
    tblproperties (
        'orc.compress'='snappy'
        );


drop table szsm_pt.zhjc_hot_words_do;

show partitions ods.hgjj_mic_index_con_df;


insert overwrite table ods.hgjj_mic_index_con_df partition (dt='2023-08-15')
-- 国民经济指标
select         f_level_one_code   ,-- string comment '一级指标编码',
               '国民经济指标' f_level_one_name   ,-- string comment '一级指标中文名称',
               f_level_two_code   ,-- string comment '二级指标编码',
               f_level_two_name   ,-- string comment '二级指标中文名称',
               f_level_thr_code   ,-- string comment '三级指标编码',
               f_level_thr_name   ,-- string comment '三级指标中文名称',
               f_code             ,-- string comment '指标编码',
               f_name             ,-- string comment '指标中文名称',
               f_code_from_ld_code-- string comment '龙盾指标编码',
from ods.hgjj_mic_index_con_df
where dt='2023-08-15' and f_level_one_name='国内生产总值'
union all
select         f_level_one_code   ,-- string comment '一级指标编码',
               f_level_one_name   ,-- string comment '一级指标中文名称',
               f_level_two_code   ,-- string comment '二级指标编码',
               f_level_two_name   ,-- string comment '二级指标中文名称',
               f_level_thr_code   ,-- string comment '三级指标编码',
               f_level_thr_name   ,-- string comment '三级指标中文名称',
               f_code             ,-- string comment '指标编码',
               f_name             ,-- string comment '指标中文名称',
               f_code_from_ld_code-- string comment '龙盾指标编码',
from ods.hgjj_mic_index_con_df
where dt='2023-08-15' and f_level_one_name<>'国内生产总值'
;




show partitions ods.hgjj_mic_index_con_df;

select * from ods.hgjj_mic_index_con_df where dt='2023-07-06';

select t1.*
from ads.hgjj_macro_eco_index_do t1
         join ads.hgjj_mic_index_con_do t2 on t1.f_code=t2.f_code
where t2.f_level_one_name='国民经济指标' and t2.f_code in ('A01010101')
;

select * from ads.hgjj_mic_index_con_do where f_level_one_name like '%民营经济%'


select f_level_one_name
from ads.hgjj_mic_index_con_do
group by f_level_one_name



show partitions ods.hgjj_longdun_data_df;

select * from ods.hgjj_longdun_data_df where dt='2023-09' and f_code='A0101002'


select f_name,f_data
from ads.hgjj_macro_eco_index_do t1
where t1.f_name in ('粮食产量',
                    '夏收粮食产量',
                    '秋粮产量',
                    '早稻产量',
                    '棉花产量')
      and f_time='2022年' and f_type='年度数据'
;


select
       f_typecode  ,-- string comment '数据类型代码',
       f_type      ,-- string comment '数据类型',
       f_code      ,-- string comment '指标代码',
       f_name      ,-- string comment '指标代码中文',
       f_time      ,-- string comment '指标时间',
       if(instr(f_data,))cast(f_data as double) f_data      ,-- string comment '指标数据',
       f_area      ,-- string comment '地区',
       insert_time ,-- timestamp comment '插入时间',
       change_time ,-- timestamp comment '修改时间',
       status      ,-- string comment '状态',
       id           -- string comment '自增列'
from ads.hgjj_macro_eco_index_do
where f_name='军费支出（现价本币单位）'
union all
select
    f_typecode  ,-- string comment '数据类型代码',
    f_type      ,-- string comment '数据类型',
    f_code      ,-- string comment '指标代码',
    f_name      ,-- string comment '指标代码中文',
    f_time      ,-- string comment '指标时间',
    f_data      ,-- string comment '指标数据',
    f_area      ,-- string comment '地区',
    insert_time ,-- timestamp comment '插入时间',
    change_time ,-- timestamp comment '修改时间',
    status      ,-- string comment '状态',
    id           -- string comment '自增列'
from ads.hgjj_macro_eco_index_do
where f_name<>'军费支出（现价本币单位）'


select *
from ads.hgjj_macro_eco_index_do
where f_name='民营企业出口总额' and f_area='320000'




select *
from ads.hgjj_macro_eco_index_do t1
where t1.f_name in ('粮食产量',
                    '夏收粮食产量',
                    '秋粮产量',
                    '早稻产量',
                    '棉花产量')
  and f_time='2022年' and f_type='年度数据'


select *
from ods.hgjj_longdun_data_df
where dt='2023-09' and f_type='年度数据' and f_name='早稻产量'



select *
from ods.hgjj_macro_eco_index_df
where dt='2023-09' and flag='3' and instr(f_code,"A")>0

insert overwrite table ads.hgjj_mic_index_con_do
select
    replace(f_level_one_code,'A','B') as f_level_one_code,-- string comment '一级指标编码',
    f_level_one_name ,-- string comment '一级指标中文名称',
    replace(f_level_two_code,'A','B') as f_level_two_code ,-- string comment '二级指标编码',
    f_level_two_name ,-- string comment '二级指标中文名称',
    replace(f_level_thr_code ,'A','B') as f_level_thr_code,-- string comment '三级指标编码',
    f_level_thr_name ,-- string comment '三级指标中文名称',
    replace(f_code,'A','B') as  f_code ,-- string comment '指标编码',
    f_name           ,-- string comment '指标中文名称',
    insert_time      ,-- timestamp comment '插入时间',
    f_status         ,-- string comment '状态',
    change_time      ,-- timestamp comment '修改时间',
    id                -- string comment '自增列'
from ads.hgjj_mic_index_con_do
where f_level_one_name='世界经济数据'
union all
select
    replace(f_level_one_code,'A','C') as f_level_one_code,-- string comment '一级指标编码',
    f_level_one_name ,-- string comment '一级指标中文名称',
    replace(f_level_two_code,'A','C') as f_level_two_code ,-- string comment '二级指标编码',
    f_level_two_name ,-- string comment '二级指标中文名称',
    replace(f_level_thr_code ,'A','C') as f_level_thr_code,-- string comment '三级指标编码',
    f_level_thr_name ,-- string comment '三级指标中文名称',
    replace(f_code,'A','C') as  f_code ,-- string comment '指标编码',
    f_name           ,-- string comment '指标中文名称',
    insert_time      ,-- timestamp comment '插入时间',
    f_status         ,-- string comment '状态',
    change_time      ,-- timestamp comment '修改时间',
    id                -- string comment '自增列'
from ads.hgjj_mic_index_con_do
where f_level_one_name='民营经济发展指标'
union all
select *
from ads.hgjj_mic_index_con_do
where f_level_one_name not in ('民营经济发展指标','世界经济数据')
;


insert overwrite table ods.hgjj_mic_index_con_df partition (dt='2023-09')
select     replace(f_level_one_code,'A','B') as f_level_one_code,-- string comment '一级指标编码',
           f_level_one_name ,-- string comment '一级指标中文名称',
           replace(f_level_two_code,'A','B') as f_level_two_code ,-- string comment '二级指标编码',
           f_level_two_name ,-- string comment '二级指标中文名称',
           replace(f_level_thr_code ,'A','B') as f_level_thr_code,-- string comment '三级指标编码',
           f_level_thr_name ,-- string comment '三级指标中文名称',
           replace(f_code,'A','B') as  f_code ,-- string comment '指标编码',
       f_name              ,-- string comment '指标中文名称',
       f_code_from_ld_code  -- string comment '龙盾指标编码',
from ods.hgjj_mic_index_con_df
where dt='2023-09' and f_level_one_name='世界经济数据'
union all
select        replace(f_level_one_code,'A','C') as f_level_one_code,-- string comment '一级指标编码',
              f_level_one_name ,-- string comment '一级指标中文名称',
              replace(f_level_two_code,'A','C') as f_level_two_code ,-- string comment '二级指标编码',
              f_level_two_name ,-- string comment '二级指标中文名称',
              replace(f_level_thr_code ,'A','C') as f_level_thr_code,-- string comment '三级指标编码',
              f_level_thr_name ,-- string comment '三级指标中文名称',
              replace(f_code,'A','C') as  f_code ,-- string comment '指标编码',
           f_name              ,-- string comment '指标中文名称',
           f_code_from_ld_code  -- string comment '龙盾指标编码',
from ods.hgjj_mic_index_con_df
where dt='2023-09' and f_level_one_name='民营经济发展指标'
union all
select f_level_one_code    ,-- string comment '一级指标编码',
       '国民经济指标' as f_level_one_name    ,-- string comment '一级指标中文名称',
       f_level_two_code    ,-- string comment '二级指标编码',
       f_level_two_name    ,-- string comment '二级指标中文名称',
       f_level_thr_code    ,-- string comment '三级指标编码',
       f_level_thr_name    ,-- string comment '三级指标中文名称',
       f_code              ,-- string comment '指标编码',
       f_name              ,-- string comment '指标中文名称',
       f_code_from_ld_code  -- string comment '龙盾指标编码',
from ods.hgjj_mic_index_con_df
where dt='2023-09' and f_level_one_name not in ('民营经济发展指标','世界经济数据')
;




select f_level_one_code,
       f_level_one_name,
       f_level_two_code,
       f_level_two_name,
       f_level_thr_code,
       f_level_thr_name,
       f_code,
       f_name,
       f_code_from_ld_code
from ods.hgjj_mic_index_con_df
where dt='2023-10'
and f_code in (
               'A01010101',
               'A01010102',
               'A01010103',
               'A01010104',
               'A01010105',
               'A01010106',
               'A01010107',
               'A01010108'
    );



select f_time,f_data
from ads.hgjj_macro_eco_index_do
where  f_code='A01010102'
  and f_time in('2022年第四季度',
                '2021年第四季度',
               '2020年第四季度',
               '2019年第四季度',
               '2018年第四季度',
               '2017年第四季度')
order by f_time desc


select f_name,f_data
from ads.hgjj_macro_eco_index_do t1
where t1.f_name in ('粮食产量',
                    '夏收粮食产量',
                    '秋粮产量',
                    '早稻产量',
                    '棉花产量')
  and f_time='2022年' and f_type='年度数据'

insert overwrite table ads.hgjj_macro_eco_index_do
select f_typecode,
       f_type,
       f_code,
       f_name,
       f_time,
       f_data,
       f_area,
       current_date() insert_time,
       current_date() change_time,
       '1' as status,
       null as id
from ods.hgjj_macro_eco_index_df
where dt='2023-11'
;

insert into table ads.hgjj_macro_eco_index_do
  -- 手动添加的数据
select f_typecode,
       f_type,
       f_code,
       f_name,
       f_time,
       f_data,
       f_area,
       current_date() insert_time,
       current_date() change_time,
       '1' as status,
       null as id
from ods.hgjj_macro_eco_index_shoudong
;
insert into table ads.hgjj_macro_eco_index_do
select f_typecode,
       f_type,
       f_code,
       f_name,
       f_time,
       f_data,
       f_area,
       current_date() insert_time,
       current_date() change_time,
       '1' as status,
       null as id
from dwd.hgjj_macro_eco_index_do
;



create table tmp_lk.hgjj_macro_eco_index_do_231106 as
select * from ads.hgjj_macro_eco_index_do



insert into table ads.hgjj_macro_eco_index_do
select f_typecode,
       f_type,
       f_code,
       f_name,
       f_time,
       cast(f_data as double),
       f_area,
       current_date() as insert_time,
       current_date() as change_time,
       '1' as status,
       null as id
from ods.hgjj_macro_eco_index_df
where dt='2023-11' and flag='3'
;



select f_name,f_data
from ads.hgjj_macro_eco_index_do
where   f_name in ('粮食产量',
                    '夏收粮食产量',
                    '秋粮产量',
                    '早稻产量',
                    '棉花产量')
  and f_time='2022年' and f_type='年度数据'

create table tmp_lk.hgjj_macro_eco_index_do_linshi as
select f_typecode,
       f_type,
       f_code,
       f_name,
       f_time,
       f_data,
       f_area,
       current_date() as insert_time,
       current_date() as change_time,
       '1' as status,
       null as id
from ods.hgjj_macro_eco_index_df
where  dt='2023-11' and  f_name='军费支出（现价本币单位）'
  and f_time='2022'
  and f_area in ('中国',
                 '中非共和国',
                 '丹麦',
                 '乌克兰',
                 '俄罗斯联邦',
                 '加拿大')


select date_add(current_date(),1);

insert into table ads.hgjj_macro_eco_index_do
select f_typecode,
       f_type,
       f_code,
       f_name,
       f_time,
       f_data,
       f_area,
       date_add(current_date(),2) as insert_time,
       date_add(current_date(),2) as change_time,
       '1' as status,
       null as id
from
    (
        select 'C' as f_typecode,'年度数据' as f_type,'B03020901' as f_code,'军费支出(现价本币单位)' as f_name,'2022' as f_time,'26195200000' as f_data,'中非共和国'  as f_area
        union all
        select 'C' as f_typecode,'年度数据' as f_type,'B03020901' as f_code,'军费支出(现价本币单位)' as f_name,'2022' as f_time,'35003000000' as f_data,'加拿大' as f_area
        union all
        select 'C' as f_typecode,'年度数据' as f_type,'B03020901' as f_code,'军费支出(现价本币单位)' as f_name,'2022' as f_time,'1964343311987' as f_data,'中国' as f_area
        union all
        select 'C' as f_typecode,'年度数据' as f_type,'B03020901' as f_code,'军费支出(现价本币单位)' as f_name,'2022' as f_time,'38676000000' as f_data,'丹麦' as f_area
        union all
        select 'C' as f_typecode,'年度数据' as f_type,'B03020901' as f_code,'军费支出(现价本币单位)' as f_name,'2022' as f_time,'6032900000000' as f_data,'俄罗斯联邦' as f_area
        union all
        select 'C' as f_typecode,'年度数据' as f_type,'B03020901' as f_code,'军费支出(现价本币单位)' as f_name,'2022' as f_time,'1435223032600' as f_data,'乌克兰' as f_area
    ) t



select f_area,f_data
from ads.hgjj_macro_eco_index_do
where  f_name='军费支出（现价本币单位）'
  and f_time='2022'
  and f_area in ('中国',
                 '中非共和国',
                 '丹麦',
                 '乌克兰',
                 '俄罗斯联邦',
                 '加拿大')


select cast('1.21208E+11' as double);


select f_time,f_data
from ods.hgjj_macro_eco_index_df
where  dt='2023-11' and  f_code='A01010102'
  and f_time in('2022年第四季度',
                '2021年第四季度',
                '2020年第四季度',
                '2019年第四季度',
                '2018年第四季度',
                '2017年第四季度')
order by f_time desc







insert overwrite table ads.hgjj_macro_eco_index_do
select  f_typecode  , -- string comment '数据类型代码',
        f_type      , -- string comment '数据类型',
        f_code      , -- string comment '指标代码',
        f_name      , -- string comment '指标代码中文',
        f_time      , -- string comment '指标时间',
        f_data      , -- string comment '指标数据',
        f_area      , -- string comment '地区',
        insert_time , -- timestamp comment '插入时间',
        change_time , -- timestamp comment '修改时间',
        status      , -- string comment '状态',
        id            -- string comment '自增列'
from(
        select
            f_typecode  , -- string comment '数据类型代码',
            f_type      , -- string comment '数据类型',
            f_code      , -- string comment '指标代码',
            f_name      , -- string comment '指标代码中文',
            f_time      , -- string comment '指标时间',
            f_data      , -- string comment '指标数据',
            f_area      , -- string comment '地区',
            insert_time , -- timestamp comment '插入时间',
            change_time , -- timestamp comment '修改时间',
            status      , -- string comment '状态',
            id          , -- string comment '自增列'
            row_number() over(partition by f_typecode,f_code,f_time,f_area order by insert_time desc ) rk
        from ads.hgjj_macro_eco_index_do
    ) t1
where rk=1
;



insert overwrite table ads.hgjj_macro_eco_index_do
select
    f_typecode  , -- string comment '数据类型代码',
    f_type      , -- string comment '数据类型',
    f_code      , -- string comment '指标代码',
    f_name      , -- string comment '指标代码中文',
    f_time      , -- string comment '指标时间',
    f_data      , -- string comment '指标数据',
    f_area      , -- string comment '地区',
    insert_time , -- timestamp comment '插入时间',
    change_time , -- timestamp comment '修改时间',
    status      , -- string comment '状态',
    id            -- string comment '自增列'
from ads.hgjj_macro_eco_index_do
where length(f_data)>0 and f_data is not null
;





select t2.f_level_one_name,
       t2.f_level_two_name,
       t2.f_level_thr_name,
       t1.f_name,
       count(1) cnt
from ads.hgjj_macro_eco_index_do t1
join ads.hgjj_mic_index_con_do t2 on t1.f_code=t2.f_code
group by t2.f_level_one_name,
         t2.f_level_two_name,
         t2.f_level_thr_name,
         t1.f_name
;



select t2.f_level_one_name,
       count(1) cnt
from ads.hgjj_macro_eco_index_do t1
         join ads.hgjj_mic_index_con_do t2 on t1.f_code=t2.f_code
group by t2.f_level_one_name
;



-- 2131160
select count(1) cnt
from ads.hgjj_macro_eco_index_do t1
join ads.hgjj_mic_index_con_do t2 on t1.f_code=t2.f_code;



select *
from ads.hgjj_mic_index_con_do
;



select f_level_one_name,f_level_two_name
from ods.hgjj_mic_index_con_df
where dt='2023-08-15'
group by f_level_one_name,f_level_two_name
;



select *
from ads.hgjj_macro_eco_index_do
where f_name='新登记私营企业数量'
;
-- B03020701
-- C040100101
-- C04010101
select * from ads.hgjj_mic_index_con_do where f_level_one_name = '民营经济发展指标';


ALTER TABLE ods.hgjj_macro_eco_index_df DROP PARTITION (dt='2023-11', flag='3');

select * from ods.hgjj_macro_eco_index_df where dt='2023-11' and flag='3' and f_name='新登记私营企业数量'

select t1.*,t2.f_name,t2.f_code
from ods.hgjj_mic_index_con_df t1
join ads.hgjj_mic_index_con_do t2 on t1.f_name=t2.f_name
where dt='2023-08-15'
;


select f_level_one_name,f_level_two_name
from ods.hgjj_mic_index_con_df where dt='2023-07-07'
group by f_level_one_name,f_level_two_name



show partitions ods.hgjj_mic_index_con_df



select f_code,count(1)
from  ads.hgjj_mic_index_con_do
group by f_code
having count(1) >1


select *
from  ads.hgjj_mic_index_con_do
where f_code in(
'B03070101',
'B03070201',
'B03070301',
'B03070401',
'B03070501',
'B03070601',
'B03070701',
'B03070801',
'B03070901',
'B03071001');



select *
from ods.hgjj_macro_eco_index_df
where dt='2023-11'
      and flag='1'
  and f_code='B03020901'
  and f_time='2022'
and f_area in ('中国',
              '中非共和国',
              '丹麦',
              '乌克兰',
              '俄罗斯联邦',
              '加拿大')


select f_time,f_data
from ads.hgjj_macro_eco_index_do
where  f_name='民营企业出口总额' and f_area='江苏'




select a.*
from ods.hgjj_mic_index_con_df a
where a.dt='2023-11';

show partitions ods.hgjj_mic_index_con_df;



