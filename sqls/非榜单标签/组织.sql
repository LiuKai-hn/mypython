
DROP TABLE IF EXISTS tmp_lk.org_member_cnt_tb;
CREATE TABLE tmp_lk.org_member_cnt_tb (
                                         org_hkey string comment '工商联hash',
                                         cnt string comment '会员数'
) comment '工商联会员数(临时表)'
    row format delimited fields terminated by ','
        lines terminated by '\n'
    stored as textfile;


DROP TABLE IF EXISTS tmp_lk.org_afir_tb;
CREATE TABLE tmp_lk.org_afir_tb (
                                          org_hkey string comment '工商联hash',
                                          afir_name string comment '参加活动名称'
) comment '工商联事件数(临时表)'
    row format delimited fields terminated by ','
        lines terminated by '\n'
    stored as textfile;


DROP TABLE IF EXISTS tmp_lk.org_other_cnt_tb;
CREATE TABLE tmp_lk.org_other_cnt_tb (
                                          org_hkey string comment '工商联hash',
                                          party_num string comment '党组织人数',
                                          party_org string comment '党组织机构', --党组织机构
                                          is_league_org string comment '是否建立党组织',  -- 是否建立党组织
                                          league_member_num string comment '团员人数'
) comment '工商联其他数统计(临时表)'
    row format delimited fields terminated by ','
        lines terminated by '\n'
    stored as textfile;


DROP TABLE IF EXISTS tmp_lk.org_other_label_tb;
CREATE TABLE tmp_lk.org_other_label_tb (
                                         org_hkey string comment '工商联hash',
                                         admini_level string comment '级别',
                                         org_basistype string comment '机构性质',
                                         org_cnt string comment '下属单位数',
                                         org_member_cnt string comment '会员数',

                                         party_org string comment '党组织类型',
                                         party_num string comment '党员人数规模',
                                         is_league_member string comment '建立团组织',
                                         league_member_num string comment '团员人数规模',
                                         afir_name string comment '活动事件名称'
) comment '工商联其他数统计(临时表)'
    row format delimited fields terminated by ','
        lines terminated by '\n'
    stored as textfile;



select * from tmp_lk.org_member_cnt_tb;


insert overwrite table tmp_lk.org_other_label_tb
select t1.org_hkey,
       dim_admini_level_tb.code_cn as admini_level, -- string comment '行政级别' --级别
       dim_org_basistype_tb.code_cn as org_basistype, --     string comment '组织基础分类' -- 机构性质
       t2.cnt as org_cnt,-- 下属单位数
       t3.cnt as org_member_cnt, --工商联会员数
       t4.party_org ,--string comment '党组织机构', --党组织机构
       concat(cast(t4.party_num/5 as bigint)*5,'-',(cast(t4.party_num/5 as bigint)+1)*5) as party_num ,--string comment '党组织人数',
       if(t4.league_member_num >0,'建立团组织',null) as is_league_member,--string comment '是否建立团组织',  -- 是否建立党组织
       concat(cast(t4.league_member_num/5 as bigint)*5,'-',(cast(t4.league_member_num/5 as bigint)+1)*5) as league_member_num, --string comment '团员人数'
       t5.afir_name -- 活动事件名称
from ods_jcsj.ods_jcsj_sat_org_basic_info_mi t1
left join tmp_lk.org_cnt_tb t2 on t1.org_hkey=t2.org_hkey_p
left join tmp_lk.org_member_cnt_tb t3 on t1.org_hkey=t3.org_hkey
left join tmp_lk.org_other_cnt_tb t4 on t1.org_hkey=t4.org_hkey
left join tmp_lk.org_afir_tb t5 on t1.org_hkey=t5.org_hkey
left join dwd.sys_dic_code_info_do dim_admini_level_tb on trim(t1.admini_level_code)=dim_admini_level_tb.code and dim_admini_level_tb.item_name_cn='行政级别代码' and dim_admini_level_tb.code not in ('00','98','99')
left join dwd.sys_dic_code_info_do dim_org_basistype_tb on trim(t1.org_basistype)=dim_org_basistype_tb.code and dim_org_basistype_tb.item_name_cn='组织基础分类代码' and dim_admini_level_tb.code not in ('00','99')

;



select league_member_num,
       concat(cast(league_member_num/5 as bigint)*5,'-',(cast(league_member_num/5 as bigint)+1)*5) as league_member_num
from tmp_lk.org_other_cnt_tb
where league_member_num is not null and length(league_member_num)>0;


select * from ods_jcsj.ods_jcsj_sat_org_basic_info_mi








select org_hkey,
       afir_name
from tmp_lk.org_other_label_tb
where afir_name is not null and length(afir_name)>0
group by afir_name
limit 100;







create table tmp_lk.org_cnt_tb as
select  -- 一级
        t1.org_hkey_p,
        count(1) as cnt
from ods_jcsj.ods_jcsj_link_org_mi t1
         left join ods_jcsj.ods_jcsj_link_org_mi t2 on t1.org_hkey_p=t2.org_hkey
group by t1.org_hkey_p
union all
select t2.org_hkey_p, -- 二级
       sum(cnt) as cnt
from(
        select
            t1.org_hkey_p,
            count(1) as cnt
        from ods_jcsj.ods_jcsj_link_org_mi t1
                 left join ods_jcsj.ods_jcsj_link_org_mi t2 on t1.org_hkey_p=t2.org_hkey
        group by t1.org_hkey_p
    ) t1
join ods_jcsj.ods_jcsj_link_org_mi t2 on t1.org_hkey_p=t2.org_hkey
group by t2.org_hkey_p
union all
select t2.org_hkey_p,
       sum(cnt) as cnt
from(
        select t2.org_hkey_p, -- 三级
               sum(cnt) as cnt
        from(
                select
                    t1.org_hkey_p,
                    count(1) as cnt
                from ods_jcsj.ods_jcsj_link_org_mi t1
                         left join ods_jcsj.ods_jcsj_link_org_mi t2 on t1.org_hkey_p=t2.org_hkey
                group by t1.org_hkey_p
            ) t1
                join ods_jcsj.ods_jcsj_link_org_mi t2 on t1.org_hkey_p=t2.org_hkey
        group by t2.org_hkey_p
    ) t1
join ods_jcsj.ods_jcsj_link_org_mi t2 on t1.org_hkey_p=t2.org_hkey
group by t2.org_hkey_p
union all
select t2.org_hkey_p,
       sum(cnt) as cnt
from (select t2.org_hkey_p,
             sum(cnt) as cnt
      from (select t2.org_hkey_p, -- 四级
                   sum(cnt) as cnt
            from (select t1.org_hkey_p,
                         count(1) as cnt
                  from ods_jcsj.ods_jcsj_link_org_mi t1
                           left join ods_jcsj.ods_jcsj_link_org_mi t2 on t1.org_hkey_p = t2.org_hkey
                  group by t1.org_hkey_p) t1
                     join ods_jcsj.ods_jcsj_link_org_mi t2 on t1.org_hkey_p = t2.org_hkey
            group by t2.org_hkey_p) t1
               join ods_jcsj.ods_jcsj_link_org_mi t2 on t1.org_hkey_p = t2.org_hkey
      group by t2.org_hkey_p
      ) t1
join ods_jcsj.ods_jcsj_link_org_mi t2 on t1.org_hkey_p=t2.org_hkey
group by t2.org_hkey_p
;





select *
from tmp_lk.org_cnt_tb


;




