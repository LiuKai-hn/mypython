

select count(bdbm) as cnt1,-- 全部的数据
       count(id) as cnt2 -- 交上的数据
from (select a.bdbm, b.id, a.enterprise_hkey
      from (select * from dwd.bdmd_gycplssjsfmd_wf where enterprise_hkey is not null) a
               left join tmp_lk.SJ_PHBDFB b on a.bdbm = b.id
      group by a.bdbm, b.id, a.enterprise_hkey
      )t
;

select * from dwd.bdmd_hdzhhjjdmyqyj_wf where person_hkey is not null;

-- coalesce(t.nd,'-') as nd


select * from dwd.bdmd_hdzhhjjdmyqyj_wf where bdbm is not null;




SELECT collect_set(concat(tag_level,":",tag_level_cnt)) tag_levels,-- 标签层级
       collect_set(concat(district,":",district_cnt)) districts, -- 地域
       collect_set(concat(nd,":",nd_cnt)) nds-- 年度
from (
     select tag_level,
             count(1) over (partition by tag_level) tag_level_cnt,
             district,
             count(1) over (partition by district)  district_cnt,
             nd,
             count(1) over (partition by nd)        nd_cnt
      FROM (
               select t.person_hkey as rk,
                      '三八红旗手' as tag_value,
                      t.tag_level,-- 标签层级
                      t.district, -- 地域
                      t.nd-- 年度

               from
                   (
                       select
                           CASE
                               WHEN instr(b.dy , '省') > 0 THEN '省级'
                               when b.dy  in( '内蒙古自治区', '西藏自治区','新疆维吾尔自治区','宁夏回族自治区','广西壮族自治区','香港特区','澳门特区','北京市','天津市','上海市','重庆市') then '省级'
                               when b.dy ='全国' then '全国'
                               else b.dy
                               END as tag_level,-- 标签层级
                           province as district, -- 地域
                           a.nd,-- 年度
                           qybm,-- 企业编码
                           person_hkey,
                           bdbm -- 榜单编码
                       from dwd.bdmd_sbhqs_wf a
                                join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
                   ) t
               where person_hkey is not null
               group by
                   t.person_hkey,
                   t.tag_level,-- 标签层级
                   t.district, -- 地域
                   t.nd
            ) t1
      )t2
;




select t.person_hkey as rk,
       '中国光彩事业奖章' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd-- 年度
from
    (
        select
            ryxm,-- 人员姓名
            CASE
                WHEN instr(b.dy , '省') > 0 THEN '省级'
                when b.dy  in( '内蒙古自治区', '西藏自治区','新疆维吾尔自治区','宁夏回族自治区','广西壮族自治区','香港特区','澳门特区','北京市','天津市','上海市','重庆市') then '省级'
                when b.dy ='全国' then '全国'
                else b.dy
                END as tag_level,-- 标签层级
            concat_ws('',province,replace(city,province,''),replace(district,replace(city,province,''),'')) as district, -- 地域
            a.nd,-- 年度
            qybm,-- 企业编码
            person_hkey,
            rybm,-- 人员编码
            bdbm -- 榜单编码
        from dwd.bdmd_zggcsyjz_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd
;


select person_hkey, bdbm from dwd.bdmd_zggcsyjz_wf  a;
                  -- join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id;


select * from dwd.bdmd_2023myqy20q_ai limit 10;