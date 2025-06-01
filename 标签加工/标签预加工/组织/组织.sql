
-- “四好”商会
 -- 此表 dwd.bdmd_qgshsh_wf 的榜单编码为null 不知为啥

select t.cc_hkey as rk,
       '“四好”商会' as tag_value,
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
            --concat_ws('',province,replace(city,province,''),replace(district,replace(city,province,''),'')) as district, -- 地域
            b.dy as district,
            a.nd,-- 年度
            cc_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_qgshsh_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
group by
    t.cc_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- “五好”县级工商联
select org_hkey as rk,
       '“五好”县级工商联' as tag_value,
       year, --录入年份
       province_source
from ods_jcsj.ods_jcsj_sat_org_wuhao_info_mi
where is_wuhao_org='1';







