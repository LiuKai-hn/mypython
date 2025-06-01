

select person_hkey as rk,
       '优秀中国特色社会主义建设者' as tag_value,
       tag_level,-- 标签层级
       district, -- 地域
       nd-- 年度
from
    (
        select hjmc,-- 获奖名称
               ryxm,-- 人员姓名
               qymc,-- 企业名称
               CASE
                   WHEN instr(b.dy , '省') > 0 THEN '省级'
                   when b.dy  in( '内蒙古自治区', '西藏自治区','新疆维吾尔自治区','宁夏回族自治区','广西壮族自治区','香港特区','澳门特区','北京市','天津市','上海市','重庆市') then '省级'
                   when b.dy ='全国' then '全国'
                   else b.dy
                   END as tag_level,-- 标签层级
               concat_ws('',province,replace(city,province,''),replace(district,replace(city,province,''),'')) as district, -- 地域
               a.nd,-- 年度
               b.dy, -- 地域
               qybm,-- 企业编码
               rybm,-- 人员编码
               person_hkey,
               bdbm -- 榜单编码
        from dwd.bdmd_yxzgtsshzyjsz_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where person_hkey is not null
group by
    person_hkey,
    tag_level,-- 标签层级
    district, -- 地域
    nd;


-- 工商联直属商会会长、副会长

select t.person_hkey as rk,
       '工商联直属商会会长、副会长' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd-- 年度

from
    (
        select
            ryxm,-- 人员姓名
            qymc,-- 企业名称
            CASE
                WHEN instr(b.dy , '省') > 0 THEN '省级'
                when b.dy  in( '内蒙古自治区', '西藏自治区','新疆维吾尔自治区','宁夏回族自治区','广西壮族自治区','香港特区','澳门特区','北京市','天津市','上海市','重庆市') then '省级'
                when b.dy ='全国' then '全国'
                else b.dy
                END as tag_level,-- 标签层级
            concat_ws('',province,replace(city,province,''),replace(district,replace(city,province,''),'')) as district, -- 地域
            a.nd,-- 年度
            qybm,-- 企业编码
            rybm,-- 人员编码
            person_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_gslzsshhzfhz_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;



-- “四好”商会会长、副会长
select t.person_hkey as rk,
       '“四好”商会会长、副会长' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd-- 年度
from
    (
        select
            ryxm,-- 人员姓名
            qymc,-- 企业名称
            CASE
                WHEN instr(b.dy , '省') > 0 THEN '省级'
                when b.dy  in( '内蒙古自治区', '西藏自治区','新疆维吾尔自治区','宁夏回族自治区','广西壮族自治区','香港特区','澳门特区','北京市','天津市','上海市','重庆市') then '省级'
                when b.dy ='全国' then '全国'
                else b.dy
                END as tag_level,-- 标签层级
            concat_ws('',province,replace(city,province,''),replace(district,replace(city,province,''),'')) as district, -- 地域
            a.nd,-- 年度
            qybm,-- 企业编码
            rybm,-- 人员编码
            person_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_shshhzfhz_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;



-- 中国光彩事业奖章
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
    t.nd;

-- 全国和谐劳动关系创建工作先进个人


-- 脱贫攻坚先进个人
select t.person_hkey as rk,
       '脱贫攻坚先进个人' as tag_value,
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
            rybm,-- 人员编码
            person_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_tpgjgr_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;




-- 获得中华环境奖的民营企业家
select t.person_hkey as rk,
       '获得中华环境奖的民营企业家' as tag_value,
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
            bdbm -- 榜单编码
        from dwd.bdmd_hdzhhjjdmyqyj_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;

-- 全国和谐劳动关系创建工作先进个人

-- 抗击新冠肺炎疫情先进个人
select t.person_hkey as rk,
       '抗击新冠肺炎疫情先进个人' as tag_value,
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
            bdbm -- 榜单编码
        from dwd.bdmd_kjxgfyyqxjgr_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- 劳动模范
select t.person_hkey as rk,
       '劳动模范' as tag_value,
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
            bdbm -- 榜单编码
        from dwd.bdmd_ldmf_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;

-- 五一劳动奖章
select t.person_hkey as rk,
       '五一劳动奖章' as tag_value,
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
            bdbm -- 榜单编码
        from dwd.bdmd_wyldjz_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- 五四青年奖章
select t.person_hkey as rk,
       '五四青年奖章' as tag_value,
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
            bdbm -- 榜单编码
        from dwd.bdmd_wsjzb_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;

-- 三八红旗手
   -- 该榜单没有person_jkey

-- 获得光彩事业国土绿化贡献奖的民营企业家
select t.person_hkey as rk,
       '获得光彩事业国土绿化贡献奖的民营企业家' as tag_value,
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
            bdbm -- 榜单编码
        from dwd.bdmd_hdgcsygtlhgxjdmyqyj_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;




