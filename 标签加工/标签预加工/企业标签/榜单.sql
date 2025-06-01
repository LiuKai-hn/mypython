-- 《财富》世界500强
select t.enterprise_hkey as rk,
       '《财富》世界500强' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_cfsj500qb_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;

-- 劳动模范
select t.enterprise_hkey as rk,
       '劳动模范' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_ldmfjt_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- 五一劳动奖章
select t.enterprise_hkey as rk,
       '五一劳动奖章' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_wyldjzjt_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;

-- 五四青年奖章
select t.enterprise_hkey as rk,
       '五四青年奖章' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_wsjzjtb_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;

-- 中国民营企业500强
select t.enterprise_hkey as rk,
       '中国民营企业500强' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_zgmyqy500q_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;



-- 中国民营企业制造业500强
select t.enterprise_hkey as rk,
       '中国民营企业制造业500强' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_zgmyqyzzy500q_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- 中国民营企业服务业100强
select t.enterprise_hkey as rk,
       '中国民营企业服务业100强' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_zgmyqyfwy100q_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- 最具影响力品牌企业
select t.enterprise_hkey as rk,
       '最具影响力品牌企业' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_zjyxlppqy_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;



-- 两化融合试点示范企业
select t.enterprise_hkey as rk,
       '两化融合试点示范企业' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_lhrhsdsfqy_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- 制造业单项冠军企业
select t.enterprise_hkey as rk,
       '制造业单项冠军企业' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_zzydxgjqy_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- 工信部隐形冠军企业
select t.enterprise_hkey as rk,
       '工信部隐形冠军企业' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_gxbyxgjqy_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;





-- 工信部专精特新“小巨人”企业
select t.enterprise_hkey as rk,
       '工信部专精特新“小巨人”企业' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_gxbzjtxxjrqy_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;




-- “专精特新”企业名单
select t.enterprise_hkey as rk,
       '“专精特新”企业名单' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_zjtxxjrqymd_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;



-- 专精特新中小企业
select t.enterprise_hkey as rk,
       '专精特新中小企业' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_zjtxzxqy_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;



-- 技术创新示范企业名单
select t.enterprise_hkey as rk,
       '技术创新示范企业名单' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_jscxsfqymd_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;








-- 获得专利奖的企业
select t.enterprise_hkey as rk,
       '获得专利奖的企业' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_hdzljdqy_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;

-- 福布斯最具创新能力企业榜
select t.enterprise_hkey as rk,
       '福布斯最具创新能力企业榜' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_fbszjcxnlqyb_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;

-- 中国高科技高成长50强
select t.enterprise_hkey as rk,
       '中国高科技高成长50强' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_zggkjgcz50q_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;

-- 全国供应链创新与应用示范企业
select t.enterprise_hkey as rk,
       '全国供应链创新与应用示范企业' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_qggylcxyyysfqy_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- 获得科技进步奖
select t.enterprise_hkey as rk,
       '获得科技进步奖' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_hdkjjbj_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- 获得技术发明奖
select t.enterprise_hkey as rk,
       '获得技术发明奖' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_hdjsfmj_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;



-- 脱贫攻坚先进集体
select t.enterprise_hkey as rk,
       '脱贫攻坚先进集体' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_tpgjxjjt_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- “绿色制造名单”企业（含绿色工厂和绿色供应链管理示范企业）
select t.enterprise_hkey as rk,
       '“绿色制造名单”企业（含绿色工厂和绿色供应链管理示范企业）' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_lszzqymd_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- 工信部“工业产品绿色设计示范企业”
select t.enterprise_hkey as rk,
       '工信部“工业产品绿色设计示范企业”' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_gycplssjsfmd_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- 抗击新冠肺炎疫情先进集体
select t.enterprise_hkey as rk,
       '抗击新冠肺炎疫情先进集体' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_kjxgfyyqxjjt_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;



-- 中华慈善奖表彰企业
select t.enterprise_hkey as rk,
       '中华慈善奖表彰企业' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_zhcsjxjjt_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;

-- 就业与社会保障先进民营企业、关爱员工实现双赢先进集体
select t.enterprise_hkey as rk,
       '就业与社会保障先进民营企业、关爱员工实现双赢先进集体' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_jyyshbzxjmyqy_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;


-- 中国质量奖企业
select t.enterprise_hkey as rk,
       '中国质量奖企业' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_zgzljqy_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;




-- 民营企业研发投入 500 家榜单
select t.enterprise_hkey as rk,
       '民营企业研发投入500家榜单' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            a.nf as nd,-- 年度
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_myqyyftr500jbd_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;



-- 民营企业发明专利500家榜单
select t.enterprise_hkey as rk,
       '民营企业发明专利500家榜单' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            a.nf as nd,-- 年度
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_myqyfmzl500jbd_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;



-- 全国和谐劳动关系创建示范企业
select t.enterprise_hkey as rk,
       '全国和谐劳动关系创建示范企业' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       t.nd -- 年度
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
            b.nd,-- 年度
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_qghxldgxcjsfqymd_wf a
                 join tmp_lk.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;

