


show create table bdmd_zgmyqyzzy500q_ai;


select * from bdmd_zgmyqyzzy500q_ai;





show create table BDMD_QGSBHQSJTB_AI;

drop table BDMD_QGSBHQSB_AI;
create table BDMD_QGSBHQSB_AI (
                                    ID          string    comment 'ID',
                                    LX          string    comment '类型',
                                    ND          string    comment '年份',
                                    XM          string    comment '姓名',
                                    qymc        string    comment '企业名称',
                                    ZW          string    comment '职务',
                                    RYBM        string    comment '人员编码',
                                    QYBM        string    comment '企业编码',
                                    BDBM        string    comment '榜单编码',
                                    PROVINCE    string    comment '省',
                                    CREATE_TIME TimeStamp comment '数据创建时间',
                                    UPDATE_TIME TimeStamp comment '数据更新时间',
                                    CHANGE_TYPE string    comment '数据更新类型',
                                    STATUS      string    comment '数据状态'

) comment '全国-三八红旗手表（个人与标兵）'
    row format delimited fields terminated by '\t'
        lines terminated by '\n'
    stored as textfile;


load data local inpath '/zbw/BDMD_QGLDMF_AI.csv' overwrite into table ods.BDMD_QGLDMF_AI;

select * from ods.BDMD_QGLDMF_AI;

select count(1) from ods.BDMD_QGLDMF_AI;

insert overwrite table ods.BDMD_QGLDMF_AI
select
    regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") ID          ,-- string     comment 'ID',
RYXM        ,-- string     comment '人员姓名',
QYMC        ,-- string     comment '企业名称',
ZW          ,-- string     comment '职务',
RYBM        ,-- string     comment '人员编码',
PROVINCE    ,-- string     comment '省份',
JB          ,-- string     comment '届别',
'2020' as ND          ,-- string     comment '年度',
'B59B90AF94D9A4122C83679U76249' as BDBM        ,-- string     comment '榜单编码',
    current_timestamp() CREATE_TIME ,-- TimeStamp  comment '数据创建时间',
    current_timestamp() UPDATE_TIME ,-- TimeStamp  comment '数据更新时间',
CHANGE_TYPE ,-- string     comment '数据更新类型',
'1' STATUS      ,-- string     comment '数据状态',
LB           -- string     comment '类别'
from ods.BDMD_QGLDMF_AI;



load data local inpath '/zbw/BDMD_QGSBHQSJTB_AI.csv' overwrite into table ods.BDMD_QGSBHQSJTB_AI;



select * from ods.BDMD_QGSBHQSB_AI;









insert overwrite table ods.BDMD_QGSBHQSB_AI
select
    regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") ID          ,--string    comment 'ID',
    LX          ,--string    comment '类型',
    ND          ,--string    comment '年份',
    XM          ,--string    comment '姓名',
    qymc        ,--string    comment '企业名称',
    ZW          ,--string    comment '职务',
    RYBM        ,--string    comment '人员编码',
    QYBM        ,--string    comment '企业编码',
    'BBAI0DF676104203B3921FBBFB082301' BDBM        ,--string    comment '榜单编码',
    PROVINCE    ,--string    comment '省',
    current_timestamp() CREATE_TIME ,-- TimeStamp  comment '数据创建时间',
    current_timestamp() UPDATE_TIME ,-- TimeStamp  comment '数据更新时间',
    CHANGE_TYPE ,-- string     comment '数据更新类型',
    '1' STATUS       -- string     comment '数据状态',
from ods.BDMD_QGSBHQSB_AI ;




select * from ods.BDMD_QGSBHQSJTB_AI;

insert overwrite table ods.BDMD_QGSBHQSJTB_AI
select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") ID         ,
       SF         ,
       LX         ,
       ND         ,
       QYMC       ,
       QYBM       ,
       'BBAI0DF679854203B3921FBBFB081311' BDBM       ,
       current_timestamp() CREATE_TIME ,-- TimeStamp  comment '数据创建时间',
       current_timestamp() UPDATE_TIME ,-- TimeStamp  comment '数据更新时间',
       CHANGE_TYPE ,-- string     comment '数据更新类型',
       '1' STATUS
from ods.BDMD_QGSBHQSJTB_AI;



create table BDMD_ZGGCSYJZ_AI (
ID          string    comment 'ID',
XH          string    comment '序号',
RYXM        string    comment '人员姓名',
RYBM        string    comment '人员编码',
QYMC        string    comment '企业名称',
ZW          string    comment '职务',
DWJZW       string    comment '单位及职务',
QYBM        string    comment '企业编码',
PC          string    comment '批次',
PROVINCE    string    comment '省份',
ND          string    comment '年度',
BDBM        string    comment '榜单编码',
CREATE_TIME TimeStamp comment '数据创建时间',
UPDATE_TIME TimeStamp comment '数据更新时间',
CHANGE_TYPE string    comment '数据更新类型',
STATUS      string    comment '数据状态'
) comment '中国光彩事业奖章'
    row format delimited fields terminated by '\t'
        lines terminated by '\n'
    stored as textfile;



load data local inpath '/zbw/BDMD_ZGGCSYJZ_AI.csv' overwrite into table ods.BDMD_ZGGCSYJZ_AI;



select * from BDMD_ZGGCSYJZ_AI;
// 15430 + 84 + 84 +1 + 1 + 64 + 297 + 130 + 2802 + 15 + 57177 + 21623376
select count(1) from ads.zhjc_ent_cert_qualified_info_do;

select * from ads.zhjc_ent_patent_info_do;

select * from ods_jcsj.ods_jcsj_sat_ent_patent_info_mi;

insert overwrite table ods.BDMD_ZGGCSYJZ_AI
select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") ID,
       XH          ,--string    comment '序号',
       RYXM        ,--string    comment '人员姓名',
       RYBM        ,--string    comment '人员编码',
       QYMC        ,--string    comment '企业名称',
       ZW          ,--string    comment '职务',
       DWJZW       ,--string    comment '单位及职务',
       QYBM        ,--string    comment '企业编码',
       PC          ,--string    comment '批次',
       PROVINCE    ,--string    comment '省份',
       ND          ,--string    comment '年度',
       'BBAI0DF67659OP03B3921FBBFB081321' BDBM        ,--string    comment '榜单编码',
       current_timestamp() CREATE_TIME ,-- TimeStamp  comment '数据创建时间',
       current_timestamp() UPDATE_TIME ,-- TimeStamp  comment '数据更新时间',
       CHANGE_TYPE ,-- string     comment '数据更新类型',
       '1' STATUS
from ods.BDMD_ZGGCSYJZ_AI
;


drop table BDMD_QGWHXJGSL_AI;


create table BDMD_QGWHXJGSL_AI (
                                   ID          string    comment 'ID',
                                   XH          string    comment '序号',
                                   GSLMC       string    comment '工商联名称',
                                   GSLBM       string    comment '工商联编码',
                                   PROVINCE    string    comment '省份',
                                   CITY        string    comment '城市',
                                   DISTRICT    string    comment '地区',
                                   ND          string    comment '年度',
                                   BDBM        string    comment '榜单编码',
                                   CREATE_TIME TimeStamp comment '数据创建时间',
                                   UPDATE_TIME TimeStamp comment '数据更新时间',
                                   CHANGE_TYPE string    comment '数据更新类型',
                                   STATUS      string    comment '数据状态'

) comment '“五好”县级工商联'
    row format delimited fields terminated by '\t'
        lines terminated by '\n'
    stored as textfile;



load data local inpath '/zbw/BDMD_QGWHXJGSL_AI.csv' overwrite into table ods.BDMD_QGWHXJGSL_AI;



select * from BDMD_QGWHXJGSL_AI;


insert overwrite table ods.BDMD_QGWHXJGSL_AI
select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") ID,
    XH          ,-- string    comment '序号',
    GSLMC       ,-- string    comment '工商联名称',
    GSLBM       ,-- string    comment '工商联编码',
    PROVINCE    ,-- string    comment '省份',
    CITY        ,-- string    comment '城市',
    DISTRICT    ,-- string    comment '地区',
    ND          ,-- string    comment '年度',
    '0C3A9C949CD24505B31CFD542A011021' BDBM        ,-- string    comment '榜单编码',
    current_timestamp() CREATE_TIME ,-- TimeStamp  comment '数据创建时间',
    current_timestamp() UPDATE_TIME ,-- TimeStamp  comment '数据更新时间',
    CHANGE_TYPE ,-- string     comment '数据更新类型',
    '1' STATUS
from BDMD_QGWHXJGSL_AI;



select bdbm
from ods.bdmd_gsmyqy100q_ai
group by bdbm;

select bdsf,nd from ods.bdmd_gsmyqy100q_ai group by bdsf,nd


select * from ods.bdmd_gsmyqy100q_ai where bdsf='北京市' and nd='2020'

select * from ods.bdmd_gsmyqyzzy100q_ai where bdbm is null

insert overwrite table ods.bdmd_zgmyqyfwy100q_ai
select
    id          ,--string,
    pm          ,--string,
    qymc        ,--string,
    qybm        ,--string comment '企业编码',
    hy          ,--string comment '行业',
    ys          ,--string comment '营收(万元)',
    sf          ,--string comment '省份',
    nd          ,--string comment '年度',
    '20FE4DB12E2045CHY542728534886456' bdbm        ,--string comment '榜单编码',
    create_time ,--timestamp comment '数据创建时间',
    update_time ,--timestamp comment '数据更新时间',
    change_type ,--string comment '数据更新类型',
    status      ,--string comment '数据状态',
    city         --string comment '城市'
from  ods.bdmd_zgmyqyfwy100q_ai
where bdbm is null
union all
select
    id          ,--string,
    pm          ,--string,
    qymc        ,--string,
    qybm        ,--string comment '企业编码',
    hy          ,--string comment '行业',
    ys          ,--string comment '营收(万元)',
    sf          ,--string comment '省份',
    nd          ,--string comment '年度',
    bdbm        ,--string comment '榜单编码',
    create_time ,--timestamp comment '数据创建时间',
    update_time ,--timestamp comment '数据更新时间',
    change_type ,--string comment '数据更新类型',
    status      ,--string comment '数据状态',
    city         --string comment '城市'
from  ods.bdmd_zgmyqyfwy100q_ai
where bdbm is not null
;
select * from dwd.bdmd_qgyxzgtsshzyjsz_wf;



show partitions ods_jcsj.ods_jcsj_sat_ent_basic_info_mi;



create table ods.SJ_PHBDFB
(
    ID       string comment 'ID' ,
    BDM      string comment '榜单名',
    SJBM     string comment '数据表名',
    FBSJ     string comment '发布时间',
    FBJG     string comment '发布机构',
    DY       string comment '地域',
    BDLX     string comment '榜单类型',
    BDLY     string comment '榜单领域',
    BQ       string comment '标签',
    BDRKSJ   string comment '表单入库时间',
    CJSJ     string comment '创建时间',
    CJRY     string comment '创建人员',
    CJBM     string comment '创建部门',
    SPSJ     string comment '审批时间',
    SPRY     string comment '审批人员',
    SPBM     string comment '审批部门',
    RCKBZ    string comment '人才库显示标志',
    SJLY     string comment '数据来源',
    RKSJ     string comment '入库时间',
    GXSJ     string comment '更新时间',
    GXRY     string comment '更新人员',
    GXBM     string comment '更新部门',
    ND       string comment '年度',
    BDDX     string comment '榜单对象',
    BCBD     string comment '是否补充榜单',
    BQJLRQ   string comment '标签建立日期（包括标签挂接）',
    BQPPRQ   string comment '完成标签数据匹配的日期（一对多情况以最后完成日期填写）',
    SFGXQYBQ string comment '企业标签是否更新 1-需要更新；0-无需更新',
    SFPPZSJ  string comment '是否匹配主数据（企业） 1-需要更新；0-无需更新',
    SCBZ     string comment '删除标志',
    SFGXRYBQ string comment '人员标签是否更新 1-需要更新；0-无需更新',
    RYBQZJBZ string comment '人员标签质检标志',
    BQLX     string comment 'RYBQ\QYBQ',
    JB       string comment '届别',
    PC       string comment '批次'
) comment '排行榜单发布'
    row format delimited fields terminated by '\t'
        lines terminated by '\n'
    stored as textfile;



select * from ods.SJ_PHBDFB;














select count(1)
from (
select person_hkey,
             '优秀中国特色社会主义建设者'  as tag_value,
             coalesce(tag_level, '-')      as tag_level,-- 标签层级
             if(dy = '全国', dy, district) as district, -- 地域
             coalesce(jb, nd, '-')         as nd-- 年度
      from (select CASE
                       WHEN instr(b.dy, '省') > 0 THEN '省级'
                       when b.dy in
                            ('内蒙古自治区', '西藏自治区', '新疆维吾尔自治区', '宁夏回族自治区', '广西壮族自治区',
                             '香港特区', '澳门特区', '北京市', '天津市', '上海市', '重庆市') then '省级'
                       when b.dy = '全国' then '全国'
                       else b.dy
                       END                                                       as tag_level,-- 标签层级
                   concat_ws('', province, replace(city, province, ''),
                             replace(district, replace(city, province, ''), '')) as district, -- 地域
                   a.nd,-- 年度
                   b.dy,                                                                      -- 地域
                   b.pc,
                   b.jb,
                   person_hkey,
                   bdbm                                                                       -- 榜单编码
            from (-- select province,city,district,nd,person_hkey,bdbm from dwd.bdmd_yxzgtsshzyjsz_wf
                     -- union all
                     select province, city, district, nd, person_hkey, bdbm from dwd.bdmd_qgyxzgtsshzyjsz_wf) a
                     join ods.SJ_PHBDFB b on a.bdbm = b.id and b.jb='第五届') t
      where person_hkey is not null
) t



select b.jb from dwd.bdmd_qgyxzgtsshzyjsz_wf a
                     join ods.SJ_PHBDFB b on a.bdbm = b.id
group by b.jb;

select a.* from dwd.bdmd_qgyxzgtsshzyjsz_wf a join ods.SJ_PHBDFB b on a.bdbm = b.id and b.jb='第五届' and person_hkey is null


select jb from dwd.bdmd_qgyxzgtsshzyjsz_wf group by jb ;

select count(1) from dwd.bdmd_qgyxzgtsshzyjsz_wf where jb='5';



select * from dwd.bdmd_zggcsyjz_wf ;



select t.person_hkey as rk,
       '中国光彩事业奖章' as tag_value,
       t.tag_level,-- 标签层级
       t.district, -- 地域
       nd-- 年度
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
            b.dy as district, -- 地域
            coalesce(b.pc, b.nd, '-')         as nd,
            qybm,-- 企业编码
            person_hkey,
            rybm,-- 人员编码
            bdbm -- 榜单编码
        from dwd.bdmd_zggcsyjz_wf a
                 join ods.SJ_PHBDFB b on a.bdbm=b.id
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd




select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count(a.person_hkey) as where_cnt,
       count( distinct if(a.person_hkey is not null and b.id is not null,a.person_hkey,null)) as joined_where_distinct_cnt
from dwd.bdmd_zggcsyjz_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id


select count(1)
from dwd.bdmd_zggcsyjz_wf a
        join ods.SJ_PHBDFB b on a.bdbm=b.id



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
            b.dy as district, -- 地域
            coalesce(b.nd,b.jb,b.pc,'-') as nd,-- 年度
            b.jb,
            b.pc,
            person_hkey
        from dwd.bdmd_sbhqs_wf a
                 join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国' and b.nd='2020'
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd



select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count(a.person_hkey) as where_cnt,
       count( distinct if(a.person_hkey is not null and b.id is not null,a.person_hkey,null)) as joined_where_distinct_cnt
from dwd.bdmd_qgyxzgtsshzyjsz_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id  and b.jb='第五届'




select t.person_hkey as rk,
       '五四奖章' as tag_value,
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
            b.dy as district, -- 地域
            coalesce(b.jb,b.nd,b.pc,'-') as nd,-- 年度
            b.jb,
            b.pc,
            person_hkey
        from dwd.bdmd_qgwsjzb_wf a
                 join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国' and b.jb='第二十五届'
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd




select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count( distinct if(a.person_hkey is not null and b.id is not null,a.person_hkey,null)) as joined_filter_distinct_cnt
from dwd.bdmd_qgwsjzb_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id  and b.jb='第二十五届' and b.dy='全国';




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
            -- concat_ws('',province,replace(city,province,''),replace(district,replace(city,province,''),'')) as district, -- 地域
            b.dy as district,
            b.nd,-- 年度
            b.jb,
            b.pc,
            person_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_qgldmf_wf a
                 join ods.SJ_PHBDFB b on a.bdbm=b.id and b.nd='2020'  and b.dy='全国'
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd;



select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count( distinct if(a.person_hkey is not null and b.id is not null,a.person_hkey,null)) as joined_filter_distinct_cnt
from dwd.bdmd_qgldmf_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id  and b.nd='2020'  and b.dy='全国';

select * from ods_jcsj.ods_jcsj_sat_org_basic_info_mi;



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
            b.nd,-- 年度
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_cfsj500qb_wf a
                 join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全球'
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd
;


select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count( distinct if(b.id is not null,concat(a.enterprise_hkey,'-',b.nd),null)) as joined_filter_distinct_cnt
from dwd.bdmd_cfsj500qb_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全球';




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
            b.nd,-- 年度
            enterprise_hkey
        from dwd.bdmd_zgmyqy500q_wf a
        join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国'
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd
;


select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count( distinct if(b.id is not null,concat(a.enterprise_hkey,'-',b.nd),null)) as joined_filter_distinct_cnt
from dwd.bdmd_zgmyqy500q_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国';


select t.enterprise_hkey as rk,
       '中国企业500强' as tag_value,
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
            b.nd,-- 年度
            enterprise_hkey
        from dwd.bdmd_zgqy500q_wf a
                 join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国'
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd
;


select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count( distinct if(b.id is not null,concat(a.enterprise_hkey,'-',b.nd),null)) as joined_filter_distinct_cnt
from dwd.bdmd_zgqy500q_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国';



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
            b.nd,-- 年度
            enterprise_hkey
        from dwd.bdmd_zgmyqyzzy500q_wf a
                 join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国'
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd
;



select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count( distinct if(b.id is not null,concat(a.enterprise_hkey,'-',b.nd),null)) as joined_filter_distinct_cnt
from dwd.bdmd_zgmyqyzzy500q_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国';


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
            b.nd,-- 年度
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_zgmyqyfwy100q_wf a
                 join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国'
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd
;




select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count( distinct if(b.id is not null,concat(a.enterprise_hkey,'-',b.nd),null)) as joined_filter_distinct_cnt
from dwd.bdmd_zgmyqyfwy100q_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国';



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
            b.nd,-- 年度
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_tpgjxjjt_wf a
                 join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国'
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd


select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count( distinct if(b.id is not null,concat(a.enterprise_hkey,'-',b.nd),null)) as joined_filter_distinct_cnt
from dwd.bdmd_tpgjxjjt_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国';




select t.enterprise_hkey as rk,
       '各省民营经济服务业百强榜单' as tag_value,
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
            qybm,-- 企业编码
            enterprise_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_gsmyqyfwy100q_wf a
                 join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy<>'全国'
    ) t
where enterprise_hkey is not null
group by
    t.enterprise_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd
;



select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count( distinct if(b.id is not null,concat(a.enterprise_hkey,'-',b.nd,'-',b.dy),null)) as joined_filter_distinct_cnt
from dwd.bdmd_gsmyqyfwy100q_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy<>'全国';


select dy,
       concat_ws(',',collect_set(concat(nd,':',yd_nd_cnt))) nd_cnts
from
(
    select b.dy,
             b.nd,
             count(distinct concat(b.dy,'-',b.nd,'-',enterprise_hkey)) yd_nd_cnt
      from dwd.bdmd_gsmyqyzzy100q_wf a
               join ods.SJ_PHBDFB b on a.bdbm = b.id and b.dy <> '全国'
      group by b.dy,
               b.nd
) t
group by dy
    ;



SELECT SUM(yd_nd_cnt)
FROM
    (

        select b.dy,
               b.nd,
               count(distinct concat(b.dy,'-',b.nd,'-',enterprise_hkey)) yd_nd_cnt
        from dwd.bdmd_gsmyqyzzy100q_wf a
                 join ods.SJ_PHBDFB b on a.bdbm = b.id and b.dy <> '全国'
        group by b.dy,
                 b.nd
    ) T
;



select distinct a.bdbm
from dwd.bdmd_gsmyqyzzy100q_wf a
         join ods.SJ_PHBDFB b on a.bdbm = b.id and b.dy ='浙江省'

create table dwd.bdmd_gsmyqyzzy100q_wf_bf_231015 as select * from dwd.bdmd_gsmyqyzzy100q_wf ;


select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count( distinct if(b.id is not null,a.person_hkey,null)) as joined_where_distinct_cnt
from dwd.bdmd_qgyxzgtsshzyjsz_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id  and b.jb='第五届'
;




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
            b.nd,-- 年度
            cc_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_qgshsh_wf a
                 join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国' and b.nd='2021-2022'
    ) t
where cc_hkey is not null
group by
    t.cc_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd
;


select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count( distinct if(b.id is not null,a.cc_hkey,null)) as joined_where_distinct_cnt
from dwd.bdmd_qgshsh_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy='全国' and b.nd='2021-2022'
;


select dy,
       concat_ws(',',collect_set(concat(nd,':',yd_nd_cnt))) nd_cnts
from
    (
        select b.dy,
               b.nd,
               count(distinct concat(b.dy,'-',b.nd,'-',enterprise_hkey)) yd_nd_cnt
        from dwd.bdmd_gsmyqyfwy100q_wf a
                 join ods.SJ_PHBDFB b on a.bdbm = b.id and b.dy <> '全国'
        group by b.dy,
                 b.nd
    ) t
group by dy
;

-- 1412
select sum(yd_nd_cnt)
from(

        select b.dy,
               b.nd,
               count(distinct concat(b.dy,'-',b.nd,'-',enterprise_hkey)) yd_nd_cnt
        from dwd.bdmd_gsmyqyfwy100q_wf a
                 join ods.SJ_PHBDFB b on a.bdbm = b.id and b.dy <> '全国'
        group by b.dy,
                 b.nd
    ) t ;



select count(1) as all_cnt,
       count(b.id) as joined_cnt,
       count( distinct if(b.id is not null,concat(a.enterprise_hkey,'-',b.nd,'-',b.dy),null)) as joined_filter_distinct_cnt
from dwd.bdmd_gsmyqyfwy100q_wf a
         left join ods.SJ_PHBDFB b on a.bdbm=b.id and b.dy<>'全国';



select count(1)
from
(
      select person_hkey as rk,
             '党代表'    as tag_value,
             jb
      from dwd.bdmd_20dddb_WF
      where jb='第二十届'
      group by person_hkey, jb
) t
;

select count(1),count(distinct concat(rk,'_',year))
from (select org_hkey           as rk,
             '“五好”县级工商联' as tag_value,
             year, --录入年份
             '全国'             as dy
      from ods_jcsj.ods_jcsj_sat_org_wuhao_info_mi a
      where create_dts >= '2023/10/15'
      ) t
;


select count(1),count(distinct concat_ws('_',rk,nd))
from (select t.cc_hkey    as rk,
             '“四好”商会' as tag_value,
             '全国'       as tag_level,-- 标签层级
             '全国'       as district, -- 地域
             year         as nd-- 年度
      from ods_jcsj.ods_jcsj_sat_cc_sihao_info_mi t
      where create_dts >= '2023/10/15'
      ) t

select count(1)
from
(
      select t.cc_hkey as rk,
             year      as nd-- 年度
      from ods_jcsj.ods_jcsj_sat_cc_sihao_info_mi t
      where create_dts >= '2023/10/15'
) t


