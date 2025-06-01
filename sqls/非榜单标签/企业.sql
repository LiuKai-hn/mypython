




-- 基本信息
insert overwrite table tmp_lk.label_ent_other
select t1.enterprise_hkey,
       concat(cast(trim(registered_fund)/500 as bigint)*500,'-',(cast(trim(registered_fund)/500 as bigint)+1)*500,'万元') as registered_fund, -- 注册资金规模
       concat((cast((year(current_date())-substr(trim(establish_time),1,4))/5 as bigint))*5,'-',
              (cast((year(current_date())-substr(trim(establish_time),1,4))/5 as bigint)+1)*5,'年')
           as establish_time, -- 成立年限
       -- operating_status, -- 经营状态
       dim_operating_tb.code_cn as operating_status_value,-- 经营状态
       -- enterprise_basistype, --企业类型(企业性质)
       dim_basistype_tb.code_cn as enterprise_basistype_value, --企业类型(企业性质)
       -- industry, --行业类型
       dim_industry_tb.code_cn as industry_value,-- 行业类型
       business_scope, --经营范围
       jiang_tb.jiang_cnt, -- 获奖次数
       dangzuzhi_tb.PARTY_STRUCTURE, -- 党组织结构
       dangzuzhi_tb.PARTY_MEMBER_NUM, -- 党员人数
       case when dangzuzhi_tb.is_labor_union='1' then '是'
            when dangzuzhi_tb.is_labor_union='0' then '否'
            else null end as is_labor_union, -- 是否建立工会组织
       dangzuzhi_tb.LABOR_UNION_MEMBER_NUM, -- 工会会员人数
       case when dangzuzhi_tb.is_league='1' then '是'
           when dangzuzhi_tb.is_league='0' then '否'
               else null end as is_league, -- 是否建立共青团组织
       dangzuzhi_tb.LEAGUE_MEMBER_NUM, -- 共青团团员人数
       grow_honor_tb.intellectual_property_total,-- 知识产权数(项)
       grow_honor_tb.valid_trademark_num, --商标注册量
       grow_honor_tb.patent_num,--                  string comment '专利数',
       grow_honor_tb.valid_soft_copyright, --        string comment '软件著作权数'
       websit_tb.websit_cnt,-- 网站备案数
       work_tb.work_cnt -- 作品数
from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi t1
left join dwd.sys_dic_code_info_do dim_operating_tb on trim(t1.operating_status)=dim_operating_tb.code and dim_operating_tb.item_name_cn='经营状态代码' and dim_operating_tb.code not in ('00','99')
left join dwd.sys_dic_code_info_do dim_basistype_tb on trim(t1.enterprise_basistype)=dim_basistype_tb.code and dim_basistype_tb.item_name_cn='企业基础分类代码' and dim_basistype_tb.code not in ('99','00')
left join dwd.sys_dic_code_info_do dim_industry_tb on trim(t1.industry)=dim_industry_tb.code and dim_industry_tb.item_name_cn='国民经济行业分类代码' and dim_industry_tb.code not in ('0000','9999')
left join tmp_lk.dangzuzhi dangzuzhi_tb on t1.enterprise_hkey=dangzuzhi_tb.ENTERPRISE_HKEY
left join tmp_lk.jiang_tb jiang_tb on t1.enterprise_hkey=jiang_tb.enterprise_hkey
left join (
    select ent_inov_grow_hkey as enterprise_hkey,
           intellectual_property_total,-- string comment '自主知识产权数'
           valid_trademark_num, --商标注册量
           patent_num,--                  string comment '专利数',
           valid_soft_copyright --        string comment '软件著作权数'
    from ods_jcsj.ods_jcsj_sat_ent_inov_grow_honor_mi
    where dt='jcsjk700'
) grow_honor_tb on t1.enterprise_hkey=grow_honor_tb.enterprise_hkey
left join (
    select enterprise_hkey,
           count(1) websit_cnt -- 网站备案数
    from ods_jcsj.ods_jcsj_sat_ent_web_on_file_info_mi
    where dt='jcsjk700'
    group by enterprise_hkey
) websit_tb on t1.enterprise_hkey=websit_tb.enterprise_hkey
left join tmp_lk.work_cnt_tb work_tb on t1.enterprise_hkey=work_tb.enterprise_hkey
where t1.dt='jcsjk700'
;

-- 这三个字段没有数
-- valid_trademark_num         string comment '商标注册量',
-- patent_num                  string comment '专利数',
-- valid_soft_copyright 软件著作权数

select work_cnt
from tmp_lk.label_ent_other
where work_cnt is not null and length(work_cnt)>0
limit 100;


insert overwrite table tmp_lk.label_ent_other
select
    t1.enterprise_hkey             ,--string comment '企业hash',
    t1.registered_fund             ,--string comment '注册资金规模',
    t1.establish_time              ,--string comment '成立年限',
    t1.operating_status_value      ,--string comment '经营状态',
    t1.enterprise_basistype_value  ,--string comment '企业类型(企业性质)',
    t1.industry_value              ,--string comment '行业类型',
    t1.business_scope              ,--string,
    t1.jiang_cnt                   ,--string comment '获奖次数',
    t1.party_structure             ,--string comment '党组织结构',
    t1.party_member_num            ,--string comment '党员人数',
    t1.is_labor_union              ,--string comment '是否建立工会组织',
    t1.labor_union_member_num      ,--string comment '工会会员人数',
    t1.is_league                   ,--string comment '是否建立共青团组织',
    t1.league_member_num           ,--string comment '共青团团员人数',
    t1.intellectual_property_total ,--string comment '知识产权数(项)',
    valid_trademark_num_tb.valid_trademark_num         ,--string comment '商标注册量',
    patent_num_tb.patent_num                  ,--string comment '专利数',
    valid_soft_copyright_tb.valid_soft_copyright        ,--string comment '软件著作权数',
    t1.websit_cnt                  ,--string comment '网站备案数',
    t1.work_cnt                     --string comment '作品数'
from tmp_lk.label_ent_other t1
left join (
    select enterprise_hkey,
           count(1) as valid_trademark_num -- 商标注册量
    from ods_jcsj.ods_jcsj_sat_ent_brand_info_mi
    where dt='jcsjk700'
    group by enterprise_hkey
) valid_trademark_num_tb on t1.enterprise_hkey=valid_trademark_num_tb.enterprise_hkey
left join
    (select enterprise_hkey,
                  count(1) as patent_num -- 专利数
           from ods_jcsj.ods_jcsj_sat_ent_patent_info_mi
           where dt = 'jcsjk700'
           group by enterprise_hkey
           ) patent_num_tb on t1.enterprise_hkey=patent_num_tb.enterprise_hkey
left join
     (select enterprise_hkey,
             count(1) as valid_soft_copyright -- 软件著作权数
      from ods_jcsj.ods_jcsj_sat_ent_software_copyright_mi
      where dt = 'jcsjk700'
      group by enterprise_hkey
      ) valid_soft_copyright_tb on t1.enterprise_hkey=valid_soft_copyright_tb.enterprise_hkey



show partitions ads.zhjc_enterprise_cost;


select enterprise_scale,
       count(1)
from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi
where dt='jcsjk700' and length(trim(enterprise_scale))>0
group by enterprise_scale


select *
from dwd.sys_dic_code_info_do dim_basistype_tb
where dim_basistype_tb.code='01' and dim_basistype_tb.item_name_cn='企业类型代码' and dim_basistype_tb.code not in ('9900')



select *
from tmp_lk.dangzuzhi
where dangzuzhi.LABOR_UNION_MEMBER_NUM is not null ;



select ent_inov_grow_hkey as enterprise_hkey,
       intellectual_property_total,-- string comment '自主知识产权数'
       valid_trademark_num, --商标注册量
       patent_num,--                  string comment '专利数',
       valid_soft_copyright --        string comment '软件著作权数'
from ods_jcsj.ods_jcsj_sat_ent_inov_grow_honor_mi
where dt='jcsjk700'
;



select enterprise_hkey,
       count(1) websit_cnt -- 网站备案数
from ods_jcsj.ods_jcsj_sat_ent_web_on_file_info_mi
where dt='jcsjk700'
group by enterprise_hkey
;







-- 作品数
select t2.enterprise_hkey,
       count(1) work_cnt
from ods_jcsj.ods_jcsj_sat_item_work_copyright_mi t1
join ods_jcsj.ods_jcsj_link_item_ent_mi t2 on t1.item_hkey=t2.item_hkey
where t1.dt='jcsjk700' and t2.dt='jcsjk700'
group by t2.enterprise_hkey


select item_hkey,
       enterprise_hkey
from ods_jcsj.ods_jcsj_link_item_ent_mi
where dt='jcsjk700'






select enterprise_hkey as rk,
       establish_time
from tmp_lk.label_ent_other
where establish_time is not null



select enterprise_hkey,
       '注册资本规模' as tag_name,
       concat(floor(REGISTERED_FUND/500)*500,"-",ceil(REGISTERED_FUND/500)*500,'万元') as tag_value
from (
         select enterprise_hkey,
                REGISTERED_FUND,
                row_number() over (partition by enterprise_hkey order by update_dts desc ) rk
         from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi
     ) t
where t.rk=1


select enterprise_hkey,
       league_member_num
from tmp_lk.label_ent_other
where league_member_num is not null


select enterprise_hkey,
       REGISTERED_FUND,
       concat(floor(REGISTERED_FUND/500)*500,"-",ceil(REGISTERED_FUND/500)*500,'万元') as tag_value
from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi
where dt='jcsjk700'







