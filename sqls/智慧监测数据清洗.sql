
select enterprise_hkey,
       a.industry_code,
       industry_value,
       dim_industry.dic_name industry_value1,
       enterprise_scale_code,
       enterprise_scale_value,
       dim_enterprise_scale.dic_name as enterprise_scale_value1,
       is_listed,
       industry_class, -- 未解决
       org_id,
       -- main_business,
       business_registration_type,
       business_registration_value,
       dim_business.dic_name business_registration_value1,
       business_scope, -- 未解决
       main_products_services
from dwd.dim_ent_basic_info_do a
left join dwd.dim_ent_dic_do dim_business on a.business_registration_type=dim_business.dic_code and dim_business.dic_type='工商注册类型代码'
left join dwd.dim_ent_dic_do dim_enterprise_scale on a.enterprise_scale_code=dim_enterprise_scale.dic_code and dim_enterprise_scale.dic_type='企业规模代码'
left join dwd.dim_ent_dic_do dim_industry on a.industry_code=dim_industry.dic_code and dim_industry.dic_type='国民经济行业分类代码'




select industry_class
from dwd.dim_ent_basic_info_do
group by industry_class



select industry_class,
       count(1) cnt
from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi
where dt='jcsjk700'
group by industry_class
;




select id,
       words,
       popularity,
       concat_ws(',',collect_set(enterprise_hkey)) ents
from ads.zhjc_hot_words_do
group by id,
         words,
         popularity
;


show partitions ods_jcsj.ods_jcsj_link_org_ent_mi;



select * from ods_jcsj.ods_jcsj_link_org_ent_mi;

select * from dwd.dim_ent_industry_dic_do;


select *
from
    (
        select *,
               row_number() over(partition by sat_id order by update_dts desc) rk
        from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi
        where enterprise_hkey in (
            select ENTERPRISE_HKEY
            from
                (
                    -- 小巨人
                    select enterprise_hkey from dwd.bdmd_gxbzjtxxjrqy_wf
                    union
                    select enterprise_hkey from dwd.bdmd_zjtxxjrqymd_wf
                    union
                    -- 执常委企业
                    select b.ENTERPRISE_HKEY
                    from
                        (
                            select ENT_EXE_HKEY
                            from
                                (
                                    select
                                        ENT_EXE_HKEY,
                                        row_number() over(partition by ENT_EXE_HKEY order by update_dts desc ) rk
                                    from ods_jcsj.ods_jcsj_sat_exe_committee_ent_mi
                                    where dt='jcsjk700'
                                ) t
                            where t.rk=1
                        ) a
                            join
                        (
                            select ENT_EXE_HKEY,
                                   ENTERPRISE_HKEY
                            from
                                (
                                    select ENT_EXE_HKEY,
                                           ENTERPRISE_HKEY,
                                           row_number() over(partition by ENT_EXE_HKEY,ENTERPRISE_HKEY order by update_dts desc ) rk
                                    from ods_jcsj.ods_jcsj_link_ent_exe_mi
                                    where dt='jcsjk700'
                                ) t
                            where t.rk=1
                        ) b on a.ENT_EXE_HKEY=b.ENT_EXE_HKEY
                ) tt group by enterprise_hkey
        ) and dt='jcsjk700' and unified_social_credit_code is not null and length(unified_social_credit_code)>0
    ) t
where rk=1;




select industry_class,count(1)
from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi
where dt='jcsjk700' and industry_class is not null and length(industry_class)>0
group by  industry_class limit 100
;


select ENTERPRISE_HKEY,
       REGISTERATION_FUND,
       register_address
from ods_jcsj.ods_jcsj_SAT_ENT_LISTING_INFO_mi
where dt='jcsjk700';





-- ods_jcsj.ods_jcsj_sat_org_basic_info_mi
--

select * from ods_jcsj.ods_jcsj_sat_org_basic_info_mi;







select count(*) from dwd.ent_committee_info_do;





select committee_type_code,
       b.dic_name
    from ods_jcsj.ods_jcsj_SAT_EXE_COMMITTEE_INFO_mi a
join dwd.dim_ent_dic_do b on a.committee_type_code=b.dic_code and b.dic_type='执常委身份代码'
limit 100;




show partitions ods_jcsj.ods_jcsj_sat_org_basic_info_mi;



select * from ods_jcsj.ods_jcsj_sat_represent_congress_mi where dt='jcsjk700'






select * from ods_jcsj.ods_jcsj_sat_ent_type_info_mi where dt='jcsjk700';



select count(1)
from (select enterprise_hkey, enterprise_name
      from dwd.dim_ent_basic_info_do
      group by enterprise_hkey, enterprise_name
      ) t
;



select enterprise_hkey, count(1)
from dwd.dim_ent_basic_info_do
group by enterprise_hkey
having count(1)>1
limit 100;


select sum(cnt)
from (select unified_social_credit_code-- , count(1) cnt
      from dwd.dim_ent_basic_info_do
      group by unified_social_credit_code
      having count(1) > 1
      ) t
limit 100;






select * from dim_ent_basic_info_do
where unified_social_credit_code in (
                     '51610000748643145T',
                     '91110102717707599K',
                     '91110108306626516Q',
                     '91110108551385082Q',
                     '91110108562144110X',
                     '911101087868925051',
                     '91120000758104575G',
                     '91120106578309661B',
                     '91130124792667603Y',
                     '912101006753207829',
                     '912101051177315283',
                     '91211100122544049B',
                     '912201015563540669',
                     '91220422664266370A',
                     '91220501244585084W',
                     '912301081274212679',
                     '91310000341984589Y',
                     '91310000703257697D',
                     '91310115051251125K',
                     '91310118342373536G',
                     '91320106751253359F',
                     '91320214MA1MLB3M2A',
                     '913205007149960577',
                     '9132060071496854X3',
                     '91330100759518526A',
                     '91330108699849074K',
                     '91330500753970802B',
                     '91340100097192693N',
                     '91340100570434353J',
                     '91340400798112983X',
                     '913502006120063749',
                     '91350723157307916W',
                     '91360000759950474A',
                     '9136010815826062X2',
                     '91360521558456576Y',
                     '91360902563810293X',
                     '91370000743395445T',
                     '91370281MA3CGHB07Q',
                     '91370400169901867G',
                     '913705027465621145',
                     '913713125677099268',
                     '914101002717447221',
                     '914102007065333199',
                     '91420000707103812E',
                     '91420100717910327G',
                     '91420100737509805P',
                     '91420500798767365X',
                     '91422800343511312R',
                     '91430100788042096T',
                     '914301023528482162',
                     '91430121077191431K',
                     '9143020067078086X5',
                     '91433126066351268C',
                     '9144060067707962X1',
                     '91440705739866136J',
                     '914419006844054388',
                     '914502001986024442',
                     '914502006953649851',
                     '915002242032293661',
                     '9150024067336276XY',
                     '91510000207305821R',
                     '91510000752346848U',
                     '915101000833108553',
                     '9151010020237318XW',
                     '91520000067735650R',
                     '91520100785460452C',
                     '9152011530874695XC',
                     '91520115556624329D',
                     '91520115MA6DMFBM07',
                     '915203827095211887',
                     '915300007343187128',
                     '91532300787384545P',
                     '916103027552413556',
                     '91620100767735088Y',
                     '91630100226701336D',
                     '91640100694329053P',
                     '91640200750828800U',
                     '91650100751664553B',
                     '916532240760713236'
    );





select count(1) from dwd.dim_ent_basic_info_do;

select * from dwd.dim_ent_basic_info_do where enterprise_hkey in (
                                                                  '07EFD836CCED0D8CD69FAEDCB15F0A1C',
                                                                  '5530A1DD70C16DF045D4950111152C7D',
                                                                  'AACC8EEA1F2BA6DF5ABB06D2CCEAAB11',
                                                                  'B1885208D61B5388908CA614E6512B60',
                                                                  'E06167F56C60674BEEC97D1BCBA3942E',
                                                                  '7AC666D85CC0EBF2512B034254DEF9ED',
                                                                  'D0E88EB6FA8723AF94D149768A813E3F'


    );

select *
from dwd.dim_ent_dic_do dim_industry
where dim_industry.dic_code='15' and dim_industry.dic_type='国民经济行业分类代码'


select enterprise_hkey,
       enterprise_name,
       unified_social_credit_code
from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi
where dt='jcsjk700' and enterprise_hkey='0028C360B5EE190DC4A8FA2A33923A0A'


-- 修正 将多个抓换成省级别
select enterprise_hkey,
       province_code
from (select enterprise_hkey,
             concat(substr(province_code, 1, 2), '0000') as province_code
      from (select enterprise_hkey,
                   province_code
            from dwd.dim_ent_basic_info_do
                     lateral view explode(split(reg_code, '，')) tmp as province_code
            where reg_code is not null
              and length(province_code) > 3) t1
      ) t2
group by enterprise_hkey,
         province_code
;



insert overwrite table dwd.dim_ent_basic_info_do
select
    t1.enterprise_hkey             ,--string comment 'hash主键',
    enterprise_name             ,--string comment '企业名称',
    reg_number                  ,--string comment '工商注册号',
    business_registration_type  ,--string comment '工商注册类型',
    business_registration_value ,--string comment '工商注册类型值',
    enterprise_scale_code       ,--string comment '企业规模编码',
    enterprise_scale_value      ,--string comment '企业规模值',
    date_format(t1.establish_time,'yyyy-MM-dd')              ,--string comment '成立时间',
    industry_code               ,--string comment '所属行业代码',
    industry_value              ,--string comment '所属行业值',
    reg_status                  ,--string comment '注册状态',
    registered_fund_currency    ,--string comment '注册资本市种',
    registered_fund             ,--string comment '注册资金',
    legal_represent_name        ,--string comment '法人代表名称',
    reg_office                  ,--string comment '登记机关',
    province                    ,--string comment '省',
    city                        ,--string comment '市',
    county                      ,--string comment '县',
    t2.province_code reg_code                    ,--string comment '企业所在行政区域代码',
    country                     ,--string comment '企业所在国家或地区',
    date_format(t1.operating_period_start,'yyyy-MM-dd')      ,--string comment '经营期限起始日期',
    date_format(t1.operating_period,'yyyy-MM-dd')            ,--string comment '经营期限截止日期',
    operating_status            ,--string comment '经营状态',
    main_business               ,--string comment ' 主营业务',
    main_busi_industry          ,--string comment '主营业务所属行业',
    registered_place            ,--string comment '企业注册地',
    registered_admini           ,--string comment ' 企业注册地行政区域',
    industry_class              ,--string comment '产业分类',
    industry_class_value        ,--string comment '产业分类值',
    date_format(t1.reg_time,'yyyy-MM-dd')                     ,--string comment '注册时间',
    main_products_services      ,--string comment '主要产品与服务',
    is_listed                   ,--string comment '企业是否上市',
    unified_social_credit_code  ,--string comment '统一社会信用代码',
    business_scope              ,--string comment '经营业务范围',
    business_scope_value        ,--string comment '经营范围值',
    date_format(t1.revoke_date,'yyyy-MM-dd')                 ,--string comment '吊销日期',
    date_format(t1.logoff_date,'yyyy-MM-dd')                 ,--string comment '注销日期',
    REGISTERATION_FUND          ,--string comment '注册资本',
    ENTERPRISE_ADDRESS          ,--string comment '企业地址',
    fundamental_state           ,--string comment '企业基本情况简介',
    is_often                    ,--string comment '是否为执常委企业',
    commi_level                 ,--string comment '执常委级别',
    is_litter                   ,--string comment '是否小巨人企业',
    is_chamber                  ,--string comment '是否商会企业',
    org_id                      ,--string comment '所属工商联',
    create_time                 ,--string comment '创建时间',
    change_time                 ,--string comment '更新时间',
    is_deleted                   --string comment '删除标识'
from dwd.dim_ent_basic_info_do t1
         join (
    select enterprise_hkey,
           province_code
    from (select enterprise_hkey,
                 concat(substr(province_code, 1, 2), '0000') as province_code
          from (select enterprise_hkey,
                       province_code
                from dwd.dim_ent_basic_info_do
                         lateral view explode(split(reg_code, '，')) tmp as province_code
                where reg_code is not null
                  and length(province_code) > 3) t1
         ) t2
    group by enterprise_hkey,
             province_code
) t2 on t1.enterprise_hkey=t2.enterprise_hkey
;







select * from tmp_lk.ent_times;

insert overwrite table dwd.dim_ent_basic_info_do
select
    enterprise_hkey             ,--string comment 'hash主键',
    enterprise_name             ,--string comment '企业名称',
    reg_number                  ,--string comment '工商注册号',
    business_registration_type  ,--string comment '工商注册类型',
    business_registration_value ,--string comment '工商注册类型值',
    enterprise_scale_code       ,--string comment '企业规模编码',
    enterprise_scale_value      ,--string comment '企业规模值',
    date_format(t2.establish_time,'yyyy-MM-dd')              ,--string comment '成立时间',
    industry_code               ,--string comment '所属行业代码',
    industry_value              ,--string comment '所属行业值',
    reg_status                  ,--string comment '注册状态',
    registered_fund_currency    ,--string comment '注册资本市种',
    registered_fund             ,--string comment '注册资金',
    legal_represent_name        ,--string comment '法人代表名称',
    reg_office                  ,--string comment '登记机关',
    province                    ,--string comment '省',
    city                        ,--string comment '市',
    county                      ,--string comment '县',
    reg_code                    ,--string comment '企业所在行政区域代码',
    country                     ,--string comment '企业所在国家或地区',
    date_format(t2.operating_period_start,'yyyy-MM-dd')      ,--string comment '经营期限起始日期',
    date_format(t2.operating_period,'yyyy-MM-dd')            ,--string comment '经营期限截止日期',
    operating_status            ,--string comment '经营状态',
    main_business               ,--string comment ' 主营业务',
    main_busi_industry          ,--string comment '主营业务所属行业',
    registered_place            ,--string comment '企业注册地',
    registered_admini           ,--string comment ' 企业注册地行政区域',
    industry_class              ,--string comment '产业分类',
    industry_class_value        ,--string comment '产业分类值',
    date_format(t2.reg_time,'yyyy-MM-dd')                     ,--string comment '注册时间',
    main_products_services      ,--string comment '主要产品与服务',
    is_listed                   ,--string comment '企业是否上市',
    unified_social_credit_code  ,--string comment '统一社会信用代码',
    business_scope              ,--string comment '经营业务范围',
    business_scope_value        ,--string comment '经营范围值',
    date_format(t2.revoke_date,'yyyy-MM-dd')                 ,--string comment '吊销日期',
    date_format(t2.logoff_date,'yyyy-MM-dd')                 ,--string comment '注销日期',
    REGISTERATION_FUND          ,--string comment '注册资本',
    ENTERPRISE_ADDRESS          ,--string comment '企业地址',
    fundamental_state           ,--string comment '企业基本情况简介',
    is_often                    ,--string comment '是否为执常委企业',
    commi_level                 ,--string comment '执常委级别',
    is_litter                   ,--string comment '是否小巨人企业',
    is_chamber                  ,--string comment '是否商会企业',
    org_id                      ,--string comment '所属工商联',
    create_time                 ,--string comment '创建时间',
    change_time                 ,--string comment '更新时间',
    is_deleted                   --string comment '删除标识'
from dwd.dim_ent_basic_info_do t1
join tmp_lk.ent_times t2 on t1.enterprise_hkey=t2.key
;

select count(1) from tmp_lk.ent_times;

select count(1) from dwd.dim_ent_basic_info_do;

select t2.establish_time,
       t2.operating_period_start,
       t2.operating_period,
       t2.reg_time,
       t2.revoke_date,
       t2.logoff_date,
       t2.reg_code
from dwd.dim_ent_basic_info_do t2;



select * from dim_ent_basic_info_do where reg_number='DEE7F9A6757BFFCE9174C57719AD7F2D';


select count(*) from dwd.dim_ent_basic_info_do;

select key,count(1)
from tmp_lk.ent_times
group by key
having count(1)>1
;




select enterprise_hkey,count(1) cnt
from dwd.ent_committee_info_do
group by enterprise_hkey;


show partitions ods_jcsj.ods_jcsj_link_org_ent_mi;



select enterprise_hkey,
       org_hkey,
       count(1)
from ods_jcsj.ods_jcsj_link_org_ent_mi
where dt='jcsjk700'
group by enterprise_hkey,
         org_hkey
having count(1)>1
limit 100
;




show partitions ods_jcsj.ods_jcsj_sat_org_wuhao_info_mi;

truncate table ods_jcsj.ods_jcsj_sat_org_wuhao_info_mi;



select count(1)
from (

         select enterprise_hkey from ods_jcsj.ods_jcsj_sat_ent_type_info_mi where dt='jcsjk700' and srdi_type='03'group by enterprise_hkey

     ) t;


select ENTERPRISE_HKEY,org_id,count(1)
from(
        select * from dwd.ent_committee_info_do

    ) t
group by ENTERPRISE_HKEY,org_id
having count(1)>1
limit 100;

select * from dwd.ent_committee_info_do where enterprise_hkey='61A8DB76A12F078B94B1062C2BB76FE6'


select count(1)
from (select enterprise_hkey from dwd.ent_committee_info_do group by enterprise_hkey) t




select count(*) from dwd.ent_committee_info_do;


select * from dwd.zhjc_ent_basic_info_df where dt='2023-10-18';

select * from ads.zhjc_ent_basic_info_do;

create table tmp_lk.dim_ent_basic_info_do_bk_1019 as
select * from dwd.dim_ent_basic_info_do;


select count(*) from dwd.dim_ent_basic_info_do;


insert overwrite table dwd.dim_ent_basic_info_do
        select
            t1.enterprise_hkey             ,--string comment 'hash主键',
            enterprise_name             ,--string comment '企业名称',
            reg_number                  ,--string comment '工商注册号',
            business_registration_type  ,--string comment '工商注册类型',
            business_registration_value ,--string comment '工商注册类型值',
            enterprise_scale_code       ,--string comment '企业规模编码',
            enterprise_scale_value      ,--string comment '企业规模值',
            date_format(t1.establish_time,'yyyy-MM-dd')              ,--string comment '成立时间',
            industry_code               ,--string comment '所属行业代码',
            industry_value              ,--string comment '所属行业值',
            reg_status                  ,--string comment '注册状态',
            registered_fund_currency    ,--string comment '注册资本市种',
            registered_fund             ,--string comment '注册资金',
            legal_represent_name        ,--string comment '法人代表名称',
            reg_office                  ,--string comment '登记机关',
            province                    ,--string comment '省',
            city                        ,--string comment '市',
            county                      ,--string comment '县',
            t2.province_code reg_code                    ,--string comment '企业所在行政区域代码',
            country                     ,--string comment '企业所在国家或地区',
            date_format(t1.operating_period_start,'yyyy-MM-dd')      ,--string comment '经营期限起始日期',
            date_format(t1.operating_period,'yyyy-MM-dd')            ,--string comment '经营期限截止日期',
            operating_status            ,--string comment '经营状态',
            main_business               ,--string comment ' 主营业务',
            main_busi_industry          ,--string comment '主营业务所属行业',
            registered_place            ,--string comment '企业注册地',
            registered_admini           ,--string comment ' 企业注册地行政区域',
            industry_class              ,--string comment '产业分类',
            industry_class_value        ,--string comment '产业分类值',
            date_format(t1.reg_time,'yyyy-MM-dd')                     ,--string comment '注册时间',
            main_products_services      ,--string comment '主要产品与服务',
            is_listed                   ,--string comment '企业是否上市',
            unified_social_credit_code  ,--string comment '统一社会信用代码',
            business_scope              ,--string comment '经营业务范围',
            business_scope_value        ,--string comment '经营范围值',
            date_format(t1.revoke_date,'yyyy-MM-dd')                 ,--string comment '吊销日期',
            date_format(t1.logoff_date,'yyyy-MM-dd')                 ,--string comment '注销日期',
            REGISTERATION_FUND          ,--string comment '注册资本',
            ENTERPRISE_ADDRESS          ,--string comment '企业地址',
            fundamental_state           ,--string comment '企业基本情况简介',
            is_often                    ,--string comment '是否为执常委企业',
            commi_level                 ,--string comment '执常委级别',
            is_litter                   ,--string comment '是否小巨人企业',
            is_chamber                  ,--string comment '是否商会企业',
            org_id                      ,--string comment '所属工商联',
            create_time                 ,--string comment '创建时间',
            change_time                 ,--string comment '更新时间',
            is_deleted                   --string comment '删除标识'
        from dwd.dim_ent_basic_info_do t1
                 left join (
            select enterprise_hkey,
                   province_code
            from (select enterprise_hkey,
                         concat(substr(province_code, 1, 2), '0000') as province_code
                  from (select enterprise_hkey,
                               province_code
                        from dwd.dim_ent_basic_info_do
                                 lateral view explode(split(reg_code, '，')) tmp as province_code
                        where reg_code is not null
                          and length(province_code) > 3
                        ) t1
                 ) t2
            group by enterprise_hkey,
                     province_code
        ) t2 on t1.enterprise_hkey=t2.enterprise_hkey
;




select dic_type,
       dic_code,
       count(1) cnt
from dwd.dim_ent_dic_do
where dic_type='国民经济行业分类代码'
group by dic_type,
         dic_code
having count(1)>1
;



select news_title,release_website
from ads.zhjc_news_info_do
group by release_website;



select news_source
from ods_jcsj.ods_jcsj_sat_news_info_mi
where dt='jcsjk700' and news_source is not null
group by release_website
;

select * from ods_jcsj.ods_jcsj_LINK_AFIR_ENT_mi
where dt='jcsjk700'
;


select news_source
from dwd.zhjc_news_info_df
where dt='2023-10-18'
and news_source is not null
  and length(news_source)>0
group by news_source;






insert overwrite table dwd.zhjc_news_info_df partition(dt='2023-10-18')
select
    COALESCE(new_tb.id,old_tb.id) as id,
    COALESCE(new_tb.affair_hkey,old_tb.affair_hkey) as NEWS_HKEY,
    COALESCE(new_tb.enterprise_hkey,old_tb.enterprise_hkey) as enterprise_hkey,
    COALESCE(new_tb.published_time,old_tb.published_time) as published_time,
    COALESCE(new_tb.news_title,old_tb.news_title) as news_title,
    COALESCE(new_tb.news_abstract,old_tb.news_abstract) as news_abstract,
    COALESCE(new_tb.release_website,old_tb.release_website) as release_website,
    COALESCE(new_tb.news_source,old_tb.news_source) as news_source,
    COALESCE(new_tb.news_url,old_tb.news_url) as news_url,
    COALESCE(new_tb.news_content,old_tb.news_content) as news_content,
    -- COALESCE(new_tb.pos_or_neg,old_tb.pos_or_neg) as pos_or_neg,
    null as pos_or_neg,
    if(old_tb.change_time>=new_tb.update_dts,COALESCE(old_tb.change_time,'2023-10-18'),'2023-10-18') as change_time , --  '更新时间',
    case
        when new_tb.is_deleted='000000001' then '2'
        when old_tb.id is not null and new_tb.id is not null then '1'
        else '0'
        end as change_type                 , --  '数据更新类型（0：新增，1：修改，2：删除）',
    case when new_tb.is_deleted='000000001' then '0'
         else '1'
        end  as status,                        --  '数据状态'
    COALESCE(new_tb.begin_time,old_tb.begin_time) as begin_time,
    COALESCE(new_tb.finish_time,old_tb.finish_time) as finish_time
from (
         select *
         from dwd.zhjc_news_info_df
         where dt = '${pre_etl_time}'
     )old_tb
         full outer join
     (
         select a.*,
                c.affair_hkey,
                c.enterprise_hkey
         from
             (
                 select sat_id as id,
                        NEWS_HKEY as NEWS_HKEY             ,-- 'id',
                        from_unixtime(cast(published_time as bigint), 'yyyy-MM-dd HH:mm:ss') as published_time          ,-- '发布时间',
                        news_title      ,-- '新闻标题',
                        news_abstract, -- 新闻摘要
                        release_website,
                        news_url            ,-- '新闻网址',
                        news_source, -- 新闻来源
                        news_content ,-- '新闻内容'
                        -- emotional_state as pos_or_neg,-- '情感分析状态',
                        a.is_deleted,
                        date_format(replace(a.update_dts,'/','-'),'yyyy-MM-dd') update_dts, -- 更新时间
                        if(a.IS_DELETED='000000001','2','1') as change_type ,-- 数据更新类型
                        if(a.IS_DELETED='000000001','0','1') as status, --数据状态
                        date_format(replace(a.PUBLISHED_TIME,'/','-'),'yyyy-MM-dd')       as begin_time, -- 开始时间
                        if(a.IS_DELETED='1',date_format(replace(a.update_dts,'/','-'),'yyyy-MM-dd'),null)        as finish_time -- 结束时间
                 from
                     (
                         select * ,
                                row_number() over(partition by sat_id order by update_dts desc ) rk
                         from ods_jcsj.ods_jcsj_sat_news_info_mi
                         where dt='jcsjk700'
                     ) a
                 where a.rk=1
             ) a
                 join
             (
                 select affair_hkey,
                        news_hkey
                 from
                     (
                         select affair_hkey,news_hkey ,
                                row_number() over(partition by affair_hkey,news_hkey order by update_dts desc ) rk
                         from ods_jcsj.ods_jcsj_LINK_AFIR_NEWS_mi
                         where dt='jcsjk700'
                     ) a
                 where a.rk=1
             ) b on a.NEWS_HKEY=b.NEWS_HKEY
                 join
             (
                 select affair_hkey,enterprise_hkey
                 from
                     (
                         select affair_hkey,enterprise_hkey ,
                                row_number() over(partition by affair_hkey,enterprise_hkey order by update_dts desc ) rk
                         from ods_jcsj.ods_jcsj_LINK_AFIR_ENT_mi
                         where dt='jcsjk700'
                     ) a
                 where a.rk=1
             ) c on b.affair_hkey=c.affair_hkey
     ) new_tb on old_tb.id=new_tb.id

;



insert overwrite table ads.zhjc_news_info_do
select
    id  , --'id',
    affair_hkey  , --'hash主键',
    enterprise_hkey, -- 企业主键
    published_time , --'发布时间',
    news_title  , --'新闻标题',
    news_abstract  , --'新闻摘要',
    release_website  , --'发布网站',
    news_url  , --'新闻网址',
    news_content  , --'新闻正文',
    pos_or_neg  , --'舆情正负面',


    change_time , -- comment '更新时间',
    change_type , -- comment '数据更新类型',
    status , -- comment '数据状态',
    begin_time , -- comment '开始时间',
    finish_time  -- comment '结束时间'
from dwd.zhjc_news_info_df old_tb
where old_tb.dt='2023-10-18' and old_tb.change_time='2023-10-18'

;





select *
from ods_jcsj.ods_jcsj_sat_ent_poor_credit_mi
where dt='jcsjk700'



select * from ads.zhjc_ent_poor_credit_do;


insert overwrite table dwd.zhjc_ent_dishonest_execution_info_df partition (dt = '2023-10-18')
select COALESCE(new_tb.id, old_tb.id)                                                            as id,          --  'id',
       COALESCE(new_tb.enterprise_hkey, old_tb.enterprise_hkey),                                                 --  'hash主键',
       COALESCE(new_tb.enterprise_name, old_tb.enterprise_name),                                                 --  '企业名称',
       COALESCE(new_tb.case_code, old_tb.case_code),                                                             --  '案号
       COALESCE(new_tb.name, old_tb.iname),                                                                      --  '失信被执行人名称',
       COALESCE(new_tb.cardnum, old_tb.cardnum),                                                                 --  '组织机构代码',
       COALESCE(new_tb.CERTIFICATE_TYPE, old_tb.CERTIFICATE_TYPE),                                               --  '证件类型',
       COALESCE(new_tb.CERTIFICATE_NO, old_tb.CERTIFICATE_NO),                                                   --  '证件号码',
       COALESCE(new_tb.buesinessentity, old_tb.buesinessentity),                                                 --  '企业法人实体',
       COALESCE(new_tb.court_name, old_tb.court_name),                                                           --  '执行法院',
       COALESCE(new_tb.area_id, old_tb.area),                                                                    --  '地域',
       COALESCE(new_tb.area_name, old_tb.area_name),                                                             --  '地域名称',
       COALESCE(new_tb.party_type, old_tb.party_type_name),                                                      --  '标识自然人或企业法人',
       COALESCE(new_tb.gist_cid, old_tb.gist_cid),                                                               --  '执行依据文号',
       COALESCE(new_tb.gist_unit, old_tb.gist_unit)                                              as gist_unit,   --  '作出执行依据单位',
       COALESCE(new_tb.duty, old_tb.duty),                                                                       --  '法律生效文书确定的义务',
       COALESCE(new_tb.performance, old_tb.performance),                                                         --  '失信被执行人具体情形',
       COALESCE(new_tb.disreput_type_name, old_tb.disreput_type_name),                                           --  '失信被执行人具体情形',
       COALESCE(new_tb.publish_date, old_tb.publish_time),                                                       --  '发布时间',
       COALESCE(new_tb.reg_date, old_tb.reg_time),                                                               --  '立案时间',
       COALESCE(new_tb.performed_part, old_tb.performed_part),                                                   --  '已履行部分',
       COALESCE(new_tb.unperform_part, old_tb.unperform_part),                                                   --  '未履行部分',
       COALESCE(new_tb.trans_dm_tong_insertdate, old_tb.trans_dm_tong_insertdate),--  '操作类型',
       if(old_tb.change_time>=new_tb.update_dts,COALESCE(old_tb.change_time,'2023-10-18'),'2023-10-18') as change_time , --  '更新时间',
       case
           when new_tb.is_deleted = '1' then '2'
           when old_tb.id is not null and new_tb.id is not null then '1'
           else '0'
           end                                                                                   as change_type, --  '数据更新类型（0：新增，1：修改，2：删除）',
       case
           when new_tb.is_deleted = '1' then '0'
           else '1'
           end                                                                                   as status,      --  '数据状态'
       COALESCE(new_tb.begin_time, old_tb.begin_time)                                            as begin_time,
       COALESCE(new_tb.finish_time, old_tb.finish_time)                                          as finish_time
from (
         select *
         from dwd.zhjc_ent_dishonest_execution_info_df
         where dt = '${pre_etl_time}'
     ) old_tb
         full outer join
     (
         select a.SAT_ID                                           as id,                 -- id 目前是唯一的
                dim_ent.enterprise_hkey,                                                  -- 企业名称
                dim_ent.enterprise_name,
                a.case_code,                                                              -- 案号
                a.name,                                                                   -- 失信被执行人名称
                a.cardnum,                                                                -- 组织机构代码
                a.CERTIFICATE_NO,                                                         -- 证件号码
                a.CERTIFICATE_TYPE,                                                       -- 证件类型
                a.buesinessentity,                                                        -- 企业法人实体
                a.court_name,                                                             -- 执行法院
                null                                               as area_id,            -- 地域
                a.area_name,                                                              -- 地域名称
                a.party_type,-- 标识自然人或企业法人
                a.gist_cid,                                                               -- 执行依据文号
                a.gist_unit,                                                              -- 做出执行依据单位
                a.duty,                                                                   -- 法律生肖文书确定的义务
                a.performance,                                                            -- 被执行人的履行情况
                a.disreput                                            disreput_type_name, -- 失信被执行人具体情形
                replace(a.publish_date,'/','-') publish_date,                                                           -- 发布时间
                replace(a.reg_date,'/','-') as reg_date,                                                               -- 立案时间
                a.performed_part,                                                         -- 已履行部分
                a.unperform_part,                                                         -- 未履行部分
                a.is_deleted,
                a.trans_dm_tong_insertdate,                                               -- 操作类型
                date_format(replace(a.UPDATE_DTS,'/','-'),'yyyy-MM-dd')               update_dts,         -- 更新时间
                if(a.IS_DELETED = '000000001', '0', '1')           as status,             --数据状态
                date_format(replace(a.publish_date,'/','-'),'yyyy-MM-dd')                                     as begin_time,         -- 开始时间
                if(a.IS_DELETED = '000000001', date_format(replace(a.UPDATE_DTS,'/','-'),'yyyy-MM-dd'), null) as finish_time         --结束时间
         from (
                  select *
                  from (
                           select *,
                                  row_number() over (partition by sat_id order by update_dts desc ) rk
                           from ods_jcsj.ods_jcsj_sat_dishonest_execution_info_mi
                           where dt='jcsjk700'
                       ) t
                  where t.rk = 1
              ) a
                  join dwd.zhjc_ent_basic_info_df dim_ent on a.ENTERPRISE_HKEY = dim_ent.ENTERPRISE_HKEY
         where dim_ent.dt = '2023-10-18'
     ) new_tb on old_tb.id = new_tb.id
;


insert overwrite table ads.zhjc_ent_dishonest_execution_info_do
select id,                       -- 'id',
       enterprise_hkey,          -- 'hash主键',
       enterprise_name,          -- '企业名称',
       case_code,                -- '案号',
       iname,                    -- '失信被执行人名称',
       cardnum,                  -- '组织机构代码',
       certificate_type,         -- '证件类型',
       certificate_no,           -- '证件号码',
       buesinessentity,          -- '企业法人实体',
       court_name,               -- '执行法院',
       area,                     -- '地域',
       area_name,                -- '地域名称',
       party_type_name,          -- '标识自然人或企业法人',
       gist_cid,                 -- '执行依据文号',
       gist_unit,                -- '作出执行依据单位',
       duty,                     -- '法律生效文书确定的义务',
       performance,              -- '被执行人的履行情况',
       disreput_type_name,       -- '失信被执行人具体情形',
       publish_time,             -- '发布时间',
       reg_time,                 -- '立案时间',
       performed_part,           -- '已履行部分',
       unperform_part,           -- '未履行部分',
       trans_dm_tong_insertdate, -- '操作类型',
       change_time,              -- comment '更新时间',
       change_type,              -- '数据更新类型',
       status,                   -- '数据更新类型（0：新增，1：修改，2：删除）',
       begin_time,               -- comment '开始时间',
       finish_time               -- comment '结束时间'
from dwd.zhjc_ent_dishonest_execution_info_df old_tb
where old_tb.dt = '2023-10-18'
  and change_time = '2023-10-18';


select * from ads.zhjc_ent_dishonest_execution_info_do;


select count(*) from ads.zhjc_ent_dishonest_execution_info_do;




select *
from ods_jcsj.ods_jcsj_sat_dishonest_execution_info_mi
where dt='jcsjk700'


-- 异常经营名录信息表
-- 初始化

-- 每日调度
insert overwrite table dwd.zhjc_ent_ab_operation_list_df partition(dt='2023-10-18')
select
    COALESCE(new_tb.id,old_tb.id) as id      , --  'id',
    COALESCE(new_tb.enterprise_hkey,old_tb.enterprise_hkey)            , --  'hash主键',
    COALESCE(new_tb.busexclist,old_tb.busexclist) , --  '经营异常名录
    COALESCE(new_tb.pripid,old_tb.pripid)                   , --  '主体身份代码',
    COALESCE(new_tb.enterprise_name,old_tb.enterprise_name)       , --  '企业名称',
    COALESCE(new_tb.unified_social_credit_code,old_tb.unified_social_credit_code)         , --  '统一社会信用代码',
    COALESCE(new_tb.regno,old_tb.regno)         , --  '注册号',
    COALESCE(new_tb.lerep,old_tb.lerep)             , --  '行政处罚决定书文号',
    COALESCE(new_tb.certificate_type,old_tb.certificate_type)           , --  '证件类型',
    COALESCE(new_tb.certificate_no,old_tb.certificate_no)                  , --  '证件号码',
    COALESCE(new_tb.specause,old_tb.specause)             , --  '列入经营异常名录原因类型',
    COALESCE(new_tb.specausename,old_tb.specausename) , --  '列入经营异常名录原因类型名称',
    COALESCE(new_tb.abntime,old_tb.abntime)             , --  '设立日期',
    COALESCE(new_tb.decorg,old_tb.decorg) as decorg  , --  '列入决定机关',
    COALESCE(new_tb.decorgname,old_tb.decorgname)             , --  '列入决定机关名称',
    if(old_tb.change_time>=new_tb.update_dts,COALESCE(old_tb.change_time,'2023-10-18'),'2023-10-18') as change_time , --  '更新时间',
    case
        when new_tb.is_deleted='1' then '2'
        when old_tb.id is not null and new_tb.id is not null then '1'
        else '0'
        end as change_type                 , --  '数据更新类型（0：新增，1：修改，2：删除）',
    case when new_tb.is_deleted='1' then '0'
         else '1'
        end  as status,                        --  '数据状态'
    COALESCE(new_tb.begin_time,old_tb.begin_time) as begin_time,
    COALESCE(new_tb.finish_time,old_tb.finish_time) as finish_time
from (
         select *
         from dwd.zhjc_ent_ab_operation_list_df
         where dt = '${pre_etl_time}'
     )old_tb
         full outer join
     (
         select
             a.SAT_ID as id,--id
             dim_ent.enterprise_hkey, -- hash主键
             a.busexclist, -- 经营异常名录
             a.pripid, -- 主体身份代码
             a.entname enterprise_name, -- 企业名称
             a.UNIFIED_SOCIAL_CREDIT_CODE, -- 统一社会信用代码
             a.regno, -- 注册号
             a.lerep, -- 法定代表人
             a.CERTIFICATE_TYPE, -- 证件类型
             a.CERTIFICATE_NO, -- 证件号码
             a.specause, -- 列入经营异常名录原因类型
             a.specausename, -- 列入经营异常名录原因类型名称
             date_format(replace(a.abntime,'/','-'),'yyyy-MM-dd') abntime, -- 设立日期
             a.decorg, -- 列入决定机关
             a.decorgname, -- 列入决定机关名称
             a.is_deleted,
             date_format(replace(a.UPDATE_DTS,'/','-'),'yyyy-MM-dd') update_dts, -- 更新时间
             if(a.IS_DELETED='000000001','2','1') as change_type ,-- 数据更新类型
             if(a.IS_DELETED='000000001','0','1') as status, --数据状态
             date_format(replace(a.abntime,'/','-'),'yyyy-MM-dd') as begin_time, -- 开始时间
             if(a.IS_DELETED='000000001',date_format(replace(a.UPDATE_DTS,'/','-'),'yyyy-MM-dd'),null) as finish_time --结束时间
         from
             (
                 select *
                 from
                     (
                         select * ,
                                row_number() over(partition by sat_id order by update_dts desc ) rk
                         from ods_jcsj.ods_jcsj_sat_ent_ab_operation_list_mi
                         where dt='jcsjk700'
                     ) t
                 where t.rk=1
             ) a
                 join dwd.zhjc_ent_basic_info_df dim_ent on a.ENTERPRISE_HKEY=dim_ent.ENTERPRISE_HKEY
         where dim_ent.dt='2023-10-18'
     ) new_tb on old_tb.id=old_tb.id


;


insert overwrite table ads.zhjc_ent_ab_operation_list_do
select
    id , -- 'id',
    enterprise_hkey , -- 'hash主键',
    busexclist , -- '经营异常名录',
    pripid , -- '主体身份代码',
    enterprise_name , -- '企业名称',
    unified_social_credit_code , -- '统一社会信用代码',
    regno , -- '注册号',
    lerep , -- '法定代表人',
    certificate_type , -- '证件类型',
    certificate_no , -- '证件号码',
    specause , -- '列入经营异常名录原因类型',
    specausename , -- '列入经营异常名录原因类型名称',
    abntime , -- '设立日期',
    decorg , -- '列入决定机关',
    decorgname , -- '列入决定机关名称',
    change_time , -- comment '更新时间',
    change_type , -- comment '数据更新类型',
    status , -- comment '数据状态',
    begin_time , -- comment '开始时间',
    finish_time  -- comment '结束时间'
from dwd.zhjc_ent_ab_operation_list_df old_tb
where old_tb.dt='2023-10-18' and old_tb.change_time='2023-10-18'


;

select count(1) from ads.zhjc_ent_ab_operation_list_do;

select *
from ads.zhjc_ent_ab_operation_list_do
where enterprise_hkey='08B2DEB2E430E5A57AA730BC19DCE94D';




select abntime
from ods_jcsj.ods_jcsj_sat_ent_ab_operation_list_mi
where dt='jcsjk700' and enterprise_hkey='08B2DEB2E430E5A57AA730BC19DCE94D';


select count(1) from ads.zhjc_ent_dishonest_execution_info_do;




select f_area from ods.hgjj_longdun_data_df where dt='2023-09' group by f_area;


select * from ods.hgjj_longdun_data_df where dt='2023-09' and f_code='A0401011';

select * from ads.zhjc_ent_poor_credit_do;

show partitions ods.hgjj_longdun_data_df;


select * from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi;


