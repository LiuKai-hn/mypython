

select * from dwd.dim_ent_basic_info_do ;

select * from ads.zhjc_ent_ab_operation_list_do;

select * from ods_jcsj.ods_jcsj_sat_ent_poor_credit_mi;


select a.*
from ods_jcsj.ods_jcsj_sat_ent_ab_operation_list_mi a
join dwd.dim_ent_basic_info_do b on  a.entname=b.enterprise_name
;




show create table ads.zhjc_news_info;


show partitions ods_jcsj.ods_jcsj_link_ent_exe_mi;


-- truncate table ods_jcsj.ods_jcsj_sat_news_info_mi;
select * from ods_jcsj.ods_jcsj_sat_news_info_mi;

select * from dwd.zhjc_ent_basic_info_df where dt='2023-09-18';



select * from ads.zhjc_news_info_do limit 10;



show create table ads.zhjc_enterprise_cost;



show partitions ods_jcsj.ods_jcsj_sat_ent_basic_info_mi;


select count(1) from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi where dt='jcsjk200 2023-09-16';

select * from ads.zhjc_ent_patent_info_do;

select * from ods_jcsj.ods_jcsj_sat_public_sentiment_mi;



show create table ods_jcsj.ods_jcsj_sat_news_info_mi;

insert overwrite table tmp_lk.ods_jcsj_sat_news_info_mi
select get_json_object(json_line,'$.sat_id'            ) as sat_id,
       get_json_object(json_line,'$.news_hkey'         ) as news_hkey,
       get_json_object(json_line,'$.published_time'    ) as published_time ,
       get_json_object(json_line,'$.news_theme'        ) as news_theme     ,
       get_json_object(json_line,'$.news_title'        ) as news_title     ,
       get_json_object(json_line,'$.news_abstract'     ) as news_abstract  ,
       get_json_object(json_line,'$.release_website'   ) as release_website,
       get_json_object(json_line,'$.news_url'          ) as news_url       ,
       get_json_object(json_line,'$.news_content'      ) as news_content   ,
       get_json_object(json_line,'$.create_dts'        ) as create_dts     ,
       get_json_object(json_line,'$.update_dts'        ) as update_dts     ,
       get_json_object(json_line,'$.is_deleted'        ) as is_deleted     ,
       get_json_object(json_line,'$.record_source'     ) as record_source  ,
       get_json_object(json_line,'$.qlty_id'           ) as qlty_id        ,
       get_json_object(json_line,'$.influence_range'   ) as influence_range,
       get_json_object(json_line,'$.news_type'         ) as news_type      ,
       get_json_object(json_line,'$.news_source'       ) as news_source    ,
       get_json_object(json_line,'$.news_keyword'      ) as news_keyword   ,
       get_json_object(json_line,'$.attachment_id'     ) as attachment_id  ,
       get_json_object(json_line,'$.deptcode'          ) as deptcode       ,
       get_json_object(json_line,'$.orgcode'           ) as orgcode        ,
       get_json_object(json_line,'$.news_id'           ) as news_id        ,
       get_json_object(json_line,'$.crawl_time'        ) as crawl_time
from tmp_lk.json_line;

select news_source from tmp_lk.ods_jcsj_sat_news_info_mi group by news_source;

-- 新闻文本输出，去掉一些无用字符
SELECT sat_id,
       regexp_replace(REGEXP_REPLACE(replace(news_content,'&nbsp;',''), '<[^>]+>', ''), 'https?://[^ ]+', '') AS text_without_tags
from tmp_lk.ods_jcsj_sat_news_info_mi;


select sat_id,
       replace(replace(news_content,'\t',""),'\n','') as news_content
from tmp_lk.ods_jcsj_sat_news_info_mi where sat_id is not null and length(news_content)>0;


insert overwrite table dwd.zhjc_news_info_df partition(dt='2023-09-16')
select
    COALESCE(new_tb.id,old_tb.id) as id,
    COALESCE(new_tb.affair_hkey,old_tb.affair_hkey) as NEWS_HKEY,
    COALESCE(new_tb.enterprise_hkey,old_tb.enterprise_hkey) as enterprise_hkey,
    COALESCE(new_tb.published_time,old_tb.published_time) as published_time,
    COALESCE(new_tb.news_title,old_tb.news_title) as news_title,
    COALESCE(new_tb.news_abstract,old_tb.news_abstract) as news_abstract,
    COALESCE(new_tb.release_website,old_tb.release_website) as release_website,
    COALESCE(new_tb.news_url,old_tb.news_url) as news_url,
    COALESCE(new_tb.news_content,old_tb.news_content) as news_content,
    COALESCE(new_tb.pos_or_neg,old_tb.pos_or_neg) as pos_or_neg,
    if(old_tb.change_time>=new_tb.update_dts,COALESCE(old_tb.change_time,'${etl_time}'),'${etl_time}') as change_time , --  '更新时间',
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
                c.enterprise_hkey,
                d.pos_or_neg
         from
             (
                 select sat_id as id,
                        NEWS_HKEY as NEWS_HKEY             ,-- 'id',
                        from_unixtime(cast(published_time as bigint), 'yyyy-MM-dd HH:mm:ss') as published_time          ,-- '发布时间',
                        news_title      ,-- '新闻标题',
                        news_abstract, -- 新闻摘要
                        release_website,
                        news_url            ,-- '新闻网址',
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
                         from tmp_lk.ods_jcsj_sat_news_info_mi
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
                     ) a
                 where a.rk=1
             ) c on b.affair_hkey=c.affair_hkey
         join ods.zhjc_news_pos_or_neg_do d on a.id=d.id
     ) new_tb on old_tb.id=new_tb.id
;

select * from ods.zhjc_news_pos_or_neg_do;

select count(*) from dwd.zhjc_news_info_df where dt='2023-09-16';




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
where old_tb.dt='2023-09-16' and old_tb.change_time='2023-09-16';

select
id ,
words ,
popularity ,
insert_time,
status ,
update_time,
change_type,
begin_time,
finish_time
from ads.zhjc_hot_words_do






select
    id ,
    enterprise_hkey ,
    enterprise_name ,
    reg_number ,
    industry,
    legal_representative_name,
    registered_fund ,
    registered_place,
    main_business,
    enterprise_scale ,
    if_listed ,
    industry_class,
    business_registration_type ,
    establish_time ,
    revoke_logoff_date ,
    business_scope ,
    reg_status ,
    reg_office,
    province ,
    city ,
    county ,
    administrative_region_code ,
    registered_admini ,
    is_commi ,
    main_products_services,
    fundamental_state,
    change_time,
    change_type,
    status
from ads.zhjc_ent_basic_info_do



select cast(change_time as date) -- ,date_format(change_time, 'yyyy-MM-dd HH:mm:ss.SSSSSS')
    from ads.zhjc_ent_basic_info_do
group by change_time
;

select * from ads.zhjc_ent_basic_info_do;


select CURRENT_TIMESTAMP();

select to_date(change_time);

select * from tmp_lk.ods_jcsj_sat_news_info_mi;

insert overwrite table tmp_lk.ods_jcsj_sat_news_info_mi
select get_json_object(json_line,'$.sat_id'            ) as sat_id,
       get_json_object(json_line,'$.news_hkey'         ) as news_hkey,
       replace(get_json_object(json_line,'$.published_time'    ),'T',' ') as published_time ,
       replace(replace(get_json_object(json_line,'$.news_theme'        ),'\t',""),'\n','') as news_theme     ,
       replace(replace(get_json_object(json_line,'$.news_title'        ),'\t',""),'\n','') as news_title     ,
       replace(replace(get_json_object(json_line,'$.news_abstract'     ),'\t',""),'\n','') as news_abstract  ,
       replace(replace(get_json_object(json_line,'$.release_website'   ),'\t',""),'\n','') as release_website,
       get_json_object(json_line,'$.news_url'          ) as news_url       ,
       replace(replace(get_json_object(json_line,'$.news_content'      ),'\t',""),'\n','') as news_content   ,
       replace(get_json_object(json_line,'$.create_dts'        ),'T',' ') as create_dts     ,
       replace(get_json_object(json_line,'$.update_dts'        ),'T',' ') as update_dts     ,
       get_json_object(json_line,'$.is_deleted'        ) as is_deleted     ,
       get_json_object(json_line,'$.record_source'     ) as record_source  ,
       get_json_object(json_line,'$.qlty_id'           ) as qlty_id        ,
       get_json_object(json_line,'$.influence_range'   ) as influence_range,
       get_json_object(json_line,'$.news_type'         ) as news_type      ,
       get_json_object(json_line,'$.news_source'       ) as news_source    ,
       replace(replace(get_json_object(json_line,'$.news_keyword'      ),'\t',""),'\n','') as news_keyword   ,
       get_json_object(json_line,'$.attachment_id'     ) as attachment_id  ,
       get_json_object(json_line,'$.deptcode'          ) as deptcode       ,
       get_json_object(json_line,'$.orgcode'           ) as orgcode        ,
       get_json_object(json_line,'$.news_id'           ) as news_id        ,
      replace(get_json_object(json_line,'$.crawl_time'        ),'T',' ') as crawl_time
from tmp_lk.json_line;


select * from ads.zhjc_hot_words;

select count(*) from tmp_lk.json_line;
truncate table tmp_lk.json_line;

show partitions dwd.zhjc_ent_basic_info_df;

-- main_products_services     string comment '主要产品与服务',
--    fundamental_state          string comment '企业基本情况简介',

select * from ads.zhjc_ent_basic_info_do;

select max(length(industry_class ) ) max_len_main_products_services-- , -- 385
    from ads.zhjc_ent_basic_info_do
;

select * from dwd.dim_ent_basic_info_do;



CREATE TABLE tmp_lk.ods_jcsj_sat_news_info_mi(
                                                 sat_id string COMMENT '主键',
                                                 news_hkey string COMMENT 'hash主键',
                                                 published_time string COMMENT '发布时间',
                                                 news_theme string COMMENT '新闻主题',
                                                 news_title string COMMENT '新闻标题',
                                                 news_abstract string COMMENT '新闻摘要',
                                                 release_website string COMMENT '发布网站',
                                                 news_url string COMMENT '新闻网址',
                                                 news_content string COMMENT '新闻正文',
                                                 create_dts string COMMENT '创建时间',
                                                 update_dts string COMMENT '更新时间',
                                                 is_deleted string COMMENT '删除标识',
                                                 record_source string COMMENT '数据来源',
                                                 qlty_id string COMMENT '质量标识',
                                                 influence_range string COMMENT '影响范围 ',
                                                 news_type string COMMENT '新闻类型 ',
                                                 news_source string COMMENT '新闻来源 ',
                                                 news_keyword string COMMENT '新闻关键词',
                                                 attachment_id string COMMENT '附件ID',
                                                 deptcode string COMMENT '部门编码',
                                                 orgcode string COMMENT '组织代码',
                                                 news_id string COMMENT '新闻ID',
                                                 crawl_time string COMMENT '爬取时间')
    COMMENT '新闻动态信息表'
    stored as orc
    tblproperties (
        'orc.compress'='snappy'
        );


select * from ads.zhjc_news_info_do;


show partitions ods_jcsj.ods_jcsj_sat_court_decision_doc_mi;



select max(length(`type`)) from ods_jcsj.ods_jcsj_sat_court_decision_doc_mi;

select `per_type` from ods_jcsj.ods_jcsj_sat_court_decision_doc_mi group by `per_type`;



insert overwrite table ads.zhjc_court_decision_doc_do
select
    id , --'id',
    enterprise_hkey , --'hash主键',
    enterprise_name , --'企业名称',
    area , --'地区',
    case_type , --'案件类型',
    execution_court , --'执行法院',
    title , --'标题',
    case_num , --'案号',
    trial_date , --'审判日期',
    submission_time , --'提交时间',
    duc_type , --'文书类型',
    trial_type , --'审判类型',
    if(length(type)>100,null,type) , --'当事人类型',
    content , --'正文',


    change_time , -- comment '更新时间',
    change_type , -- comment '数据更新类型',
    status , -- comment '数据状态',
    begin_time , -- comment '开始时间',
    finish_time  -- comment '结束时间'
from dwd.zhjc_court_decision_doc_df old_tb
where old_tb.dt='2023-09-04' and old_tb.change_time='2023-09-04';



insert overwrite table ads.zhjc_ent_basic_info_do
select
    id                         , -- string comment 'id',
    enterprise_hkey            , -- string comment 'hash主键',
    enterprise_name            , -- string comment '企业名称',
    if(length(reg_number               )>100,null,reg_number )  , -- string comment '社会统一信用代码',
    if(length(industry                 )>100,null,industry)  , -- string comment '所属行业',
    if(length(legal_representative_name)>100,null,legal_representative_name)  , -- string comment '法人',
    if(length(registered_fund          )>100,null,registered_fund )  , -- string comment '注册资本',
    if(length(registered_place         )>100,null,registered_place)  , -- string comment '企业地址',
    if(length(main_business            )>100,null,main_business)  , -- string comment '主营业务',
    if(length(enterprise_scale          )>100,null,enterprise_scale           ) , -- string comment '企业规模',
    if(length(if_listed                 )>100,null,if_listed                  ) , -- string comment '是否为上市企业',
    if(length(industry_class            )>10 ,null,industry_class             ) , -- string comment '产业分类',
    if(length(business_registration_type)>100,null,business_registration_type ) , -- string comment '工商注册类型',
    if(length(establish_time            )>100,null,establish_time             ) , -- string comment '成立时间',
    if(length(revoke_logoff_date        )>100,null,revoke_logoff_date         ) , -- string comment '注吊销时间',
    if(length(business_scope            )>100,null,business_scope             ) , -- string comment '经营范围',
    if(length(reg_status                )>100,null,reg_status                 ) , -- string comment '注册状态',
    if(length(reg_office                )>100,null,reg_office                 ) , -- string comment '登记机关',
    if(length(province                  )>100,null,province                   ) , -- string comment '省级行政区划',
    if(length(city                      )>100,null,city                       ) , -- string comment '市级行政区划',
    if(length(county                    )>100,null,county                     ) , -- string comment '县级行政区划',
    if(length(administrative_region_code)>100,null,administrative_region_code ) , -- string comment '企业所在行政区域代码',
    if(length(registered_admini         )>100,null,registered_admini          ) , -- string comment '企业注册地行政区域',
    if(length(is_commi                  )>100,null,is_commi                   ) , -- string comment '是否为执常委企业',
    main_products_services     , -- string comment '主要产品与服务',
    fundamental_state          , -- string comment '企业基本情况简介',
    change_time                , -- timestamp comment '更新时间',
    change_type                , -- string comment '数据更新类型（0：新增，1：修改，2：删除）',
    status                      -- string comment '数据状态'
from dwd.zhjc_ent_basic_info_df old_tb
where old_tb.dt='2023-09-04' and change_time='2023-09-04'










select reg_number,COUNT(1)
from ads.zhjc_ent_basic_info_do
group by reg_number
having count(1)>1;



SELECT * FROM ads.zhjc_ent_basic_info_do where reg_number='9122082155977797XR';


select is_listed from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi ;

select is_listed from dwd.dim_ent_basic_info_do ;


select * from ads.zhjc_hot_words_do







select count(1) from ads.zhjc_ent_basic_info_do;


show partitions dwd.zhjc_ent_basic_info_df;

select dt,count(1) from dwd.zhjc_ent_basic_info_df group by dt ;


select * from ods.health_ent_score_df;


insert overwrite table dwd.health_ent_score_df partition (dt='2023-10-14')
select a.name as enterprise_name,
       a.score,
       a.change_date,
       b.enterprise_hkey
from ods.health_ent_score_df a
left join
    (select enterprise_name,
            enterprise_hkey
     from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi
     where dt='jcsjk700'
     ) b on a.name=b.enterprise_name
;


select * from ods.health_ent_score_df  ;

show partitions ods_jcsj.ods_jcsj_sat_ent_basic_info_mi;

select * from dwd.health_ent_score_df;


insert overwrite table dwd.health_ent_score_df partition (dt='2023-10-15')
select a.name as enterprise_name,
       a.score,
       a.change_date,
       b.enterprise_hkey
from ods.health_ent_score_df a
join dwd.zhjc_ent_basic_info_df b on a.enterprise_hkey=b.enterprise_hkey and b.dt='2023-09-18';


select dt,count(1) from dwd.health_ent_score_df group by dt ;


-- 企业经营状况能监测 3476 家企业，企业舆情能监测到 4259 家企业。
select count(distinct enterprise_hkey) from ads.zhjc_news_info_do;



insert overwrite table ads.zhjc_news_info_do
select
    a.id  , --'id',
    a.affair_hkey  , --'hash主键',
    a.enterprise_hkey, -- 企业主键
    a.published_time , --'发布时间',
    a.news_title  , --'新闻标题',
    a.news_abstract  , --'新闻摘要',
    a.release_website  , --'发布网站',
    a.news_url  , --'新闻网址',
    a.news_content  , --'新闻正文',
    a.pos_or_neg  , --'舆情正负面',
    a.change_time , -- comment '更新时间',
    a.change_type , -- comment '数据更新类型',
    a.status , -- comment '数据状态',
    a.begin_time , -- comment '开始时间',
    a.finish_time  -- comment '结束时间'
from ads.zhjc_news_info_do a
         join ads.zhjc_ent_basic_info_do b on a.ENTERPRISE_HKEY=b.enterprise_hkey


