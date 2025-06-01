create table tmp_zhj_affair_label as
select t1.affair_hkey,-- 事件hash
       t1.affair_name, -- 事件名称
       t1.involved_company,-- 涉及企业
       t1.involved_person,--  string comment '涉及人员'
       t1.involved_org,--     string comment '涉及工商联'
       t3.news_hkey,-- 新闻hash
       t3.news_title,-- 新闻标题
       t3.release_website, -- 发布网站
       t3.influence_range,-- 影响范围
       t4.enterprise_hkey,-- 关联企业
       t5.org_hkey, -- 关联组织
       t6.person_hkey-- 关联人员
from sat_affair_basic_info t1
         left join link_afir_news t2 on t1.affair_hkey=t2.affair_hkey
         left join (select * from sat_news_info where record_source='WBSJ_DZWJKGLXT') t3 on t2.news_hkey=t3.news_hkey
         left join link_afir_ent t4 on t1.affair_hkey=t4.affair_hkey
         left join link_afir_org t5 on t1.affair_hkey=t5.affair_hkey
         left join link_afir_per t6 on t1.affair_hkey=t6.affair_hkey






select *
from ods_jcsj.ods_jcsj_sat_person_honor_info_mi


select * from ods_jcsj.ods_jcsj_link


