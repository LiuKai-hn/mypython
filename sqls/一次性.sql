


select * from dwd.zhjc_ent_basic_info_df where dt='2024-03-12'

select * from ads.hgjj_macro_eco_index_do


-- 抽取 news.txt 全量新闻格式
select id,
       news_title,
       news_content
from dwd.zhjc_news_info_df
where dt>='2023-12-25'


-- 抽取 news.txt 全量新闻格式
select id,
       ent_id,
       published_time,
       news_title,
       news_content
from (select id,
             enterprise_hkey,
             date_format(published_time, 'yyyy-MM-dd') as published_time,
             news_title,
             news_content
      from dwd.zhjc_news_info_df
      where dt >= '2023-12-25'
      ) t
lateral view explode(split(enterprise_hkey,',')) tmp as ent_id


select ent_id,
        max(published_time) as max_published_time,
        min(published_time) as min_published_time
from (select enterprise_hkey,
             date_format(published_time, 'yyyy-MM-dd') as published_time
      from dwd.zhjc_news_info_df
      where dt >= '2024-03-04'
      ) t
lateral view explode(split(enterprise_hkey,',')) tmp as ent_id
group by ent_id


truncate table dwd.zhjc_jcsj2hive_status_df




