create table dwd.dim_sat_ent_addr_do(
                                        enterprise_hkey string comment 'hash主键',
                                        tag_value_jiangzhehu string comment '是否为金三角地区(用于标签标注)',
                                        tag_value_jingjinji string comment '是否为京津冀地区(用于标签标注)',
                                        tag_value_zhusanjiao string comment '是否为珠三角地区(用于标签标注)',
                                        tag_value_changsanjiao string comment '是否为长三角地区(用于标签标注)',
                                        tag_value_diqu string comment '企业所在地区(东部地区、中部地区、西部地区、东北地区)(用于标签标注)'
) comment '标签标注-企业维度信息表'
    stored as orc
    tblproperties (
        'orc.compress'='snappy'
        );


-- 标签标注用到的表
insert overwrite table dwd.dim_sat_ent_addr_do
select ENTERPRISE_HKEY,
       tag_value_jiangzhehu,
       tag_value_jingjinji,
       tag_value_zhusanjiao,
       tag_value_changsanjiao,
       tag_value_diqu
from (
         select ENTERPRISE_HKEY,
                if(substr(REGION_CODE, 1, 2) in ('31', '32', '33'), '江浙沪', null)                          as tag_value_jiangzhehu,
                if(substr(REGION_CODE, 1, 2) in ('11', '12')
                       or substr(REGION_CODE, 1, 4) in
                          ('1306', '1310', '1302', '1307', '1308', '1303', '1309', '1311', '1305', '1304',
                           '1301'), '京津冀',
                   null)                                                                                  as tag_value_jingjinji,
                if(substr(REGION_CODE, 1, 4) in
                   ('4401', '4403', '4406', '4407', '4404', '4419', '4420', '4413', '4412'), '珠三角',
                   null)                                                                                  as tag_value_zhusanjiao,
                if(substr(REGION_CODE, 1, 2) in ('31', '32', '33'), '长三角', null)                          as tag_value_changsanjiao,
                case
                    when substr(REGION_CODE, 1, 2) in
                         ('11', '12', '31', '13', '32', '33', '37', '35', '44', '46') then '东部地区'
                    when substr(REGION_CODE, 1, 2) in ('14', '41', '42', '43', '34', '36') then '中部地区'
                    when substr(REGION_CODE, 1, 2) in
                         ('50', '51', '61', '62', '63', '53', '52', '45', '15', '64', '65', '54')
                        or substr(REGION_CODE, 1, 4) in ('4331', '4228') then '西部地区'
                    when substr(REGION_CODE, 1, 2) in ('21', '22', '23') then '东北地区'
                    else REGION_CODE
                    end                                                                                   as tag_value_diqu
         from (
                  select ENTERPRISE_HKEY,
                         REG_CODE
                  from
                      (
                          select trim(REG_CODE) as REG_CODE,
                                 trim(ENTERPRISE_HKEY) as ENTERPRISE_HKEY,
                                 row_number() over(partition by trim(ENTERPRISE_HKEY),trim(REG_CODE) order by trim(update_dts) desc ) rk
                          from ods_jcsj.ods_jcfu_sat_ent_basic_info_mi
                          where trim(REG_CODE) is not null
                      ) tt
                  where rk=1 and REG_CODE is not null and length(REG_CODE) != 0
              ) t
                  lateral view explode(split(REG_CODE, ',')) tmp as REGION_CODE
     ) tt
;



-- 企业基础信息标签
-- 注册资本规模
select enterprise_hkey,
       '注册资本规模' as tag_name,
       concat(floor(REGISTERED_FUND/500)*500,"-",ceil(REGISTERED_FUND/500)*500,'万元') as tag_value
from (
         select enterprise_hkey,
                REGISTERED_FUND,
                row_number() over (partition by sat_id order by update_dts desc ) rk
         from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi
     ) t
where t.rk=1;



--会长企业
select per_ent.ENTERPRISE_HKEY,
       '会长企业' as tag_value
from
    (
        select *,
               row_number() over(partition by sat_id order by update_dts desc ) rk
        from ods_jcsj.ods_jcsj_SAT_FIC_DUTY_INFO_mi where is_ON_JOB='1'
    ) a  -- 这里只有在职
        join
    (   select * ,
               row_number() over(partition by org_per_hkey,org_hkey order by update_dts desc ) rk
        from ods_jcsj.ods_jcsj_LINK_ORG_PER_mi
    ) b on a.org_per_hkey=b.org_per_hkey
        join
    (   select * from tmp_lk.DM_SJZD where zdxz like '%会长') d on a.duty_code=d.zdxbm
        join
    (
        select t1.person_hkey,
               t1.name,
               t2.ENTERPRISE_HKEY
        from
            (
                select *,
                       row_number() over(partition by sat_id order by update_dts desc ) rk
                from ods_jcsj.ods_jcsj_sat_person_basic_info_mi
            ) t1
                join
            (
                select *,
                       row_number() over(partition by per_ent_hkey,person_hkey order by update_dts desc ) rk
                from ods_jcsj.ods_jcsj_LINK_PER_ENT_mi
            ) t2 on t1.person_hkey=t2.person_hkey
        where t1.name is not null and t1.rk=1 and t2.rk=1
    ) per_ent on b.person_hkey=per_ent.person_hkey
where a.rk=1 and b.rk=1
group by per_ent.ENTERPRISE_HKEY;

-- A级纳税人
select enterprise_hkey,
       'A级纳税人' as tag_value
from (
         select enterprise_hkey, row_number() over (partition by sat_id order by update_dts desc) rk
         from ods_jcsj.ods_jcsj_sat_per_a_taypayer_info_mi
     ) t
where rk=1;


-- 被列入经营异常名录
select enterprise_hkey,
       '被列入经营异常名录' as tag_value
from ods_jcsj.ods_jcsj_SAT_ENT_AB_OPERATION_LIST_mi
where IS_DELETED='0'
group by enterprise_hkey
;

-- 近一年受到行政处罚
select enterprise_hkey,
       '近一年受到行政处罚' as tag_value
from (
         select enterprise_hkey,
                max(from_unixtime(unix_timestamp(punlimit, 'yyyy/MM/dd HH:mm:ss.SSS'), 'yyyy-MM-dd')) AS punlimit_date
         from ods_jcsj.ods_jcsj_sat_ent_punish_info_mi
         where IS_DELETED = '0'
         group by enterprise_hkey
     ) t
where datediff(current_date(),punlimit_date)>356;


-- 企业失信被执行人
select enterprise_hkey,
       '企业失信被执行人' as tag_value
from ods_jcsj.ods_jcsj_sat_dishonest_execution_info_mi
where IS_DELETED='0'
group by enterprise_hkey;

-- 拖欠农民工工资黑名单
select enterprise_hkey,
       '拖欠农民工工资黑名单' as tag_value
from ods_jcsj.ods_jcsj_SAT_ARREAR_WORKERS_BLACKLIST_mi
where IS_DELETED='0'
group by enterprise_hkey;


-- 严重违法超限超载运输失信当事人
select enterprise_hkey,
       '严重违法超限超载运输失信当事人' as tag_value
from ods_jcsj.ods_jcsj_SAT_SERIOUS_VIOLATE_TRA_mi
where IS_DELETED='0'
group by enterprise_hkey;


-- 政府采购不良行为记录
select enterprise_hkey,
       '政府采购不良行为记录' as tag_value
from ods_jcsj.ods_jcsj_sat_age_bad_behavior_info_mi
where IS_DELETED='0'
group by enterprise_hkey;

-- 重大税收违法案件当事人
select enterprise_hkey,
       '重大税收违法案件当事人' as tag_value
from ods_jcsj.ods_jcsj_sat_major_tax_violator_mi
where IS_DELETED='0'
group by enterprise_hkey;


-- 严重违法失信名单
select enterprise_hkey,
       '严重违法失信名单' as tag_value
from ods_jcsj.ods_jcsj_SAT_SERIOUS_ILLEGAL_LIST_mi
where IS_DELETED='0'
group by enterprise_hkey;


-- 统计上严重失信企业
select enterprise_hkey,
       '统计上严重失信企业' as tag_value
from ods_jcsj.ods_jcsj_sat_ent_poor_credit_mi
where IS_DELETED='0'
group by enterprise_hkey;




-- 文化及相关产业
-- 国家旅游及相关产业
-- 高技术产业（服务业）
-- 国家科技服务业
-- 战略新兴产业
-- 国家体育产业
-- 高技术产业（制造业）
-- 健康服务业
-- 知识产权（专利）密集型产业
-- 新产业新业态新商业模式业
-- 生产性服务业
-- 生活性服务业
-- 养老产业
-- 农业及相关产业
-- 教育培训及相关产业
-- 数字经济及其核心产业
-- 节能环保清洁产业
select t1.enterprise_hkey,
       '文化及相关产业' as tag_value
from
    (
        select enterprise_hkey,
               INDUSTRY,
               row_number() over (partition by sat_id order by update_dts desc ) rk
        from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi
    ) t1
        join tmp_lk.longdun_hangyezhibiao_industry_map t2 on t2.hang_ye_zhi_biao=t1.INDUSTRY
and t2.tag_name='文化及相关产业'
where t1.rk=1
;



-- 江沪浙
-- 京津冀
-- 珠三角
-- 长三角
-- 企业所属地区


select enterprise_hkey,
       '长三角' as tag_value
from dwd.dim_ENT_BASIC_INFO_do
where tag_value_changsanjiao is not null
;



select enterprise_hkey,
       tag_value_diqu
from dwd.dim_ENT_BASIC_INFO_do
where tag_value_diqu is not null
;






