
DROP TABLE IF EXISTS tmp_lk.person_other_label;
CREATE TABLE tmp_lk.person_other_label (
                                         person_hkey string comment '人员hash',
                                         unit_position string comment '职位',
                                         edu_name string comment '学历名称',
                                         grad_date string comment '毕业日期',
                                         degree_name string comment '学位名称'
) comment '人员其他标签'
    row format delimited fields terminated by ','
        lines terminated by '\n'
    stored as textfile;




DROP TABLE IF EXISTS tmp_lk.person_label;
CREATE TABLE tmp_lk.person_label (
                                           person_hkey string comment '人员hash',
                                           sex string comment '性别',
                                           cnt_reg string comment '国籍',
                                           nation string comment '民族',
                                           native_place string comment '籍贯',
                                           age string comment '年龄段',
                                           unit_position string comment '职位',
                                           org_duty_name string comment '工商联职务',
                                           person_type string comment '人员类型',
                                           edu_name string comment '学历',
                                           degree_name string comment '教育水平',
                                           grad_date string comment '毕业日期',
                                           politics_status string comment '政治面貌'

) comment '人员其他标签'
    row format delimited fields terminated by ','
        lines terminated by '\n'
    stored as textfile;



insert overwrite table tmp_lk.person_label
select t1.person_hkey,
       dim_sex.code_cn as sex, -- 性别
       dim_native_plac.code_cn as cnt_reg, -- 国籍
       dim_nation.code_cn as nation, -- 民族
       native_place_tb.native_place as native_place     ,-- string comment '籍贯',
       concat(cast((year(current_date())- substr(birthdate,1,4))/5 as bigint)*5,
              '-',
              (cast((year(current_date())- substr(birthdate,1,4))/5 as bigint)+1)*5) as age             ,-- string comment '年龄段',
       per_other_tb.unit_position,-- 职位
       org_duty_tb.org_duty_name, -- 工商联职务
       person_type_tb.person_type as person_type, -- 人员类型
       per_other_tb.edu_name, -- 学历
       per_other_tb.degree_name, --学位(教育水平)
       dim_politics_status.code_cn as politics_status, -- 政治面貌代码
       null as grad_date -- 毕业日期
from (select * from ods_jcsj.ods_jcsj_sat_person_basic_info_mi where dt='jcsjk700') t1
left join dwd.sys_dic_code_info_do dim_sex on t1.gender_code=dim_sex.code and dim_sex.item_name_cn='性别代码' and dim_sex.code not in ('0','9')
left join dwd.sys_dic_code_info_do dim_nation on t1.nation_code=dim_nation.code and dim_nation.item_name_cn='民族代码' and dim_nation.code not in ('00','98','99')
left join dwd.sys_dic_code_info_do dim_native_plac on t1.native_place_code=dim_native_plac.code and dim_native_plac.item_name_cn='国家或地区代码' and dim_native_plac.code not in ('000','999')
-- join dwd.sys_dic_code_info_do dim_edu on t1.edu_code=dim_edu.code and dim_edu.item_name_cn='学历代码' and dim_edu.code not in ('00','99')
left join dwd.sys_dic_code_info_do dim_politics_status on t1.politics_status_code=dim_politics_status.code and dim_politics_status.item_name_cn='政治面貌代码' and dim_politics_status.code not in ('00','99')
left join (
    -- 工商联职务
    select person_hkey,
           t2.duty_code,
           dim_duty.code_cn as org_duty_name
    from ods_jcsj.ods_jcsj_link_org_per_mi t1
             join ods_jcsj.ods_jcsj_sat_fic_duty_info_mi t2 on t1.org_per_hkey=t2.org_per_hkey
             join dwd.sys_dic_code_info_do dim_duty on t2.duty_code=dim_duty.code and dim_duty.item_name_cn = ('工商联职务代码')
) org_duty_tb on t1.person_hkey=org_duty_tb.person_hkey
left join (
    select person_hkey,
           concat_ws(',',collect_set(person_type)) as person_type
    from (select person_hkey, -- 代表类型
                 '代表类型' as person_type
          from ods_jcsj.ods_jcsj_link_per_rep_mi
          union all
          select person_hkey, -- 执常委类型
                 '执委人员类型' as person_type
          from dwd.zhjc_ent_executive_do
          union all
          select person_hkey, -- 个人会员类型
                 '个人会员类型' as person_type
          from ods_jcsj.ods_jcsj_link_per_mem_p_mi
         ) t group by person_hkey
) person_type_tb on t1.person_hkey=person_type_tb.person_hkey
left join (
    select person_hkey,
           coalesce(county_tb.county,city_tb.city,province_tb.province) as native_place
    from (select person_hkey,
                 split(native_place_code, '，')[size(split(native_place_code, '，')) - 1] as native_place_code
          from ods_jcsj.ods_jcsj_sat_person_basic_info_mi
         ) t1
             left join dwd.dim_zhjc_region_do county_tb on t1.native_place_code=county_tb.county_code
             left join dwd.dim_zhjc_region_do city_tb on t1.native_place_code=city_tb.county_code
             left join dwd.dim_zhjc_region_do province_tb on t1.native_place_code=province_tb.county_code
    group by person_hkey,
             coalesce(county_tb.county,city_tb.city,province_tb.province)
) native_place_tb on t1.person_hkey=native_place_tb.person_hkey
left join tmp_lk.person_other_label per_other_tb on t1.person_hkey=per_other_tb.person_hkey
;


-- age





select age
from tmp_lk.person_label
where age is not null and length(age)>0
limit 100;


select birthdate,
        concat(cast((year(current_date())- substr(birthdate,1,4))/5 as bigint)*5,
              '-',
              (cast((year(current_date())- substr(birthdate,1,4))/5 as bigint)+1)*5) as age
from ods_jcsj.ods_jcsj_sat_person_basic_info_mi
where dt='jcsjk700'
limit 100
;


select person_hkey,
        sex
from tmp_lk.person_label
where sex is not null


;


select * from dwd.bdmd_tpgjgr_wf;


select person_hkey,
       sex
from tmp_lk.person_label
where sex is not null
;



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
                WHEN instr(province , '省') > 0 THEN '省级'
                when province  in( '内蒙古自治区', '西藏自治区','新疆维吾尔自治区','宁夏回族自治区','广西壮族自治区','香港特区','澳门特区','北京市','天津市','上海市','重庆市') then '省级'
                when province ='全国' then '全国'
                else province
                END as tag_level,-- 标签层级
            concat_ws('',province,replace(city,province,''),replace(district,replace(city,province,''),'')) as district, -- 地域
            a.nd,-- 年度
            qybm,-- 企业编码
            person_hkey,
            bdbm -- 榜单编码
        from dwd.bdmd_wyldjz_wf a
    ) t
where person_hkey is not null
group by
    t.person_hkey,
    t.tag_level,-- 标签层级
    t.district, -- 地域
    t.nd

select * from dwd.bdmd_wyldjz_wf


