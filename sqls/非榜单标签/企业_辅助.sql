drop table if exists tmp_lk.label_ent_other;
create table tmp_lk.label_ent_other
(
    enterprise_hkey             string comment '企业hash',
    registered_fund             string comment '注册资金规模',
    establish_time              string comment '成立年限',
    operating_status_value      string comment '经营状态',
    enterprise_basistype_value  string comment '企业类型(企业性质)',
    industry_value              string comment '行业类型',
    business_scope              string comment '经营范围',
    jiang_cnt                   string comment '获奖次数',
    PARTY_STRUCTURE             string comment '党组织结构',
    PARTY_MEMBER_NUM            string comment '党员人数',
    is_labor_union              string comment '是否建立工会组织',
    LABOR_UNION_MEMBER_NUM      string comment '工会会员人数',
    is_league                   string comment '是否建立共青团组织',
    LEAGUE_MEMBER_NUM           string comment '共青团团员人数',
    intellectual_property_total string comment '知识产权数(项)',
    valid_trademark_num         string comment '商标注册量',
    patent_num                  string comment '专利数',
    valid_soft_copyright        string comment '软件著作权数',
    websit_cnt                  string comment '网站备案数',
    work_cnt                    string comment '作品数'
) comment '企业其他标签(临时)'
    stored as orc
    tblproperties (
        'orc.compress' = 'SNAPPY'
        );


DROP TABLE IF EXISTS tmp_lk.work_cnt_tb;
CREATE TABLE tmp_lk.work_cnt_tb (
                                         enterprise_hkey string comment '企业hash',
                                         work_cnt string comment '作品数'
) comment '临时表'
    row format delimited fields terminated by ','
        lines terminated by '\n'
    stored as textfile;

select * from tmp_lk.work_cnt_tb;






create table tmp_lk.jiang_tb as
select enterprise_hkey,
       count(1) as jiang_cnt
from (select enterprise_hkey
      from dwd.bdmd_cfsj500qb_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_fbszjcxnlqyb_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_fpbgtlvlnbzmd_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_fpbtpgjxjjt_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_grxfhjt_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_gsmyqy100q_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_gsmyqyfwy100q_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_gsmyqyzzy100q_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_gsqybq_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_gxbyxgjqy_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_gxbzjtxxjrqy_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_gycplssjsfmd_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_hdgcsygtlhgxjdmyqyj_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_hdjsfmj_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_hdkjjbj_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_hdzhhjjdmyqyj_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_hdzljdqy_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_jscxsfqymd_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_jyyshbzxjmyqy_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_kjxgfyyqxjjt_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_ldmfjt_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_lhrhsdsfqy_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_lszzqymd_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_myqyfmzl500jbd_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_myqyfmzlbd_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_myqyyftr500jbd_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_myqyyftrbd_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_qggylcxyyysfqy_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_qghxldgxcjsfqymd_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_qgjyyshbzxjmyqyxjjtygr_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_qgsbhqsjtb_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_qgtpgjxjjt_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_qgwsjzjtb_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_qgyxzgtsshzyjsz_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_sbhqsjt_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_tpgjxjjt_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_whxjgsl_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_wqbwcxjmyqysbzmd_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_wsjzjtb_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_wyldjzjt_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_yxzgtsshzyjsz_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zggcsyjz_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zggkjgcz50q_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zgmyqy500q_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zgmyqy500q_wf_bf
      union all
      select enterprise_hkey
      from dwd.bdmd_zgmyqyfwy100q_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zgmyqyzzy500q_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zgqy500q_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zgzljqy_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zgzzymyqy100q_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zhcsjxjjt_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zjtxxjrqymd_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zjtxzxqy_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zjyxlppqy_wf
      union all
      select enterprise_hkey
      from dwd.bdmd_zzydxgjqy_wf
     ) jiang_tb
group by enterprise_hkey
;




-- 基本信息
select enterprise_hkey,
       concat(cast(trim(registered_fund)/500 as bigint)*500,'-',(cast(trim(registered_fund)/500 as bigint)+1)*500,'万元') as registered_fund, -- 注册资金规模
       concat((cast((year(current_date())-substr(trim(establish_time),1,4))/5 as bigint))*5,'-',
              (cast((year(current_date())-substr(trim(establish_time),1,4))/5 as bigint)+1)*5,'年')
           as establish_time, -- 成立年限
       operating_status, -- 经营状态
       dim_operating_tb.code_cn as operating_status_value,-- 经营状态
       enterprise_basistype, --企业类型(企业性质)
       dim_basistype_tb.code_cn as enterprise_basistype_value, --企业类型(企业性质)
       industry, --行业类型
       dim_industry_tb.code_cn as industry_value,-- 行业类型
       business_scope, --经营范围
       jiang_tb.jiang_cnt, -- 获奖次数

from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi t1
left join dwd.sys_dic_code_info_do dim_operating_tb on trim(t1.operating_status)=dim_operating_tb.code and dim_operating_tb.item_name_cn='经营状态代码' and dim_operating_tb.code not in ('00','99')
left join dwd.sys_dic_code_info_do dim_basistype_tb on trim(t1.enterprise_basistype)=dim_basistype_tb.code and dim_basistype_tb.item_name_cn='企业基础分类代码' and dim_basistype_tb.code not in ('99','00')
left join dwd.sys_dic_code_info_do dim_industry_tb on trim(t1.industry)=dim_industry_tb.code and dim_industry_tb.item_name_cn='国民经济行业分类代码' and dim_industry_tb.code not in ('0000','9999')
left join tmp_lk.dangzuzhi dangzuzhi_tb on t1.enterprise_hkey=dangzuzhi_tb.ENTERPRISE_HKEY
left join (
    select enterprise_hkey,
           count(1) as jiang_cnt
    from (select enterprise_hkey
          from dwd.bdmd_cfsj500qb_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_fbszjcxnlqyb_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_fpbgtlvlnbzmd_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_fpbtpgjxjjt_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_grxfhjt_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_gsmyqy100q_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_gsmyqyfwy100q_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_gsmyqyzzy100q_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_gsqybq_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_gxbyxgjqy_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_gxbzjtxxjrqy_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_gycplssjsfmd_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_hdgcsygtlhgxjdmyqyj_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_hdjsfmj_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_hdkjjbj_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_hdzhhjjdmyqyj_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_hdzljdqy_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_jscxsfqymd_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_jyyshbzxjmyqy_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_kjxgfyyqxjjt_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_ldmfjt_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_lhrhsdsfqy_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_lszzqymd_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_myqyfmzl500jbd_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_myqyfmzlbd_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_myqyyftr500jbd_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_myqyyftrbd_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_qggylcxyyysfqy_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_qghxldgxcjsfqymd_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_qgjyyshbzxjmyqyxjjtygr_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_qgsbhqsjtb_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_qgshsh_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_qgtpgjxjjt_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_qgwhxjgsl_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_qgwsjzjtb_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_qgyxzgtsshzyjsz_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_sbhqsjt_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_tpgjxjjt_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_whxjgsl_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_wqbwcxjmyqysbzmd_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_wsjzjtb_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_wyldjzjt_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_yxzgtsshzyjsz_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zggcsyjz_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zggkjgcz50q_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zgmyqy500q_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zgmyqy500q_wf_bf
          union all
          select enterprise_hkey
          from dwd.bdmd_zgmyqyfwy100q_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zgmyqyzzy500q_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zgqy500q_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zgzljqy_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zgzzymyqy100q_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zhcsjxjjt_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zjtxxjrqymd_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zjtxzxqy_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zjyxlppqy_wf
          union all
          select enterprise_hkey
          from dwd.bdmd_zzydxgjqy_wf
         ) jiang_tb
    group by enterprise_hkey
) jiang_tb on t1.enterprise_hkey=jiang_tb.enterprise_hkey
where t1.dt='jcsjk700'
;








select enterprise_scale,
       count(1)
from ods_jcsj.ods_jcsj_sat_ent_basic_info_mi
where dt='jcsjk700' and length(trim(enterprise_scale))>0
group by enterprise_scale


select *
from dwd.sys_dic_code_info_do dim_basistype_tb
where dim_basistype_tb.code='01' and dim_basistype_tb.item_name_cn='企业类型代码' and dim_basistype_tb.code not in ('9900')


-- 党团组织信息


create table tmp_lk.dangzuzhi as
select a.ENTERPRISE_HKEY,
       case when trim(PARTY_STRUCTURE)='0' then '未知'
            when trim(PARTY_STRUCTURE)='1' then '党委'
            when trim(PARTY_STRUCTURE)='2' then '党总支'
            else '党支部' end as PARTY_STRUCTURE, -- 党组织结构
       case when trim(PARTY_MEMBER_NUM)='0' then '0-5'
            else concat(floor(trim(PARTY_MEMBER_NUM)/5)*5,"-",ceil(trim(PARTY_MEMBER_NUM)/5)*5) end as PARTY_MEMBER_NUM, -- 党员人数
       trim(a.is_labor_union) as is_labor_union, -- 是否建立工会组织
       case when trim(a.is_labor_union)='1' and trim(LABOR_UNION_MEMBER_NUM)='0' then '0-5'
            else concat(cast(trim(LABOR_UNION_MEMBER_NUM)/5 as bigint)*5,"-",cast((trim(LABOR_UNION_MEMBER_NUM)/5) +1 as bigint)*5) end as LABOR_UNION_MEMBER_NUM, -- 工会会员人数
       trim(is_league) as is_league, -- 是否建立共青团组织
       case when trim(is_league)='1' and trim(LEAGUE_MEMBER_NUM)='0' then '0-5'
            else concat(cast(trim(LEAGUE_MEMBER_NUM)/5 as bigint)*5,"-",cast((trim(LEAGUE_MEMBER_NUM)/5)+1 as bigint)*5) end as LEAGUE_MEMBER_NUM -- 共青团团员人数
from ods_jcsj.ods_jcsj_SAT_ENT_ESTABLISHORG_INFO_mi a
where a.dt='jcsjk700'
;


select *
from tmp_lk.dangzuzhi
where dangzuzhi.LABOR_UNION_MEMBER_NUM is not null ;



select *
from ods_jcsj.ods_jcsj_sat_ent_establishorg_info_mi
where dt='jcsjk700'


































