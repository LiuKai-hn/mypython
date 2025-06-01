create materialized view v_org_period_executive as
select a.org_hkey,
       a.period,
       b.executive_hkey
from (select max(a1.period) as period, a9.org_hkey
      from jcsjk.sat_exe_committee_info a1
               join jcsjk.link_exe_exeboard a2 on a1.executive_hkey = a2.executive_hkey
               join jcsjk.link_exeboard_repcongress a3 on a2.exeboard_hkey = a3.exeboard_hkey
               join jcsjk.link_org_repcongress a4 on a3.repcongress_hkey = a4.repcongress_hkey
               join (select org_hkey, province_code, city_code, county_code, admini_level_code
                     from jcsjk.sat_org_basic_info
                     where admini_level_code in ('01', '02', '03', '04')
                       and org_name not in ('测试工商联', 'o9oojj', '验证1', '线上测试')) a9
                    on a4.org_hkey = a9.org_hkey
      where a1.period is not null
      group by a9.org_hkey) a
         join
     (select a10.org_hkey, a8.executive_hkey, a8.period
      from (select org_hkey, province_code, city_code, county_code, admini_level_code
            from jcsjk.sat_org_basic_info
            where admini_level_code in ('01', '02', '03', '04')
              and org_name not in ('测试工商联', 'o9oojj', '验证1', '线上测试')) a10
               join jcsjk.link_org_repcongress a5 on a10.org_hkey = a5.org_hkey
               join jcsjk.link_exeboard_repcongress a6 on a5.repcongress_hkey = a6.repcongress_hkey
               join jcsjk.link_exe_exeboard a7 on a6.exeboard_hkey = a7.exeboard_hkey
               join jcsjk.sat_exe_committee_info a8 on a7.executive_hkey = a8.executive_hkey
      where a8.period is not null) b on a.period = b.period and a.org_hkey = b.org_hkey