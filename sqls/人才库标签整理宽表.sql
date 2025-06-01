-- 企业 标签 目录 

select '人才库标签' as first_index
       ,fflxx.zt  as second_index -- '一级目录'
       ,fflxx.bqflmc  as third_index -- '二级目录'
       ,zflxx.bqflmc  as four_index -- '三级目录'
     -- ... --四级目录
    from jcsjr.BQ_FL_FLGLGX fflglgx
    join jcsjr.BQ_FL_FLXX fflxx on fflglgx.FFLID = fflxx.id and fflxx.ZT = '非公经济优秀企业家-分类标签'
    join jcsjr.BQ_FL_FLXX zflxx on fflglgx.zFLID = zflxx.id and zflxx.ZT = '非公经济优秀企业家-分类标签'


-- 企业标签信息数据, 
select d.enterprise_hkey,
       a.bqbm,
       b.bqmc,
       b.bqmc 
from jcsjr.bq_qy_phb a
join jcsjr.bq_bqdy b on a.bqbm = b.bqbm
join jcsj.qy_qyjbxx c on a.qybm=c.jbxxzj
join jcsj.sat_ent_basic_info_rd d on c.qymc=d.enterprise_name




-- 人员  一共 348 个标签 

select a.rybm,
       c.xm,
       a.bqbm,
       b.bqmc
from jcsjr.bq_ry_phb_rh a
         join jcsjr.bq_bqdy b on a.bqbm = b.bqbm
         join jcsj.ry_ryjbxx c on a.rybm = c.rybm -- 目前匹配不到 
         join jcsj.qy_qyjbxx c on c.qybm = c.jbxxzj

select count(1)
from (
-- 人才库 人员-目录映射表，目前由于没有join到数据，需要等到治理完毕后才能进一步改造 
select '人才库标签' as first_index
     ,flxx.second_flmc as second_index -- '一级目录'
     ,flxx.third_flmc  as third_index -- '二级目录'
     ,flxx.four_flmc  as four_index -- '三级目录'
    -- ,person_tb.PERSON_HKEY -- 人员hashkey
     ,ry_phb.bqbm -- 标签编码
     ,bqdy.bqmc -- 标签名称
     ,bqdy.bqmc -- 标签值
from jcsjr.bq_ry_phb_rh ry_phb
         join jcsjr.bq_bqdy bqdy on ry_phb.bqbm = bqdy.bqbm
         --join jcsj.qy_qyjbxx qyjbxx on ry_phb.qybm = qyjbxx.jbxxzj
        -- join jcsj.sat_person_basic_info_rd person_tb on qyjbxx.qymc = person_tb.enterprise_name
         join jcsjr.bq_fl_bqglgx bqglgx on bqdy.id = bqglgx.bqid
         join
     (
         select fflxx.zt as second_flmc-- '主题'
              , fflxx.bqflmc as third_flmc -- '一级分类名称'
              -- ,fflglgx.fflid
              , zflxx.bqflmc as four_flmc -- '二级分类名称'
              , fflglgx.zflid
         from jcsjr.bq_fl_flglgx fflglgx
                  join jcsjr.bq_fl_flxx fflxx on fflglgx.fflid = fflxx.id and fflxx.zt = '非公经济优秀企业家-分类标签'
                  join jcsjr.bq_fl_flxx zflxx on fflglgx.zflid = zflxx.id and zflxx.zt = '非公经济优秀企业家-分类标签'
     ) flxx on bqglgx.flid = flxx.zflid
) t

-- 人才库 非公经济优秀企业家-分类标签 目录 

-- select fflxx.zt-- '主题'
--        ,fflxx.bqflmc-- '分类名称'
--        -- ,fflglgx.fflid
--        ,zflxx.bqflmc -- '分类名称'
--        ,fflglgx.zflid
--        --,bqglgx.flid
-- from jcsjr.bq_fl_flglgx fflglgx
--          join jcsjr.bq_fl_flxx fflxx on fflglgx.fflid = fflxx.id and fflxx.zt = '非公经济优秀企业家-分类标签'
--          join jcsjr.bq_fl_flxx zflxx on fflglgx.zflid = zflxx.id and zflxx.zt = '非公经济优秀企业家-分类标签'


-- 行政区划分类暂时没弄好，因为没有join出来数据 
select flxx.id,
       flxx.zt ,-- '主题'
       flxx.bqflmc ,-- '分类名称'
       flxx.xh ,-- '序号'
       flxx.jb ,-- '级别' 标注目录级别
       flxx.bqfldx -- '标签分类对象（人员标签/企业标签）'
    from jcsjr.bq_fl_flxx flxx where zt='行政区划分类'

select fflxx.zt,-- '主题'
       fflxx.bqflmc,-- '分类名称'
       -- fflglgx.fflid,
       zflxx.bqflmc--,-- '分类名称'
       -- fflglgx.zflid
from jcsjr.bq_fl_flglgx fflglgx
         left join jcsjr.bq_fl_flxx fflxx on fflglgx.fflid = fflxx.id and fflxx.zt = '行政区划分类'
         left join jcsjr.bq_fl_flxx zflxx on fflglgx.zflid = zflxx.id and zflxx.zt = '行政区划分类'
;


-- flxx.zt -- '主题' -- 一级目录
--,flxx.first_flmc -- '二级目录'
--,flxx.second_flmc -- '三级目录'

-- 企业 目录-标签映射关系
select count(1)
from (select '人才库标签'        as first_index
           , flxx_tb.second_flmc as second_index -- '一级目录'
           , flxx_tb.thrid_flmc  as third_index  -- '二级目录'
           , flxx_tb.four_flmc   as four_index   -- '三级目录'
           , ent.enterprise_hkey as rk           -- 企业hashkey
           , qy_phb.bqbm         as tag_code     -- 标签编码
           , bqdy.bqmc           as tag_name     -- 标签名称
           , bqdy.bqmc           as tag_value    -- 标签值
      from jcsjr.bq_qy_phb qy_phb
               join jcsjr.bq_bqdy bqdy on qy_phb.bqbm = bqdy.bqbm
               join jcsj.qy_qyjbxx qyjbxx on qy_phb.qybm = qyjbxx.jbxxzj
               join jcsj.sat_ent_basic_info_rd ent on qyjbxx.qymc = ent.enterprise_name
               join jcsjr.bq_fl_bqglgx bqglgx on bqdy.id = bqglgx.bqid
               join
           (select fflxx.zt     as second_flmc-- '主题'
                 , fflxx.bqflmc as thrid_flmc -- '一级分类名称'
                 -- ,fflglgx.fflid
                 , zflxx.bqflmc as four_flmc  -- '二级分类名称'
                 , fflglgx.zflid
            from jcsjr.bq_fl_flglgx fflglgx
                     join jcsjr.bq_fl_flxx fflxx on fflglgx.fflid = fflxx.id and fflxx.zt = '非公经济优秀企业家-分类标签'
                     join jcsjr.bq_fl_flxx zflxx
                          on fflglgx.zflid = zflxx.id and zflxx.zt = '非公经济优秀企业家-分类标签') flxx_tb
           on bqglgx.flid = flxx_tb.zflid
      group by flxx_tb.second_flmc -- '一级目录'
             , flxx_tb.thrid_flmc  -- '二级目录'
             , flxx_tb.four_flmc   -- '三级目录'
             , ent.enterprise_hkey -- 企业hashkey
             , qy_phb.bqbm         -- 标签编码
             , bqdy.bqmc -- 标签名称
     ) t
    ;

select qy_phb.*,qyjbxx.*,bqdy.*
from jcsj.bq_qy_phb qy_phb
               join jcsj.bq_bqdy bqdy on qy_phb.bqbm = bqdy.bqbm
               join jcsj.qy_qyjbxx qyjbxx on qy_phb.qybm = qyjbxx.jbxxzj
               left join jcsj.bq_fl_bqglgx bqglgx on bqdy.id = bqglgx.bqid
where bqglgx.BQID is null

select count(1)
from (select distinct bqbm,QYBM
      from jcsj.bq_qy_phb) t

select count(1)
from (select '人才库标签'        as first_index
           , flxx_tb.second_flmc as second_index -- '一级目录'
           , flxx_tb.thrid_flmc  as third_index  -- '二级目录'
           , flxx_tb.four_flmc   as four_index   -- '三级目录'
           , null                as enterprise_hkey
           , qyjbxx.qymc         as enterprise_name
           -- , ent.enterprise_hkey as rk           -- 企业hashkey
           , qy_phb.bqbm         as tag_code     -- 标签编码
           , bqdy.bqmc           as tag_name     -- 标签名称
           , bqdy.bqmc           as tag_value    -- 标签值
      from jcsj.bq_qy_phb qy_phb
               left join jcsj.bq_bqdy bqdy on qy_phb.bqbm = bqdy.bqbm
               left join jcsj.qy_qyjbxx qyjbxx on qy_phb.qybm = qyjbxx.jbxxzj
               -- join jcsj.sat_ent_basic_info_rd ent on qyjbxx.qymc = ent.enterprise_name
               left join jcsj.bq_fl_bqglgx bqglgx on bqdy.id = bqglgx.bqid
               left join
           (select  fflxx.zt     as second_flmc-- '主题'
                 , fflxx.bqflmc as thrid_flmc -- '一级分类名称'
                 -- ,fflglgx.fflid
                 , zflxx.bqflmc as four_flmc  -- '二级分类名称'
                 , fflglgx.zflid
                 , fflglgx.fflid
                 , zflxx.id
            from jcsj.bq_fl_flglgx fflglgx
                     join jcsj.bq_fl_flxx fflxx on fflglgx.fflid = fflxx.id and fflxx.zt = '非公经济优秀企业家-分类标签'
                     join jcsj.bq_fl_flxx zflxx
                          on fflglgx.zflid = zflxx.id and zflxx.zt = '非公经济优秀企业家-分类标签'
            ) flxx_tb
           on bqglgx.flid = flxx_tb.id
      group by flxx_tb.second_flmc -- '一级目录'
             , flxx_tb.thrid_flmc  -- '二级目录'
             , flxx_tb.four_flmc   -- '三级目录'
             , qyjbxx.qymc
             -- , ent.enterprise_hkey -- 企业hashkey
             , qy_phb.bqbm         -- 标签编码
             , bqdy.bqmc -- 标签名称
     ) t


select distinct --qybm jbxxzj,
                '人才库标签'        as first_index
                ,'非公经济优秀企业家-分类标签' as second_index -- '一级目录'
                , yjflmc   as third_index  -- '二级目录'
                , ejflmc   as four_index   -- '三级目录'
                , null                as enterprise_hkey
                , qymc         as enterprise_name
                -- ,qylb
                ,t.bqbm
                ,b.bqmc
                ,b.bqmc
      from jcsjr.BQ_QY_PHB t,
           (select bq.bqbm, bq.bqmc, fl.yjflmc, fl.ejflmc, fl.qylb, fl.fflid, fl.zflid
            from (select bqbm, b.bqmc, t.flid
                  from jcsjr.BQ_BQDY b
                           join jcsjr.BQ_FL_BQGLGX t
                                on b.id = t.bqid) bq
                     join
                 (select fflid,
                         zflid,
                         a.bqflmc yjflmc,
                         b.bqflmc ejflmc,
                         b.qylb
                  from jcsjr.BQ_FL_FLGLGX t
                           left join (select id, bqflmc, xh
                                      from jcsjr.BQ_FL_FLXX t
                                      where zt = '非公经济优秀企业家-分类标签'
                                        and jb = '一级'
                                        and scbz = '1') a
                                     on t.fflid = a.id
                           left join (select id, bqflmc, xh, QYLB
                                      from jcsjr.BQ_FL_FLXX t
                                      where zt = '非公经济优秀企业家-分类标签'
                                        and jb = '二级'
                                        and scbz = '1') b
                                     on t.zflid = b.id
                  where t.scbz = '1') fl on bq.flid = fl.zflid
            ) b,jcsj.qy_qyjbxx qyjbxx
      where t.bqbm = b.bqbm and t.qybm = qyjbxx.jbxxzj


























select fflxx.zt     as second_flmc-- '主题'
     , fflxx.bqflmc as thrid_flmc -- '一级分类名称'
     -- ,fflglgx.fflid
     , zflxx.bqflmc as four_flmc  -- '二级分类名称'
     , fflglgx.zflid
from jcsj.bq_fl_flglgx fflglgx
         join jcsj.bq_fl_flxx fflxx on fflglgx.fflid = fflxx.id and fflxx.zt = '非公经济优秀企业家-分类标签'
         join jcsj.bq_fl_flxx zflxx
              on fflglgx.zflid = zflxx.id and zflxx.zt = '非公经济优秀企业家-分类标签'










-- 1276 
select count(1)
from (
         select flxx.zt             -- '主题'
              , flxx.first_flmc     -- '一级分类名称'
              , flxx.second_flmc    -- '分类名称'
              -- , ent.enterprise_hkey -- 企业hashkey
              , qy_phb.bqbm         -- 标签编码
         from jcsj.bq_qy_phb qy_phb
                  join jcsj.bq_bqdy bqdy on qy_phb.bqbm = bqdy.bqbm
                  join jcsj.qy_qyjbxx qyjbxx on qy_phb.qybm = qyjbxx.jbxxzj
                  join jcsj.sat_ent_basic_info_rd ent on qyjbxx.qymc = ent.enterprise_name
                  join jcsj.bq_fl_bqglgx bqglgx on bqdy.id = bqglgx.bqid
                  join
              (
                  select fflxx.zt-- '主题'
                       , fflxx.bqflmc as first_flmc -- '一级分类名称'
                       -- ,fflglgx.fflid
                       , zflxx.bqflmc as second_flmc -- '二级分类名称'
                       , fflglgx.zflid
                  from jcsj.bq_fl_flglgx fflglgx
                           join jcsj.bq_fl_flxx fflxx on fflglgx.fflid = fflxx.id and fflxx.zt = '非公经济优秀企业家-分类标签'
                           join jcsj.bq_fl_flxx zflxx on fflglgx.zflid = zflxx.id and zflxx.zt = '非公经济优秀企业家-分类标签'
              ) flxx on bqglgx.flid = flxx.zflid
    group by flxx.zt             -- '主题'
           , flxx.first_flmc     -- '一级分类名称'
           , flxx.second_flmc    -- '分类名称'
           -- , ent.enterprise_hkey -- 企业hashkey
           , qy_phb.bqbm         -- 标签编码
     ) t



-- 行政区划码树形结构 (目录)

select z_tb.zt
     -- , z_tb.ff_xh                -- 父父 编码
     , ff_tb.bqflmc as ff_bqflmc -- 父父 名称
     , z_tb.f_xh                 -- 父 编码
     , f_tb.bqflmc  as f_bqflmc  -- 父 名称
     , z_tb.xh                   -- 子 编码
     , z_tb.bqflmc               -- 子名称
from (
         select flxx.zt                                        -- '主题'
              , flxx.bqflmc                                    -- '分类名称'
              , flxx.xh                                        -- '序号'
              , concat(substr(flxx.xh, 1, 4), '00')   as f_xh  -- '父序号'
              , concat(substr(flxx.xh, 1, 2), '0000') as ff_xh -- '父父序号'
         from jcsjr.bq_fl_flxx flxx
         where zt = '行政区划分类'
     ) z_tb
         join
     (
         select flxx.zt                                        -- '主题'
              , flxx.bqflmc                                    -- '分类名称'
              , flxx.xh                                        -- '序号'
         from jcsjr.bq_fl_flxx flxx
         where zt = '行政区划分类'
     ) f_tb on z_tb.f_xh = f_tb.xh
         join
     (
         select flxx.zt                                        -- '主题'
              , flxx.bqflmc                                    -- '分类名称'
              , flxx.xh                                        -- '序号'
         from jcsjr.bq_fl_flxx flxx
         where zt = '行政区划分类'
     ) ff_tb on z_tb.ff_xh = ff_tb.xh

-- 验证阶段，是否出现一对多的情况  
-- 4221
select count(1)
from (
         select z_tb.zt
              , z_tb.ff_xh                -- 父父 编码
              , ff_tb.bqflmc as ff_bqflmc -- 父父 名称
              , z_tb.f_xh                 -- 父 编码
              , f_tb.bqflmc  as f_bqflmc  -- 父 名称
              , z_tb.xh                   -- 子 编码
              , z_tb.bqflmc               -- 子名称

         from (
                  select flxx.zt                                        -- '主题'
                       , flxx.bqflmc                                    -- '分类名称'
                       , flxx.xh                                        -- '序号'
                       , concat(substr(flxx.xh, 1, 4), '00')   as f_xh  -- '父序号'
                       , concat(substr(flxx.xh, 1, 2), '0000') as ff_xh -- '父父序号'
                  from jcsjr.bq_fl_flxx flxx
                  where zt = '行政区划分类'
              ) z_tb
                  join
              (
                  select flxx.zt                                        -- '主题'
                       , flxx.bqflmc                                    -- '分类名称'
                       , flxx.xh                                        -- '序号'
                  from jcsjr.bq_fl_flxx flxx
                  where zt = '行政区划分类'
              ) f_tb on z_tb.f_xh = f_tb.xh
                  join
              (
                  select flxx.zt                                        -- '主题'
                       , flxx.bqflmc                                    -- '分类名称'
                       , flxx.xh                                        -- '序号'
                  from jcsjr.bq_fl_flxx flxx
                  where zt = '行政区划分类'
              ) ff_tb on z_tb.ff_xh = ff_tb.xh
     ) t


-- 4221 数据量验证完毕 
select count(1)
from (

         select flxx.zt                                        -- '主题'
              , flxx.bqflmc                                    -- '分类名称'
              , flxx.xh                                        -- '序号'
              , concat(substr(flxx.xh, 1, 4), '00')   as f_xh  -- '父序号'
              , concat(substr(flxx.xh, 1, 2), '0000') as ff_xh -- '父父序号'
         from jcsjr.bq_fl_flxx flxx
         where zt = '行政区划分类'
         )



-- 行政区划目录-标签映射关系 
select fflxx.zt
     , fflxx.ff_xh     -- 父父 编码
     , fflxx.ff_bqflmc -- 父父 名称
     , fflxx.f_xh      -- 父 编码
     , fflxx.f_bqflmc  -- 父 名称
     , fflxx.xh        -- 子 编码
     , fflxx.bqflmc    -- 子名称
     , bqdy.bqbm       -- 标签编码
     , bqdy.bqmc       -- 标签名称
     , bqdy.bqmc       -- 标签值
from (
         select z_tb.id
              , z_tb.zt
              , z_tb.ff_xh                -- 父父 编码
              , ff_tb.bqflmc as ff_bqflmc -- 父父 名称
              , z_tb.f_xh                 -- 父 编码
              , f_tb.bqflmc  as f_bqflmc  -- 父 名称
              , z_tb.xh                   -- 子 编码
              , z_tb.bqflmc               -- 子名称

         from (
                  select flxx.zt                                        -- '主题'
                       , flxx.bqflmc                                    -- '分类名称'
                       , flxx.xh                                        -- '序号'
                       , concat(substr(flxx.xh, 1, 4), '00')   as f_xh  -- '父序号'
                       , concat(substr(flxx.xh, 1, 2), '0000') as ff_xh -- '父父序号'
                       , flxx.id
                  from jcsjr.bq_fl_flxx flxx
                  where zt = '行政区划分类'
              ) z_tb
                  join
              (
                  select flxx.zt     -- '主题'
                       , flxx.bqflmc -- '分类名称'
                       , flxx.xh     -- '序号'
                  from jcsjr.bq_fl_flxx flxx
                  where zt = '行政区划分类'
              ) f_tb on z_tb.f_xh = f_tb.xh
                  join
              (
                  select flxx.zt     -- '主题'
                       , flxx.bqflmc -- '分类名称'
                       , flxx.xh     -- '序号'
                  from jcsjr.bq_fl_flxx flxx
                  where zt = '行政区划分类'
              ) ff_tb on z_tb.ff_xh = ff_tb.xh
     ) fflxx
         join jcsjr.bq_fl_bqglgx bqglgx on fflxx.id = bqglgx.flid
         join jcsjr.bq_bqdy bqdy on bqdy.id = bqglgx.bqid
where fflxx.zt = '行政区划分类'




一、标签定义表（ bq_bqdy ）:
二、标签分类定义表（ bq_fl_flxx ）：
       1、主题字段（zt）区分人才库下挂的目录“非公经济优秀企业家-分类标签”和“行政区划”；
       2、级别（jb）标注目录级别；
       3、序号（xh）；
       4、id与分类管理关系（ bq_fl_flglgx ）fflid为父分类id zflid为子分类id，标签关联关系形成外键关系（ bq_fl_bqglgx ）；


三、分类关联关系表（ bq_fl_flglgx ）表达分类与分类的上下级关系；标签关联关系表（ bq_fl_bqglgx ）表达分类与标签的上下级关系；
四、企业标签表（ bq_qy_phb ）的企业编码qybm在数据采集完成后会变更；人员标签表（ bq_ry_phb_rh ）在数据采集完成后会变更；

原则：
一、新标签命名,五一劳动奖章获得这_全国_2019
二、导入的存量标签值和标签名称一致
