select BDM,SJBM
from
    (
        SELECT XH,
               BQFLMC,
               BQMC,
               BQBM,
               BDM,
               BDBM,
               SJBM
        FROM
            (
                SELECT EJ.ID,
                       EJ.BQFLMC,
                       EJ.XH,
                       BQ.BQMC,
                       BQ.BQBM,
                       FB.ID AS BDBM,
                       FB.BDM,
                       FB.SJBM
                FROM JCSJR.BQ_FL_FLXX EJ,JCSJR.BQ_FL_BQGLGX BQGX ,JCSJR.BQ_BQDY BQ ,JCSJR.SJ_PHBDFB FB
                WHERE EJ.ID = BQGX.FLID AND BQGX.BQID = BQ.ID AND BQ.LYBM = FB.ID
                  AND EJ.ZT ='非公经济优秀企业家-分类标签'
                  AND EJ.JB ='二级'
                  AND EJ.SCBZ ='1'
                  AND EJ.XH IS NOT NULL
            ) t1
    ) t2
where bqflmc like '%专利%' -- and bqflmc like '%省%'
group by BDM,SJBM




