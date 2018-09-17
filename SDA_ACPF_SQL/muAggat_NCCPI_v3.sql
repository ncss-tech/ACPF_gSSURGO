SELECT
     mu.mukey,
         muagg.musym as MUsymbol,
         muagg.muname as MUname,
         muagg.wtdepaprjunmin as WTDepAprJun,
         muagg.flodfreqdcd as FloodFreq,
         CAST(muagg.pondfreqprs AS smallint) as PondFreq,
         muagg.drclassdcd as DrainCls,
         muagg.drclasswettest as DrainClsWet,
         muagg.hydgrpdcd as HydroGrp,
         CAST(muagg.hydclprs AS smallint) as Hydric,

     ROUND((SELECT SUM (interphr * comppct_r ) FROM mapunit AS mui1  
        INNER JOIN component AS cint1 ON cint1.mukey=mui1.mukey  
        INNER JOIN cointerp AS coint1 ON cint1.cokey = coint1.cokey   AND majcompflag = 'yes' 
        AND mui1.mukey = mu.mukey AND ruledepth  = 1
       AND interphrc = 'Corn'
       AND mrulename = 'NCCPI - National Commodity Crop Productivity Index (Ver 3.0)'  
        GROUP BY mui1.mukey),2) AS NCCPIcs_First,


     ROUND((SELECT SUM (interphr * comppct_r)  FROM mapunit AS mui2  INNER JOIN component AS cint2 ON cint2.mukey=mui2.mukey INNER JOIN cointerp AS coint2 ON cint2.cokey = coint2.cokey AND majcompflag = 'yes'  AND mui2.mukey = mu.mukey AND ruledepth  = 1 
        AND mrulename = 'NCCPI - National Commodity Crop Productivity Index (Ver 3.0)'  
        AND interphrc = 'Small grains'
       AND (interphr) IS NOT NULL GROUP BY mui2.mukey ),2)  as NCCPIsg_First,

    (SELECT SUM (comppct_r) FROM mapunit  AS mui3  
       INNER JOIN component AS cint3 ON cint3.mukey=mui3.mukey  
       INNER JOIN cointerp AS coint3 ON cint3.cokey = coint3.cokey 
       AND majcompflag = 'yes' AND mui3.mukey = mu.mukey AND ruledepth = 0 AND mrulename = 'NCCPI - National Commodity Crop Productivity Index (Ver 3.0)' 
       AND (interphr) IS NOT NULL  GROUP BY mui3.mukey) AS sum_com

     INTO #main
     FROM (legend INNER JOIN (mapunit AS mu INNER JOIN muaggatt AS muagg ON mu.mukey = muagg.mukey) ON legend.lkey = mu.lkey   --AND mu.mukey IN ('540689'))
       AND mu.mukey IN ('1406546','540689','540690','540693','540694','540695','540696','540705','540706','540707','540708','540720','540721','540722','540724','540731','540732','540737','540738','540739','540740','540745','540746','540747','540748','540749','540750','540751','540752','540753','540757','540758','540759','540760','540761','540762','540763','540764','540766','540767','540769','540770','540773','540774','541262','541263','541265','541271','541272','541273','541274','541275','541278','541279','541283','541284','541287','541288','541289','541295','541299','541300','541301','541302','541310','541311','541313','541314','541315','541316','541317','541318','541319','541320','541321','541323','541324','541327','541328','541329','541331','541333','541334','541336','541337','541338','541339','541340','541341','541342','541347','542715','542723','542724','542725','542726','542727','542729','542730','542731','542732','542746','542747','542748','542750','542764','542765','542766','542767','542768','542769','542778','542781','542782','542785','542791','542793','542794','542795','542796','542797','542798','542800','542802','557302','557319','557320','558454','559062','559063','559067','740652','740653'))

       SELECT
         mukey,
         MUsymbol,
         MUname,
         WTDepAprJun,
         FloodFreq,
         PondFreq,
         DrainCls,
         DrainClsWet,
         HydroGrp,
         Hydric,
     ROUND ((NCCPIcs_First/sum_com),2) AS NCCPIcs,
     ROUND ((NCCPIsg_First/sum_com),2) AS NCCPIsg
     FROM #main
     DROP TABLE #main
