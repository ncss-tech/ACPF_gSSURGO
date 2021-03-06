-- Select_MWoilsFY16_DominantComponent.sql
-- Select the dominant component for each Mapunit in US
-- This will be the first (Row Number = 1) of the components
-- sorted by compct_r in a descending order
--
-- Only the dominant component will be used to extract the 
-- soil prarmeters for use in WEPP soil loss modeling
-- original coding: D. James 03/2013, updated 06/2013

USE MWSoilsFY16

IF OBJECT_ID('dbo.mw_DomComponents') IS NOT NULL 
        DROP TABLE dbo.mw_DomComponents

SELECT mukey
      ,cokey
      ,compname
      ,comppct_r   
  INTO dbo.mw_DomComponents
  FROM 
     ( SELECT MU.mukey
             ,C.cokey
             ,C.compname
             ,C.comppct_r
             ,rowNbr = ROW_NUMBER() OVER (PARTITION BY MU.mukey ORDER BY C.comppct_r DESC)
        
        FROM [MWSoilsFY16].[dbo].[mw_MapUnitTable] MU left join [USSoilsFY16].[dbo].[component] C ON  MU.mukey = C.mukey  
        WHERE  C.majcompflag = 'Yes' 
      ) muSelect
  where muSelect.rowNbr = 1 
  order by mukey, comppct_r DESC
GO


