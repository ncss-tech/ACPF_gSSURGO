-- mwACPF_extSurfaceTexture.sql
-- Extract surface Texture soil parameters for ACPF Soils database.
-- This program creates the ACPF_SurfaceTexture soil table by extracting information from 
-- the NRCS Soils Data Mart (SDM) national soils database -- also known as SSURGO by some.
-- The COKEY field will allow for joins to the ACPF_SurfaceHorizon table.
--
-- I have made every attempt to generate a viable dataset that characterizes the mapunit's 
-- surface horizon, but as there may be more than one dominant or 'major' component of any
-- mapunit there is not a strict cardinality among this dataset and the mapunit raster. I have
-- ordered the dataset by mapunit and the component percent (descending), such that the ArcGIS
-- join should be to the component with the highest contributing proportion. The mukey, cokey,
-- and chkey remain if users wish to extend their reach back to the original SDM data.
-- 
-- Note: 2014 - Added the use of US_DomComponents, a table of my creation (see associated
--   script). Uses MajorCompFlag = 1 and the largest compPct to select the "ONE" best component.
-- Note: 3/2016 - modified slightly to extend to the 12 Midwest states (ND-OH) to support both
--    the ACPF and the DEP
--
-- I have gone to the trouble of documenting the selection criteria (WHERE...) for this extraction 
--  because of the use of the RVIndicators and the klugy PMOrder structure...even though they are
--  not included in the output.
--
--SELECTION CRITERIA
-- Table Physical Name: chorizon
-- Table Label: Horizon
--   Column Physical Name: hzdept_r 
--   Column Label: Horizon depth - top
--    The distance from the top of the soil to the upper boundary of the soil horizon...
--     or NULL if there are no horizon entries -- non-soil data, i.e. water, pits, etc.
--   Criteria: and hrz.hzdept_r = 0 or hrz.hzdept_r is null
--
-- Table Physical Name: copmgrp
-- Table Label: Parent Material Group
--   Column Physical Name: rvindicator 
--   Column Label: RV?
--    A yes/no field that indicates if a value or row (set of values) is representative for the component.
--   Criteria: and copmgrp.rvindicator = 'Yes' 
--
-- Table Physical Name: chtexturegrp
-- Table Label: Horizon Texture Group
--   Column Physical Name: rvindicator 
--   Column Label: RV?
--    A yes/no field that indicates if a value or row (set of values) is representative for the component.
--   Criteria: and chtgrp.rvindicator = 'Yes' 
--
-- Table Physical Name: copmgrp
-- Table Label: Component Parent Material Group
--   Column Physical Name: pmorder 
--   Column Label: Vertical Order
--    The sequence in which the parent material occurs, when more than one parent material 
--    exists for one soil profile. If only one parent material occurs for a soil no entry is required.
--   Criteria: and ( copm.pmorder = 1 or copm.pmorder is null )
--


USE MWSoilsFY16

IF OBJECT_ID('dbo.mwACPF_SurfTextureTable') IS NOT NULL 
        DROP TABLE dbo.mwACPF_SurfTextureTable
        
SELECT C.cokey
      ,C.comppct_r
      ,chtgrp.texture as Texture
      --,chtgrp.rvindicator as TEX_rvindicator
      ,cht.texcl as TextCls
      ,copmgrp.pmgroupname as ParMatGrp
      --,copmgrp.rvindicator as PMGRPrvindicator
      ,copm.pmkind as ParMatKind
      --,copm.pmorder
  INTO MWSoilsFY16.dbo.mwACPF_SurfTextureTable
  FROM MWSoilsFY16.dbo.mw_MapUnitTable MU LEFT OUTER JOIN [MWSoilsFY16].[dbo].mw_DomComponents DC ON MU.mukey = DC.mukey
      LEFT OUTER JOIN USSoilsFY16.[dbo].[COMPONENT] C ON  C.cokey = DC.cokey
       LEFT OUTER JOIN USSoilsFY16.[dbo].[CHORIZON] hrz ON hrz.cokey = C.cokey 
        LEFT OUTER JOIN USSoilsFY16_2.[dbo].[copmgrp] copmgrp ON copmgrp.cokey = C.cokey
         LEFT OUTER JOIN USSoilsFY16_2.[dbo].[copm] copm ON copm.copmgrpkey = copmgrp.copmgrpkey
          LEFT OUTER JOIN USSoilsFY16_2.[dbo].[chtexturegrp] chtgrp ON hrz.chkey = chtgrp.chkey 
           LEFT OUTER JOIN USSoilsFY16_2.[dbo].[chtexture] cht ON chtgrp.chtgkey = cht.chtgkey 

  where ( hrz.hzdept_r  = 0 or hrz.hzdept_r is null) and ( copm.pmorder = 1 or copm.pmorder is null)
        and copmgrp.rvindicator = 'Yes' and chtgrp.rvindicator = 'Yes' 
  order by MU.mukey, C.comppct_r DESC
GO

