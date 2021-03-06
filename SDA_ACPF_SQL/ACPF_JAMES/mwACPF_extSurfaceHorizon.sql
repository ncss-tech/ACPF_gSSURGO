-- ACPF_extSurfaceHorizon.sql
-- Extract surface horizon soil parameters for ACPF Soils database.
-- This program creates the ACPF_SurfaceHorizon soil table by extracting information from 
-- the NRCS Soils Data Mart (SDM) national soils database -- also known as SSURGO by some.
-- The MUKEY field will allow for joins to the mapunit rasters.
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
--
--SELECTION CRITERIA
-- Table Physical Name: component
-- Table Label: Component
--   Column Physical Name: majcompflag Column Label: Major Component
--    Indicates whether or not a component is a major component in the mapunit.
--   Criteria: C.majcompflag = 'Yes'
--
-- Table Physical Name: chorizon
-- Table Label: Horizon
--   Column Physical Name: hzdept_r Column Label: RV
--    The distance from the top of the soil to the upper boundary of the soil horizon.
--   Criteria: and (hrz.hzdept_r = 0 or hrz.hzdept_r IS NULL)
--


USE MWSoilsFY16

IF OBJECT_ID('mwACPF_SurfHorizonTable') IS NOT NULL 
        DROP TABLE mwACPF_SurfHorizonTable
        
SELECT MU.mukey
      ,C.cokey
      ,hrz.chkey
      ,C.comppct_r as CompPct
      ,C.compname as  CompName
      ,C.compkind as  CompKind
      ,C.taxclname as TaxCls
      ,hrz.hzdepb_r - hrz.hzdept_r as HrzThick
      ,hrz.om_r as OM
      ,hrz.ksat_r as KSat
      ,hrz.kffact
      ,hrz.kwfact
      ,hrz.sandtotal_r as totalSand 		-- total sand, silt and clay fractions 
      ,hrz.silttotal_r as totalSilt
      ,hrz.claytotal_r as totalClay
      ,hrz.sandvf_r	as VFSand		        -- sand sub-fractions 
      ,hrz.dbthirdbar_r as DBthirdbar		-- bulk density

  INTO MWSoilsFY16.dbo.mwACPF_SurfHorizonTable
  FROM MWSoilsFY16.dbo.mw_MapUnitTable MU LEFT OUTER JOIN [MWSoilsFY16].[dbo].[mw_DomComponents] DC ON MU.mukey = DC.mukey
       LEFT OUTER JOIN [USSoilsFY16].[dbo].[COMPONENT] C ON  C.cokey = DC.cokey
        LEFT OUTER JOIN [USSoilsFY16].[dbo].[CHORIZON] hrz ON hrz.cokey = C.cokey 

  where C.majcompflag = 'Yes' and ( hrz.hzdept_r  = 0  or hrz.hzdept_r is null )
  order by MU.mukey, C.comppct_r DESC
GO

ALTER TABLE mwACPF_SurfHorizonTable ALTER COLUMN mukey varchar(30) NULL
ALTER TABLE mwACPF_SurfHorizonTable ALTER COLUMN OM Decimal(8,3) NULL
ALTER TABLE mwACPF_SurfHorizonTable ALTER COLUMN KSat Decimal(8,3) NULL
ALTER TABLE mwACPF_SurfHorizonTable ALTER COLUMN Kffact Decimal(8,3) NULL
ALTER TABLE mwACPF_SurfHorizonTable ALTER COLUMN Kwfact Decimal(8,3) NULL
ALTER TABLE mwACPF_SurfHorizonTable ALTER COLUMN totalSand Decimal(8,3) NULL
ALTER TABLE mwACPF_SurfHorizonTable ALTER COLUMN totalSilt Decimal(8,3) NULL
ALTER TABLE mwACPF_SurfHorizonTable ALTER COLUMN totalClay Decimal(8,3) NULL
ALTER TABLE mwACPF_SurfHorizonTable ALTER COLUMN VFSand Decimal(8,3) NULL
ALTER TABLE mwACPF_SurfHorizonTable ALTER COLUMN DBthirdbar Decimal(8,3) NULL
