#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Charles.Ferguson
#
# Created:     23/03/2017
# Copyright:   (c) Charles.Ferguson 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------

class MyError(Exception):
    pass


def AddMsgAndPrint(msg, severity=0):
    # prints message to screen if run as a python script
    # Adds tool message to the geoprocessor
    #
    #Split the message on \n first, so that if it's multiple lines, a GPMessage will be added for each line
    try:

        for string in msg.split('\n'):
            #Add a geoprocessing message (in case this is run as a tool)
            if severity == 0:
                arcpy.AddMessage(string)

            elif severity == 1:
                arcpy.AddWarning(string)

            elif severity == 2:
                #arcpy.AddMessage("    ")
                arcpy.AddError(string)

    except:
        pass


def errorMsg():
    try:
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        theMsg = tbinfo + " \n" + str(sys.exc_type)+ ": " + str(sys.exc_value)
        AddMsgAndPrint(theMsg, 2)

    except:
        AddMsgAndPrint("Unhandled error in errorMsg method", 2)
        pass


def tabRequest(qry, name):


    try:

        theURL = "https://sdmdataaccess.nrcs.usda.gov"
        url = theURL + "/Tabular/SDMTabularService/post.rest"

        # Create request using JSON, return data as JSON
        request = {}
        request["format"] = "JSON+COLUMNNAME+METADATA"
        request["query"] = qry

        #json.dumps = serialize obj (request dictionary) to a JSON formatted str
        data = json.dumps(request)

        # Send request to SDA Tabular service using urllib2 library
        # because we are passing the "data" argument, this is a POST request, not a GET
        req = urllib2.Request(url, data)
        response = urllib2.urlopen(req)

        # read query results
        qResults = response.read()

        # Convert the returned JSON string into a Python dictionary.
        qData = json.loads(qResults)

        # get rid of objects
        del qResults, response, req

        Msg = 'Successfully collected ' + name + ' for ' + ws[3:]

        return True, Msg, qData

    except socket.timeout as e:
        Msg = 'Soil Data Access timeout error'
        return False, Msg, None

    except socket.error as e:
        Msg = str(e)
        return False, Msg, None

    except HTTPError as e:
        Msg = str(e)
        return False, Msg, None

    except URLError as e:
        Msg = str(e)
        return False, Msg, None

    except:
        errorMsg()
        Msg = 'Unknown error collecting tabular data for ' + ws[3:]
        return False, Msg, None



def surfHoriz(keys):

    surfHorQuery = """SELECT
        CAST (mapunit.mukey AS VARCHAR (30)) AS mukey,
        CAST (component.cokey AS VARCHAR (30)) AS cokey,
        CAST (chorizon.chkey AS VARCHAR (30))  AS chkey ,
        component.comppct_r AS CompPct,
        component.compname AS  CompName,
        component.compkind AS  CompKind,
        component.taxclname AS TaxCls,
        CASE WHEN (chorizon.hzdepb_r-chorizon.hzdept_r) IS NULL THEN 0 ELSE CAST ((hzdepb_r-hzdept_r)  AS INT) END AS HrzThick,
        CAST (chorizon.kffact AS DECIMAL (8,3)) AS kffact,
        CAST (chorizon.kwfact AS DECIMAL (8,3)) AS kwfact,
        CAST (chorizon.sandtotal_r AS DECIMAL (8,3)) AS totalSand ,		-- total sand, silt and clay fractions
        CAST (chorizon.silttotal_r AS DECIMAL (8,3)) AS totalSilt,
        CAST (chorizon.claytotal_r AS DECIMAL (8,3)) AS totalClay,
        CAST (chorizon.sandvf_r	AS DECIMAL (8,3)) AS VFSand,		        -- sand sub-fractions
        CAST (chorizon.dbthirdbar_r AS DECIMAL (8,3)) AS DBthirdbar,
        CAST (chorizon.om_r AS DECIMAL (8,3)) AS OM,
        CAST (chorizon.ksat_r AS DECIMAL (8,3)) AS  KSat
        FROM legend
        INNER JOIN mapunit ON mapunit.lkey = legend.lkey AND mapunit.mukey IN ("""  + ",".join(map("'{0}'".format, keys)) + """)
        INNER JOIN component ON component.mukey=mapunit.mukey AND majcompflag = 'yes' AND component.cokey =
        (SELECT TOP 1 c1.cokey FROM component AS c1
        INNER JOIN mapunit AS c ON c1.mukey=c.mukey AND c.mukey=mapunit.mukey ORDER BY c1.comppct_r DESC,CASE WHEN LEFT (muname,2)= LEFT (compname,2) THEN 1 ELSE 2 END ASC, c1.cokey)
        LEFT JOIN (chorizon LEFT JOIN chtexturegrp ON chorizon.chkey = chtexturegrp.chkey) ON component.cokey = chorizon.cokey
        AND (((chorizon.hzdept_r)=(SELECT Min(chorizon.hzdept_r) AS MIN_hor_depth_r
        FROM chorizon LEFT JOIN chtexturegrp ON chorizon.chkey = chtexturegrp.chkey
        WHERE chtexturegrp.texture Not In ('SPM','HPM', 'MPM') AND chtexturegrp.rvindicator='Yes' AND component.cokey = chorizon.cokey ))AND ((chtexturegrp.rvindicator)='yes'))"""

    #arcpy.AddMessage(surfHorQuery)

    #send the query to SDA
    surfHLogic, surfMsg, surfHrzRes = tabRequest(surfHorQuery, "Surface Horizon")


    if surfHLogic:


        #populate the table
        if "Table" in surfHrzRes:

            # get its value
            resLst = surfHrzRes["Table"]  # Data as a list of lists. All values come back as string.

            #number 2 accounts for colnames, colinfo
            if len(resLst) - 2 == iCnt:
                arcpy.AddMessage('\t' + surfMsg)

                tbl = "SurfHrz" + ws[3:]
                outputTable = os.path.join(gdb, tbl)

                columnNames = resLst.pop(0)
                columnInfo = resLst.pop(0)


                newTable = CreateNewTable(outputTable, columnNames, columnInfo)

                with arcpy.da.InsertCursor(newTable, columnNames) as cursor:

                    for row in resLst:
                        cursor.insertRow(row)

                arcpy.conversion.TableToTable(newTable, os.path.join(inDir, gdb), tbl)

            else:
                arcpy.AddWarning('\t' + surfMsg + " but recieved no records or does not match raster count")
                if not ws[3:] in wLst:
                    wLst.append(ws[3:])

        else:
            arcpy.AddWarning('\tNo surface horizon table returned for ' + ws[3:])
            if not ws[3:] in wLst:
                wLst.append(ws[3:])

    else:
        arcpy.AddWarning('\t' + surfMsg + " : " + ws[3:])
        if not ws[3:] in wLst:
            wLst.append(ws[3:])


def surfTex(keys):

    surfTexQuery = """SELECT
        component.cokey,
        component.comppct_r,
        chtexturegrp.texture as Texture,
        (SELECT TOP 1 chtexture.texcl FROM chtexturegrp AS cht INNER JOIN  chtexture ON cht.chtgkey=chtexture.chtgkey AND cht.rvindicator='yes' AND cht.chtgkey=chtexturegrp.chtgkey)  as TextCls,
        (SELECT TOP 1 cop1.pmgroupname
        FROM component   AS c3
        INNER JOIN copmgrp AS cop1 ON cop1.cokey=c3.cokey AND component.cokey=c3.cokey AND cop1.rvindicator ='Yes') as ParMatGrp ,
        (SELECT TOP 1  copm1.pmkind  FROM component AS c2 	  INNER JOIN copmgrp AS cop2 ON cop2.cokey=c2.cokey  INNER JOIN copm AS copm1 ON copm1.copmgrpkey=cop2.copmgrpkey AND component.cokey=c2.cokey AND cop2.rvindicator ='Yes') as ParMatKind
        FROM legend
        INNER JOIN mapunit ON mapunit.lkey = legend.lkey AND mapunit.mukey IN ("""  + ",".join(map("'{0}'".format, keys)) +\
        """)INNER JOIN component ON component.mukey=mapunit.mukey AND majcompflag = 'yes'
        AND component.cokey =
        (SELECT TOP 1 c1.cokey FROM component AS c1
        INNER JOIN mapunit AS c ON c1.mukey=c.mukey AND c.mukey=mapunit.mukey ORDER BY c1.comppct_r DESC,CASE WHEN LEFT (muname,2)= LEFT (compname,2) THEN 1 ELSE 2 END ASC, c1.cokey)
        LEFT JOIN (chorizon LEFT JOIN chtexturegrp ON chorizon.chkey = chtexturegrp.chkey) ON component.cokey = chorizon.cokey
        AND (((chorizon.hzdept_r)=(SELECT Min(chorizon.hzdept_r) AS MIN_hor_depth_r
        FROM chorizon LEFT JOIN chtexturegrp ON chorizon.chkey = chtexturegrp.chkey
        WHERE chtexturegrp.texture Not In ('SPM','HPM', 'MPM') AND chtexturegrp.rvindicator='Yes' AND component.cokey = chorizon.cokey ))AND ((chtexturegrp.rvindicator)='yes'))"""

    #arcpy.AddMessage(surfTexQuery)

    #send the query to SDA
    surfTexLogic, surfTexMsg, surfTexRes = tabRequest(surfTexQuery, "Surface Texture")

    if surfTexLogic:

        #populate the table
        if "Table" in surfTexRes:

            # get its value
            resLst = surfTexRes["Table"]  # Data as a list of lists. All values come back as string.

            #the number 2 accounts for colnames and colinfo
            if len(resLst) - 2 == iCnt:

                arcpy.AddMessage('\t' + surfTexMsg)

                tbl = "SurfTex" + ws[3:]
                outputTable = os.path.join(gdb, tbl)

                columnNames = resLst.pop(0)
                columnInfo = resLst.pop(0)

                newTable = CreateNewTable(outputTable, columnNames, columnInfo)

                with arcpy.da.InsertCursor(newTable, columnNames) as cursor:

                    for row in resLst:
                        cursor.insertRow(row)

                arcpy.conversion.TableToTable(newTable, os.path.join(inDir, gdb), tbl)


            else:
                arcpy.AddWarning('\t' + surfTexMsg + " but recieved no records or does not match raster count")
                if not ws[3:] in wLst:
                    wLst.append(ws[3:])

        else:
            arcpy.AddWarning('\tNo surface texture table returned for ' + ws[3:])
            if not ws[3:] in wLst:
                wLst.append(ws[3:])

    else:
        arcpy.AddWarning('\t' + surfTexMsg + " : " + ws[3:])
        if not ws[3:] in wLst:
            wLst.append(ws[3:])



def getHull(poly):

    try:

        if wsSR.PCSName != "":
                # AOI layer has a projected coordinate system, so geometry will always have to be projected
                bProjected = True

        elif wsSR.GCS.name != wgs.GCS.name:
            # AOI must be NAD 1983
            bProjected = True

        else:
            bProjected = False

        if bProjected:
            with arcpy.da.SearchCursor(poly, ["SHAPE@"]) as cur:
                for rec in cur:
                    cHullPolygon = rec[0].convexHull()              # simplified geometry
                    wgsPolygon = cHullPolygon.projectAs(wgs, tm)        # simplified geometry, projected to WGS 1984
                    #ClipPolygon = rec[0].projectAs(gcs)        # original geometry projected to WGS 1984
            wkt = wgsPolygon.WKT

        else:
            #not projected
            with arcpy.da.SearchCursor(poly, ["SHAPE@"]) as cur:
                for rec in cur:
                    cHullPolygon = rec[0].convexHull()                # simplified geometry
                    clipPolygon = rec[0]                              # original geometry
            wkt = cHullPolygon.WKT

        wkt = wkt.replace("MULTIPOLYGON (", "")[:-1]


        return True,wkt

    except:
        errorMsg()
        msg = 'Error building convex hull'
        return False, msg

def geoRequest(aoi):

    try:

        arcpy.management.CreateFeatureclass(env.workspace, sdaWGS, "POLYGON", None, None, None, wgs)
        arcpy.management.AddField(sdaWGS, "t_mukey", "TEXT", None, None, "30")
        arcpy.management.AddField(sdaWGS, "mukey", "LONG")

        gQry = """ --   Define a AOI in WGS84
        ~DeclareGeometry(@aoi)~
        select @aoi = geometry::STPolyFromText('polygon """ + aoi + """', 4326)\n

        --   Extract all intersected polygons
        ~DeclareIdGeomTable(@intersectedPolygonGeometries)~
        ~GetClippedMapunits(@aoi,polygon,geo,@intersectedPolygonGeometries)~

        --   Return the polygonal geometries
        select * from @intersectedPolygonGeometries
        where geom.STGeometryType() = 'Polygon'"""

##        --   Convert geometries to geographies so we can get areas
##        ~DeclareIdGeogTable(@intersectedPolygonGeographies)~
##        ~GetGeogFromGeomWgs84(@intersectedPolygonGeometries,@intersectedPolygonGeographies)~

        #uncomment next line to print geoquery
        #arcpy.AddMessage(gQry)

        arcpy.AddMessage('\t' + 'Sending coordinates to Soil Data Access...')
        theURL = "https://sdmdataaccess.nrcs.usda.gov"
        url = theURL + "/Tabular/SDMTabularService/post.rest"

        # Create request using JSON, return data as JSON
        request = {}
        request["format"] = "JSON"
        request["query"] = gQry

        #json.dumps = serialize obj (request dictionary) to a JSON formatted str
        data = json.dumps(request)

        # Send request to SDA Tabular service using urllib2 library
        # because we are passing the "data" argument, this is a POST request, not a GET
        req = urllib2.Request(url, data)
        response = urllib2.urlopen(req)

        # read query results
        qResults = response.read()

        # Convert the returned JSON string into a Python dictionary.
        qData = json.loads(qResults)

        # get rid of objects
        del qResults, response, req

        # if dictionary key "Table" is found
        if "Table" in qData:


            # get its value
            resLst = qData["Table"]  # Data as a list of lists. All values come back as string

            rows =  arcpy.da.InsertCursor(sdaWGS, ["SHAPE@WKT", "t_mukey", "mukey"])

            keyDict = dict()

            for e in resLst:

                mukey = e[0]
                imukey = int(e[0])
                geog = e[1]

                #arcpy.AddMessage(mukey)

                if not mukey in keyDict:
                    keyDict[mukey] = int(mukey)

                value = geog, mukey, imukey
                rows.insertRow(value)

            del rows
            arcpy.AddMessage('\tReceived SSURGO polygons information successfully.')


            return True, None

        else:
            Msg = 'Unable to translate request into valid geometry'
            arcpy.AddMessage(Msg)
            return False, None

    except socket.timeout as e:
        Msg = 'Soil Data Access timeout error'
        return False, Msg

    except socket.error as e:
        Msg = str(e)
        return False, Msg

    except HTTPError as e:
        Msg = str(e)
        return False, Msg

    except URLError as e:
        Msg = str(e)
        return False, Msg

    except:
        errorMsg()
        Msg = 'Unknown error collecting geometries'
        return False, Msg


def muaggat(keys):

    """ This is the old NCCPI V2 query-
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

     ROUND((SELECT SUM (interphr * comppct_r) FROM mapunit AS mui1  INNER JOIN component AS cint1 ON cint1.mukey=mui1.mukey  INNER JOIN cointerp AS coint1 ON cint1.cokey = coint1.cokey 	AND majcompflag = 'yes' AND mui1.mukey = mu.mukey AND ruledepth <> 0 AND mrulename = 'NCCPI - NCCPI Corn and Soybeans Submodel (II)' AND (interphr) IS NOT NULL  GROUP BY mui1.mukey),2) AS NCCPIcs_First,
     ROUND((SELECT SUM (interphr * comppct_r)  FROM mapunit AS mui2  INNER JOIN component AS cint2 ON cint2.mukey=mui2.mukey INNER JOIN cointerp AS coint2 ON cint2.cokey = coint2.cokey AND majcompflag = 'yes'  AND mui2.mukey = mu.mukey AND ruledepth <> 0 AND mrulename ='NCCPI - NCCPI Small Grains Submodel (II)'  AND (interphr) IS NOT NULL GROUP BY mui2.mukey ),2)  as NCCPIsg_First,
    (SELECT SUM (comppct_r) FROM mapunit  AS mui3  INNER JOIN component AS cint3 ON cint3.mukey=mui3.mukey  INNER JOIN cointerp AS coint3 ON cint3.cokey = coint3.cokey AND majcompflag = 'yes' AND mui3.mukey = mu.mukey AND ruledepth <> 0 AND mrulename = 'NCCPI - National Commodity Crop Productivity Index (Ver 2.0)' AND (interphr) IS NOT NULL  GROUP BY mui3.mukey) AS sum_com

     INTO #main
     FROM (legend INNER JOIN (mapunit AS mu INNER JOIN muaggatt AS muagg ON mu.mukey = muagg.mukey) ON legend.lkey = mu.lkey AND mu.mukey IN (""" + ",".join(map("'{0}'".format, keys)) + """))

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
     DROP TABLE #main"""

    muAgQry = """SELECT
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
       AND mu.mukey IN (""" + ",".join(map("'{0}'".format, keys)) + """))

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
     DROP TABLE #main"""

    #arcpy.AddMessage('\n\n' + muAgQry)

    #send the query to SDA
    muAgLogic, muAgMsg, muAgRes = tabRequest(muAgQry, "Muaggat")


    if muAgLogic:

        #name the table to create and create return table path
        tbl ="muaggat"
        jTbl = os.path.join(gdb, tbl)

        #populate the table
        if "Table" in muAgRes:

            # get its value
            resLst = muAgRes["Table"]  # Data as a list of lists. All values come back as string.

            #the nmumber 2 account for colnames, colinfo
            if len(resLst) - 2 == iCnt:

                arcpy.AddMessage('\t' + muAgMsg)

                columnNames = resLst.pop(0)
                columnInfo = resLst.pop(0)

                newTable = CreateNewTable(jTbl, columnNames, columnInfo)

                with arcpy.da.InsertCursor(newTable, columnNames) as cursor:

                    for row in resLst:
                        cursor.insertRow(row)

                arcpy.conversion.TableToTable(newTable, os.path.join(inDir, gdb), tbl)

                return True, jTbl

            else:
                arcpy.AddWarning('\t' + muAgMsg + " but recieved no records or does not match raster count")
                if not ws[3:] in wLst:
                    wLst.append(ws[3:])
                return False, None

        else:
            arcpy.AddWarning('\tNo muaggat table returned for ' + ws[3:])
            if not ws[3:] in wLst:
                wLst.append(ws[3:])
            return False, None

    else:
        arcpy.AddWarning('\t' + muAgMsg + " : " + ws[3:])
        if not ws[3:] in wLst:
            wLst.append(ws[3:])
        return False, None


def rootZnDep(keys):


    rootZnDepQry ="""SELECT sacatalog.areasymbol AS AREASYMBOL,
    mapunit.mukey AS mukey,
    mapunit.musym AS MUSYM,
    mapunit.muname AS MUNAME
    INTO #main
    FROM sacatalog
    INNER JOIN legend ON legend.areasymbol = sacatalog.areasymbol
    INNER JOIN mapunit ON mapunit.lkey = legend.lkey AND mapunit.mukey IN
    (""" + ",".join(map("'{0}'".format, keys)) + """)
    --AND mukind = 'Complex'
    ORDER BY sacatalog.areasymbol, mapunit.mukey, mapunit.muname
    ---
    --Gets the component information
    ---Min Top Restriction Depth
    ---Major Components Only
    ---Excludes Miscellaneous area or Where compkind is null
    SELECT
    #main.AREASYMBOL,
    #main.mukey,
    #main.MUSYM,
    #main.MUNAME,
    CONCAT(#main.mukey, ' - ', cokey) AS MUCOMPKEY,
    compname AS COMPNAME,
    comppct_r AS COMPPCT_R,
    component.cokey,
    compkind,
    majcompflag,
    ISNULL((SELECT TOP 1 MIN (resdept_r)
    FROM component AS c
    INNER JOIN corestrictions ON corestrictions.cokey=c.cokey AND reskind IN ('Densic bedrock', 'Lithic bedrock','Paralithic bedrock', 'Fragipan','Duripan','Sulfuric')
     AND c.cokey=component.cokey  GROUP BY c.cokey, resdept_r), 150) AS RV_FIRST_RESTRICTION,
    ISNULL((SELECT TOP 1 reskind
    FROM component AS c
    INNER JOIN corestrictions ON corestrictions.cokey=c.cokey AND reskind IN ('Densic bedrock', 'Lithic bedrock','Paralithic bedrock', 'Fragipan','Duripan','Sulfuric')

    --Lithic bedrock, Paralithic bedrock, Densic bedrock, Fragipan, Duripan, Sulfuric
    AND c.cokey=component.cokey  GROUP BY c.cokey, reskind, resdept_r, corestrictkey ORDER BY resdept_r, corestrictkey ), 'No Data') AS FIRST_RESTRICTION_KIND
    INTO #co_main
    FROM #main
    INNER JOIN component ON component.mukey=#main.mukey AND majcompflag = 'yes'
    AND CASE WHEN compkind = 'Miscellaneous area' THEN 2
    WHEN compkind IS NULL THEN 2 ELSE 1 END = 1
    ORDER BY #main.AREASYMBOL,
    #main.MUNAME,
    #main.mukey,
    comppct_r DESC, component.cokey

    ---
    ---Gets the horizon information
    ---
    SELECT
    #co_main.AREASYMBOL,
    #co_main.mukey,
    #co_main.MUSYM,
    #co_main.MUNAME,
    #co_main.MUCOMPKEY,
    #co_main.COMPNAME,
    #co_main.compkind,
    #co_main.COMPPCT_R,
    #co_main.majcompflag,
    #co_main.cokey,
    hzname,
    hzdept_r,
    hzdepb_r,
    chorizon.chkey,
    ph1to1h2o_r AS pH,
    ec_r AS ec,
    dbthirdbar_r - ((( sandtotal_r * 1.65 ) / 100.0 ) + (( silttotal_r * 1.30 ) / 100.0 ) + (( claytotal_r * 1.25 ) / 100.0)) AS a,
    ( 0.002081 * sandtotal_r ) + ( 0.003912 * silttotal_r ) + ( 0.0024351 * claytotal_r ) AS b,
    CASE WHEN dbthirdbar_r - ((( sandtotal_r * 1.65 ) / 100.0 ) + (( silttotal_r * 1.30 ) / 100.0 ) + (( claytotal_r * 1.25 ) / 100.0))> ( 0.002081 * sandtotal_r ) + ( 0.003912 * silttotal_r ) + ( 0.0024351 * claytotal_r ) THEN hzdept_r ELSE 150 END AS  Dense_Restriction,
    RV_FIRST_RESTRICTION,
    FIRST_RESTRICTION_KIND,
    CASE WHEN ph1to1h2o_r <3.5 THEN hzdept_r ELSE 150 END AS pH_Restriction,
    CASE WHEN ec_r >=16 THEN hzdept_r ELSE 150 END AS ec_Restriction
    INTO #Hor_main
    FROM #co_main
    INNER JOIN chorizon ON chorizon.cokey=#co_main.cokey AND hzname NOT LIKE '%O%'
    ORDER BY #co_main.AREASYMBOL,
    #co_main.MUNAME,
    #co_main.mukey,
    #co_main.comppct_r DESC, #co_main.cokey, hzdept_r ASC,
    hzdepb_r ASC, chorizon.chkey

    ---
    ---Merging the Min Restrictions together
    ---(pH_Restriction), (ec_Restriction), (Dense_Restriction), (RV_FIRST_RESTRICTION)
    SELECT #Hor_main.cokey,
    #Hor_main.AREASYMBOL,
    #Hor_main.mukey,
    #Hor_main.MUSYM,
    #Hor_main.MUNAME,
    #Hor_main.MUCOMPKEY,
    #Hor_main.COMPNAME,
    #Hor_main.compkind,
    #Hor_main.COMPPCT_R,
    #Hor_main.majcompflag,
    #Hor_main.hzname,
    #Hor_main.hzdept_r,
    #Hor_main.hzdepb_r,
    #Hor_main.chkey,
    #Hor_main.pH_Restriction,
    #Hor_main.ec_Restriction,
    #Hor_main.Dense_Restriction,
    #Hor_main.RV_FIRST_RESTRICTION, MinValue
    INTO #Hor_main2
    FROM #Hor_main
    CROSS APPLY (SELECT MIN(e) MinValue FROM (VALUES (pH_Restriction), (ec_Restriction), (Dense_Restriction), (RV_FIRST_RESTRICTION)) AS a(e)) A

    ---SELECTS THE MIN VALUE BY Component AND Map Unit
    SELECT
    --#Hor_main2.cokey,
    #Hor_main2.AREASYMBOL,
    #Hor_main2.mukey,
    #Hor_main2.MUNAME,
    --#Hor_main2.COMPNAME,
    --#Hor_main2.compkind,
    --#Hor_main2.COMPPCT_R,
    --#Hor_main2.majcompflag,
    --MIN(MinValue) over(partition by #Hor_main2.cokey) as Comp_RootZnDepth,
    MIN(MinValue) over(partition by #Hor_main2.mukey) as MU_RootZnDepth
    INTO #Hor_main3
    FROM #Hor_main2
    GROUP BY #Hor_main2.AREASYMBOL,#Hor_main2.mukey, #Hor_main2.MUNAME,
    --#Hor_main2.COMPNAME, #Hor_main2.compkind, #Hor_main2.cokey, #Hor_main2.COMPPCT_R,#Hor_main2.majcompflag,
     MinValue

    ---Time to come home. Go to your home data. Leave the nest
    SELECT
     #main.AREASYMBOL,
     #main.mukey,
     #main.MUSYM,
     #main.MUNAME,
     #Hor_main3.MU_RootZnDepth AS RootZnDepth
     INTO #last_step
     FROM  #main
     LEFT OUTER JOIN #Hor_main3 ON #Hor_main3.mukey=#main.mukey
     GROUP BY  #main.AREASYMBOL,
     #main.mukey,
     #main.MUSYM,
     #main.MUNAME,
     #Hor_main3.MU_RootZnDepth

    --Extra Step
    SELECT
    #last_step.mukey,
    #last_step.MUSYM,
    #last_step.MUNAME,
    #last_step.RootZnDepth
    FROM #last_step"""

    #arcpy.AddMessage(rootZnDepQry)

    rtZnDepLogic, rtZnDepMsg, rtZnDepRes = tabRequest(rootZnDepQry, "Root Zone Depth")

    if rtZnDepLogic:

        #name the table to create and return path
        tbl = "rtZnDep"
        jTbl = os.path.join(gdb, tbl)

        #create the table
        #arcpy.management.CreateTable(gdb, tbl , tTable)

        #identify fields for the cursor
        #rtZnDepFlds = ['MUKEY','RootZnDepth']

        #populate the table
        if "Table" in rtZnDepRes:

            # get its value
            resLst = rtZnDepRes["Table"]  # Data as a list of lists. All values come back as string.

            # number 2 account for colname and colinfo
            if len(resLst) - 2 == iCnt:

                arcpy.AddMessage('\t' + rtZnDepMsg)

                columnNames = resLst.pop(0)
                columnInfo = resLst.pop(0)

                newTable = CreateNewTable(jTbl, columnNames, columnInfo)

                with arcpy.da.InsertCursor(newTable, columnNames) as cursor:

                    for row in resLst:
                        cursor.insertRow(row)

                arcpy.conversion.TableToTable(newTable, os.path.join(inDir, gdb), tbl)

                return True, jTbl

            else:
                arcpy.AddWarning('\t' + rtZnDepMsg + " but recieved no records or does not match raster count")
                if not ws[3:] in wLst:
                    wLst.append(ws[3:])
                return False, None
        else:
            arcpy.AddWarning('\tNo root zone depth table returned for ' + ws[3:])
            if not ws[3:] in wLst:
                wLst.append(ws[3:])
            return False, None
    else:
        arcpy.AddWarning('\t' + rtZnDepMsg + " : " + ws[3:])
        if not ws[3:] in wLst:
            wLst.append(ws[3:])
        return False, None


def soc(keys):

    socQry = """SELECT areasymbol, areaname, mapunit.mukey, mapunit.mukey AS mulink, mapunit.musym, nationalmusym, mapunit.muname, mukind, muacres
        INTO #main
        FROM legend
        INNER JOIN mapunit on legend.lkey=mapunit.lkey --AND mapunit.mukey = 2809839
        INNER JOIN muaggatt AS mt1 on mapunit.mukey=mt1.mukey
        AND mapunit.mukey IN (""" + ",".join(map("'{0}'".format, keys)) + """)


        SELECT
        -- grab survey area data
        LEFT((areasymbol), 2) AS state,
         l.areasymbol,
         l.areaname,
        (SELECT SUM (DISTINCT comppct_r) FROM mapunit  AS mui3  INNER JOIN component AS cint3 ON cint3.mukey=mui3.mukey INNER JOIN chorizon AS chint3 ON chint3.cokey=cint3.cokey AND cint3.cokey = c.cokey GROUP BY chint3.cokey) AS sum_comp,
        --grab map unit level information

         mu.mukey,
         mu.musym,

        --grab component level information

         c.majcompflag,
         c.comppct_r,
         c.compname,
         compkind,
         localphase,
         slope_l,
         slope_r,
         slope_h,
        (SELECT CAST(MIN(resdept_r) AS INTEGER) FROM component LEFT OUTER JOIN corestrictions ON component.cokey = corestrictions.cokey WHERE component.cokey = c.cokey AND reskind  IS NOT NULL) AS restrictiondepth,
        (SELECT CASE WHEN MIN (resdept_r) IS NULL THEN 200 ELSE CAST (MIN (resdept_r) AS INT) END FROM component LEFT OUTER JOIN corestrictions ON component.cokey = corestrictions.cokey WHERE component.cokey = c.cokey AND reskind IS NOT NULL) AS restrictiodepth,
        (SELECT TOP 1  reskind  FROM component LEFT OUTER JOIN corestrictions ON component.cokey = corestrictions.cokey WHERE component.cokey = c.cokey AND corestrictions.reskind IN ('Lithic bedrock','Duripan', 'Densic bedrock', 'Paralithic bedrock', 'Fragipan', 'Natric', 'Ortstein', 'Permafrost', 'Petrocalcic', 'Petrogypsic')
        AND reskind IS NOT NULL ORDER BY resdept_r) AS TOPrestriction, c.cokey,

        ---begin selection of horizon properties
         hzname,
         hzdept_r,
         hzdepb_r,
         CASE WHEN (hzdepb_r-hzdept_r) IS NULL THEN 0 ELSE CAST((hzdepb_r-hzdept_r) AS INT) END AS thickness,

          om_r, dbthirdbar_r,
          (SELECT CASE WHEN SUM (cf.fragvol_r) IS NULL THEN 0 ELSE CAST (SUM(cf.fragvol_r) AS INT) END FROM chfrags cf WHERE cf.chkey = ch.chkey) as fragvol,
        brockdepmin,
          texture,
          ch.chkey
        INTO #acpf
        FROM legend  AS l
        INNER JOIN mapunit AS mu ON mu.lkey = l.lkey
        AND mu.mukey IN (""" + ",".join(map("'{0}'".format, keys)) + """)
        INNER JOIN muaggatt AS  mt on mu.mukey=mt.mukey
        INNER JOIN component AS  c ON c.mukey = mu.mukey
        INNER JOIN chorizon AS ch ON ch.cokey = c.cokey and CASE WHEN hzdept_r IS NULL THEN 2
        WHEN om_r IS NULL THEN 2
        WHEN om_r = 0 THEN 2
        WHEN dbthirdbar_r IS NULL THEN 2
        WHEN dbthirdbar_r = 0 THEN 2
        ELSE 1 END = 1
        INNER JOIN chtexturegrp ct ON ch.chkey=ct.chkey and ct.rvindicator = 'yes'
        ORDER by l.areasymbol, mu.musym, hzdept_r

        ---Sums the Component Percent and eliminate duplicate values by cokey
        SELECT mukey, cokey,  SUM (DISTINCT sum_comp) AS sum_comp2
        INTO #muacpf
        FROM #acpf AS acpf2
        WHERE acpf2.cokey=cokey
        GROUP BY mukey, cokey

        ---Sums the component percent in a map unit
        SELECT mukey, cokey, sum_comp2,  SUM (sum_comp2) over(partition by #muacpf.mukey ) AS sum_comp3 --, SUM (sum_comp2) AS sum_comp3
        INTO #muacpf2
        FROM #muacpf
        GROUP BY mukey, cokey, sum_comp2

        ---Gets the Weighted component percent. Example from "Column F" up top
        SELECT mukey, cokey,  CASE WHEN sum_comp2 = sum_comp3 THEN 1
        ELSE CAST (CAST (sum_comp2 AS  decimal (5,2)) / CAST (sum_comp3 AS decimal (5,2)) AS decimal (5,2)) END AS WEIGHTED_COMP_PCT
        INTO #muacpf3
        FROM #muacpf2

        ---grab top depth for the mineral soil and will use it later to get mineral surface properties
        SELECT compname, cokey, MIN(hzdept_r) AS min_t
        INTO #hortopdepth
        FROM #acpf
        WHERE texture NOT LIKE '%PM%' and texture NOT LIKE '%DOM' and texture NOT LIKE '%MPT%' AND texture NOT LIKE '%MUCK' AND texture NOT LIKE '%PEAT%'
        GROUP BY compname, cokey

        ---combine the mineral surface to grab surface mineral properties

        SELECT #hortopdepth.cokey,
        hzname,
        hzdept_r,
        hzdepb_r,
        thickness,
        texture AS texture_surf,
        om_r AS om_surf,
        dbthirdbar_r AS db_surf,
        fragvol AS frag_surf,
        chkey
        INTO #acpf2
        FROM #hortopdepth
        INNER JOIN #acpf on #hortopdepth.cokey=#acpf.cokey AND #hortopdepth.min_t = #acpf.hzdept_r
        ORDER BY #hortopdepth.cokey, hzname

        SELECT
        mukey,
        cokey,
        hzname,
        restrictiodepth,
        hzdept_r,
        hzdepb_r,
        CASE WHEN (hzdepb_r-hzdept_r) IS NULL THEN 0 ELSE CAST ((hzdepb_r-hzdept_r) AS INT) END AS thickness,
        texture,
        CASE WHEN dbthirdbar_r IS NULL THEN 0 ELSE dbthirdbar_r  END AS dbthirdbar_r,
        CASE WHEN fragvol IS NULL THEN 0 ELSE fragvol  END AS fragvol,
        CASE when om_r IS NULL THEN 0 ELSE om_r END AS om_r,
        chkey
        INTO #acpfhzn
        FROM #acpf


        --- depth ranges for SOC ----
        SELECT hzname, chkey, comppct_r, hzdept_r, hzdepb_r, thickness,
        CASE  WHEN hzdept_r < 150 then hzdept_r ELSE 0 END AS InRangeTop,
        CASE  WHEN hzdepb_r <= 150 THEN hzdepb_r WHEN hzdepb_r > 150 and hzdept_r < 150 THEN 150 ELSE 0 END AS InRangeBot,

        CASE  WHEN hzdept_r < 20 then hzdept_r ELSE 0 END AS InRangeTop_0_20,
        CASE  WHEN hzdepb_r <= 20  THEN hzdepb_r WHEN hzdepb_r > 20  and hzdept_r < 20 THEN 20  ELSE 0 END AS InRangeBot_0_20,


        -------CASE  WHEN hzdept_r < 50 then hzdept_r ELSE 20 END AS InRangeTop_20_50,
        --------CASE  WHEN hzdepb_r <= 50  THEN hzdepb_r WHEN hzdepb_r > 50  and hzdept_r < 50 THEN 50  ELSE 20 END AS InRangeBot_20_50,

        --CASE    WHEN hzdept_r < 20 THEN 20
        --		WHEN hzdept_r < 50 then hzdept_r ELSE 20 END AS InRangeTop_20_50,

        --CASE    WHEN hzdepb_r < 20 THEN 20
        --WHEN hzdepb_r <= 50 THEN hzdepb_r  WHEN hzdepb_r > 50 and hzdept_r < 50 THEN 50 ELSE 20 END AS InRangeBot_20_50,

        CASE    WHEN hzdepb_r < 20 THEN 0
        WHEN hzdept_r >50 THEN 0
        WHEN hzdepb_r >= 20 AND hzdept_r < 20 THEN 20
        WHEN hzdept_r < 20 THEN 0
        		WHEN hzdept_r < 50 then hzdept_r ELSE 20 END AS InRangeTop_20_50 ,


        CASE   WHEN hzdept_r > 50 THEN 0
        WHEN hzdepb_r < 20 THEN 0
        WHEN hzdepb_r <= 50 THEN hzdepb_r  WHEN hzdepb_r > 50 and hzdept_r < 50 THEN 50 ELSE 20 END AS InRangeBot_20_50,



        CASE    WHEN hzdepb_r < 50 THEN 0
        WHEN hzdept_r >100 THEN 0
        WHEN hzdepb_r >= 50 AND hzdept_r < 50 THEN 50
        WHEN hzdept_r < 50 THEN 0
        		WHEN hzdept_r < 100 then hzdept_r ELSE 50 END AS InRangeTop_50_100 ,


        CASE   WHEN hzdept_r > 100 THEN 0
        WHEN hzdepb_r < 50 THEN 0
        WHEN hzdepb_r <= 100 THEN hzdepb_r  WHEN hzdepb_r > 100 and hzdept_r < 100 THEN 100 ELSE 50 END AS InRangeBot_50_100,
        --CASE    WHEN hzdept_r < 50 THEN 50
        --		WHEN hzdept_r < 100 then hzdept_r ELSE 50 END AS InRangeTop_50_100,

        --CASE    WHEN hzdepb_r < 50 THEN 50
        --WHEN hzdepb_r <= 100 THEN hzdepb_r  WHEN hzdepb_r > 100 and hzdept_r < 100 THEN 100 ELSE 50 END AS InRangeBot_50_100,

        om_r, fragvol, dbthirdbar_r, cokey, mukey, 100.0 - fragvol AS frag_main
        INTO #SOC
        FROM #acpf
        ORDER BY cokey, hzdept_r ASC, hzdepb_r ASC, chkey


        SELECT mukey, cokey, hzname, chkey, comppct_r, hzdept_r, hzdepb_r, thickness,
        InRangeTop_0_20,
        InRangeBot_0_20,

        InRangeTop_20_50,
        InRangeBot_20_50,

        InRangeTop_50_100 ,
        InRangeBot_50_100,
        (( ((InRangeBot_0_20 - InRangeTop_0_20) * ( ( om_r / 1.724 ) * dbthirdbar_r )) / 100.0 ) * ((100.0 - fragvol) / 100.0)) * ( comppct_r * 100 ) AS HZ_SOC_0_20,
        ((((InRangeBot_20_50 - InRangeTop_20_50) * ( ( om_r / 1.724 ) * dbthirdbar_r )) / 100.0 ) * ((100.0 - fragvol) / 100.0)) * ( comppct_r * 100 ) AS HZ_SOC_20_50,
        ((((InRangeBot_50_100 - InRangeTop_50_100) * ( ( om_r / 1.724 ) * dbthirdbar_r )) / 100.0 ) * ((100.0 - fragvol) / 100.0)) * ( comppct_r * 100 ) AS HZ_SOC_50_100
        INTO #SOC2
        FROM #SOC
        ORDER BY  mukey ,cokey, comppct_r DESC, hzdept_r ASC, hzdepb_r ASC, chkey

        ---Aggregates and sum it by component.
        SELECT DISTINCT cokey, mukey,
        ROUND (SUM (HZ_SOC_0_20) over(PARTITION BY cokey) ,3) AS CO_SOC_0_20,
        ROUND (SUM (HZ_SOC_20_50) over(PARTITION BY cokey),3) AS CO_SOC_20_50,
        ROUND (SUM (HZ_SOC_50_100) over(PARTITION BY cokey),3)  AS CO_SOC_50_100
        INTO #SOC3
        FROM #SOC2

        SELECT DISTINCT #main.mukey, ROUND (SUM (CO_SOC_0_20) over(PARTITION BY #SOC3.mukey) ,3) AS SOC_0_20,
        ROUND (SUM (CO_SOC_20_50) over(PARTITION BY #SOC3.mukey),3) AS SOC_20_50,
        ROUND(SUM (CO_SOC_50_100) over(PARTITION BY #SOC3.mukey),3)  AS SOC_50_100
        FROM #SOC3
        RIGHT OUTER JOIN #main ON #main.mukey=#SOC3.mukey"""

    #arcpy.AddMessage(socQry)

    socLogic, socMsg, socRes = tabRequest(socQry, "SOC")

    if socLogic:

        #name the table to create and return path
        tbl = "soc"
        jTbl = os.path.join(gdb, tbl)

        #populate the table
        if "Table" in socRes:

            # get its value
            resLst = socRes["Table"]  # Data as a list of lists. All values come back as string.

            #number 2 accounts for colname, colinfo
            if len(resLst) - 2 == iCnt:

                arcpy.AddMessage('\t' + socMsg)

                columnNames = resLst.pop(0)
                columnInfo = resLst.pop(0)

                newTable = CreateNewTable(jTbl, columnNames, columnInfo)

                with arcpy.da.InsertCursor(newTable, columnNames) as cursor:

                    for row in resLst:
                        cursor.insertRow(row)

                arcpy.conversion.TableToTable(newTable, os.path.join(inDir, gdb), tbl)

                return True, jTbl
            else:
                arcpy.AddWarning('\t' + socMsg + " but recieved no records or does not match raster count")
                if not ws[3:] in wLst:
                    wLst.append(ws[3:])
                return False, None
        else:
            arcpy.AddWarning('\tNo soc depth table returned for ' + ws[3:])
            if not ws[3:] in wLst:
                wLst.append(ws[3:])
            return False, None
    else:
        arcpy.AddWarning('\t' + socMsg + " : " + ws[3:])
        if not ws[3:] in wLst:
            wLst.append(ws[3:])
        return False, None

def potWet(keys):

    potWetQry = """SELECT
     areasymbol,
     musym,
     muname,
     mu.mukey/1  AS mukey,
     (SELECT TOP 1 COUNT_BIG(*)
     FROM mapunit AS mu1
     INNER JOIN component ON component.mukey=mu1.mukey AND mu1.mukey = mu.mukey) AS comp_count,
     (SELECT TOP 1 COUNT_BIG(*)
     FROM mapunit AS mu2
     INNER JOIN component ON component.mukey=mu2.mukey AND mu2 .mukey = mu.mukey
     AND majcompflag = 'Yes') AS count_maj_comp,
     (SELECT TOP 1 COUNT_BIG(*)
     FROM mapunit AS mu3
     INNER JOIN component ON component.mukey=mu3.mukey AND mu3.mukey = mu.mukey
     AND hydricrating = 'Yes' ) AS all_hydric,
     (SELECT TOP 1 COUNT_BIG(*)
     FROM mapunit AS mu4
     INNER JOIN component ON component.mukey=mu4.mukey AND mu4.mukey = mu.mukey
     AND majcompflag = 'Yes' AND hydricrating = 'Yes') AS maj_hydric,
     (SELECT TOP 1 COUNT_BIG(*)
     FROM mapunit AS mu5
     INNER JOIN component ON component.mukey=mu5.mukey AND mu5.mukey = mu.mukey
     AND majcompflag = 'Yes' AND hydricrating != 'Yes') AS maj_not_hydric,
      (SELECT TOP 1 COUNT_BIG(*)
     FROM mapunit AS mu6
     INNER JOIN component ON component.mukey=mu6.mukey AND mu6.mukey = mu.mukey
     AND majcompflag != 'Yes' AND hydricrating  = 'Yes' ) AS hydric_inclusions,
     (SELECT TOP 1 COUNT_BIG(*)
     FROM mapunit AS mu7
     INNER JOIN component ON component.mukey=mu7.mukey AND mu7.mukey = mu.mukey
     AND hydricrating  != 'Yes') AS all_not_hydric,
      (SELECT TOP 1 COUNT_BIG(*)
     FROM mapunit AS mu8
     INNER JOIN component ON component.mukey=mu8.mukey AND mu8.mukey = mu.mukey
     AND hydricrating  IS NULL ) AS hydric_null ,
       (SELECT SUM (comppct_r)
     FROM mapunit AS mu9
     INNER JOIN component ON component.mukey=mu9.mukey AND mu9.mukey = mu.mukey
    AND hydricrating  = 'Yes' ) AS MU_comppct_SUM

     INTO #main_query
     FROM legend  AS l
     INNER JOIN mapunit AS mu ON mu.lkey = l.lkey AND mu.mukey IN (""" + ",".join(map("'{0}'".format, keys)) + """)
     ---Getting the component data and criteria together for the Component Percent.
     SELECT  #main_query.areasymbol, #main_query.muname, #main_query.mukey, cokey, compname, hydricrating, localphase, drainagecl,
     CASE
     WHEN compkind = 'Miscellaneous area' AND compname = 'Water' 			THEN 999
     WHEN compkind = 'Miscellaneous area' AND compname LIKE '% water' 		THEN 999
     WHEN compkind = 'Miscellaneous area' AND compname LIKE  '% Ocean' 		THEN 999
     WHEN compkind = 'Miscellaneous area' AND compname LIKE '% swamp' 		THEN 999
     WHEN compkind = 'Miscellaneous area' AND muname = 'Water'			 	THEN 999

     WHEN compkind IS NULL AND compname = 'Water' 			THEN 999
     WHEN compkind IS NULL  AND compname LIKE '% water' 	THEN 999
     WHEN compkind IS NULL  AND compname LIKE  '% Ocean' 	THEN 999
     WHEN compkind IS NULL  AND compname LIKE '% swamp' 	THEN 999
     WHEN compkind IS NULL  AND muname = 'Water' 			THEN 999  END AS Water999,

     CASE WHEN hydricrating = 'Yes' THEN comppct_r
     WHEN hydricrating = 'Unranked' AND localphase LIKE '%drained%' 	THEN comppct_r
     WHEN hydricrating = 'Unranked' AND localphase LIKE '%channeled%' 	THEN comppct_r
     WHEN hydricrating = 'Unranked' AND localphase LIKE '%protected%' 	THEN comppct_r
     WHEN hydricrating = 'Unranked' AND localphase LIKE '%ponded%' 		THEN comppct_r
     WHEN hydricrating = 'Unranked' AND localphase LIKE '%flooded%' 	THEN comppct_r
     END AS hydric_sum
     INTO #mu_agg
     FROM #main_query
     INNER JOIN component ON component.mukey=#main_query.mukey

     SELECT  DISTINCT mukey, muname, areasymbol,
    CASE WHEN Water999 = 999 THEN MAX (999) over(PARTITION BY mukey)  ELSE SUM (hydric_sum) over(PARTITION BY mukey) END AS mu_hydric_sum
     INTO #mu_agg2
     FROM #mu_agg

      SELECT  DISTINCT mukey, muname, areasymbol,
    mu_hydric_sum
     INTO #mu_agg3
     FROM #mu_agg2 WHERE mu_hydric_sum IS NOT NULL


    SELECT  ---#main_query.areasymbol,
    ---#main_query.musym,
     ---#main_query.muname,
    #main_query.mukey, #mu_agg3.mu_hydric_sum AS PotWetandSoil,
    CASE WHEN comp_count = all_not_hydric + hydric_null THEN  'Nonhydric'
    WHEN comp_count = all_hydric  THEN 'Hydric'
    WHEN comp_count != all_hydric AND count_maj_comp = maj_hydric THEN 'Predominantly Hydric'
    WHEN hydric_inclusions >= 0.5 AND  maj_hydric < 0.5 THEN  'Predominantly Nonydric'
    WHEN maj_not_hydric >= 0.5  AND  maj_hydric >= 0.5 THEN 'Partially Hydric' ELSE 'Error' END AS hydric_rating
    FROM #main_query
    LEFT OUTER JOIN #mu_agg3 ON #mu_agg3.mukey=#main_query.mukey"""

    potWetLogic, potWetMsg, potWetRes = tabRequest(potWetQry, "Potential Wetland")

    if potWetLogic:

        #name the table to create and return path
        tbl = "potwet"
        jTbl = os.path.join(gdb, tbl)

        #populate the table
        if "Table" in potWetRes:

            # get its value
            resLst = potWetRes["Table"]  # Data as a list of lists. All values come back as string.

            #number 2 accounts for colnames, colinfo
            if len(resLst) - 2 == iCnt:

                arcpy.AddMessage('\t' + potWetMsg)

                columnNames = resLst.pop(0)
                columnInfo = resLst.pop(0)

                newTable = CreateNewTable(jTbl, columnNames, columnInfo)

                with arcpy.da.InsertCursor(newTable, columnNames) as cursor:

                    for row in resLst:
                        cursor.insertRow(row)

                arcpy.conversion.TableToTable(newTable, os.path.join(inDir, gdb), tbl)

                return True, jTbl

            else:
                arcpy.AddWarning('\t' + potWetMsg + " but recieved no records or does not match raster count")
                if not ws[3:] in wLst:
                    wLst.append(ws[3:])
                return False, None

        else:
            arcpy.AddWarning('\tNo potential wetland table returned for ' + ws[3:])
            if not ws[3:] in wLst:
                wLst.append(ws[3:])
            return False, None

    else:
        arcpy.AddWarning('\t' + potWetMsg + " : " + ws[3:])
        if not ws[3:] in wLst:
            wLst.append(ws[3:])
        return False, None



def ksat50150(keys):

    ksatQry = """SELECT areasymbol, areaname, mapunit.mukey, musym, nationalmusym, muname, mukind
        INTO #main
        FROM legend
        INNER JOIN mapunit on mapunit.lkey=legend.lkey AND mapunit.mukey IN ( """ + ",".join(map("'{0}'".format, keys)) + """)



        ---Gets only the dominant component
        SELECT
        #main.mukey,
        muname,
        cokey,
        compname,
        comppct_r
        INTO #acpf
        FROM #main
        INNER JOIN component ON component.mukey=#main.mukey
        AND component.cokey =
        (SELECT TOP 1 c1.cokey FROM component AS c1
        INNER JOIN mapunit AS m ON c1.mukey=m.mukey AND c1.mukey=#main.mukey ORDER BY c1.comppct_r DESC, CASE WHEN LEFT (muname,2)= LEFT (compname,2) THEN 1 ELSE 2 END ASC, c1.cokey)

        --Gets only the horizons that intersect 50 AND 150
        SELECT #acpf.mukey,
        muname,
        #acpf.cokey,
        #acpf.compname,
        #acpf.comppct_r, hzname, chkey, hzdept_r,  hzdepb_r,

        ksat_r,

        CASE    WHEN hzdepb_r < 50 THEN 0
        WHEN hzdept_r >150 THEN 0
        WHEN hzdepb_r >= 50 AND hzdept_r < 50 THEN 50
        WHEN hzdept_r < 50 THEN 0
        		WHEN hzdept_r < 150 then hzdept_r ELSE 50 END AS InRangeTop_50_100 ,


        CASE   WHEN hzdept_r > 150 THEN 0
        WHEN hzdepb_r < 50 THEN 0
        WHEN hzdepb_r <= 150 THEN hzdepb_r  WHEN hzdepb_r > 150 and hzdept_r < 150 THEN 150 ELSE 50 END AS InRangeBot_50_100
        INTO #acpf2
        FROM #acpf
        INNER JOIN chorizon ON chorizon.cokey=#acpf.cokey
        AND CASE    WHEN hzdepb_r < 50 THEN 0
        WHEN hzdept_r >150 THEN 0
        WHEN hzdepb_r >= 50 AND hzdept_r < 50 THEN 50
        WHEN hzdept_r < 50 THEN 0
        		WHEN hzdept_r < 150 then hzdept_r ELSE 50 END  >= 50 AND

        CASE   WHEN hzdept_r > 150 THEN 0
        WHEN hzdepb_r < 50 THEN 0
        WHEN hzdepb_r <= 150 THEN hzdepb_r  WHEN hzdepb_r > 150 and hzdept_r < 150 THEN 150 ELSE 50 END  <=150 AND ksat_r IS NOT NULL
        ORDER BY
        muname,
        mukey,
        comppct_r DESC, compname, cokey, hzdept_r ASC, hzdepb_r ASC, chkey

        SELECT mukey,
        muname,
        cokey,
        compname,
        comppct_r, hzname, chkey, hzdept_r,  hzdepb_r,
        ksat_r AS Initial_KSAT
        INTO #acpf3
        FROM #acpf2
        ORDER BY
        muname,
        mukey,
        comppct_r DESC, compname, cokey, hzdept_r ASC, hzdepb_r ASC, chkey

        SELECT DISTINCT  muname,
        mukey, MAX(Initial_KSAT) over(PARTITION BY compname) as Initial_KSAT2
        INTO #last_step
        FROM #acpf3

        SELECT DISTINCT  muname,
        mukey, ISNULL (Initial_KSAT2, 0) AS KSat50_150
        INTO #last_step2
        FROM #last_step

        SELECT #main.mukey,
        #main.muname,
        KSat50_150
        FROM #last_step2
        RIGHT OUTER JOIN #main ON #main.mukey=#last_step2.mukey
        ORDER BY #main.muname ASC, #main.mukey, KSat50_150"""

    ksat50150Logic , ksat50150Msg, ksat50150Res = tabRequest(ksatQry, "KSat 50_150")

    if ksat50150Logic:

        #name the table to create
        tbl = "KSat50_150"
        jTbl = os.path.join(gdb, tbl)

        #populate the table
        if "Table" in ksat50150Res:

            # get its value
            resLst = ksat50150Res["Table"]  # Data as a list of lists. All values come back as string.

            #number 2 accounts for colnames, colinfo
            if len(resLst) - 2 == iCnt:

                arcpy.AddMessage('\t' + ksat50150Msg)

                columnNames = resLst.pop(0)
                columnInfo = resLst.pop(0)

                newTable = CreateNewTable(jTbl, columnNames, columnInfo)

                with arcpy.da.InsertCursor(newTable, columnNames) as cursor:

                    for row in resLst:
                        cursor.insertRow(row)

                arcpy.conversion.TableToTable(newTable, os.path.join(inDir, gdb), tbl)

                return True, jTbl

            else:
                arcpy.AddWarning('\t' + ksat50150Msg+ " but recieved no records or does not match raster count")
                if not ws[3:] in wLst:
                    wLst.append(ws[3:])
                return False, None
        else:
            arcpy.AddWarning('\tNo KSat 50 - 150 depth table returned for ' + ws[3:])
            if not ws[3:] in wLst:
                wLst.append(ws[3:])
            return False, None

    else:
        arcpy.AddWarning('\t' + ksat50150Msg + " : " + ws[3:])
        if not ws[3:] in wLst:
            wLst.append(ws[3:])
        return False, None

def rootZnAwsDrt(keys):

    rootZnAwsDrtQry = """SELECT areASymbol, areaname, mapunit.mukey, mapunit.musym, nationalmusym, mapunit.muname, mukind, muacres, aws0150wta
    INTO #main
    FROM legend
    INNER JOIN mapunit on legend.lkey=mapunit.lkey AND mapunit.mukey IN (
    """ + ",".join(map("'{0}'".format, keys)) + """)
    INNER JOIN muaggatt AS mt1 on mapunit.mukey=mt1.mukey


    SELECT
    -- grab survey area data
    LEFT((areasymbol), 2) AS state,
     l.areASymbol,
     l.areaname,
    (SELECT SUM (DISTINCT comppct_r) FROM mapunit  AS mui3  INNER JOIN component AS cint3 ON cint3.mukey=mui3.mukey INNER JOIN chorizon AS chint3 ON chint3.cokey=cint3.cokey AND cint3.cokey = c.cokey GROUP BY chint3.cokey) AS sum_comp,
    --grab map unit level information

     mu.mukey,
     mu.musym,

    --grab component level information

     c.majcompflag,
     c.comppct_r,
     c.compname,
     compkind,
     localphase,
     slope_l,
     slope_r,
     slope_h,
    (SELECT CAST(MIN(resdept_r) AS INTEGER) FROM component LEFT OUTER JOIN corestrictions ON component.cokey = corestrictions.cokey WHERE component.cokey = c.cokey AND reskind  IS NOT NULL) AS restrictiondepth,
    (SELECT CASE WHEN MIN (resdept_r) IS NULL THEN 200 ELSE CAST (MIN (resdept_r) AS INT) END FROM component LEFT OUTER JOIN corestrictions ON component.cokey = corestrictions.cokey WHERE component.cokey = c.cokey AND reskind IS NOT NULL) AS restrictiodepth,
    (SELECT TOP 1  reskind  FROM component LEFT OUTER JOIN corestrictions ON component.cokey = corestrictions.cokey WHERE component.cokey = c.cokey AND corestrictions.reskind IN ('bedrock, lithic', 'duripan', 'bedrock, densic', 'bedrock, paralithic', 'fragipan', 'natric', 'ortstein', 'permafrost', 'petrocalcic', 'petrogypsic')

    AND reskind IS NOT NULL ORDER BY resdept_r) AS TOPrestriction, c.cokey,

    ---begin selection of horizon properties
     hzname,
     hzdept_r,
     hzdepb_r,
     CASE WHEN (hzdepb_r-hzdept_r) IS NULL THEN 0 ELSE CAST((hzdepb_r-hzdept_r) AS INT) END AS thickness,
    --  thickness in inches
      awc_r,
      aws025wta,
      aws050wta,
      aws0100wta,
      aws0150wta,
      brockdepmin,
      texture,
      ch.chkey
    INTO #acpf
    FROM legend  AS l
    INNER JOIN mapunit AS mu ON mu.lkey = l.lkey
    INNER JOIN muaggatt mt on mu.mukey=mt.mukey AND mu.mukey IN (
    """ + ",".join(map("'{0}'".format, keys)) + """)
    INNER JOIN component c ON c.mukey = mu.mukey AND c.majcompflag = 'yes'
    INNER JOIN chorizon ch ON ch.cokey = c.cokey and CASE WHEN hzdept_r IS NULL THEN 2
    WHEN awc_r IS NULL THEN 2
    WHEN awc_r = 0 THEN 2 ELSE 1 END = 1
    INNER JOIN chtexturegrp ct ON ch.chkey=ct.chkey and ct.rvindicator = 'yes'
    ORDER by l.areasymbol, mu.musym, hzdept_r

    ---Sums the Component Percent and eliminate duplicate values by cokey
    SELECT mukey, cokey,  SUM (DISTINCT sum_comp) AS sum_comp2
    INTO #muacpf
    FROM #acpf AS acpf2
    WHERE acpf2.cokey=cokey
    GROUP BY mukey, cokey

    ---Sums the component percent in a map unit
    SELECT mukey, cokey, sum_comp2,  SUM (sum_comp2) over(partition by #muacpf.mukey ) AS sum_comp3 --, SUM (sum_comp2) AS sum_comp3
    INTO #muacpf2
    FROM #muacpf
    GROUP BY mukey, cokey, sum_comp2

    ---Gets the Weighted component percent. Example from "Column F" up top
    SELECT mukey, cokey,  CASE WHEN sum_comp2 = sum_comp3 THEN 1
    ELSE CAST (CAST (sum_comp2 AS  decimal (5,2)) / CAST (sum_comp3 AS decimal (5,2)) AS decimal (5,2)) END AS WEIGHTED_COMP_PCT
    INTO #muacpf3
    FROM #muacpf2

    ---grab top depth for the mineral soil and will use it later to get mineral surface properties

    SELECT compname, cokey, MIN(hzdept_r) AS min_t
    INTO #hortopdepth
    FROM #acpf
    WHERE texture NOT LIKE '%PM%' and texture NOT LIKE '%DOM' and texture NOT LIKE '%MPT%' AND texture NOT LIKE '%MUCK' AND texture NOT LIKE '%PEAT%'
    GROUP BY compname, cokey

    ---combine the mineral surface to grab surface mineral properties

    SELECT #hortopdepth.cokey,
    hzname,
    hzdept_r,
    hzdepb_r,
    thickness,
    texture AS texture_surf,
    awc_r AS awc_surf,
    chkey
    INTO #acpf2
    FROM #hortopdepth
    INNER JOIN #acpf on #hortopdepth.cokey=#acpf.cokey AND #hortopdepth.min_t = #acpf.hzdept_r
    ORDER BY #hortopdepth.cokey, hzname


    SELECT
    mukey,
    cokey,
    hzname,
    restrictiodepth,
    hzdept_r,
    hzdepb_r,
    CASE WHEN (hzdepb_r-hzdept_r) IS NULL THEN 0 ELSE CAST ((hzdepb_r-hzdept_r) AS INT) END AS thickness,
    texture,
    CASE when awc_r IS NULL THEN 0 ELSE awc_r END AS awc_r,
    chkey
    INTO #acpfhzn
    FROM #acpf


    --- depth ranges for AWS ----
    SELECT
    CASE  WHEN hzdepb_r <= 150 THEN hzdepb_r WHEN hzdepb_r > 150 and hzdept_r < 150 THEN 150 ELSE 0 END AS InRangeBot,
    CASE  WHEN hzdept_r < 150 then hzdept_r ELSE 0 END AS InRangeTop,

    CASE  WHEN hzdepb_r <= 20  THEN hzdepb_r WHEN hzdepb_r > 20  and hzdept_r < 20 THEN 20  ELSE 0 END AS InRangeBot_0_20,
    CASE  WHEN hzdept_r < 20 then hzdept_r ELSE 0 END AS InRangeTop_0_20,


    CASE  WHEN hzdepb_r <= 50  THEN hzdepb_r WHEN hzdepb_r > 50  and hzdept_r < 50 THEN 50  ELSE 20 END AS InRangeBot_20_50,
    CASE  WHEN hzdept_r < 50 then hzdept_r ELSE 20 END AS InRangeTop_20_50,

    CASE  WHEN hzdepb_r <= 100  THEN hzdepb_r WHEN hzdepb_r > 100  and hzdept_r < 100 THEN 100  ELSE 50 END AS InRangeBot_50_100,
    CASE  WHEN hzdept_r < 100 then hzdept_r ELSE 50 END AS InRangeTop_50_100,
    awc_r, cokey, mukey
    INTO #aws
    FROM #acpf
    ORDER BY cokey

    SELECT mukey, cokey,
    SUM((InRangeBot - InRangeTop)*awc_r) AS aws150,

    SUM((InRangeBot_0_20 - InRangeTop_0_20)*awc_r) AS aws_0_20,

    SUM((InRangeBot_20_50 - InRangeTop_20_50)*awc_r) AS aws_20_50,

    SUM((InRangeBot_50_100 - InRangeTop_50_100)*awc_r) AS aws_50_100
    INTO #aws150
    FROM #aws
    GROUP BY  mukey, cokey

    ---return to weighted averages, using the thickness times the non-null horizon properties
    SELECT mukey, cokey, chkey,
     thickness,
     restrictiodepth,
    (awc_r*thickness) as th_awc_r
    INTO #acpf3
    FROM #acpfhzn
    ORDER BY mukey, cokey, chkey

    ---sum all horizon properties to gather the final product for the component

    SELECT mukey, cokey, restrictiodepth,
    CAST(sum(thickness) AS float(2)) AS sum_thickness,
    CAST(sum(th_awc_r) AS float(2)) AS sum_awc_r
    INTO #acpf4
    FROM #acpf3
    GROUP BY mukey, cokey, restrictiodepth
    ORDER BY mukey

    ---find the depth to use in the weighted average calculation

    SELECT mukey, cokey, CASE WHEN sum_thickness < restrictiodepth then sum_thickness  else restrictiodepth end AS restrictiondepth
    INTO #depthtest
    FROM #acpf4

    ---sql to create weighted average by dividing by the restriction depth found in the above query

    SELECT #acpf4.mukey, #acpf4.cokey,
     sum_thickness,
     #depthtest.restrictiondepth,
    (sum_awc_r) AS profile_Waterstorage,
    (sum_awc_r/#depthtest.restrictiondepth)  AS wtavg_awc_r_to_restrict
    INTO #acpfwtavg
    FROM #acpf4
    INNER JOIN #depthtest on #acpf4.cokey=#depthtest.cokey
    ---WHERE sum_awc_r != 0
    ORDER by #acpf4.mukey, #acpf4.cokey


    --time to put it all together using a lot of CASTs to change the data to reflect the way I want it to appear

    SELECT DISTINCT
      #acpf.state,
      #acpf.areasymbol,
      #acpf.areaname,
      #acpf.musym,
      #acpf.mukey,
      #acpf.cokey,
      majcompflag,
      comppct_r,
      #acpf.compname,
      compkind,
      localphase,
      slope_l,
      slope_r,
      slope_h,
      CAST(aws150 AS Decimal(5,1)) AS aws150_dcp,
    	CAST(aws_0_20 AS Decimal(5,1)) AS aws_0_20_dcp,
    		CAST(aws_20_50 AS Decimal(5,1)) AS aws_20_50_dcp,
    			CAST(aws_50_100 AS Decimal(5,1)) AS aws_50_100_dcp,


      CAST(profile_Waterstorage AS Decimal(5,1)) AS AWS_profile_dcp,
      CAST(wtavg_awc_r_to_restrict AS Decimal(5,1)) AS AWS_restrict_dcp,
      sum_thickness,
      CAST(#acpfwtavg.restrictiondepth/2.54 AS int)  restrictiondepth_IN,
      #acpfwtavg.restrictiondepth,
      TOPrestriction,
      #acpf2.chkey,
      #acpf2.hzname,
    CAST(#acpf2.hzdept_r/2.54 AS int)  AS hzdept_r,
    CAST(#acpf2.hzdepb_r/2.54 AS int) AS hzdeb_r
    INTO #alldata
    FROM #acpf2
    INNER JOIN #acpf on #acpf.cokey = #acpf2.cokey
    LEFT OUTER JOIN #aws150 on #acpf.cokey = #aws150.cokey
    LEFT OUTER JOIN #acpfwtavg on #acpf.cokey = #acpfwtavg.cokey
    ORDER BY #acpf.state, #acpf.areasymbol, #acpf.areaname, #acpf.musym

    ---Uses the above query and the query on line 89
    SELECT  #alldata.mukey,  #alldata.cokey, #alldata.aws150_dcp, WEIGHTED_COMP_PCT ,
    CAST (CASE WHEN #alldata.aws150_dcp IS NULL THEN 0 ELSE #alldata.aws150_dcp END * CASE WHEN #alldata.aws150_dcp IS NULL THEN 0 ELSE WEIGHTED_COMP_PCT   END AS Decimal(5,2)) AS AWC_COMP_SUM,
    CAST (CASE WHEN #alldata.aws_0_20_dcp IS NULL THEN 0 ELSE #alldata.aws_0_20_dcp END * CASE WHEN #alldata.aws_0_20_dcp IS NULL THEN 0 ELSE WEIGHTED_COMP_PCT   END AS Decimal(5,2)) AS AWC_COMP_SUM_0_20,
    CAST (CASE WHEN #alldata.aws_20_50_dcp IS NULL THEN 0 ELSE #alldata.aws_20_50_dcp END * CASE WHEN #alldata.aws_20_50_dcp IS NULL THEN 0 ELSE WEIGHTED_COMP_PCT   END AS Decimal(5,2)) AS AWC_COMP_SUM_20_50,
    CAST (CASE WHEN #alldata.aws_50_100_dcp IS NULL THEN 0 ELSE #alldata.aws_50_100_dcp END * CASE WHEN #alldata.aws_50_100_dcp IS NULL THEN 0 ELSE WEIGHTED_COMP_PCT   END AS Decimal(5,2)) AS AWC_COMP_SUM_50_100

    INTO #alldata2
    FROM #alldata
    INNER JOIN #muacpf3 ON #alldata.cokey=#muacpf3.cokey


    SELECT #alldata2.mukey , CAST (SUM (AWC_COMP_SUM) over(partition by #alldata2.mukey )AS Decimal(5,2)) AS MU_AWC_WEIGHTED_AVG0_150,
    CAST (SUM (AWC_COMP_SUM_0_20) over(partition by #alldata2.mukey )AS Decimal(5,2)) AS MU_AWC_WEIGHTED_AVG_0_20,
    CAST (SUM (AWC_COMP_SUM_20_50) over(partition by #alldata2.mukey )AS Decimal(5,2)) AS MU_AWC_WEIGHTED_AVG_20_50,
    CAST (SUM (AWC_COMP_SUM_50_100) over(partition by #alldata2.mukey )AS Decimal(5,2)) AS MU_AWC_WEIGHTED_AVG_50_100
    INTO #alldata3
    FROM #alldata2


    SELECT --state,
    -- #main.areasymbol,
    -- #main.areaname,
     #main.mukey,
    -- #main.musym,
    -- muname,
    ---- nationalmusym,
    --- mukind,
    MU_AWC_WEIGHTED_AVG0_150 AS RootZnAWS,
    CASE WHEN MU_AWC_WEIGHTED_AVG0_150 IS NULL THEN NULL
    WHEN MU_AWC_WEIGHTED_AVG0_150 >=15.24 THEN 0 ELSE 1 END AS Droughty

    --aws0150wta AS MuAGG_aws0150wta,
    --MU_AWC_WEIGHTED_AVG_0_20 AS aws0_20,
    --MU_AWC_WEIGHTED_AVG_20_50 AS aws20_50,
    --MU_AWC_WEIGHTED_AVG_50_100 AS aws50_100

    FROM #main
    LEFT OUTER JOIN #alldata on #main.mukey=#alldata.mukey
    LEFT OUTER JOIN #alldata3 on #main.mukey=#alldata3.mukey
    GROUP BY
    --state, #main.areasymbol,  #main.areaname,
    #main.mukey,   muname,
    --#main.musym,  nationalmusym,  mukind,
    MU_AWC_WEIGHTED_AVG0_150
    ---, aws0150wta, MU_AWC_WEIGHTED_AVG_0_20, MU_AWC_WEIGHTED_AVG_20_50, MU_AWC_WEIGHTED_AVG_50_100
    --ORDER BY areasymbol, musym"""

    #arcpy.AddMessage(rootZnAwsDrtQry)

    rtZnAwsDrtLogic, rtZnAwsDrtMsg, rtZnAwsDrtRes = tabRequest(rootZnAwsDrtQry, "Root Zone AWS & Drought")

    if rtZnAwsDrtLogic:

        #name the table to create and return path
        tbl = "rtZnAwsDrt"
        jTbl = os.path.join(gdb, tbl)

        #populate the table
        if "Table" in rtZnAwsDrtRes:

            # get its value
            resLst = rtZnAwsDrtRes["Table"]  # Data as a list of lists. All values come back as string.

            #number 2 accounts for colnames, colinfo
            if len(resLst) - 2 == iCnt:

                arcpy.AddMessage('\t' + rtZnAwsDrtMsg)

                columnNames = resLst.pop(0)
                columnInfo = resLst.pop(0)

                newTable = CreateNewTable(jTbl, columnNames, columnInfo)

                with arcpy.da.InsertCursor(newTable, columnNames) as cursor:

                    for row in resLst:
                        cursor.insertRow(row)

                arcpy.conversion.TableToTable(newTable, os.path.join(inDir, gdb), tbl)

                return True, jTbl

            else:
                arcpy.AddWarning('\t' + rtZnAwsDrtMsg + " but recieved no records or does not match raster count")
                if not ws[3:] in wLst:
                    wLst.append(ws[3:])
                return False, None
        else:
            arcpy.AddWarning('\tNo root zone AWS-Drought table returned for ' + ws[3:])
            if not ws[3:] in wLst:
                wLst.append(ws[3:])
            return False, None
    else:
        arcpy.AddWarning('\t' + rtZnAwsDrtMsg + " : " + ws[3:])
        if not ws[3:] in wLst:
            wLst.append(ws[3:])
        return False, None


def om(keys):
    omQry = """SELECT areasymbol, musym, muname, mukey
     INTO #kitchensink
     FROM legend  AS lks
     INNER JOIN  mapunit AS muks ON muks.lkey = lks.lkey AND muks.mukey IN (""" + ",".join(map("'{0}'".format, keys)) +""")


     SELECT mu1.mukey, cokey, comppct_r, SUM (comppct_r) over(partition by mu1.mukey ) AS SUM_COMP_PCT
     INTO #comp_temp
     FROM legend  AS l1
     INNER JOIN  mapunit AS mu1 ON mu1.lkey = l1.lkey AND mu1.mukey IN (""" + ",".join(map("'{0}'".format, keys)) +""")
     INNER JOIN  component AS c1 ON c1.mukey = mu1.mukey AND majcompflag = 'Yes'
     AND c1.cokey =
    (SELECT TOP 1 c2.cokey FROM component AS c2
    INNER JOIN mapunit AS mm1 ON c2.mukey=mm1.mukey AND c2.mukey=mu1.mukey ORDER BY c2.comppct_r DESC, c2.cokey)

     SELECT cokey, SUM_COMP_PCT, CASE WHEN comppct_r = SUM_COMP_PCT THEN 1
     ELSE CAST (CAST (comppct_r AS  decimal (5,2)) / CAST (SUM_COMP_PCT AS decimal (5,2)) AS decimal (5,2)) END AS WEIGHTED_COMP_PCT
     INTO #comp_temp3
     FROM #comp_temp


     SELECT
     areasymbol, musym, muname, mu.mukey/1  AS MUKEY, c.cokey AS COKEY, ch.chkey/1 AS CHKEY, compname, hzname, hzdept_r, hzdepb_r, CASE WHEN hzdept_r <0  THEN 0 ELSE hzdept_r END AS hzdept_r_ADJ,
     CASE WHEN hzdepb_r > 100  THEN 100 ELSE hzdepb_r END AS hzdepb_r_ADJ,
     CAST (CASE WHEN hzdepb_r > 100  THEN 100 ELSE hzdepb_r END - CASE WHEN hzdept_r <0 THEN 0 ELSE hzdept_r END AS decimal (5,2)) AS thickness,
     comppct_r,
     CAST (SUM (CASE WHEN hzdepb_r > 100  THEN 100 ELSE hzdepb_r END - CASE WHEN hzdept_r <0 THEN 0 ELSE hzdept_r END) over(partition by c.cokey) AS decimal (5,2)) AS sum_thickness,
     CAST (ISNULL (om_r, 0) AS decimal (5,2))AS om_r INTO #main FROM legend  AS l
     INNER JOIN  mapunit AS mu ON mu.lkey = l.lkey AND mu.mukey IN (""" + ",".join(map("'{0}'".format, keys)) +""")
     INNER JOIN  component AS c ON c.mukey = mu.mukey
     INNER JOIN chorizon AS ch ON ch.cokey=c.cokey

     AND hzname NOT LIKE '%r%'
     AND hzdepb_r >0 AND hzdept_r <100 INNER JOIN chtexturegrp AS cht ON ch.chkey=cht.chkey  WHERE cht.rvindicator = 'yes' AND  ch.hzdept_r IS NOT NULL
     AND texture NOT LIKE '%PM%' and texture NOT LIKE '%DOM'
     and texture NOT LIKE '%br%' and texture NOT LIKE '%wb%'
     ORDER BY areasymbol, musym, muname, mu.mukey, comppct_r DESC, cokey,  hzdept_r, hzdepb_r


     SELECT #main.areasymbol, #main.musym, #main.muname, #main.MUKEY,
     #main.COKEY, #main.CHKEY, #main.compname, hzname, hzdept_r, hzdepb_r, hzdept_r_ADJ, hzdepb_r_ADJ, thickness, sum_thickness, om_r, comppct_r, SUM_COMP_PCT, WEIGHTED_COMP_PCT ,
     SUM((thickness/sum_thickness ) * om_r )over(partition by #main.COKEY)AS COMP_WEIGHTED_AVERAGE
     INTO #comp_temp2
     FROM #main
     INNER JOIN #comp_temp3 ON #comp_temp3.cokey=#main.cokey
     ORDER BY #main.areasymbol, #main.musym, #main.muname, #main.MUKEY, comppct_r DESC,  #main.COKEY,  hzdept_r, hzdepb_r


     SELECT #comp_temp2.MUKEY,#comp_temp2.COKEY, WEIGHTED_COMP_PCT * COMP_WEIGHTED_AVERAGE AS COMP_WEIGHTED_AVERAGE1
     INTO #last_step
     FROM #comp_temp2
     GROUP BY  #comp_temp2.MUKEY,#comp_temp2.COKEY, WEIGHTED_COMP_PCT, COMP_WEIGHTED_AVERAGE


     SELECT areasymbol, musym, muname,
     #kitchensink.mukey, #last_step.COKEY,
     CAST (SUM (COMP_WEIGHTED_AVERAGE1) over(partition by #kitchensink.mukey) as decimal(5,2))AS om_r
     INTO #last_step2 FROM #last_step
     RIGHT OUTER JOIN #kitchensink ON #kitchensink.mukey=#last_step.mukey
     GROUP BY #kitchensink.areasymbol, #kitchensink.musym, #kitchensink.muname, #kitchensink.mukey, COMP_WEIGHTED_AVERAGE1, #last_step.COKEY
     ORDER BY #kitchensink.areasymbol, #kitchensink.musym, #kitchensink.muname, #kitchensink.mukey


     SELECT #last_step2.areasymbol, #last_step2.musym, #last_step2.muname,
     #last_step2.mukey, #last_step2.om_r
     FROM #last_step2
     LEFT OUTER JOIN #last_step ON #last_step.mukey=#last_step2.mukey
     GROUP BY #last_step2.areasymbol, #last_step2.musym, #last_step2.muname, #last_step2.mukey, #last_step2.om_r
     ORDER BY #last_step2.areasymbol, #last_step2.musym, #last_step2.muname, #last_step2.mukey, #last_step2.om_r"""

    #arcpy.AddMessage(omQry)

    omLogic, omMsg, omRes = tabRequest(omQry, "Organic Matter")

    if omLogic:

        #name the table to create and return path
        tbl = "om"
        jTbl = os.path.join(gdb, tbl)

        #populate the table
        if "Table" in omRes:

            # get its value
            resLst = omRes["Table"]  # Data as a list of lists. All values come back as string.

            #number 2 accounts for colnames, colinfo

            if len(resLst) - 2 == iCnt:

                arcpy.AddMessage('\t' + omMsg)

                columnNames = resLst.pop(0)
                columnInfo = resLst.pop(0)

                newTable = CreateNewTable(jTbl, columnNames, columnInfo)

                with arcpy.da.InsertCursor(newTable, columnNames) as cursor:

                    for row in resLst:
                        cursor.insertRow(row)

                arcpy.conversion.TableToTable(newTable, os.path.join(inDir, gdb), tbl)

                return True, jTbl

            else:
                arcpy.AddWarning('\t' + omMsg + " but recieved no records or does not match raster count")
                if not ws[3:] in wLst:
                    wLst.append(ws[3:])
                return False, None
        else:
            arcpy.AddWarning('\tNo OM table returned for ' + ws[3:])
            if not ws[3:] in wLst:
                wLst.append(ws[3:])
            return False, None

    else:
        arcpy.AddWarning('\t' + omMsg + " : " + ws[3:])
        if not ws[3:] in wLst:
            wLst.append(ws[3:])
        return False, None


def coarseFrag(keys):

    coarseFragQry = """SELECT areasymbol, areaname, mapunit.mukey, musym, nationalmusym, muname, mukind
        INTO #main
        FROM legend
        INNER JOIN mapunit on mapunit.lkey=legend.lkey  --AND mapunit.mukey= 753505
        AND mapunit.mukey IN (""" + ",".join(map("'{0}'".format, keys)) +""")

        ---Gets only the dominant component
        SELECT
        #main.mukey,
        muname,
        cokey,
        compname,
        comppct_r
        INTO #acpf
        FROM #main
        INNER JOIN component ON component.mukey=#main.mukey
        AND component.cokey =
        (SELECT TOP 1 c1.cokey FROM component AS c1
        INNER JOIN mapunit AS m ON c1.mukey=m.mukey AND c1.mukey=#main.mukey ORDER BY c1.comppct_r DESC, CASE WHEN LEFT (muname,2)= LEFT (compname,2) THEN 1 ELSE 2 END ASC, c1.cokey)

        --Gets only the horizons that intersect 50 AND 150
        SELECT #acpf.mukey,
        muname,
        #acpf.cokey,
        #acpf.compname,
        #acpf.comppct_r, hzname, chkey, hzdept_r,  hzdepb_r,

        CASE WHEN frag3to10_r IS NULL THEN 0
        WHEN frag3to10_r = '' THEN 0
        ELSE frag3to10_r END AS frag3to10_r,

        CASE WHEN fraggt10_r IS NULL THEN 0
        WHEN fraggt10_r = '' THEN 0
        ELSE fraggt10_r END AS fraggt10_r,

        CASE WHEN sieveno10_r IS NULL THEN 0
        WHEN sieveno10_r  = '' THEN 0
         ELSE sieveno10_r END AS sieveno10_r,


        CASE WHEN sandtotal_r IS NULL THEN 0
        WHEN sandtotal_r  = '' THEN 0
         ELSE sandtotal_r END AS sandtotal_r,

        CASE    WHEN hzdepb_r < 50 THEN 0
        WHEN hzdept_r >150 THEN 0
        WHEN hzdepb_r >= 50 AND hzdept_r < 50 THEN 50
        WHEN hzdept_r < 50 THEN 0
        		WHEN hzdept_r < 150 then hzdept_r ELSE 50 END AS InRangeTop_50_100 ,


        CASE   WHEN hzdept_r > 150 THEN 0
        WHEN hzdepb_r < 50 THEN 0
        WHEN hzdepb_r <= 150 THEN hzdepb_r  WHEN hzdepb_r > 150 and hzdept_r < 150 THEN 150 ELSE 50 END AS InRangeBot_50_100
        INTO #acpf2
        FROM #acpf
        INNER JOIN chorizon ON chorizon.cokey=#acpf.cokey
        AND CASE    WHEN hzdepb_r < 50 THEN 0
        WHEN hzdept_r >150 THEN 0
        WHEN hzdepb_r >= 50 AND hzdept_r < 50 THEN 50
        WHEN hzdept_r < 50 THEN 0
        		WHEN hzdept_r < 150 then hzdept_r ELSE 50 END  >= 50 AND

        CASE   WHEN hzdept_r > 150 THEN 0
        WHEN hzdepb_r < 50 THEN 0
        WHEN hzdepb_r <= 150 THEN hzdepb_r  WHEN hzdepb_r > 150 and hzdept_r < 150 THEN 150 ELSE 50 END  <=150
        ORDER BY
        muname,
        mukey,
        comppct_r DESC, compname, cokey, hzdept_r ASC, hzdepb_r ASC, chkey

        --------------------------------------
        SELECT mukey,
        muname,
        cokey,
        compname,
        comppct_r, hzname, chkey, hzdept_r,  hzdepb_r,
        CASE
        WHEN frag3to10_r IS NULL AND fraggt10_r IS NULL AND sandtotal_r  IS NULL THEN 0
        WHEN frag3to10_r = 0  AND fraggt10_r = 0  AND sandtotal_r  = 0  THEN 0 ELSE
        ROUND((frag3to10_r + fraggt10_r) +
        	                        ((100 - (frag3to10_r + fraggt10_r)) - sieveno10_r +
        	                        (sieveno10_r * (sandtotal_r * 0.01)) * ((100 - (frag3to10_r + fraggt10_r)) * 0.01)),2) END  AS Initial_totCoarse
        INTO #acpf3
        FROM #acpf2
        ORDER BY
        muname,
        mukey,
        comppct_r DESC, compname, cokey, hzdept_r ASC, hzdepb_r ASC, chkey

        ------------------------------------------------
        SELECT DISTINCT  muname,
        mukey, MAX(Initial_totCoarse) over(PARTITION BY compname) as Initial_totCoarse2
        INTO #last_step
        FROM #acpf3

        SELECT DISTINCT  muname,
        mukey, ISNULL (Initial_totCoarse2, 0) AS totCoarse
        INTO #last_step2
        FROM #last_step

        SELECT #main.mukey,
        #main.muname,
        totCoarse AS Coarse50_150
        FROM #last_step2
        RIGHT OUTER JOIN #main ON #main.mukey=#last_step2.mukey
        ORDER BY #main.muname ASC, #main.mukey, totCoarse"""

    #arcpy.AddMessage(coarseFragQry)

    coarseFLogic , coarseFMsg, coarseFRes = tabRequest(coarseFragQry, "Coarse Fragments")

    if coarseFLogic:

        #name the table to create
        tbl = "coarse_frag"
        jTbl = os.path.join(gdb, tbl)

        #populate the table
        if "Table" in coarseFRes:

            # get its value
            resLst = coarseFRes["Table"]  # Data as a list of lists. All values come back as string.

            #number 2 accounts for colnames, colinfo
            if len(resLst) - 2 == iCnt:

                arcpy.AddMessage('\t' + coarseFMsg)

                columnNames = resLst.pop(0)
                columnInfo = resLst.pop(0)

                newTable = CreateNewTable(jTbl, columnNames, columnInfo)

                with arcpy.da.InsertCursor(newTable, columnNames) as cursor:

                    for row in resLst:
                        cursor.insertRow(row)

                arcpy.conversion.TableToTable(newTable, os.path.join(inDir, gdb), tbl)

                return True, jTbl

            else:
                arcpy.AddWarning('\t' + coarseFMsg+ " but recieved no records or does not match raster count")
                if not ws[3:] in wLst:
                    wLst.append(ws[3:])
                return False, None
        else:
            arcpy.AddWarning('\tNo coarse frag table returned for ' + ws[3:])
            if not ws[3:] in wLst:
                wLst.append(ws[3:])
            return False, None
    else:
        arcpy.AddWarning('\t' + coarseFMsg + " : " + ws[3:])
        if not ws[3:] in wLst:
            wLst.append(ws[3:])
        return False, None



def aws(keys):

    awsQry = """SELECT areASymbol, areaname, mapunit.mukey, mapunit.musym, nationalmusym, mapunit.muname, mukind, muacres, aws0150wta
    INTO #main
    FROM legend
    INNER JOIN mapunit on legend.lkey=mapunit.lkey
    INNER JOIN muaggatt AS mt1 on mapunit.mukey=mt1.mukey
    AND mapunit.mukey IN ("""  + ",".join(map("'{0}'".format, keys)) +""")


    SELECT
    -- grab survey area data
    LEFT((areasymbol), 2) AS state,
     l.areASymbol,
     l.areaname,
    (SELECT SUM (DISTINCT comppct_r) FROM mapunit  AS mui3  INNER JOIN component AS cint3 ON cint3.mukey=mui3.mukey INNER JOIN chorizon AS chint3 ON chint3.cokey=cint3.cokey AND cint3.cokey = c.cokey GROUP BY chint3.cokey) AS sum_comp,
    --grab map unit level information

     mu.mukey,
     mu.musym,

    --grab component level information

     c.majcompflag,
     c.comppct_r,
     c.compname,
     compkind,
     localphase,
     slope_l,
     slope_r,
     slope_h,
    (SELECT CAST(MIN(resdept_r) AS INTEGER) FROM component LEFT OUTER JOIN corestrictions ON component.cokey = corestrictions.cokey WHERE component.cokey = c.cokey AND reskind  IS NOT NULL) AS restrictiondepth,
    (SELECT CASE WHEN MIN (resdept_r) IS NULL THEN 200 ELSE CAST (MIN (resdept_r) AS INT) END FROM component LEFT OUTER JOIN corestrictions ON component.cokey = corestrictions.cokey WHERE component.cokey = c.cokey AND reskind IS NOT NULL) AS restrictiodepth,
    (SELECT TOP 1  reskind  FROM component LEFT OUTER JOIN corestrictions ON component.cokey = corestrictions.cokey WHERE component.cokey = c.cokey AND corestrictions.reskind IN ('bedrock, lithic', 'duripan', 'bedrock, densic', 'bedrock, paralithic', 'fragipan', 'natric', 'ortstein', 'permafrost', 'petrocalcic', 'petrogypsic')

    AND reskind IS NOT NULL ORDER BY resdept_r) AS TOPrestriction, c.cokey,

    ---begin selection of horizon properties
     hzname,
     hzdept_r,
     hzdepb_r,
     CASE WHEN (hzdepb_r-hzdept_r) IS NULL THEN 0 ELSE CAST((hzdepb_r-hzdept_r) AS INT) END AS thickness,
    --  thickness in inches
      awc_r,
      aws025wta,
      aws050wta,
      aws0100wta,
      aws0150wta,
      brockdepmin,
      texture,
      ch.chkey
    INTO #acpf
    FROM legend  AS l
    INNER JOIN mapunit AS mu ON mu.lkey = l.lkey
    AND mu.mukey IN (""" + ",".join(map("'{0}'".format, keys)) +""")
    INNER JOIN muaggatt mt on mu.mukey=mt.mukey
    INNER JOIN component c ON c.mukey = mu.mukey
    INNER JOIN chorizon ch ON ch.cokey = c.cokey and CASE WHEN hzdept_r IS NULL THEN 2
    WHEN awc_r IS NULL THEN 2
    WHEN awc_r = 0 THEN 2 ELSE 1 END = 1
    INNER JOIN chtexturegrp ct ON ch.chkey=ct.chkey and ct.rvindicator = 'yes'
    ORDER by l.areasymbol, mu.musym, hzdept_r

    ---Sums the Component Percent and eliminate duplicate values by cokey
    SELECT mukey, cokey,  SUM (DISTINCT sum_comp) AS sum_comp2
    INTO #muacpf
    FROM #acpf AS acpf2
    WHERE acpf2.cokey=cokey
    GROUP BY mukey, cokey

    ---Sums the component percent in a map unit
    SELECT mukey, cokey, sum_comp2,  SUM (sum_comp2) over(partition by #muacpf.mukey ) AS sum_comp3 --, SUM (sum_comp2) AS sum_comp3
    INTO #muacpf2
    FROM #muacpf
    GROUP BY mukey, cokey, sum_comp2

    ---Gets the Weighted component percent. Example from "Column F" up top
    SELECT mukey, cokey,  CASE WHEN sum_comp2 = sum_comp3 THEN 1
    ELSE CAST (CAST (sum_comp2 AS  decimal (5,2)) / CAST (sum_comp3 AS decimal (5,2)) AS decimal (5,2)) END AS WEIGHTED_COMP_PCT
    INTO #muacpf3
    FROM #muacpf2


    ---grab top depth for the mineral soil and will use it later to get mineral surface properties

    SELECT compname, cokey, MIN(hzdept_r) AS min_t
    INTO #hortopdepth
    FROM #acpf
    WHERE texture NOT LIKE '%PM%' and texture NOT LIKE '%DOM' and texture NOT LIKE '%MPT%' AND texture NOT LIKE '%MUCK' AND texture NOT LIKE '%PEAT%'
    GROUP BY compname, cokey

    ---combine the mineral surface to grab surface mineral properties

    SELECT #hortopdepth.cokey,
    hzname,
    hzdept_r,
    hzdepb_r,
    thickness,
    texture AS texture_surf,
    awc_r AS awc_surf,
    chkey
    INTO #acpf2
    FROM #hortopdepth
    INNER JOIN #acpf on #hortopdepth.cokey=#acpf.cokey AND #hortopdepth.min_t = #acpf.hzdept_r
    ORDER BY #hortopdepth.cokey, hzname




    SELECT
    mukey,
    cokey,
    hzname,
    restrictiodepth,
    hzdept_r,
    hzdepb_r,
    CASE WHEN (hzdepb_r-hzdept_r) IS NULL THEN 0 ELSE CAST ((hzdepb_r-hzdept_r) AS INT) END AS thickness,
    texture,
    CASE when awc_r IS NULL THEN 0 ELSE awc_r END AS awc_r,
    chkey
    INTO #acpfhzn
    FROM #acpf


    --- depth ranges for AWS ----
    SELECT
    CASE  WHEN hzdepb_r <= 150 THEN hzdepb_r WHEN hzdepb_r > 150 and hzdept_r < 150 THEN 150 ELSE 0 END AS InRangeBot,
    CASE  WHEN hzdept_r < 150 then hzdept_r ELSE 0 END AS InRangeTop,

    CASE  WHEN hzdepb_r <= 20  THEN hzdepb_r WHEN hzdepb_r > 20  and hzdept_r < 20 THEN 20  ELSE 0 END AS InRangeBot_0_20,
    CASE  WHEN hzdept_r < 20 then hzdept_r ELSE 0 END AS InRangeTop_0_20,


    CASE  WHEN hzdepb_r <= 50  THEN hzdepb_r WHEN hzdepb_r > 50  and hzdept_r < 50 THEN 50  ELSE 20 END AS InRangeBot_20_50,
    CASE  WHEN hzdept_r < 50 then hzdept_r ELSE 20 END AS InRangeTop_20_50,

    CASE  WHEN hzdepb_r <= 100  THEN hzdepb_r WHEN hzdepb_r > 100  and hzdept_r < 100 THEN 100  ELSE 50 END AS InRangeBot_50_100,
    CASE  WHEN hzdept_r < 100 then hzdept_r ELSE 50 END AS InRangeTop_50_100,
    awc_r, cokey, mukey
    INTO #aws
    FROM #acpf
    ORDER BY cokey

    SELECT mukey, cokey,
    SUM((InRangeBot - InRangeTop)*awc_r) AS aws150,

    SUM((InRangeBot_0_20 - InRangeTop_0_20)*awc_r) AS aws_0_20,

    SUM((InRangeBot_20_50 - InRangeTop_20_50)*awc_r) AS aws_20_50,

    SUM((InRangeBot_50_100 - InRangeTop_50_100)*awc_r) AS aws_50_100
    INTO #aws150
    FROM #aws
    GROUP BY  mukey, cokey

    ---return to weighted averages, using the thickness times the non-null horizon properties
    SELECT mukey, cokey, chkey,
     thickness,
     restrictiodepth,
    (awc_r*thickness) as th_awc_r
    INTO #acpf3
    FROM #acpfhzn
    ORDER BY mukey, cokey, chkey


    ---sum all horizon properties to gather the final product for the component

    SELECT mukey, cokey, restrictiodepth,
    CAST(sum(thickness) AS float(2)) AS sum_thickness,
    CAST(sum(th_awc_r) AS float(2)) AS sum_awc_r
    INTO #acpf4
    FROM #acpf3
    GROUP BY mukey, cokey, restrictiodepth
    ORDER BY mukey

    ---find the depth to use in the weighted average calculation

    SELECT mukey, cokey, CASE WHEN sum_thickness < restrictiodepth then sum_thickness  else restrictiodepth end AS restrictiondepth
    INTO #depthtest
    FROM #acpf4



    ---sql to create weighted average by dividing by the restriction depth found in the above query

    SELECT #acpf4.mukey, #acpf4.cokey,
     sum_thickness,
     #depthtest.restrictiondepth,
    (sum_awc_r) AS profile_Waterstorage,
    (sum_awc_r/#depthtest.restrictiondepth)  AS wtavg_awc_r_to_restrict
    INTO #acpfwtavg
    FROM #acpf4
    INNER JOIN #depthtest on #acpf4.cokey=#depthtest.cokey
    ---WHERE sum_awc_r != 0
    ORDER by #acpf4.mukey, #acpf4.cokey


    --time to put it all together using a lot of CASTs to change the data to reflect the way I want it to appear

    SELECT DISTINCT
      #acpf.state,
      #acpf.areasymbol,
      #acpf.areaname,
      #acpf.musym,
      #acpf.mukey,
      #acpf.cokey,
      majcompflag,
      comppct_r,
      #acpf.compname,
      compkind,
      localphase,
      slope_l,
      slope_r,
      slope_h,
      CAST(aws150 AS Decimal(5,1)) AS aws150_dcp,
    	CAST(aws_0_20 AS Decimal(5,1)) AS aws_0_20_dcp,
    		CAST(aws_20_50 AS Decimal(5,1)) AS aws_20_50_dcp,
    			CAST(aws_50_100 AS Decimal(5,1)) AS aws_50_100_dcp,


      CAST(profile_Waterstorage AS Decimal(5,1)) AS AWS_profile_dcp,
      CAST(wtavg_awc_r_to_restrict AS Decimal(5,1)) AS AWS_restrict_dcp,
      sum_thickness,
      CAST(#acpfwtavg.restrictiondepth/2.54 AS int)  restrictiondepth_IN,
      #acpfwtavg.restrictiondepth,
      TOPrestriction,
      #acpf2.chkey,
      #acpf2.hzname,
    CAST(#acpf2.hzdept_r/2.54 AS int)  AS hzdept_r,
    CAST(#acpf2.hzdepb_r/2.54 AS int) AS hzdeb_r
    INTO #alldata
    FROM #acpf2
    INNER JOIN #acpf on #acpf.cokey = #acpf2.cokey
    LEFT OUTER JOIN #aws150 on #acpf.cokey = #aws150.cokey
    LEFT OUTER JOIN #acpfwtavg on #acpf.cokey = #acpfwtavg.cokey
    ORDER BY #acpf.state, #acpf.areasymbol, #acpf.areaname, #acpf.musym

    ---Uses the above query and the query on line 89
    SELECT  #alldata.mukey,  #alldata.cokey, #alldata.aws150_dcp, WEIGHTED_COMP_PCT ,
    CAST (CASE WHEN #alldata.aws150_dcp IS NULL THEN 0 ELSE #alldata.aws150_dcp END * CASE WHEN #alldata.aws150_dcp IS NULL THEN 0 ELSE WEIGHTED_COMP_PCT   END AS Decimal(5,2)) AS AWC_COMP_SUM,
    CAST (CASE WHEN #alldata.aws_0_20_dcp IS NULL THEN 0 ELSE #alldata.aws_0_20_dcp END * CASE WHEN #alldata.aws_0_20_dcp IS NULL THEN 0 ELSE WEIGHTED_COMP_PCT   END AS Decimal(5,2)) AS AWC_COMP_SUM_0_20,
    CAST (CASE WHEN #alldata.aws_20_50_dcp IS NULL THEN 0 ELSE #alldata.aws_20_50_dcp END * CASE WHEN #alldata.aws_20_50_dcp IS NULL THEN 0 ELSE WEIGHTED_COMP_PCT   END AS Decimal(5,2)) AS AWC_COMP_SUM_20_50,
    CAST (CASE WHEN #alldata.aws_50_100_dcp IS NULL THEN 0 ELSE #alldata.aws_50_100_dcp END * CASE WHEN #alldata.aws_50_100_dcp IS NULL THEN 0 ELSE WEIGHTED_COMP_PCT   END AS Decimal(5,2)) AS AWC_COMP_SUM_50_100

    INTO #alldata2
    FROM #alldata
    INNER JOIN #muacpf3 ON #alldata.cokey=#muacpf3.cokey


    SELECT #alldata2.mukey , CAST (SUM (AWC_COMP_SUM) over(partition by #alldata2.mukey )AS Decimal(5,2)) AS MU_AWC_WEIGHTED_AVG0_150,
    CAST (SUM (AWC_COMP_SUM_0_20) over(partition by #alldata2.mukey )AS Decimal(5,2)) AS MU_AWC_WEIGHTED_AVG_0_20,
    CAST (SUM (AWC_COMP_SUM_20_50) over(partition by #alldata2.mukey )AS Decimal(5,2)) AS MU_AWC_WEIGHTED_AVG_20_50,
    CAST (SUM (AWC_COMP_SUM_50_100) over(partition by #alldata2.mukey )AS Decimal(5,2)) AS MU_AWC_WEIGHTED_AVG_50_100
    INTO #alldata3
    FROM #alldata2


    SELECT
    ---state,
    --- #main.areasymbol,
    --- #main.areaname,
     #main.mukey,
    --- #main.musym,
    --- muname,
    --- nationalmusym,
    --- mukind,
    ---MU_AWC_WEIGHTED_AVG0_150,
    ---aws0150wta AS MuAGG_aws0150wta,
    MU_AWC_WEIGHTED_AVG_0_20 AS aws0_20,
    MU_AWC_WEIGHTED_AVG_20_50 AS aws20_50,
    MU_AWC_WEIGHTED_AVG_50_100 AS aws50_100

    FROM #main
    LEFT OUTER JOIN #alldata on #main.mukey=#alldata.mukey
    LEFT OUTER JOIN #alldata3 on #main.mukey=#alldata3.mukey
    GROUP BY
    ---state, #main.areasymbol,  #main.areaname,
    #main.mukey,
    ---muname, #main.musym,  nationalmusym,  mukind, MU_AWC_WEIGHTED_AVG0_150, aws0150wta,
    MU_AWC_WEIGHTED_AVG_0_20, MU_AWC_WEIGHTED_AVG_20_50, MU_AWC_WEIGHTED_AVG_50_100
    ---ORDER BY areasymbol, musym"""


    awsLogic, awsMsg, awsRes = tabRequest(awsQry, "AWS")

    if awsLogic:

        #name the table to create and return path
        tbl = "aws"
        jTbl = os.path.join(gdb, tbl)

        #populate the table
        if "Table" in awsRes:

            # get its value
            resLst = awsRes["Table"]  # Data as a list of lists. All values come back as string.

            #number 2 accounts for colname, colinfo
            if len(resLst) - 2 == iCnt:

                arcpy.AddMessage('\t' + awsMsg)

                columnNames = resLst.pop(0)
                columnInfo = resLst.pop(0)

                newTable = CreateNewTable(jTbl, columnNames, columnInfo)

                with arcpy.da.InsertCursor(newTable, columnNames) as cursor:

                    for row in resLst:
                        cursor.insertRow(row)

                arcpy.conversion.TableToTable(newTable, os.path.join(inDir, gdb), tbl)

                return True, jTbl

            else:
                arcpy.AddWarning('\t' + awsMsg + " but recieved no records or does not match raster count")
                if not ws[3:] in wLst:
                    wLst.append(ws[3:])
                return False, None

        else:
            arcpy.AddWarning('\tNo AWS table returned for ' + ws[3:])
            if not ws[3:] in wLst:
                wLst.append(ws[3:])
            return False, None

    else:
        arcpy.AddWarning('\t' + awsMsg + " : " + ws[3:])
        if not ws[3:] in wLst:
            wLst.append(ws[3:])
        return False, None


def buildACPF(dataTbl, acpfTbl):

    #the SDA queries often return columns we don't want
    jFlds = [x.name for x in arcpy.Describe(dataTbl).fields if not x.name in ["OBJECTID", "MUKEY", "mukey", "areasymbol", "muname", "musym", "MUSYM", "MUNAME", "hydric_rating"]]

    arcpy.management.JoinField(acpfTbl, "MUKEY", dataTbl, "MUKEY", jFlds)

def soilProfileTbl(keys):

    #had to make this a function bc the other functions in main
    #put the gdb in transaction mode.  i could have called  arcpy.edit but am not

    #make the soil profile table
    path = os.path.join(inDir, gdb)
    arcpy.management.CreateTable(path, profTbl)
    arcpy.management.AddField(os.path.join(path,profTbl), "MUKEY", "TEXT", None, None, "30")


    cursor = arcpy.da.InsertCursor(path + os.sep + profTbl, "MUKEY")

    for key in keys:
        cVal = [key]
        cursor.insertRow(cVal)

def CreateNewTable(newTable, columnNames, columnInfo):
    # Create new table. Start with in-memory and then export to geodatabase table
    #
    # ColumnNames and columnInfo come from the Attribute query JSON string
    # MUKEY would normally be included in the list, but it should already exist in the output featureclass
    #
    try:
        # Dictionary: SQL Server to FGDB
        dType = dict()

        dType["int"] = "long"
        dType["smallint"] = "short"
        dType["bit"] = "short"
        dType["varbinary"] = "blob"
        dType["nvarchar"] = "text"
        dType["varchar"] = "text"
        dType["char"] = "text"
        dType["datetime"] = "date"
        dType["datetime2"] = "date"
        dType["smalldatetime"] = "date"
        dType["decimal"] = "double"
        dType["numeric"] = "double"
        dType["float"] ="double"

        # numeric type conversion depends upon the precision and scale
        dType["numeric"] = "float"  # 4 bytes
        dType["real"] = "double" # 8 bytes

        # Iterate through list of field names and add them to the output table
        i = 0

        # ColumnInfo contains:
        # ColumnOrdinal, ColumnSize, NumericPrecision, NumericScale, ProviderType, IsLong, ProviderSpecificDataType, DataTypeName
        #PrintMsg(" \nFieldName, Length, Precision, Scale, Type", 1)

        joinFields = list()
        outputTbl = os.path.join("IN_MEMORY", os.path.basename(newTable))
        arcpy.CreateTable_management(os.path.dirname(outputTbl), os.path.basename(outputTbl))

        for i, fldName in enumerate(columnNames):
            vals = columnInfo[i].split(",")
            length = int(vals[1].split("=")[1])
            precision = int(vals[2].split("=")[1])
            scale = int(vals[3].split("=")[1])
            dataType = dType[vals[4].lower().split("=")[1]]

            if fldName.lower().endswith("key"):
                # Per SSURGO standards, key fields should be string. They come from Soil Data Access as long integer.
                dataType = 'text'
                length = 30

            arcpy.AddField_management(outputTbl, fldName, dataType, precision, scale, length)


        return outputTbl

    except:
        errorMsg()
        return False

#===============================================================================

import sys, os, json, socket, arcpy, urllib2, traceback, datetime
from urllib2 import HTTPError, URLError
from arcpy import env

env.overwriteOutput = True
day = str(datetime.date.today()).replace("-", "_")

inDir = arcpy.GetParameterAsText(0)
pGDBs = arcpy.GetParameterAsText(1)
dBool = arcpy.GetParameterAsText(2)
wgs = arcpy.SpatialReference(4326)

wLst = list()
usrGDBs = pGDBs.split(";")


#separate tool messages from stock msgs
arcpy.AddMessage('\n\n')

try:

    for gdb in usrGDBs:
        env.workspace = os.path.join(inDir, gdb)
        bufL = arcpy.ListFeatureClasses("buf*", "Polygon")
        if len(bufL) == 1:

            ws = bufL[0]
            arcpy.AddMessage('Processing watershed buffer ' + ws[3:])

            try:

                snapR = arcpy.ListRasters("ws*", None)[-1]
                env.snapRaster = snapR
                arcpy.AddMessage("Snap Raster = " + env.snapRaster)

            except:

                arcpy.AddWarning("No snap raster available for "  + ws[3:])



            profTbl = 'SoilProfile' + ws[3:]

            wsSR = arcpy.Describe(ws).spatialReference
            #wsPrjName = wsSR.PCSName

            validDatums = ["D_WGS_1984", "D_North_American_1983"]

            if not wsSR.GCS.datumName in validDatums:
                raise MyError , "AOI coordinate system not supported: " + wsSR.name + ", " + wsSR.GCS.datumName

            if wsSR.GCS.datumName == "D_WGS_1984":
                tm = ""  # no datum transformation required

            elif wsSR.GCS.datumName == "D_North_American_1983":
                tm = "WGS_1984_(ITRF00)_To_NAD_1983"

            else:
                raise MyError, "AOI CS datum name: " + wsSR.GCS.datumName


            #outRaster = name for output SSURGO raster w/ input watershed coor system
            outRaster = "gSSURGO_" + day

            #sdaWGS = WGS84 features from SDA
            sdaWGS = "sda_conhull_ACPF_Shape"

            #prjFeats = WGS84 features from SDA projected back native watershed UTM coor. system
            prjFeats = env.workspace + os.sep + "sda_ch_ACPF_SSURGO"

            #finalClip = the final projected, native coor sys, clipped ssurgo features
            finalClip = env.workspace + os.sep + "final_ssurgo_" + ws

            #set spatial reference code for WGS84
            sdaSR = arcpy.SpatialReference(4326)

            # get generalized coordinates
            hullLogic, theHull = getHull(ws)

            if hullLogic:

                #feed generalized coordinates to SDA, WGS84 polys are built
                grLogic, grVal = geoRequest(theHull)

                if grLogic:

                    #project the features returned from SDA to input watershed
                    if tm != "":
                        arcpy.AddMessage("\tReprojecting SDA features to match " + os.path.basename(gdb)[:-4] + " " + wsSR.PCSName + ":" + wsSR.GCS.name)
                        arcpy.management.Project(sdaWGS, prjFeats, wsSR, tm)
                        #clip the projeted, sda features to input watesrshed
                        arcpy.analysis.Clip(prjFeats, ws, finalClip)

                    else:
                        arcpy.AddMessage("\tReprojecting SDA features to match " + os.path.basename(gdb)[:-4] + " " + wsSR.PCSName + ":" + wsSR.GCS.name)
                        #project the features returned from SDA to input watershed, no transformation needed
                        arcpy.management.Project(sdaWGS, prjFeats, wsSR)
                        #clip the projeted, sda features to input watesrshed
                        arcpy.analysis.Clip(prjFeats, ws, finalClip)


                    #converted the projected, clipped ssurgo features to a raster
                    arcpy.conversion.PolygonToRaster(finalClip, "mukey", outRaster, "MAXIMUM_COMBINED_AREA", None, "10")

                    #add a text, mukey field
                    arcpy.management.AddField(outRaster, "mukey", "TEXT", None, None, "30")

                    #populate the field (insertcursors are usually faster)
                    arcpy.management.CalculateField(outRaster, "mukey", "!VALUE!", "PYTHON_9.3")

                    #get list of mukeys from raster (not convex hull returned from geoRequest and
                    #not from clipped polys, very small polygons on border might not get converted)
                    keys = list()
                    with arcpy.da.SearchCursor(outRaster, "mukey") as rows:
                        for row in rows:
                            val = str(row[0])
                            if not val in keys:
                                keys.append(val)

                    keys.sort()




                    #get count of records in raster to ensure same number of records
                    #are returned from SDA queries
                    #cnt = arcpy.management.GetCount(outRaster)
                    #iCnt = int(cnt.getOutput(0))

                    #no need to run getCount anymore...
                    iCnt = len(keys)


                    surfHoriz(keys)
                    surfTex(keys)

                    #these queries populate the gSSURGO vat, in order
                    #if the logical is False on these, the return message comes from w/ in the function
                    muAggtLogic, tbl = muaggat(keys)
                    if muAggtLogic:
                        dataTbl = os.path.join(inDir,tbl)
                        buildACPF(dataTbl, outRaster)
                        del dataTbl, tbl

                    rootZnDepLogic, tbl = rootZnDep(keys)
                    if rootZnDepLogic:
                        dataTbl = os.path.join(inDir,tbl)
                        buildACPF(dataTbl, outRaster)
                        del dataTbl, tbl


                    rootZnAwsDrtLogic, tbl = rootZnAwsDrt(keys)
                    if rootZnAwsDrtLogic:
                        dataTbl = os.path.join(inDir,tbl)
                        buildACPF(dataTbl, outRaster)
                        del dataTbl, tbl

                    potWetLogic, tbl = potWet(keys)
                    if potWetLogic:
                        dataTbl = os.path.join(inDir,tbl)
                        buildACPF(dataTbl, outRaster)
                        del dataTbl, tbl

                    #build soil profile table
                    soilProfileTbl(keys)


                    #these queries populate the soil profile table, in order
                    #if the logical is False on these, the return message comes from w/ in the function
                    awsLogic, tbl = aws(keys)
                    if awsLogic:
                        dataTbl = os.path.join(inDir,tbl)
                        buildACPF(dataTbl, os.path.join(inDir, gdb, profTbl))
                        del dataTbl, tbl

                    socLogic, tbl = soc(keys)
                    if socLogic:
                        dataTbl = os.path.join(inDir,tbl)
                        buildACPF(dataTbl, os.path.join(inDir, gdb, profTbl))
                        del dataTbl, tbl

                    omLogic, tbl = om(keys)
                    if omLogic:
                        dataTbl = os.path.join(inDir,tbl)
                        buildACPF(dataTbl, os.path.join(inDir, gdb, profTbl))
                        del dataTbl, tbl

                    kSatLogic, tbl = ksat50150(keys)
                    if kSatLogic:
                        dataTbl = os.path.join(inDir,tbl)
                        buildACPF(dataTbl, os.path.join(inDir, gdb, profTbl))
                        del dataTbl, tbl

                    coarseLogic, tbl = coarseFrag(keys)
                    if coarseLogic:
                        dataTbl = os.path.join(inDir,tbl)
                        buildACPF(dataTbl, os.path.join(inDir, gdb, profTbl))
                        del dataTbl, tbl


                    #separate jobs for legibility
                    arcpy.AddMessage('\n')

                    del keys

                    #delete the queries & polygons??
                    if dBool == "true":
                        arcpy.management.Delete(sdaWGS)
                        arcpy.management.Delete(prjFeats)
                        arcpy.management.Delete(finalClip)

                        dTbls = ['muaggat', 'rtZnDep', 'rtZnAwsDrt', 'potwet', 'SoilProfile', 'aws', 'soc', 'om', 'KSat50_150', 'coarse_frag']

                        for tbl in arcpy.ListTables():
                            if tbl in dTbls:
                                arcpy.management.Delete(tbl)

                else:

                    arcpy.AddWarning(grVal)
                    wLst.append(ws[3:])

            else:

                arcpy.AddWarning(theHull)
                wLst.append(ws[3:])

        else:

            arcpy.AddWarning('\nUnable to resolve buffered watershed in ' + os.path.basename(gdb)[:-4] + '. None found or ambiguity in feature class names\n')

    if len(wLst)<>0:
        arcpy.AddWarning('The following watershed(s) did not execute properly:')
        for w in wLst:
            arcpy.AddWarning(w)

    #separate tool messages from stock msgs
    arcpy.AddMessage('\n\n')

except:
    errorMsg()







