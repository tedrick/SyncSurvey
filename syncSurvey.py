#-------------------------------------------------------------------------------
# Name:        SyncSurvey
# Purpose:     Initiate
#
# Author:      jame6423
#
# Created:     15/08/2016
# Copyright:   (c) jame6423 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os, tempfile, shutil
import json, re
import uuid
import datetime, time, pytz
import urllib, urllib2
import sys

import arcpy

def getToken(username, password=None, portal_URL = 'https://www.arcgis.com'):
    '''Gets a token from ArcGIS Online/Portal with the given username/password'''
    arcpy.AddMessage('\t-Getting Token')
    if password == None:
        password = getpass.getpass()
    parameters = urllib.urlencode({
        'username': username,
        'password': password,
        'client': 'referer',
        'referer': portal_URL,
        'expiration': 60,
        'f': 'json'
    })
    tokenURL = '{0}/sharing/rest/generateToken?'.format(portal_URL)
    response = urllib.urlopen(tokenURL, parameters).read()
    token = json.loads(response)['token']
    return token

def getServiceDefinition(token, survey_URL):
    '''Gets the JSON representation of the service definition.'''
    response = urllib.urlopen("{0}?f=json&token={1}".format(survey_URL, token)).read()
    serviceInfo = json.loads(response)
    return serviceInfo

def getUTCTimestamp(timezone):
    '''Returns a UTC timestamp'''
    now = datetime.datetime.now()
    timeZone = pytz.timezone(timezone)
    localNow = timeZone.localize(now)
    utcNow = localNow.astimezone(pytz.utc)
    return utcNow

def createTimestampText(datetimeObj):
    '''Format a datetime for insertion using Calculate Fields'''
    outText = ""
    timeStringFormat = "%Y-%m-%d %H:%M:%S"
    outText = datetimeObj.strftime(timeStringFormat)
    return outText

def getSurveyTables(workspace, prefix=''):
    '''Return a list of the tables participating in the survey'''
    originalWorkspace = arcpy.env.workspace
    arcpy.env.workspace = workspace
    #This is used in 2 contexts:
    #Downloaded GDB - tables have no prefix
    #Enterprise GDB - prefix is added to table name
    #The full table name (i.e. GDB.SCHEMA.NAME) is returned, so prefix is in the middle
    wildcard = '*{0}*'.format(prefix) if prefix != '' else '*'
    #List the Feature Classes & Tables
    #Tables also returns Attachment tables
    featureClasses = arcpy.ListFeatureClasses(wildcard)
    tables = arcpy.ListTables(wildcard)

    #Loop through the tables, checking for:
    #1) Is this an attachment table?
    #2) Does the prefix actually match the prefix term exactly?
    allTables = featureClasses
    allTables.extend(tables)
    outTables = []
    for t in allTables:
        tableName = t.split('.')[-1]
        nameParts = tableName.split('_')
        if '__ATTACH' not in t:
            if nameParts[0] == prefix or prefix == '':
                outTables.append(t)
    arcpy.env.workspace = originalWorkspace
    return outTables

def getLastSynchronizationTime(workspace, tableList):
    '''Looks at the existing records in the SDE and returns the latest synchronization time'''
    arcpy.AddMessage('\t-Checking Last Sychronization')
    arcpy.env.workspace = workspace
    statTables = []
    #Dummy value to compare time
    lastSync = datetime.datetime.fromtimestamp(0)
    for table in tableList:
        #Skip if empty table (i.e., no rows)
        arcpy.AddMessage('\t\t-Checking sync on {0}'.format(table))
        #Just use the last part of the table name
        tableName = table.split(".")[-1]
        rowCheck = arcpy.GetCount_management(tableName)
        rowCount = int(rowCheck.getOutput(0))
        if rowCount > 0:
            statTable = arcpy.Statistics_analysis(tableName, r'in_memory\stat_{0}'.format(tableName), "SYS_TRANSFER_DATE MAX")
            statTables.append(statTable)
    for s in statTables:
        with arcpy.da.SearchCursor(s, ['MAX_sys_transfer_date']) as rows:
            for row in rows:
                thisDate = row[0]
                if thisDate > lastSync:
                    lastSync = thisDate
    #If we get no results (i.e., no tables) return None
    if lastSync == datetime.datetime.fromtimestamp(0):
        return None
    else:
        arcpy.AddMessage('\t\t-Last Synchornized on {0}'.format(createTimestampText(lastSync)))
        return lastSync


def getReplica(token, serviceURL, serviceInfo, now, outDir=None, outDB="outSurvey.geodatabase", lastSync=None):
    '''Downloads the full replica and then process client-side'''
    # See http://resources.arcgis.com/en/help/arcgis-rest-api/#/Create_Replica/02r3000000rp000000/
    arcpy.AddMessage('\t-Getting Replica')
    createReplicaURL = '{0}/createReplica/?f=json&token={1}'.format(serviceURL, token)
    replicaParameters = {
        "geometry": "-180,-90,180,90",
        "geometryType": "esriGeometryEnvelope",
        "inSR":4326,
        "transportType":"esriTransportTypeUrl",
        "returnAttachments":True,
        "returnAttachmentsDatabyURL":False,
        "async":True,
        "syncModel":"none",
        "dataFormat":"sqlite",
    }
    if serviceInfo["syncCapabilities"]["supportsAttachmentsSyncDirection"] == True:
        replicaParameters["attachmentsSyncDirection"] = "bidirectional"
    layerList = [str(l["id"]) for l in serviceInfo["layers"]]
    tableList = [str(t["id"]) for t in serviceInfo["tables"]]
    layerList.extend(tableList)
    replicaParameters["layers"] = ", ".join(layerList)
    layerQueries = {}
    createReplReq = urllib2.urlopen(createReplicaURL, urllib.urlencode(replicaParameters))

    #This is asynchronous, so we get a jobId to check periodically for completion
    thisJob = json.loads(createReplReq.read())
    if not "statusUrl" in thisJob:
        raise Exception("invalid job: {0}".format(thisJob))
    jobUrl = thisJob["statusUrl"]
    resultUrl = ""
    #Check for a max 1000 times (10000 sec = 2hr 46 min)
    sanityCounter = 1000
    while resultUrl == "":
        checkReq = urllib2.urlopen("{0}?f=json&token={1}".format(jobUrl, token))
        statusText = checkReq.read()
        arcpy.AddMessage(statusText)
        status = json.loads(statusText)
        if "resultUrl" in status.keys():
            resultUrl = status["resultUrl"]
        if sanityCounter < 0:
            raise Exception('took too long to make replica')
        if status["status"] == "Failed" or status["status"] == "CompletedWithErrors":
            raise Exception('Create Replica Issues: {0}'.format(status["status"]))
        arcpy.AddMessage('\t\t-Check {0}: {1}'.format(str(1001-sanityCounter), status["status"]))
        sanityCounter = sanityCounter - 1
        time.sleep(10)
    #Download the sqlite .geodatabase file
    resultReq = urllib2.urlopen("{0}?token={1}".format(resultUrl, token))
    if outDir == None:
        outDir = tempfile.mkdtemp()
    outFile = os.path.join(outDir, outDB)
    with open(outFile, 'wb') as output:
        output.write(resultReq.read())
    del(output)

    #transfer from sqlite to GDB
    surveyGDB = os.path.join(outDir, 'outSurvey.gdb')
    arcpy.CopyRuntimeGdbToFileGdb_conversion(outFile, surveyGDB)
    del(outFile)
    return surveyGDB

def filterRecords(surveyGDB, now, lastSync):
    '''Filter the records to those that need to be updated'''
    #Note - This excludes new entries that are *after* the timestamp
    #       Depending on how active the survey is, there may have been new submissions
    #           after the start of the script
    #       We put in a max time to ensure consistency in operation from run to run and
    #           table to table
    arcpy.AddMessage('\t-Filtering records to new set')
    arcpy.env.workspace = surveyGDB
    nowText = createTimestampText(now)
    tableList = getSurveyTables(surveyGDB)
    dateField = arcpy.AddFieldDelimiters(surveyGDB, "CreationDate")
    excludeStatement = "CreationDate > date '{1}'".format(dateField, nowText)
    if lastSync != None:
        lastSyncText = createTimestampText(lastSync)
        excludeStatement = excludeStatement + " OR CreationDate <= date '{0}'".format(lastSyncText)
    arcpy.AddMessage('\t\t-{0}'.format(excludeStatement))
    i = 0
    for table in tableList:
        i = i + 1
        thisName = 'filterView{0}'.format(str(i))
        dsc = arcpy.Describe(table)
        if dsc.datatype == u'FeatureClass' or dsc.datatype == u'FeatureLayer':
            arcpy.AddMessage('\t\t{0}'.format('featureclass'))
            arcpy.MakeFeatureLayer_management(table, thisName, excludeStatement)
            arcpy.DeleteFeatures_management(thisName)
        else:
            arcpy.MakeTableView_management(table, thisName, excludeStatement)
            arcpy.DeleteRows_management(thisName)
        arcpy.Delete_management(thisName)

def addTimeStamp(surveyGDB, timestamp):
    '''Disables editor tracking, adds and populates the timestamp field'''
    arcpy.AddMessage('\t-Adding Syncronization Time')
    arcpy.env.workspace = surveyGDB
    tableList = getSurveyTables(surveyGDB)
    for table in tableList:
        #Disable Editor Tracking
        arcpy.DisableEditorTracking_management(table)
        #Add a synchronization field
        arcpy.AddField_management(table, 'SYS_TRANSFER_DATE', 'DATE')

    #Set it to the timestamp
    with arcpy.da.Editor(surveyGDB) as edit:
        for table in tableList:
            with arcpy.da.UpdateCursor(table, ['SYS_TRANSFER_DATE']) as rows:
                for row in rows:
                    rows.updateRow([timestamp])
    del(edit)
    return

def addKeyFields(workspace):
    '''To enable transfer of attachments with repeats, we need an additional GUID field to serve as a lookup'''
    arcpy.env.workspace = workspace
    dscW = arcpy.Describe(workspace)
    tableList = []
    relClasses = [c.name for c in dscW.children if c.datatype == u'RelationshipClass']
    for child in relClasses:
        dscRC = arcpy.Describe(child)
        if dscRC.isAttachmentRelationship:
            originTable = dscRC.originClassNames[0]
            originFieldNames = [f.name for f in arcpy.ListFields(originTable)]
            if 'parentrowid' in originFieldNames and 'rowid' not in originFieldNames:
                arcpy.AddField_management(originTable, 'rowid', 'GUID')
                with arcpy.da.Editor(workspace) as edit:
                    with arcpy.da.UpdateCursor(originTable, ['rowid']) as urows:
                        for urow in urows:
                            urow[0] = '{' + str(uuid.uuid4()) + '}'
                            urows.updateRow(urow)
                del(edit)


def createTables(surveyGDB, outWorkspace, prefix):
    '''Creates the doamins, tables and relationships of the survey in the target workspace'''
    arcpy.AddMessage('\t-Creating Tables')
    arcpy.env.workspace = surveyGDB
    allTables = getSurveyTables(surveyGDB)

    dscW = arcpy.Describe(arcpy.env.workspace)
    #migrate the domains
    arcpy.AddMessage('\t\t-Creating Domains')
    for domainName in dscW.domains:
        if domainName[0:3] == 'cvd':
            arcpy.AddMessage('\t\t\t-'.format(domainName))
            tempTable = 'in_memory\{0}'.format(domainName)
            domainTable = arcpy.DomainToTable_management(surveyGDB, domainName, tempTable,'CODE', 'DESC')
            newDomain = arcpy.TableToDomain_management(tempTable, 'CODE', 'DESC', outWorkspace, domainName, update_option='REPLACE')
            arcpy.Delete_management(tempTable)

    arcpy.AddMessage("\t\t-Creating Feature Classes & Tables")
    for table in allTables:
        dsc = arcpy.Describe(table)
        newTableName = "{0}_{1}".format(prefix, table)
        templateTable = template=os.path.join(surveyGDB, table)

        if dsc.datatype == u'FeatureClass':
            newTable = arcpy.CreateFeatureclass_management(outWorkspace, newTableName, "POINT", template=templateTable, spatial_reference=dsc.spatialReference)
        else:
            newTable = arcpy.CreateTable_management(outWorkspace, newTableName, template=templateTable)
        arcpy.AddMessage("\t\t\t-Created {0}".format(newTableName))

        #Attach domains to fields
        tableFields = arcpy.ListFields(table)
        for field in tableFields:
            if field.domain != '':
                arcpy.AssignDomainToField_management(newTable, field.name, field.domain)
        if dscW.workspaceType == "RemoteDatabase":
            arcpy.RegisterAsVersioned_management(newTable)

    arcpy.AddMessage('\t\t-Creating Relationships')
    #Reconnect Relationship classes, checking for attachments
    CARDINALITIES = {
    'OneToOne': "ONE_TO_ONE",
    'OneToMany': "ONE_TO_MANY",
    'ManyToMany': "MANY_TO_MANY"
    }

    for child in [(c.name, c.datatype) for c in dscW.children if c.datatype == u'RelationshipClass']:
        dscRC = arcpy.Describe(child[0])
        RCOriginTable = dscRC.originClassNames[0]
        RCDestTable = dscRC.destinationClassNames[0]
        newOriginTable = "{0}_{1}".format(prefix, RCOriginTable)
        newOriginPath = os.path.join(outWorkspace, newOriginTable)
        if dscRC.isAttachmentRelationship:
            #Simple case - attachments have a dedicated tool
            arcpy.EnableAttachments_management(newOriginPath)
        else:
            newDestTable = "{0}_{1}".format(prefix, RCDestTable)
            newDestPath = os.path.join(outWorkspace, newDestTable)
            newRC = os.path.join(outWorkspace, "{0}_{1}".format(prefix, child[0]))
            relationshipType = "COMPOSITE" if dscRC.isComposite else "SIMPLE"
            fwd_label = dscRC.forwardPathLabel if dscRC.forwardPathLabel != '' else 'Repeat'
            bck_label = dscRC.backwardPathLabel if dscRC.backwardPathLabel != '' else 'Main Form'
            msg_dir = dscRC.notification.upper()
            cardinality = CARDINALITIES[dscRC.cardinality]
            attributed = "ATTRIBUTED" if dscRC.isAttributed else "NONE"
            originclassKeys = dscRC.originClassKeys
            originclassKeys_dict = {}
            for key in originclassKeys:
                originclassKeys_dict[key[1]] = key[0]
            originPrimaryKey = originclassKeys_dict[u'OriginPrimary']
            originForiegnKey = originclassKeys_dict[u'OriginForeign']
            arcpy.CreateRelationshipClass_management(newOriginPath, newDestPath, newRC, relationshipType, fwd_label, bck_label, msg_dir, cardinality, attributed, originPrimaryKey, originForiegnKey)
            #Regular Relation

def getTablesWithAttachments(workspace, prefix):
    '''Lists the tables that have attachments, so that we can seperately process the attachments during migration'''
    arcpy.AddMessage('\t-Finding Attachments')
    originalWorkspace = arcpy.env.workspace
    arcpy.env.workspace = workspace
    dscW = arcpy.Describe(workspace)
    tableList = []
    relClasses = [c.name for c in dscW.children if c.datatype == u'RelationshipClass']
    for child in relClasses:
        dbNameParts = child.split(".")
        childParts = dbNameParts[-1].split("_")
        if childParts[0] == prefix:
            dscRC = arcpy.Describe(child)
            if dscRC.isAttachmentRelationship:
                originTable = dscRC.originClassNames[0]
                originParts = originTable.split(".")
                tableList.append(originParts[-1])
    arcpy.env.workspace = originalWorkspace
    return tableList

def createFieldMap(originTable, originFieldNames, destinationFieldNames):
    '''Matches up fields between tables, even if some minor alteration (capitalization, underscores) occured during creation'''
    arcpy.AddMessage('\t\t-Field Map')
    fieldMappings = arcpy.FieldMappings()
    for field in originFieldNames:
        if field != 'SHAPE':
            thisFieldMap = arcpy.FieldMap()
            thisFieldMap.addInputField(originTable, field)
            if field in destinationFieldNames:
                #Easy case- it came over w/o issue
                outField = thisFieldMap.outputField
                outField.name = field
                thisFieldMap.outputField = outField
                #arcpy.AddMessage("\t".join([field, field]))
            else:
                #Use regular expression to search case insensitve and added _ to names
                candidates = [x for i, x in enumerate(destinationFieldNames) if re.search('{0}\W*'.format(field), x, re.IGNORECASE)]
                if len(candidates) == 1:
                    outField = thisFieldMap.outputField
                    outField.name = candidates[0]
                    thisFieldMap.outputField = outField
            fieldMappings.addFieldMap(thisFieldMap)
    return fieldMappings


def appendTables(surveyGDB, workspace, prefix):
    '''Append the records from the survey into the destination database'''
    arcpy.AddMessage('\t-Adding records')
    arcpy.env.workspace = surveyGDB
    tableList = getSurveyTables(surveyGDB)
    attachmentList = getTablesWithAttachments(workspace, prefix)

    for table in tableList:
        #Normalize table fields to get schemas in alignmnet- enable all editing, make nonrequired
        fields = arcpy.ListFields(table)
        for field in fields:
            if not field.editable:
                field.editable = True
            if field.required:
                field.required = False
        destinationName = "{0}_{1}".format(prefix, table)
        destinationFC = os.path.join(workspace, destinationName)
        arcpy.AddMessage('\t\t-{0} > {1}'.format(table, destinationName))

        #First, append the table
        #Match up the fields
        originFieldNames = [f.name for f in arcpy.ListFields(table)]
        destFieldNames = [f.name for f in arcpy.ListFields(destinationFC)]
        fieldMap = createFieldMap(table, originFieldNames, destFieldNames)

        arcpy.AddMessage('\t\t-{0}'.format(table))
        arcpy.Append_management(table, destinationFC, 'NO_TEST', fieldMap)

        if destinationName in attachmentList:
            appendAttachments(table, destinationFC)



def appendAttachments(inFC, outFC, keyField='rowid', valueField = 'globalid'):
    arcpy.AddMessage('\t\t\t-Adding attachments')
    # 1) scan through both GlobalID and rowID of the old and new features and build a conversion dictionary
    GUIDFields = [keyField, valueField]
    inDict = {}
    outDict = {}
    lookup = {}
    inAttachTable = "{0}__ATTACH".format(inFC)
    outAttachTable = "{0}__ATTACH".format(outFC)
    with arcpy.da.SearchCursor(inFC, GUIDFields) as inputSearch:
        for row in inputSearch:
            inDict[row[0]] = row[1]
    with arcpy.da.SearchCursor(outFC, GUIDFields) as outputSearch:
        for row in outputSearch:
            outDict[row[0]] = row[1]
    for key, inValue in inDict.iteritems():
        if key not in outDict.keys():
            raise Exception('missing key: {0}'.format(key))
        lookup[inValue] = outDict[key]

    # 2) Copy the attachment table to an in-memory layer
    tempTableName = r'in_memory\AttachTemp'
    tempTable = arcpy.CopyRows_management(inAttachTable, tempTableName)
    # 3) update the attachment table with new GlobalIDs
    with arcpy.da.UpdateCursor(tempTable, ['REL_GLOBALID']) as uRows:
        for uRow in uRows:
            uRow[0] = lookup[uRow[0]]
            uRows.updateRow(uRow)

    # 4) Append to destination attachment table
    arcpy.Append_management(tempTable, outAttachTable, 'NO_TEST')
    arcpy.Delete_management(tempTable)

def FAIL(sectionText, err):
    arcpy.AddMessage ('======================')
    arcpy.AddMessage ('FAIL: {0}'.format(sectionText))
    arcpy.AddMessage ('exception:')
    arcpy.AddMessage (err)
    arcpy.AddMessage (err.args)
    arcpy.AddMessage (sys.exc_info()[0])
    arcpy.AddMessage (sys.exc_info()[2].tb_lineno)
    arcpy.AddMessage ('----------------------')
    arcpy.AddMessage ('arcpy messages:')
    arcpy.AddMessage (arcpy.GetMessages(1))
    arcpy.AddMessage (arcpy.GetMessages(2))
    arcpy.AddMessage ('======================')
    return

def cleanup(ops, sdeConnection, prefix, now):
    if 'append' in ops.keys():
        arcpy.env.workspace = sdeConnection
        nowTS = createTimestampText(now)
        i = 0
        for table in ops['append']:
            i = i + 1
            thisName = 'layerOrView{0}'.format(str(i))
            whereStatment = "sys_transfer_date = timestamp'{0}'".format(nowTS)
            dsc = arcpy.Describe(table)
            selection = None
            if dsc.datatype == u'FeatureClass':
                arcpy.MakeFeatureLayer_management(table, thisName, whereStatment)
                arcpy.DeleteFeatures_management(thisName)
            else:
                arcpy.MakeTableView_management(table, thisName, whereStatment)
                arcpy.DeleteRows_management(thisName)
            arcpy.Delete_management(view)

    if 'createTables' in ops.keys():
        arcpy.env.workspace = sdeConnection
        tableList = getSurveyTables(sdeConnection, prefix)
        for table in tableList:
            arcpy.Delete_management(fc)

#    if 'tempdir' in ops.keys():
#        shutil.rmtree(ops['tempdir'])

def ConfigSectionMap(cfg, section):
    dict1 = {}
    options = cfg.options(section)
    for option in options:
        try:
            dict1[option] = cfg.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

def test(section):
    import ConfigParser
    os.chdir(r'C:\Users\jame6423\Documents\Projects\SDEmigration')
    cfg = ConfigParser.ConfigParser()
    cfg.read('test.ini')
    ## Polio Test
    testConfig = ConfigSectionMap(cfg, section)

    testConfig['sde_conn'] = os.path.join(os.path.abspath(os.curdir), testConfig['sde_conn'])
    process(testConfig['sde_conn'], testConfig['prefix'], testConfig['service_url'], testConfig['username'], testConfig['password'], testConfig['timezone'])

def process(sdeConnection, prefix, featureServiceUrl, timezone, portalUrl=None, username=None, password=None):
    '''Operations:
        1) Query Feature Service endpoint for table names & IDs
        2) Check for existing tables
        3) If existing tables, get last synchronization time
        4) CreateReplica a FGDB
        5) Download the FGDB
        6) If new, create the tables
        7) Append
    '''
    now = getUTCTimestamp(timezone)
    cleanupOperations = {}
    section = 'Beginning'
    try:
        section = 'Logging in to Survey'
        tokenTest = arcpy.GetSigninToken()
        token = None
        if tokenTest == None:
            token = getToken(username, password, portalUrl)
        else:
            token = tokenTest['token']

        serviceInfo = getServiceDefinition(token, featureServiceUrl)
        if 'Sync' not in serviceInfo['capabilities']:
            arcpy.AddError('Sync Capabilities not enabled')
            raise Exception('Sync Capabilities not enabled')

        section = 'Checking Existing Data'
        existingTables = getSurveyTables(sdeConnection, prefix)
        lastSync = None
        if len(existingTables) > 0:
            lastSync = getLastSynchronizationTime(sdeConnection, existingTables)

        section = 'Downloading Survey'
        tempdir = tempfile.mkdtemp()
        #cleanupOperations['tempdir'] = tempdir
        surveyGDB = getReplica(token, featureServiceUrl, serviceInfo, now, outDir=tempdir, lastSync=lastSync)

        section = 'Preprocess Surveys for transfer'
        filterRecords(surveyGDB, now, lastSync)
        addTimeStamp(surveyGDB, now)
        addKeyFields(surveyGDB)

        if len(existingTables) == 0:
            section = 'Making Tables'
            createTables(surveyGDB, sdeConnection, prefix)
            cleanupOperations['createTables'] = True

        section = 'Updating Tables'
        cleanupOperations['append'] = existingTables
        appendTables(surveyGDB, sdeConnection, prefix)
        cleanupOperations.pop('append', None)
        cleanupOperations.pop('createTables', None)
    except Exception as e:
        FAIL(section, e)
    finally:
        #clean up
        cleanup(cleanupOperations, sdeConnection, prefix, now)
    return

def main():
    sde_conn = arcpy.GetParameterAsText(0)
    prefix   = arcpy.GetParameterAsText(1)
    featureService = arcpy.GetParameterAsText(2)
    timezone = arcpy.GetParameterAsText(3)
    portal   = arcpy.GetParameterAsText(4)
    username = arcpy.GetParameterAsText(5)
    password = arcpy.GetParameterAsText(6)
    process(sde_conn, prefix, featureService, timezone, portal, username, password)
    arcpy.SetParameterAsText(7, sde_conn)

    pass

if __name__ == '__main__':
    main()
