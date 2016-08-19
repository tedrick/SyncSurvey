#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      jame6423
#
# Created:     15/08/2016
# Copyright:   (c) jame6423 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import urllib, urllib2, arcpy, os, datetime, pytz, json, time, tempfile, re
#import AppendFeaturesWithAttachments

def getToken(username, password=None, portal_URL = 'https://www.arcgis.com'):
    print('\t-Getting Token')
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
    response = urllib.urlopen(portal_URL + '/sharing/rest/generateToken?', parameters).read()
##    print(response)
    token = json.loads(response)['token']
    return token



def readServiceDefinition(token, survey_URL):
    response = urllib.urlopen("{0}?f=json&token={1}".format(survey_URL, token)).read()
    serviceInfo = json.loads(response)
    return serviceInfo

def checkSurveyTables(workspace, prefix):
    arcpy.env.workspace = workspace
    #Get the Feature Classes and tables participating in the survey
    featureClasses = arcpy.ListFeatureClasses('*{0}*'.format(prefix))
    tables = arcpy.ListTables('*{0}*'.format(prefix))
    allTables = featureClasses
    allTables.extend(tables)
    outTables = []
    for t in allTables:
        tableName = t.split('.')[-1]
        nameParts = tableName.split('_')
        if '__ATTACH' not in t and nameParts[0] == prefix:
            outTables.append(t)
    return outTables

def getLastSynchronizationTime(workspace, tableList):
    print('\t-Checking Last Sychronization')
    arcpy.env.workspace = workspace

    statTables = []
    lastSync = datetime.datetime.fromtimestamp(0)
    #First find the latest copied date
    for table in tableList:
        if table != None:
            #Skip if empty table (i.e., no rows)
            #Just use the last part of the table name
            print '\t\t-Checking sync on ' + table
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
    if lastSync == datetime.datetime.fromtimestamp(0):
        return None
    else:
        print('\t\t-Last Synchornized on {0}'.format(createTimestampText(lastSync)))
        return lastSync

def createTimestampText(datetimeObj):
    outText = ""
    timeStringFormat = "%Y-%m-%d %H:%M:%S"
    outText = datetimeObj.strftime(timeStringFormat)
    return outText

def getReplica(token, serviceURL, serviceInfo, now, outDir=None, outDB="outSurvey.geodatabase", lastSync=None):
    print('\t-Getting Replica')
    createReplicaURL = '{0}/createReplica/?f=json&token={1}'.format(serviceURL, token)
    replicaParameters = {
##        "name":"Survey123 Synchronization",
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
##    for layer in layerList:
##        thisQuery = "CreationDate <= timestamp '{0}'".format(createTimestampText(now))
##        if lastSync != None:
##            thisQuery = thisQuery + " AND CreationDate > timestamp '{0}'".format(createTimestampText(lastSync))
##        layerQueries[layer] = {"where" : thisQuery}
##    replicaParameters["layerQueries"] = layerQueries
##    print(replicaParameters)
    createReplReq = urllib2.urlopen(createReplicaURL, urllib.urlencode(replicaParameters))
    thisJob = json.loads(createReplReq.read())
    if not "statusUrl" in thisJob:
        raise Exception("invalid job: {0}".format(thisJob))
    jobUrl = thisJob["statusUrl"]
    resultUrl = ""
    sanityCounter = 1000
    while resultUrl == "":
        checkReq = urllib2.urlopen("{0}?f=json&token={1}".format(jobUrl, token))
        statusText = checkReq.read()
        status = json.loads(statusText)
        resultUrl = status["resultUrl"]
        if sanityCounter < 0:
            raise Exception('took too long to make replica')
        if status["status"] == "Failed" or status["status"] == "CompletedWithErrors":
            raise Exception('Create Replica Issues: {0}'.format(status["status"]))
        print('\t\t-Check {0}: {1}'.format(str(1001-sanityCounter), status["status"]))
        sanityCounter = sanityCounter - 1
        time.sleep(10)
    resultReq = urllib2.urlopen("{0}?token={1}".format(resultUrl, token))
    if outDir == None:
        outDir = tempfile.mkdtemp()
    os.chdir(outDir)
    with open(outDB, 'wb') as output:
        output.write(resultReq.read())
    return os.path.join(outDir, outDB)

def filterRecords(surveyGDB, now, lastSync):
    print('\t-Filtering records to new set')
    arcpy.env.workspace = surveyGDB
    nowText = createTimestampText(now)
    tableList = arcpy.ListFeatureClasses()
    #Get Tables, dropping any _ATTACH - attachments
    surveyTbl = [t if len(t) < 7 or t[-7:] != "_ATTACH" else None for t in arcpy.ListTables()]
    tableList.extend(surveyTbl)
    i = 0
    views = []
    for table in tableList:
        if table != None:
            i = i + 1
            thisName = 'filterView{0}'.format(str(i))
            dsc = arcpy.Describe(table)
            excludeStatement = "CreationDate > timestamp'{0}'".format(nowText)
            if lastSync != None:
                lastSyncText = createTimestampText(lastSync)
                excludeStatement = excludeStatement + " OR CreationDate <= timestamp '{0}'".format(lastSyncText)
            print('\t\t-{0}'.format(excludeStatement))
            dsc = arcpy.Describe(table)
            if dsc.datatype == u'FeatureClass':
                arcpy.MakeFeatureLayer_management(table, thisName, excludeStatement)
                arcpy.DeleteFeatures_management(thisName)
            else:
                arcpy.MakeTableView_management(table, thisName, excludeStatement)
                arcpy.DeleteRows_management(thisName)
            views.append(thisName)
    for view in views:
        arcpy.Delete_management(view)

def addTimeStamp(surveyGDB, timestamp):
    print('\t-Adding Syncronization Time')
    arcpy.env.workspace = surveyGDB
    tableList = arcpy.ListFeatureClasses()
    #Get Tables, dropping any _ATTACH - attachments
    surveyTbl = [t if len(t) < 7 or t[-7:] != "_ATTACH" else None for t in arcpy.ListTables()]
    tableList.extend(surveyTbl)
    for table in tableList:
        if table != None:
            #Disable Editor Tracking -we need to later for schema comparison
            arcpy.DisableEditorTracking_management(table)
            #Add a synchronization field
            arcpy.AddField_management(table, 'SYS_TRANSFER_DATE', 'DATE')
            #Set it to the timestamp
            with arcpy.da.Editor(surveyGDB) as edit:
                with arcpy.da.UpdateCursor(table, ['SYS_TRANSFER_DATE']) as rows:
                    for row in rows:
                        rows.updateRow([timestamp])
            del(edit)
    return

def checkForAttachmentKeyFields(workspace):
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
                addKeyField(workspace, originTable, 'rowid')

def createTables(surveyGDB, workspace, prefix):
    print('\t-Creating Tables')
    arcpy.env.workspace = surveyGDB
    surveyFC = arcpy.ListFeatureClasses()
    #Get Tables, dropping any _ATTACH - attachments
    surveyTbl = [t if len(t) < 7 or t[-7:] != "_ATTACH" else None for t in arcpy.ListTables()]

    allTables = surveyFC
    allTables.extend(surveyTbl)

    dscW = arcpy.Describe(arcpy.env.workspace)
    #migrate the domains
    print('\t\t-Creating Domains')
    for domainName in dscW.domains:
        if domainName[0:3] == 'cvd':
            print('\t\t\t-domainName')
            domainTable = arcpy.DomainToTable_management(surveyGDB, domainName, 'in_memory\{0}'.format(domainName),'CODE', 'DESC')
            newDomain = arcpy.TableToDomain_management('in_memory\{0}'.format(domainName), 'CODE', 'DESC', workspace, domainName, update_option='REPLACE')
            arcpy.Delete_management('in_memory\{0}'.format(domainName))

    print("\t\t-Creating Feature Classes & Tables")
    for table in allTables:
        if table != None:
            dsc = arcpy.Describe(table)
            tableFields = arcpy.ListFields(table)
            tableFieldNames = [f.name for f in tableFields]
            #Check for repeat table with attachments- need a key field
##            if 'parentrowid' in originFieldNames:
##                addKeyField(surveyGDB, fc, 'rowid')

            newTableName = "{0}_{1}".format(prefix, table)
            templateTable = template=os.path.join(surveyGDB, table)
            if dsc.datatype == u'FeatureClass':
                newTable = arcpy.CreateFeatureclass_management(workspace, newTableName, "POINT", template=templateTable, spatial_reference=dsc.spatialReference)
            else:
                newTable = arcpy.CreateTable_management(workspace, newTableName, template=templateTable)
            print("\t\t\t-Created " + newTableName)

            #Attach Domains
            for field in tableFields:
                if field.domain != '':
                    arcpy.AssignDomainToField_management(newTable, field.name, field.domain)
            arcpy.RegisterAsVersioned_management(newTable)

##
##    for fc in surveyFC:
##        #Add a synchronization field
##        #Create a Feature Class in the destination workspace modeled on the surveyGDB
##        #print(os.path.join(surveyGDB, fc))
##        fcDSC = arcpy.Describe(fc)
##
##        #Check to see if this is a repeat table- need to create a secondary key field
##        originFields = arcpy.ListFields(fc)
##        originFieldNames = [f.name for f in originFields]
##
##        newFC = arcpy.CreateFeatureclass_management(workspace, "{0}_{1}".format(prefix, fc), "POINT", template=os.path.join(surveyGDB, fc), spatial_reference=fcDSC.spatialReference)
##        print("\t\t\t-Created " + fc)
##        #Attach domains
##        fcFields = arcpy.ListFields(os.path.join(surveyGDB, fc))
##        for field in fcFields:
##            if field.domain != '':
##                arcpy.AssignDomainToField_management(newFC, field.name, field.domain)
##        arcpy.RegisterAsVersioned_management(newFC)
##
##    for tbl in surveyTbl:
##        if tbl != None:
##            print("\t\t\t-Created " + tbl)
##
##            #Check to see if this is a repeat table- need to create a secondary key field
##            originFields = arcpy.ListFields(tbl)
##            originFieldNames = [f.name for f in originFields]
##            if 'parentrowid' in originFieldNames:
##                addKeyField(surveyGDB, tbl, 'rowid')
##
##            newTbl = arcpy.CreateTable_management(workspace, "{0}_{1}".format(prefix, tbl), template=os.path.join(surveyGDB, tbl))
##            print("created" + tbl)
##                    #Attach domains
##            tblFields = arcpy.ListFields(os.path.join(surveyGDB, tbl))
##            for field in tblFields:
##                if field.domain != '':
##                    arcpy.AssignDomainToField_management(newTbl, field.name, field.domain)
##            arcpy.RegisterAsVersioned_management(newTbl)

    print('\t\t-Creating Relationships')
    #Reconnect Relationship classes, checking for attachments
    CARDINALITIES = {
    'OneToOne': "ONE_TO_ONE",
    'OneToMany': "ONE_TO_MANY",
    'ManyToMany': "MANY_TO_MANY"
    }

    for child in [(c.name, c.datatype) for c in dscW.children if c.datatype == u'RelationshipClass']:
        dscRC = arcpy.Describe(child[0])
        o = dscRC.originClassNames[0]
        d = dscRC.destinationClassNames[0]
        newO = "{0}_{1}".format(prefix, o)
        newOriginPath = os.path.join(workspace, newO)
        if dscRC.isAttachmentRelationship:
            #attachment
            arcpy.EnableAttachments_management(newOriginPath)
        else:
            newD = "{0}_{1}".format(prefix, d)
            newDestPath = os.path.join(workspace, newD)
            newRC = os.path.join(workspace, "{0}_{1}".format(prefix, child[0]))
            relationshipType = "COMPOSITE" if dscRC.isComposite else "SIMPLE"
            fwd_label = dscRC.forwardPathLabel if dscRC.forwardPathLabel != '' else 'Repeat'
            bck_label = dscRC.backwardPathLabel if dscRC.backwardPathLabel != '' else 'Main Form'
            msg_dir = dscRC.notification.upper()
            cardinality = CARDINALITIES[dscRC.cardinality]
            attributed = "ATTRIBUTED" if dscRC.isAttributed else "NONE"
            o_classKeys = dscRC.originClassKeys
            o_classKeys_dict = {}
            for key in o_classKeys:
                o_classKeys_dict[key[1]] = key[0]
            o_primaryKey = o_classKeys_dict[u'OriginPrimary']
            o_foriegnKey = o_classKeys_dict[u'OriginForeign']
            arcpy.CreateRelationshipClass_management(newOriginPath, newDestPath, newRC, relationshipType, fwd_label, bck_label, msg_dir, cardinality, attributed, o_primaryKey, o_foriegnKey)
            #Regular Relation

def getTablesWithAttachments(workspace, prefix):
    print('\t-Finding Attachments')
    arcpy.env.workspace = workspace
    dscW = arcpy.Describe(workspace)
    tableList = []
    relClasses = [c.name for c in dscW.children if c.datatype == u'RelationshipClass']
    for child in relClasses:
        dbNameParts = child.split(".")
        childParts = dbNameParts[-1].split("_")
        if childParts[0] == prefix:
            dscRC = arcpy.Describe(child)
##            print ("\t".join([child, str(dscRC.isAttachmentRelationship)]))
            if dscRC.isAttachmentRelationship:
                originTable = dscRC.originClassNames[0]
                originParts = originTable.split(".")
                tableList.append(originParts[-1])
    return tableList

def appendTables(surveyGDB, workspace, prefix, attachmentList):
    print('\t-Adding records')
    arcpy.env.workspace = surveyGDB
    tableList = arcpy.ListFeatureClasses()
    #Get Tables, dropping any _ATTACH - attachments
    surveyTbl = [t if len(t) < 7 or t[-7:] != "_ATTACH" else None for t in arcpy.ListTables()]
    tableList.extend(surveyTbl)
    for table in tableList:
        if table != None:
            #Normalize table fields to get schemas in alignmnet- enable all editing, make nonrequired
            fields = arcpy.ListFields(table)
            for field in fields:
                if not field.editable:
                    field.editable = True
                if field.required:
                    field.required = False
            destinationName = "{0}_{1}".format(prefix, table)
            destinationFC = os.path.join(workspace, destinationName)

            #First, append the table
            #Match up the fields
            originFields = arcpy.ListFields(table)
            destinationFields = arcpy.ListFields(destinationFC)
            originFieldNames = [f.name for f in originFields]
            destFieldNames = [f.name for f in destinationFields]


            fieldMappings = arcpy.FieldMappings()
            for field in originFieldNames:
                if field != 'SHAPE':
                    thisFieldMap = arcpy.FieldMap()
                    thisFieldMap.addInputField(table, field)
                    if field in destFieldNames:
                        #Easy case- it came over w/o issue
                        outField = thisFieldMap.outputField
                        outField.name = field
                        thisFieldMap.outputField = outField
                        #print("\t".join([field, field]))
                    else:
                        #Use regular expression to search case insensitve and added _ to names
                        candidates = [x for i, x in enumerate(destFieldNames) if re.search('{0}\W*'.format(field), x, re.IGNORECASE)]
                        if len(candidates) == 1:
                            outField = thisFieldMap.outputField
                            outField.name = candidates[0]
                            thisFieldMap.outputField = outField
##                            print("\t".join([field, candidates[0]]))
                    fieldMappings.addFieldMap(thisFieldMap)
            print('\t\t-{0}'.format(table))
            arcpy.Append_management(table, destinationFC, 'NO_TEST', fieldMappings)

            if destinationName in attachmentList:
                appendAttachments(table, destinationFC)

def addKeyField(inGDB, inTable, fieldName):
    import uuid
    arcpy.env.workspace = inGDB
    arcpy.AddField_management(inTable, fieldName, 'GUID')
    with arcpy.da.Editor(inGDB) as edit:
        with arcpy.da.UpdateCursor(inTable, [fieldName]) as rows:
            for row in rows:
                row[0] = '{' + str(uuid.uuid4()) + '}'
                rows.updateRow(row)
    del(edit)


def appendAttachments(inFC, outFC, keyField='rowid', valueField = 'globalid'):
    print('\t\t\t-Adding attachments')
    # 1) scan through both GlobalID and rowID of the old and new features and build a conversion dictionary
    GUIDFields = [keyField, valueField]
    inDict = {}
    outDict = {}
    lookup = {}
    inAttachTable = inFC + "__ATTACH"
    outAttachTable = outFC + "__ATTACH"
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
    tempTable = arcpy.CopyRows_management(inAttachTable, r'in_memory\AttachTemp')
    # 3) update the attachment table with new GlobalIDs
    with arcpy.da.UpdateCursor(tempTable, ['REL_GLOBALID']) as uRows:
        for uRow in uRows:
            uRow[0] = lookup[uRow[0]]
            uRows.updateRow(uRow)

    # 4) Append to destination attachment table
    arcpy.Append_management(tempTable, outAttachTable, 'NO_TEST')
    arcpy.Delete_management(tempTable)

def workflow(username, password, portal, workspace, prefix, serviceURL):
    '''Operations:
        1) Query Feature Service endpoint for table names & IDs
        2) Check for existing tables
        3) If existing tables, get last synchronization time
        4) CreateReplica a FGDB
        5) Download the FGDB
        6)  If new, copy
            Else, Append
    '''
    token = getToken(username, password, portal=None)
    serviceInfo = readServiceDefinition(token, serviceURL)
    existingTables = checkSurveyTables(workspace, prefix)
    lastSync = None
    if len(existingTables) > 0:
        lastSync = getLastSynchronizationTime(workspace, existingTables)
    #Set 'now' for the script to set the synchronization time
    now = datetime.datetime.now()
    tempdir = tempfile.mkdtemp()
    arcpy.env.workspace = tempdir
    replica = getReplica(token, serviceURL, serviceInfo, now, outDir=tempdir, lastSync=lastSync)
    arcpy.CopyRuntimeGdbToFileGdb_conversion(replica, os.path.join(tempdir, 'outSurvey.gdb'))
    addTimeStamp(os.path.join(tempdir, 'outSurvey.gdb'), now)
    if len(existingTables) == 0:
        createTables(os.path.join(tempdir, 'outSurvey.gdb'), workspace, prefix)
    attachmentList = getTablesWithAttachments(workspace, prefix)
    appendTables(os.path.join(tempdir, 'outSurvey.gdb'), workspace, prefix)

    pass

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

def FAIL(sectionText, err):
    print ('======================')
    print ('FAIL: '.format(sectionText))
    print ('exception:')
    print (err)
    print (err.args)
    print ('----------------------')
    print ('arcpy messages:')
    print (arcpy.GetMessages())
    print ('======================')
    return

def cleanup(ops, config, now):
    if 'append' in ops.keys():
        arcpy.env.workspace = config['sde_conn']
        nowTS = createTimestampText(now)
        i = 0
        views = []
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
            views.append(thisName)
        for view in views:
            arcpy.Delete_management(view)

    if 'createTables' in ops.keys():
        arcpy.env.workspace = config['sde_conn']
        newFCs = arcpy.ListFeatureClasses(wild_card = "*{0}*".format(config['prefix']))
        newTables = arcpy.ListTables(wild_card = "*{0}*".format(config['prefix']))
        newFCs.extend(newTables)
        for fc in newFCs:
            arcpy.Delete_management(fc)

    if 'tempdir' in ops.keys():
        import shutil
        shutil.rmtree(ops['tempdir'])



def test(section):
    import os, ConfigParser
    os.chdir(r'C:\Users\jame6423\Documents\Projects\SDEmigration')
    cfg = ConfigParser.ConfigParser()
    cfg.read('test.ini')
    ## Polio Test
    testConfig = ConfigSectionMap(cfg, section)

    testConfig['sde_conn'] = os.path.join(os.path.abspath(os.curdir), testConfig['sde_conn'])
    process(testConfig)

def process(config):
##    print(config)
    now = datetime.datetime.now()
    timeZone = pytz.timezone(config['timezone'])
    localNow = timeZone.localize(now)
    utcNow = localNow.astimezone(pytz.utc)
    cleanupOperations = {}
    section = 'Beginning'
    try:
        section = 'Logging in to Survey'
        token = getToken(config['username'], config['password'])
        arcpy.AddMessage("Logging in to get survey")

        section = 'Checking Existing Data'
        serviceInfo = readServiceDefinition(token, config['service_url'])
        existingTables = checkSurveyTables(config['sde_conn'], config['prefix'])
        lastSync = None
        if len(existingTables) > 0:
            lastSync = getLastSynchronizationTime(config['sde_conn'], existingTables)
##        utcLastSync = None
##        if lastSync != None:
##            localLastSync = timeZone.localize(lastSync)
##            utcLastSync = localLastSync.astimezone(pytz.utc)

        section = 'Downloading Survey'
        tempdir = tempfile.mkdtemp()
        cleanupOperations['tempdir'] = tempdir
        arcpy.env.workspace = tempdir
        if 'Sync' not in serviceInfo['capabilities']:
            raise Exception('Sync Capabilities not enabled')
        replica = getReplica(token, config['service_url'], serviceInfo, utcNow, outDir=tempdir, lastSync=lastSync)
        surveyGDB = os.path.join(tempdir, 'outSurvey.gdb')
        arcpy.CopyRuntimeGdbToFileGdb_conversion(replica, surveyGDB)
        filterRecords(surveyGDB, utcNow, lastSync)
        addTimeStamp(surveyGDB, utcNow)
        checkForAttachmentKeyFields(surveyGDB)

        section = 'Making Tables'
        if len(existingTables) == 0:
            createTables(surveyGDB, config['sde_conn'], config['prefix'])
            cleanupOperations['createTables'] = True

        section = 'Updating Tables'
        attachmentList = getTablesWithAttachments(config['sde_conn'], config['prefix'])
        cleanupOperations['append'] = []
        appendTables(surveyGDB, config['sde_conn'], config['prefix'], attachmentList)

    except Exception as e:
        FAIL(section, e)
        #clean up
        cleanup(cleanupOperations, config, now)
        return

def main():
    pass

if __name__ == '__main__':
    main()
