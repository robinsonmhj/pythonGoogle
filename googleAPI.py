from oauth2client import file, client, tools
from googleapiclient import discovery
from apiclient.http import MediaFileUpload
import traceback

import logging.config
import logging
import yaml

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

class SpreadSheet(object):
    logger = None
    id = None
    worksheets = {}
    creds = None
    scopes = 'https://www.googleapis.com/auth/spreadsheets'
    spreadsheet = None

    def __init__(self, spreadId, spreadsheetName):
        self.getCreds()
        self.logger = logging.getLogger(__name__)
        self.spreadsheet = discovery.build('sheets', 'v4', credentials=self.creds).spreadsheets()
        if spreadId is None:
            self.id = self.createSpreadsheet(spreadsheetName)
        else:
            self.id = spreadId
        self.getWorksheets()

    def getCreds(self):
        credsFile = 'credentials.json'
        store = file.Storage(credsFile)
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('client_secret.json', self.scopes)
            creds = tools.run_flow(flow, store)
        self.creds = creds

    def getDataByWorksheetName(self,worksheet_name):
        range_ = 'A1:AB'
        range_ = worksheet_name + '!' + range_
        try:
            result = self.spreadsheet.values().get(spreadsheetId=self.id, range=range_).execute()
            values = result.get('values', [])
            return values
        except:
            self.logger(traceback.format_exc())
    
    
    # the value returned is an offset based on your range_, if you want the absoluate location, always passed range from A1
    def getLocationByValue(self, worksheetName, range_, search_items):
        if range_ is None:
            range_ = 'A1:ZZ1000'
        range_ = worksheetName + '!' + range_
        result = self.spreadsheet.values().get(spreadsheetId=self.id, range=range_).execute()
        values = result.get('values', [])
        location = []  # first n item is columnIds, last item is rowId
        if not values:
            return None
        else:
            rowNo = 1
            col_id_list = None
            for row in values:
                col_id_list = []
                try:
                    for item in search_items:
                        self.logger.debug('looking for ' + str(item))
                        col_id = row.index(item)
                        col_id_list.append(chr(col_id + 65))
                except:
                    self.logger.debug(traceback.format_exc())
                if len(col_id_list) == len(search_items):
                    location.extend(col_id_list)
                    location.append(str(rowNo))
                    return location
                rowNo += 1
        return None

    # the worksheetId is the gid you see in the brower
    def renameWorksheet(self, oldName, newName):
        worksheetId = self.getIdByName(oldName)
        # print('renameWorksheet old id is '+str(worksheetId))
        body = {'requests':{'updateSheetProperties':{'properties':{'sheetId':worksheetId, 'title':newName, }, "fields":"title", }}}
        # service=discovery.build('sheets','v4',credentials=self.creds)
        request = self.spreadsheet.batchUpdate(spreadsheetId=self.id, body=body)
        try:
            request.execute()
            self.worksheets[worksheetId] = newName
        except:
            self.logger(traceback.format_exc())
        

    def addWorksheet(self, name):
        requestBody = {'requests':[{'addSheet':{'properties':{'title':name}}}]}
        request = self.spreadsheet.batchUpdate(spreadsheetId=self.id, body=requestBody)
        try:
            request.execute()
        except:
            self.logger(traceback.format_exc())

    def deleteWorksheet(self,name):
        worksheetId = self.getIdByName(name)
        if worksheetId is None:
            logger.error('there is no such worksheet:'+name)
            return
        requestBody = {'requests':[{'deleteSheet':{'sheetId':worksheetId}}]}
        request = self.spreadsheet.batchUpdate(spreadsheetId=self.id, body=requestBody)
        try:
            request.execute()
            self.worksheets.pop(worksheetId)
        except:
            self.logger(traceback.format_exc())
        
        
        
    def getSpreadsheetId(self):
        return self.id

    def createSpreadsheet(self, name):

        spreadsheetBody = {"properties":{"title":name}}  # jason format
        request = self.spreadsheet.create(body=spreadsheetBody)
        try:
            response=request.execute()
            return response["spreadsheetId"]
        except:
            self.logger(traceback.format_exc())  # json object instead of a json string
        

    # the type of value is a list
    def insert(self, workSheetName, range_, value):
        valueInputOption = 'RAW'
        insertDataOption = 'INSERT_ROWS'
        range_ = workSheetName + '!' + range_
        body = {"range":range_, "values":value}
        request = self.spreadsheet.values().append(spreadsheetId=self.id, range=range_, valueInputOption=valueInputOption, insertDataOption=insertDataOption, body=body)
        try:
            request.execute()
            return True
        except:
            self.logger(traceback.format_exc())
        
        return False

    def update(self, worksheetName, range_, value):
        range_ = worksheetName + '!' + range_
        body = {"data":[{"range":range_, "values":value}], "valueInputOption":"USER_ENTERED"}
        request = self.spreadsheet.values().batchUpdate(spreadsheetId=self.id, body=body)
        try:
            request.execute()
            return True
        except:
            self.logger(traceback.format_exc())
        return False
    
    def clearWorksheet(self, worksheetName, range_):

        range_ = worksheetName + "!" + range_
        body = {}
        request = self.spreadsheet.values().clear(spreadsheetId=self.id, range=range_, body=body)
        request.execute()

    def getWorksheets(self):
        request = self.spreadsheet.get(spreadsheetId=self.id)
        response = request.execute()
        sheets = response["sheets"]
        results = {}
        for sheet in sheets:
            property = sheet["properties"]
            sheetId = property["sheetId"]
            title = property["title"]
            results[sheetId] = title
            self.worksheets = results

        return results

    def getIdByName(self, worksheetName):
        worksheetId = None
        for id, name in self.worksheets.iteritems():
            if name == worksheetName:
                worksheetId = id
                return worksheetId
            if worksheetId is None:
                self.getWorksheets()
            for id, name in self.worksheets.iteritems():
                if name == worksheetName:
                    return id
        return worksheetId
    def cloneWorksheet(self, fromWorksheetName, toWorksheetName):

        worksheetId = self.getIdByName(fromWorksheetName)
        body = {'destinationSpreadsheetId':self.id}
        request = self.spreadsheet.sheets().copyTo(spreadsheetId=self.id, sheetId=worksheetId, body=body)
        response = request.execute()
        newlyCreatedsheetName = response['title']
        newlyCreatedsheetId = response['sheetId']
        self.worksheets[newlyCreatedsheetId] = newlyCreatedsheetName
        self.renameWorksheet(newlyCreatedsheetName, toWorksheetName)

    # specification is list of map, for exampel [(index,sortOrder)], 0 is 'ASCENDING', 1 is 'DESCENDING'
    #based on the test, it seems that google sheet API doesn't support sort on the first index which is 0, you will get a 500 error
    def sort(self, worksheetName, startRowIndex, endRowIndex, startColumnIndex, endColumnIndex, specification):
        worksheetId = self.getIdByName(worksheetName)
        if worksheetId is None:
            self.logger.info('There is no such worksheet:'+worksheetName)
            return
        sortMap={0:'ASCENDING',1:'DESCENDING'}
        sortSpecs = []
        for t in specification:
            m = {}
            sort_order=t[1]
            if t[0]<0 or (sort_order!=0 and sort_order!=1):
                self.logger.info('invalid sort specification'+str(t))
                continue
            m['dimensionIndex'] = t[0]
            m['sortOrder'] = sortMap.get(sort_order)
            sortSpecs.append(m)

        body={
  "requests": [
    {
      "sortRange": {
        "range": {
          "sheetId": worksheetId,
          "startRowIndex": startRowIndex,
          "endRowIndex": endRowIndex,
          "endColumnIndex": endColumnIndex,
          "startColumnIndex": startColumnIndex
        },
        "sortSpecs":sortSpecs
      }
    }
  ]
}
        request = self.spreadsheet.batchUpdate(spreadsheetId=self.id, body=body)
        try:
            request.execute()
        except:
            self.logger.error(traceback.format_exc())
 
    #startRowIndex and startColumnIndex are included, endRowIndex and endColumnIndex are excluded
    #color is an array which contains RGB color, the value is from 0 to 255
    #white:255,255,255 green:0,128,0 red:255,0,0
    #fields explanation:https://developers.googleblog.com/2017/04/using-field-masks-with-update-requests.html
    #example https://developers.google.com/sheets/api/samples/formatting
    #watch out, when you call this method, make sure that you don't have any format pattern for the range, it will set the format pattern to automatic
    def format_cell(self,worksheetName,startRowIndex,endRowIndex,startColumnIndex,endColumnIndex,color):
        worksheetId = self.getIdByName(worksheetName)
        base=255.0

        body={
  "requests": [
    {
      "repeatCell": {
        "cell": {
          "userEnteredFormat": {
            "backgroundColor": {
              "red": color[0]/base,
              "alpha": 1.0,
              "blue": color[2]/base,
              "green": color[1]/base
            }
          }
        },
        "range": {
          "endColumnIndex": endColumnIndex,
          "sheetId": worksheetId,
          "startColumnIndex": startColumnIndex,
          "startRowIndex": startRowIndex,
          "endRowIndex": endRowIndex
        },
        "fields": "userEnteredFormat"
      }
    }
  ]
}
        request = self.spreadsheet.batchUpdate(spreadsheetId=self.id, body=body)
        try:
            request.execute()
        except:
            self.logger.error(traceback.format_exc())


class GoogleDrive:
    files = None
    creds = None
    logger= None
    service=None
    def __init__(self):
        self.logger=logging.getLogger(__name__)
        self.getCreds()
        self.service=discovery.build('drive', 'v3', credentials=self.creds)
        self.files = self.service.files()

    def getCreds(self):
        credsFile = 'credentials.json'
        scopes = 'https://www.googleapis.com/auth/drive'
        store = file.Storage(credsFile)
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('client_secret.json', scopes)
            creds = tools.run_flow(flow, store)
        self.creds = creds

    def cloneFile(self, originalFileId, newTitle):

        body = {'title': newTitle}
        try:
            response = self.files.copy(fileId=originalFileId, body=body).execute()
            return response['id']
        except:
            self.logger.error(traceback.format_exc())
        return None

    # right now, it filter based on the mimeType, which only find spreadsheet
    #mimeType='application/vnd.google-apps.spreadsheet'
    def getFileListByName(self, name,mime_type):
        result = {}
        pageToken = None
        while True:
            try:
                if mime_type is not None:
                    fileList = self.files.list(q="name='" + name + "' and mimeType='"+mime_type+"' and trashed=false", spaces='drive', fields='nextPageToken, files(id, name)',pageToken=pageToken).execute()
                else:
                    fileList = self.files.list(q="name='" + name +"' and trashed=false", spaces='drive',fields='nextPageToken, files(id, name)', pageToken=pageToken).execute()
                for f in fileList['files']:
                    result[f['id']] = f['name']
                pageToken = fileList.get('nextPageToken',None)
                if  pageToken is None:
                    break
            except:
                self.logger.error(traceback.format_exc())
                break
        return result

    def getUriById(self, fileId):
        try:
            response = self.files.get(fileId=fileId).execute()
            return response['alternateLink']
        except:
            self.logger.error(traceback.format_exc())
        return None

    def delete(self, file_id):
        try:
            self.files.delete(fileId=file_id).execute()
        except:
            self.logger.error(traceback.format_exc())
            
    #mime_type is such as  'image/jpeg' or 'text/csv'     
    def upload(self,local_file_name,to_file_name,mime_type,file_id):
        file_metadata={'name':local_file_name}
        if file_id is not None:
            file_metadata['id']=file_id
        media=MediaFileUpload(to_file_name,mimetype=mime_type)
        try:
            file_=self.files.create(body=file_metadata,media_body=media,fields='id').execute()
            return file_.get('id')
        except:
            self.logger.error(traceback.format_exc()) 
    def updateContent(self,file_id,file_name):
        file_metadata={'name':file_name}
        media=MediaFileUpload(file_name,mimetype='text/csv')
        try:
            file_=self.files.update(body=file_metadata,media_body=media,fileId=file_id).execute()
            return file_.get('id')
        except:
            self.logger.error(traceback.format_exc()) 
    
    #type can be user or domain, role can be writer or reader, role_name can be email or domain 
    def shareFile(self,type_,role,list_,file_id):
        batch=self.service.new_batch_http_request()
        permission_map={'type':type_,'role':role}
        if type_=='user':
            permission_map['emailAddress']=list_
        elif type_=='domain':
            permission_map['domain']=list_
        else:
            logger.error('invalid type:'+type_)
            return
        permission=self.service.permissions().create(fileId=file_id,body=permission_map,fields='id',)
        batch.add(permission)
        try:
            batch.execute()
        except:
            logger.error(traceback.format_exc())
    #not tested yet. don't use it       
    def revokePermission(self,file_id,permission_id):
        batch=self.service.new_batch_http_request()
        permission=self.service.permissions().delete(fileId=file_id,permissionId=permission_id)
        batch.add(permission)
        try:
            batch.execute()
        except:
            logger.error(traceback.format_exc())
    #don't use it, not tested yet
    def moveFile(self,file_id,to_folder_id):
        try:
            file_ = self.files.get(fileId=file_id,fields='parents').execute()     
            previous_parents = ",".join(file_.get('parents'))
            print('my current parent is '+previous_parents)
            file = self.files.update(fileId=file_id,addParents=to_folder_id,removeParents=previous_parents,fields='id, parents').execute()
        except:
            self.logger.error(traceback.format_exc())
          

if __name__=='__main__':
    with open('logging.yaml','rt') as f:
        conf=yaml.safe_load(f.read())
    logging.config.dictConfig(conf)
    logger=logging.getLogger('hahaha')
    spread_id='11iStKbsyVTtrWSYKDi3ZTULofcIqmfTBvf2n95_sqX0'
    logger.info('hello I am in the main')
    drive=GoogleDrive()
    #drive.getFileListByName('')
    spread=SpreadSheet(spread_id,None)
    #id=spread.getIdByName('bnr_robot_software_master_20180723164356286449')
    #white:255,255,255 green:0,128,0 red:255,0,0
    color=[255,0,0]#rgb
    #spread.format_cell('Summary', 3, 4, 3, 7, color)
    color=[255,255,255]
    #spread.format_cell('Summary', 3, 4, 3, 7, color)
    csv_file_name='dataSummary0713to0726.csv'
    to_folder_id='1HEgizeGgH_1vefROjwZ8R-hpMAPhTn4g'
    file_id='14jY2N3x3YHGOvE0e6myg6Hmh0ZylT3uJs9vB1dBMeXY'
    #drive.moveFile('1iP4UgY-D7GS3MgWn5gfhiLFJl6mLQNegK4Mr-D3CvpM', to_folder_id)
#     with open(csv_file_name,'w') as f:
#         worksheets=spread.getWorksheets()
#         first=True
#         for worksheet_name in worksheets.values():
#             if worksheet_name!='Summary' and worksheet_name!='Template':
#                 logger.info('writing '+worksheet_name)
#                 rows=spread.getDataByWorksheetName(worksheet_name)
#                 if first:
#                     tmp=['store_id']+rows[0]
#                     rows[0]=tmp
#                     tmp=['']+rows[1]
#                     rows[1]=tmp
#                 else:
#                     rows=rows[2:]
#                 count=0
#                 for row in rows:
#                     row_str=','.join(row)
#                     if (first and count!=0 and count!=1) or not first:
#                         row_str=worksheet_name+','+row_str
#                     f.write(row_str+'\n')
#                     count+=1
#                 first=False
#                 logger.info(worksheet_name+' has '+str(count)+' rows')

    file_list=drive.getFileListByName(csv_file_name,'text/csv')
    logger.info('start uploading/updating the file')
    if len(file_list)==0:
        mime_type='text/csv'
        #file_id=drive.upload(csv_file_name,csv_file_name,mime_type,None)
        #drive.shareFile('domain','reader','bossanova.com',file_id)
        logger.info('It seems that there is no such file, using new file id '+file_id)
    else:
        #file_id=drive.updateContent(csv_file_id,csv_file_name)
        print('I found some file=')
        for k,v in file_list.iteritems():
            print(k,v)
    logger.info('finished uploading/updating the file')
