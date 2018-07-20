from oauth2client import file, client, tools
from googleapiclient import discovery

import traceback

import logging
import logging.config
import yaml


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
  	request.execute()
	self.worksheets[worksheetId] = newName

  def addWorksheet(self, name):
  	requestBody = {'requests':[{'addSheet':{'properties':{'title':name}}}]}
  	request = self.spreadsheet.batchUpdate(spreadsheetId=self.id, body=requestBody)
  	response = request.execute()

  def getSpreadsheetId(self):
	  return self.id

  def createSpreadsheet(self, name):

	spreadsheetBody = {"properties":{"title":name}}  # jason format
	request = self.spreadsheet.create(body=spreadsheetBody)
	response = request.execute()  # json object instead of a json string
	return response["spreadsheetId"]

  # the type of value is a list
  def insert(self, workSheetName, range_, value):
    valueInputOption = 'RAW'
    insertDataOption = 'INSERT_ROWS'
    range_ = workSheetName + '!' + range_
    body = {"range":range_, "values":value}
    request = self.spreadsheet.values().append(spreadsheetId=self.id, range=range_, valueInputOption=valueInputOption, insertDataOption=insertDataOption, body=body)
    try:
      response = request.execute()
    except:
      self.logger(traceback.format_exc())

  def update(self, worksheetName, range_, value):

	range_ = worksheetName + '!' + range_
  	body = {"data":[{"range":range_, "values":value}], "valueInputOption":"USER_ENTERED"}
  	request = self.spreadsheet.values().batchUpdate(spreadsheetId=self.id, body=body)
  	response = request.execute()

  def clearWorksheet(self, worksheetName, range_):

	range_ = worksheetName + "!" + range_
	body = {}
	request = self.spreadsheet.values().clear(spreadsheetId=self.id, range=range_, body=body)
	response = request.execute()

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
		# print(id,name)
		if name == worksheetName:
			worksheetId = id
			return worksheetId
	if worksheetId is None:
		self.getWorksheets()
		for id, name in self.worksheets.iteritems():
			# print(id,name)
			if name == worksheetName:
				return id

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
  		response = request.execute()
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
#       body={
#   "requests": [
#     {
#       "repeatCell": {
#         "cell": {
#           "userEnteredFormat": {
#             "backgroundColor": {
#               "red": color[0]/base,
#               "alpha": 1.0,
#               "blue": color[2]/base,
#               "green": color[1]/base
#             },
#             "numberFormat": {
#               "type": "TIME",
#               "pattern": "hh:mm:ss"
#             }
#           }
#         },
#         "range": {
#           "endColumnIndex": endColumnIndex,
#           "sheetId": worksheetId,
#           "startColumnIndex": startColumnIndex,
#           "startRowIndex": startRowIndex,
#           "endRowIndex": endRowIndex
#         },
#         "fields": "userEnteredFormat"
#       },
#       "repeatCell": {
#         "cell": {
#           "userEnteredFormat": {
#             "backgroundColor": {
#               "red": color[0]/base,
#               "alpha": 1.0,
#               "blue": color[2]/base,
#               "green": color[1]/base
#             },
#             "numberFormat": {
#               "type": "NUMBER",
#               "pattern": "###"
#             }
#           }
#         },
#         "range": {
#           "endColumnIndex": endColumnIndex,
#           "sheetId": worksheetId,
#           "startColumnIndex": startColumnIndex,
#           "startRowIndex": startRowIndex,
#           "endRowIndex": endRowIndex
#         },
#         "fields": "userEnteredFormat"
#       }
#     }
#   ]
# }
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
        response = request.execute()
    except:
        self.logger.error(traceback.format_exc())

class GoogleDrive:
  files = None
  creds = None
  logger= None
  def __init__(self):
  	self.logger=logging.getLogger(__name__)
	self.getCreds()
	self.files = discovery.build('drive', 'v2', credentials=self.creds).files()

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
  def getFileListByName(self, name):

		result = {}
		pageToken = None
		while True:
				try:
						fileList = self.files.list(q="title='" + name + "' and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false", spaces='drive', pageToken=None).execute()
						for f in fileList['items']:
								# print(f)
								result[f['id']] = f['title']
						pageToken = fileList.get('nextPageToken')
						if not pageToken:
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

if __name__=='__main__':
    with open('logging.yaml','rt') as f:
        conf=yaml.safe_load(f.read())
    logging.config.dictConfig(conf)
    logger=logging.getLogger('hahaha')
    spread_id='1WxHR5ybOrBXyI8KWG2x6R5FvN8mOsLfJcUBarkzMOWM'
    logger.info('hello I am in the main')
    #drive=GoogleDrive()
    #drive.getFileListByName('')
    spread=SpreadSheet(spread_id,None)
    #white:255,255,255 green:0,128,0 red:255,0,0
    color=[255,0,0]#rgb
    spread.format_cell('Summary', 3, 4, 3, 7, color)
    color=[255,255,255]
    spread.format_cell('Summary', 3, 4, 3, 7, color)
