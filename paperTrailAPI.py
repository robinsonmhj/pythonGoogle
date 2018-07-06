

import requests
import time
import datetime
import logging.config
import yaml

import pickle
import os

from googleAPI import SpreadSheet

from sets import Set

with open('logging.yaml','rt') as f:
    conf=yaml.safe_load(f.read())
logging.config.dictConfig(conf)
logger=logging.getLogger('PaperTrailAPI')


    
class Label(object):
    name=None
    start_time=None
    end_time=None
    message_count=0
    update_status=False
    def __init__(self,name):
        self.name=name
    def set_start_time(self,start_time):
        self.start_time=start_time
    def set_end_time(self,end_time):
        self.end_time=end_time
    def add_message(self,count):
        self.message_count+=count
    def get_start_time(self):
        return self.start_time
    def get_end_time(self):
        return self.end_time
    def get_message_count(self):
        return self.message_count
    def get_name(self):
        return self.name
    def increase(self):
        self.message_count+=1
    def __str__(self):
        return self.name+','+self.start_time+','+self.end_time+','+str(self.message_count)
    def toArray(self):
        return [self.get_start_time(),self.get_end_time(),self.message_count]
    def set_update_status(self,update_status):
        self.update_status=update_status
    def get_update_status(self):
        return self.update_status

def write2File(result,fileName):
    f=open(fileName,'a')
    for time in result.keys():
        l=result.get(time)
        f.write(time+"="+str(len(l))+'='+str(l)+'\n')
    f.close()

#timestamp formate is like yyyy-mm-dd-hh-mm-ss
def time2Epoch(timestamp):
    t=timestamp.split('-')
    l=[]
    for v in t:
        l.append(int(v))
    return datetime.datetime(l[0],l[1],l[2],l[3],l[4],l[5]).strftime('%s')

def getCurrentDate():
    now=datetime.datetime.now()
    return now.strftime('%Y-%m-%d')


def str2Dic(string,delimiter):
    res={}
    try:
        arr=string.split(delimiter)
        for elem in arr:
            elems=elem.split('=')
            k=elems[0]
            v=elems[1]
            res[k]=v
            if 'message'==k:
                index1=v.index('api-ext.wal-mart.com')
                index2=v.index(' HTTP/1.1')
                values=v[index1:index2].split('/')
                key='zone'
                index=values.index(key)
                res[key]=values[index+1]
                key='aisle'
                index=values.index(key)
                res[key]=values[index+1]
                key='label'
                res[key]=values[len(values)-1]
    except Exception,e:
        logger.warn('parsing error for message '+string+str(e))     
    return res

def getIndex(arr,key):
    index=-1
    try:
        index=arr.index(key)
    except ValueError:
        logger.warn(key +" doesn't exist in the array")
    return index
 
def getConfiguration():
    fileName='paperTrail.properties'
    res={}
    f=open(fileName,'r')
    for line in f:
        if line.startswith('#'):
            logger.warn('Ignore line: '+line)
            continue
        lines=line.split('=')
        if len(lines)!=2:
            logger.warn('bad line:'+line)
            continue
        res[lines[0]]=lines[1].replace('\n','').split(',')
    f.close()
    return res


poll_interval=100
poll_interval_long=30*60
long_interval_hours=[20,21,22,23,12,1]
sleep_time=0.3
uri="https://papertrailapp.com/api/v1/events/search.json"
headers={"X-Papertrail-Token": "","Content-Type": "application/json"}



res=getConfiguration()
store_list=res['store_list']
component_list=res['component_list']
expected_list=res['expected_map']

#generate the filter which is used to filter data
filter='('
index=1
for component in component_list:
    filter+='/'+component
    if(index!=len(component_list)):
        filter+=' OR '
    index+=1
filter+=') INFO "Status Code: 200" ('

index=1
#used to reduce the number of googleAPI calls
sheet_location_cache={}#key is the work_sheet id, value is a map{key is the primary key, value is the row number}
#key count in the summary result map
last_count=0
asile_last_count={}
asile_map={}#key:store_id, value=asile set
#generate the expected_notification_count
expected_notification_count={}
for item in expected_list:
    items=item.split(':')
    store_id=items[0]
    expected_count=items[1]
    expected_notification_count[store_id]=expected_count
    sheet_location_cache[store_id]={}
    asile_last_count[store_id]=0
    asile_map[store_id]=Set()
    filter+='store_id='+store_id
    if(index!=len(expected_list)):
        filter+=' OR '
    index+=1
filter+=')'
logger.info(filter)

payload={}
payload['limit']=10000
payload['system_id']=''
payload['tail']='false'
payload['q']=filter
payload['min_id']=None



suffix='.data'
hidden_file_name='.pickle.bin'
summary_result={}#key is date_storeNo_mission_id, value is labelMap
label_map={}#key is the lableName, value is label class
store_result={}#key is date_storeNo_missionId_aisleNo, value is labelMap
key_delimiter='_'




if os.path.exists(hidden_file_name):
    with open(hidden_file_name,'rb') as hidden_file:
        payload['min_id']=pickle.load(hidden_file)
        summary_result=pickle.load(hidden_file)
        store_result=pickle.load(hidden_file)
        sheet_location_cache=pickle.load(hidden_file)
#         #need to comment out the following codes, used to support the feature which only use the time instead of timestamp
#         for key in store_result.keys():
#             label_map=store_result.get(key)
#             for label_name in label_map.keys():
#                 label=label_map.get(label_name)
#                 label.set_start_time(label.get_start_time()[11:19])
#                 label.set_end_time(label.get_end_time()[11:19])
#         for key in summary_result.keys():
#             label_map=summary_result.get(key)
#             for label_name in label_map.keys():
#                 label=label_map.get(label_name)
#                 label.set_start_time(label.get_start_time()[11:19])
#                 label.set_end_time(label.get_end_time()[11:19])
    hidden_file.close()
else:
    timestamp=datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    min_time=time2Epoch(timestamp)
    payload['min_time']=min_time
    payload.pop('min_id')
    logger.warn('I do not have the result for the last run, using the current time as the start\n You may get incomplete result')
        
spreadSheet=SpreadSheet('11iStKbsyVTtrWSYKDi3ZTULofcIqmfTBvf2n95_sqX0','MessageDataSummary')
labelMap={'labels':['D','F'],'topStock':['G','I'],'sectionLabels':['J','L'],'sectionBreaks':['M','O'],'products':['P','R'],'notification':['S','U'],}
#default to the current date, otherwise, if there is no events, it will give high pressure to the paperTrail API
max_generate_date=getCurrentDate()


while True: 
    result_list=[]
    r=requests.get(uri,params=payload,headers=headers)
    #logger.info(r.url)
    #logger.info(r.headers)
    try:
        json=r.json()
    except Exception,e:
        logger.info(str(e))
        continue
    events=json['events']
    max_id=json['max_id']
    payload['min_id']=max_id
    if payload.has_key('min_time'):
        payload.pop('min_time')
    logger.info('max_id='+max_id)
    #result=collections.OrderedDict()
    logger.info("I get "+str(len(events))+' events')
    for event in events:
        event_message=event['message']
        event_map=str2Dic(event_message,' | ')
        generated_time=event['generated_at']
        generate_date=generated_time[0:10]
        if(max_generate_date is None or generate_date>max_generate_date):
            max_generate_date=generate_date
        event_map['generate_time']=generated_time
        result_list.append(event_map)
        #logger.info(event_map)
        if len(event_map)<13:
            logger.info('bad event')
            continue
        #if not result.has_key(generated_time):
        #    result[generated_time]=[]
        #result[generated_time].append(event_message)
    #if len(result)>0:
    #    write2File(result,'details'+'_'+generate_date+suffix)

    
    for event_map in result_list:
        store_id=event_map['store_id']
        generate_time=event_map['generate_time']
        generate_time_only=generate_time[11:19]
        generate_date=generate_time[0:10]
        mission_id=event_map['mission_id']
        label_name=event_map['label']
        zone_name=event_map['zone']
        asile_name=event_map['aisle']
        asile=zone_name+asile_name
        
        #add each asile in the set group by store_id
        asile_map[store_id].add(asile)
        
        summary_key=generate_date+key_delimiter+store_id+key_delimiter+mission_id
        summary_value={}
        if summary_result.has_key(summary_key):
            summary_value=summary_result.get(summary_key)
        summary_label_value=Label(label_name)
        if summary_value.has_key(label_name):
            summary_label_value=summary_value.get(label_name)
        summary_label_value.increase()
        summary_label_value.set_update_status(True)
        min_time=summary_label_value.get_start_time()
        if(min_time is None or min_time>generate_time_only):
            summary_label_value.set_start_time(generate_time_only)
        max_time=summary_label_value.get_end_time()
        if(max_time is None or max_time<generate_time_only):
            summary_label_value.set_end_time(generate_time_only)
        summary_value[label_name]=summary_label_value
        summary_result[summary_key]=summary_value
         
        store_key=summary_key+key_delimiter+asile 
        store_value={}
        if(store_result.has_key(store_key)):
            store_value=store_result.get(store_key)
        store_label_value=Label(label_name)
        if(store_value.has_key(label_name)):
            store_label_value=store_value[label_name]
        store_label_value.increase()
        store_label_value.set_update_status(True)
        min_time=store_label_value.get_start_time()
        if(min_time is None or store_label_value.get_start_time()>generate_time_only):
            store_label_value.set_start_time(generate_time_only)
        max_time=store_label_value.get_end_time()
        if(max_time is None or max_time<generate_time_only):
            store_label_value.set_end_time(generate_time_only)   
        store_value[label_name]=store_label_value
        store_result[store_key]=store_value
    
    worksheet_name='Summary'
    if not sheet_location_cache.has_key(worksheet_name):
        sheet_location_cache[worksheet_name]={}
    row_location_cache=sheet_location_cache[worksheet_name]
    for key in summary_result.keys():
        key_array=key.split(key_delimiter)
        store_id=key_array[1]
        if not row_location_cache.has_key(key):
            logger.info('no cache for key '+key)
            location=spreadSheet.getLocationByValue(worksheet_name, None,key_array)
            time.sleep(sleep_time)
            if location is None or len(location)==0:
                #generate the expected data, there are 18 cells between the primary key and the expected result
                cell_no=18
                fill_stuff=[' ']*cell_no
                fill_stuff.append(expected_notification_count[store_id])
                spreadSheet.insert(worksheet_name, 'A2:V2', [key_array+fill_stuff])
                location=spreadSheet.getLocationByValue(worksheet_name, None,key_array)
            row_id=location[len(location)-1]
            row_location_cache[key]=row_id
        row_id=row_location_cache[key]
        label_map=summary_result.get(key)
        for label_name in label_map.keys():
            label=label_map.get(label_name)
            if not label.get_update_status():
                logger.debug('No update for key='+key+',label='+label_name)
                continue
            border=labelMap[label_name]
            range_=border[0]+row_id+':'+border[1]+row_id
            spreadSheet.update('Summary', range_, [label.toArray()])
            label.set_update_status(False)
            time.sleep(sleep_time)
            logger.info(key+':'+label_name)
    for key in store_result.keys():
        key_array=key.split(key_delimiter)
        store_id=key_array[1]
        array_len=len(key_array)
        key_array[1]=key_array[array_len-1]
        key_array.pop(array_len-1)#used to find the location of each label
        label_map=store_result.get(key)
        worksheet_id=spreadSheet.getIdByName(store_id)
        if(worksheet_id is None):
            spreadSheet.cloneWorksheet('Template', store_id)
            spreadSheet.insert(store_id, 'A4:C4', [key_array])
        #only query the location when it needs to insert/update data
        row_location_cache=sheet_location_cache[store_id]
        row_key=key_delimiter.join(key_array)
        if not row_location_cache.has_key(row_key):
            location=spreadSheet.getLocationByValue(store_id, None,key_array) 
            if location is None or len(location)==0:
                spreadSheet.insert(store_id, 'A4:C4', [key_array])
                location=spreadSheet.getLocationByValue(store_id, None,key_array)
            row_id=location[len(location)-1]
            row_location_cache[row_key]=row_id
        row_id=row_location_cache[row_key]
        for label_name in label_map.keys():
            label=label_map.get(label_name)
            if not label.get_update_status():
                logger.debug('No update for key='+key+',label='+label_name)
                continue
            border=labelMap[label_name]
            
            range_=border[0]+row_id+':'+border[1]+row_id
            spreadSheet.update(store_id, range_, [label.toArray()])
            label.set_update_status(False)
            time.sleep(sleep_time)
            logger.info('key='+key+':'+label_name)
    

    current_date=getCurrentDate()
    
    
    #only sleep if the application running date match the event date, otherwise, try as fast as possible to catch up
    if(max_generate_date==current_date):
        
        #need to pop out the key which is generate earlier than the current date
        for key in summary_result.keys():
            if key[0:10]<current_date:
                summary_result.pop(key)
        for key in store_result.keys():
            if key[0:10]<current_date:
                logger.debug('pop out '+key)
                store_result.pop(key)
        
        current_count=len(summary_result)
        if current_count!=last_count:
            #sort the worksheet by the 3 column(start from 0)
            spreadSheet.sort(worksheet_name, 2, 5000, 0, 25, [(0,1),(3,1)])
            last_count=current_count
            sheet_location_cache['Summary'].clear()
            logger.info('summary sorted')
        
        
        for store_id in asile_map.keys():
            current_count=len(asile_map.get(store_id))
            if current_count!=asile_last_count[store_id]:
                asile_last_count[store_id]=current_count
                spreadSheet.sort(store_id, 3, 5000, 0, 25, [(0,1),(3,1)])
                sheet_location_cache[store_id].clear()
                logger.info(store_id +' sorted')
        
        #persist the data in case the application corrupt
        with open(hidden_file_name,'wb') as hidden_file:
            protocol=pickle.HIGHEST_PROTOCOL
            pickle.dump(max_id,hidden_file,protocol)
            pickle.dump(summary_result,hidden_file,protocol)
            pickle.dump(store_result,hidden_file,protocol)
            pickle.dump(sheet_location_cache,hidden_file,protocol)
            hidden_file.close()
            
        current_hour=datetime.datetime.now().hour
        if current_hour in long_interval_hours:
            logger.info('I am going to sleep for '+str(poll_interval_long)+'s, see you soon')
            time.sleep(poll_interval_long)
        else:
            logger.info('I am going to sleep for '+str(poll_interval)+'s, see you soon')
            time.sleep(poll_interval)





