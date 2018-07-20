

import requests
import time
import datetime
import logging.config
import yaml
import traceback

import pickle
import os

import shelve

from slackAPI import sendMessage


from googleAPI import SpreadSheet

from sets import Set
from __builtin__ import str

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
    actual_count=0
    color='white'
    last_update_time=None
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
        return ["'"+self.get_start_time(),"'"+self.get_end_time(),+self.message_count,self.actual_count]
    def set_update_status(self,update_status):
        self.update_status=update_status
    def get_update_status(self):
        return self.update_status
    def add_actual_count(self,count):
        self.actual_count+=count
    def get_acutal_count(self):
        return self.actual_count
    def set_color(self,color):
        self.color=color
    
    def get_color(self):
        return self.color
    
    def set_last_update_time(self,time):
        self.last_update_time=time
        
    def get_last_update_time(self):
        return self.last_update_time
    

def alphbat2numeric(alph):
    
    res=0
    
    r_alph=alph[::-1].upper()
    
    index=0
    for c in r_alph:
        res+=alphabat_map[c]*pow(26,index)
        index+=1
    
    
    return res-1


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


def str2Dic(string,delimiter,type_):
    res={}
    try:
        arr=string.split(delimiter)
        for elem in arr:
            elems=elem.split('=')
            k=elems[0]
            v=elems[1]
            
            if 'message'==k:
                if type_=='msg':
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
                elif type_=='processor':
                    tmp_str1='Processed '
                    tmp_str2=' set of size:'
                    tmp_str3=' for header:{'
                    index1=v.index(tmp_str1)
                    index2=v.index(tmp_str2)
                    index3=v.index(tmp_str3)
                    key='label'
                    res[key]=toCamel(v[index1+len(tmp_str1):index2])
                    key='count'
                    res[key]=int(v[index2+len(tmp_str2):index3])
                    tmp_str4=v[index3+len(tmp_str3):len(v)-1]
                    tmp_arr=tmp_str4.split(',')
                    #print(tmp_arr)
                    for s in tmp_arr:
                        ss=s.split(':')
                        res[ss[0].replace('"','')]=ss[1].replace('"','')
            elif 'mission_id'==k:
                v=getMission(v)
            res[k]=v        
    except:
        logger.warn(traceback.format_exc())     
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

#the diff is measured by minute
def dateTime2Local(time_str,diff):
    time_obj=datetime.datetime.strptime(time_str,'%Y-%m-%dT%H:%M:%S')
    res=time_obj+datetime.timedelta(minutes=diff)
    return str(res)

def get_tz():
    
    offset = time.timezone if (time.localtime().tm_isdst == 0) else time.altzone
    tmp=offset/-3600
    h=str(tmp)
    if tmp>0:
        h='+'+h
    tmp= offset%60
    m=str(tmp)
    if tmp<10:
        m='0'+m
    
    return h+':'+m

def date_diff(dt1,dt2):
    dt_format='%Y-%m-%d %H:%M:%S' 
    return datetime.datetime.strptime(dt1,dt_format)-datetime.datetime.strptime(dt2,dt_format)


#the format of the parameter must be (+|-)07:00    
def timezone2min(tz):
    sign=tz[0:1]
    tmp=tz[1:].split(':')
    if len(tmp)!=2:
        logger.info('invalid format '+tz)
        return None
    hour=int(tmp[0])
    minute=int(tmp[1])
    res=hour*60+minute
    if sign=='-':
        return -res
    elif sign=='+':
        return res
    else:
        logger.info('invalid format '+tz)
        return None
        
def toCamel(s):
    if s is None:
        return None
    logger.debug('original='+s)
    s=s.lower()
    strs=s.split('_')
    index=0
    res=''
    for s in strs:
        if index>0:
            s=s.capitalize()
        res+=s
        index+=1
    logger.debug('converted='+res)
    return res

def merger2dicts(x,y):
    z=x.copy()
    z.update(y)
    return z


def getEvents(filters,filter_type,payload):
    res={'max_id':'0','value':[],'max_date':None,'msgs':Set()}
    try:
        logger.debug('filter type is '+filter_type)
        logger.debug(filters)

        payload['q']=filters[filter_type]['q']
        if filter_type=='msg':
            key_max_id='max_id'
            key_min_id='min_id'
            filters[filter_type][key_min_id]=filters[filter_type][key_max_id]
            payload[key_min_id]=filters[filter_type][key_min_id]
            if  payload.has_key(key_max_id):
                payload.pop(key_max_id)
        elif filter_type=='processor':
            key='min_id'
            payload[key]=filters['msg'][key]
            key='max_id'
            payload[key]=filters['msg'][key]
            
        #logger.info(r.url)
        #logger.info(r.headers)
        logger.debug(payload)
        r=requests.get(uri,params=payload,headers=headers)
        json=r.json()
    except:
        logger.info(traceback.format_exc())
        logger.info('status code is '+str(r.status_code))
        raise Exception('Something bad happened when requesting paperTrail')
        

    #time.sleep(1000000)
    events=json['events']
    max_id=json['max_id']
    res['max_id']=max_id
    
    
    #make sure that the event for processor doesn't exceed the
    #max id of msg, as processor data is based on the msg data 
    filters[filter_type]['max_id']=max_id


    logger.info('max_id='+max_id)
    logger.info(filter_type+": I get "+str(len(events))+' events')
    for event in events:
        event_message=event['message']
        event_map=str2Dic(event_message,' | ',filter_type)
        generated_time=event['generated_at']#the time formate is 2018-07-05T16:04:08-04:00
        #logger.info('original='+generated_time)
        store_id=event_map['store_id']
        if store_id not in walmart_stores:
            logger.warning('store '+store_id+' should not committing data to walmart')
            continue
        if not event_map.has_key('label'):
            logger.warning('no label found in the event map')
            continue
        else:
            label=event_map['label']
            if label not in components:
                logger.warning('no such label '+label+' configured')
                continue
        
        log_tz=generated_time[-6:]
        log_min=timezone2min(log_tz)
        local_tz=walmart_stores[store_id]['timeZone']
        local_min=timezone2min(local_tz)
        diff=local_min-log_min
        generated_time=dateTime2Local(generated_time[0:19], diff)
        #logger.info('localTime='+generated_time+',tz is '+local_tz)
        generate_date=generated_time[0:10]
        if(res['max_date'] is None or res['max_date']>max_generate_date):
            res['max_date']=generate_date
        event_map['generate_time']=generated_time
        if len(event_map)<13:
            logger.info('bad event below:')
            logger.info(event)
            continue
        #logger.info(event_map)
        msg_id=event_map['message_id']
        if msg_id=='NULL_VALUE':
            logger.debug(event)
        res['msgs'].add(msg_id)
        res['value'].append(event_map)
    return res
 
def updateLabels(result_list,update_type,msg_set):
    global asile_map
    global summary_result
    global store_result
    now=datetime.datetime.now()
    for event_map in result_list:
        if update_type=='msg':
            asile_name=event_map['aisle']
        elif update_type=='processor':
            asile_name=event_map['aisle_id']
            msg_id=event_map['message_id']
            if msg_id not in msg_set:
                logger.warning(update_type+':'+msg_id+' not in the set')
                logger.warning(event_map)
                continue
        store_id=event_map['store_id']
        generate_time=event_map['generate_time']
        generate_time_only=generate_time[11:19]
        generate_date=generate_time[0:10]
        mission_id=event_map['mission_id']
        label_name=event_map['label']
        zone_name=event_map['zone']
        asile=zone_name+asile_name
        
        #add each asile in the set group by store_id
        asile_map[store_id].add(asile)
        
        summary_key=generate_date+key_delimiter+store_id+key_delimiter+mission_id
        store_key=summary_key+key_delimiter+asile
        if update_type=='msg':
            summary_value={}
            if summary_result.has_key(summary_key):
                summary_value=summary_result.get(summary_key)
            summary_label_value=Label(label_name)
            if summary_value.has_key(label_name):
                summary_label_value=summary_value.get(label_name)
            summary_label_value.increase()
            summary_label_value.set_update_status(True)
            summary_label_value.set_last_update_time(now)
            min_time=summary_label_value.get_start_time()
            if(min_time is None or min_time>generate_time_only):
                summary_label_value.set_start_time(generate_time_only)
            max_time=summary_label_value.get_end_time()
            if(max_time is None or max_time<generate_time_only):
                summary_label_value.set_end_time(generate_time_only)
            summary_value[label_name]=summary_label_value
            summary_result[summary_key]=summary_value
         
             
            store_value={}
            if(store_result.has_key(store_key)):
                store_value=store_result.get(store_key)
            store_label_value=Label(label_name)
            if(store_value.has_key(label_name)):
                store_label_value=store_value[label_name]
            store_label_value.increase()
            store_label_value.set_update_status(True)
            min_time=store_label_value.get_start_time()
            if(min_time is None or min_time>generate_time_only):
                store_label_value.set_start_time(generate_time_only)
            max_time=store_label_value.get_end_time()
            if(max_time is None or max_time<generate_time_only):
                store_label_value.set_end_time(generate_time_only)   
            store_value[label_name]=store_label_value
            store_result[store_key]=store_value
        elif update_type=='processor':
            if not summary_result.has_key(summary_key):
                logger.error('there is no such summary key:'+summary_key)
                continue
            summary_value=summary_result.get(summary_key)
            if(not summary_value.has_key(label_name)):
                logger.error('there is no such label:'+label_name)
                continue  
            summary_label_value=summary_value.get(label_name)
            count=event_map['count']
            summary_label_value.add_actual_count(count)
            summary_value[label_name]=summary_label_value
            summary_result[summary_key]=summary_value
            
            if(not store_result.has_key(store_key)):
                logger.error('there is no such store key:'+store_key)
                continue
            store_value=store_result.get(store_key)    
            store_label_value=Label(label_name)
            if(not store_value.has_key(label_name)):
                logger.error('there is no such label:'+label_name)
                continue
                
            store_label_value=store_value[label_name]    
            store_label_value.add_actual_count(count)
            store_value[label_name]=store_label_value
            store_result[store_key]=store_value

def translateArr(arr):
    mission_md5=arr[2]
    mission_readable=getMission(mission_md5)
    reable_arr=list(arr)
    reable_arr[2]=mission_readable
    return reable_arr

def write2Spreadsheet():
    global summary_result
    global store_result
    global sheet_location_cache
    global spreadSheet
    worksheet_name='Summary'
    if not sheet_location_cache.has_key(worksheet_name):
        sheet_location_cache[worksheet_name]={}
    row_location_cache=sheet_location_cache[worksheet_name]
    #now=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
    now=datetime.datetime.now()
    for key in summary_result.keys():
        key_array=key.split(key_delimiter)
        store_id=key_array[1]
        if not row_location_cache.has_key(key):
            logger.info('no cache for key '+key)
            location=spreadSheet.getLocationByValue(worksheet_name, None,key_array)
            time.sleep(sleep_time)
            if location is None or len(location)==0:
                #generate the expected data, there are 24 cells between the primary key and the expected result
                cell_no=24
                fill_stuff=[' ']*cell_no
                fill_stuff.append(expected_notification_count[store_id])
                fill_stuff.append(store_tz[store_id])
                logger.info([key_array+fill_stuff])
                spreadSheet.insert(worksheet_name, 'A4:AB4', [key_array+fill_stuff])
                location=spreadSheet.getLocationByValue(worksheet_name, None,key_array)
            row_id=location[len(location)-1]
            row_location_cache[key]=row_id
        row_id=row_location_cache[key]
        label_map=summary_result.get(key)
        for label_name in label_map.keys():
            label=label_map.get(label_name)
            border=component_border[label_name]
            range_=border[0]+row_id+':'+border[1]+row_id
            endRowIndex=int(row_id)
            if not label.get_update_status():
                logger.debug('No update for key='+key+',label='+label_name)
                current_color=label.get_color()
                logger.info('current color is '+current_color)
                if current_color=='green':
                    diff=now-label.get_last_update_time()
                    logger.debug('diff is '+str(diff))
                    if diff.days>0 or diff.seconds>color_change_interal:
                        spreadSheet.format_cell(worksheet_name, endRowIndex-1, endRowIndex, alphbat2numeric(border[0]), alphbat2numeric(border[1])+1,WHITE)
                        label.set_color('white')
                elif current_color=='white':
                    end_time=label.get_end_time()
                    if end_time is None:
                        logger.debug('XXXXXXXXXXXXXXXXXXXXXend_time is None')
                        spreadSheet.format_cell(worksheet_name, endRowIndex-1, endRowIndex, alphbat2numeric(border[0])+1, alphbat2numeric(border[1])+1,RED)
                    elif str(end_time)>warning_latest_time:
                        spreadSheet.format_cell(worksheet_name, endRowIndex-1, endRowIndex, alphbat2numeric(border[0])+1, alphbat2numeric(border[0])+2,RED)
                    label.set_color('red')
                continue
            logger.info(key+':'+label_name)
            spreadSheet.format_cell(worksheet_name, endRowIndex-1, endRowIndex, alphbat2numeric(border[0])+1, alphbat2numeric(border[1])+1,BLUE)
            spreadSheet.update(worksheet_name, range_, [label.toArray()])
            time.sleep(sleep_time)
            spreadSheet.format_cell(worksheet_name, endRowIndex-1, endRowIndex, alphbat2numeric(border[0])+1, alphbat2numeric(border[1])+1,GREEN)
            label.set_update_status(False)
            label.set_color('green')
            time.sleep(sleep_time)        
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
        if not sheet_location_cache.has_key(store_id):
            sheet_location_cache[store_id]={}
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
            border=component_border[label_name]
             
            range_=border[0]+row_id+':'+border[1]+row_id
            spreadSheet.update(store_id, range_, [label.toArray()])
            label.set_update_status(False)
            time.sleep(sleep_time)
            logger.info('key='+key+':'+label_name)
     
def persistentData():
    #persist the data in case the application corrupt
        with open(hidden_file_name,'wb') as hidden_file:
            protocol=pickle.HIGHEST_PROTOCOL
            pickle.dump(filters,hidden_file,protocol)
            pickle.dump(summary_result,hidden_file,protocol)
            pickle.dump(store_result,hidden_file,protocol)
            pickle.dump(sheet_location_cache,hidden_file,protocol)

def getMission(md5):
    data=None
    global mission_map
    if mission_map.has_key(md5):
        return mission_map[md5]
    else:
        try:
            logger.info('I am going to wait for 5 minutes for the file to update,'+md5)
            time.sleep(5*60) #according to the bryan, the file is updated for every 5 minutes
            data = shelve.open(mission_file)
            mission_map=dict(data)
            if not mission_map.has_key(md5):
                msg='no value for '+md5
                logger.warning(msg)
                #the id is for bryan
                sendMessage('UB98C31UK', msg,'dataSummary')
                
            else:
                return mission_map[md5]
        except:
            logger.error(traceback.format_exc())   
        finally:
            try:
                data.close()
            except:
                logger.info('close file error')
   
    return md5

config=None
mission_map=None
with open('properties.yaml','r') as stream:
    config=yaml.load(stream)


TIMEZONE=get_tz()
GREEN=[0,128,0]
WHITE=[255,255,255]
BLUE=[0,0,255]
RED=[255,0,0]

mission_file=config['mission']['map_location']
mission_map={}
try:
    data = shelve.open(mission_file)
    mission_map=dict(data)
    data.close()
except:
    logger.warning('Something wrong with mission map')

poll_intervals=config['pollIntervals']
poll_interval=poll_intervals['normal']['interval']
long_poll_interval=poll_intervals['long']['interval']
long_poll_days=poll_intervals['long']['days']
long_poll_hours=poll_intervals['long']['hours']

spredSheet_info=config['spreadSheet']        
spreadSheet=SpreadSheet(spredSheet_info['id'],spredSheet_info['name'])
sleep_time=spredSheet_info['sleepTime']
walmart_stores=config['stores']['walmart']

alphabat_map=config['alphabet']
warnings=config['warning']
warning_latest_time=warnings['latest_time']
color_change_interal=warnings['color_change_interval']


paperTrail_info=config['paperTrail']
payload=paperTrail_info['payload']
uri=paperTrail_info['uri']
headers=paperTrail_info['headers']

filters=paperTrail_info['filterType']

filter_processor='"for header" ('
#generate the filter which is used to filter data
filter_str='('
index=1
#label location map, it is used to know the startColumn and endColumn of the component
component_border={}
components=config['components']
components_len=len(components)
for component,lable_location in components.iteritems():
    filter_str+='/'+component
    component_border[component]=[lable_location['startColumn'],lable_location['endColumn']]
    if(index!=components_len):
        filter_str+=' OR '
    index+=1
filter_str+=') INFO "Status Code: 200" ('

index=1
#used to reduce the number of googleAPI calls
sheet_location_cache={'Summary':{}}#key is the work_sheet id, value is a map{key is the primary key, value is the row number}, summary is not configured, so initialized 
#key count in the summary result map
last_count=0
asile_last_count={}
asile_map={}#key:store_id, value=asile set
#generate the expected_notification_count
expected_notification_count={}
#generate the timezone of the store
store_tz={}
walmart_store_count=len(walmart_stores)
for store_id in walmart_stores.keys():
    expected_count=walmart_stores[store_id]['expectedNotificationCount']
    expected_notification_count[store_id]=expected_count
    tz=walmart_stores[store_id]['timeZone']
    store_tz[store_id]=tz
    sheet_location_cache[store_id]={}
    asile_last_count[store_id]=0
    asile_map[store_id]=Set()
    filter_str+='store_id='+store_id
    filter_processor+='store_id='+store_id
    if(index!=walmart_store_count):
        filter_str+=' OR '
        filter_processor+=' OR '
    index+=1
filter_str+=')'
filter_processor+=')'
logger.info(filter_str)
logger.info(filter_processor)

#overwrite the stored filter 
filters['processor']['q']=filter_processor
filters['msg']['q']=filter_str


suffix='.data'
hidden_file_name='.pickle.bin'
summary_result={}#key is date_storeNo_mission_id, value is labelMap
label_map={}#key is the lableName, value is label class
store_result={}#key is date_storeNo_missionId_aisleNo, value is labelMap
key_delimiter='$$'


if os.path.exists(hidden_file_name):
    with open(hidden_file_name,'rb') as hidden_file:
        filters=pickle.load(hidden_file)
        summary_result=pickle.load(hidden_file)
        store_result=pickle.load(hidden_file)
        sheet_location_cache=pickle.load(hidden_file)
    hidden_file.close()



# #if the configuration want to rewrite the stored value, overwrite it
# if payload['min_id']!='0':
#     for key in filters.keys():
#         filters[key]['min_id']=payload['min_id']
# if payload['max_id']!='0':
#     for key in filters.keys():
#         filters[key]['max_id']=payload['min_id']


max_generate_date=getCurrentDate()


while True: 
    
    #as we don't have any run during the weekend, so the max_generate_date will be less than the current date all the time during weekend,
    #always sleep for the long poll interval during weekend and no need to run the code below it 
    day_of_week=datetime.datetime.today().weekday()
    if day_of_week in long_poll_days: 
        logger.info('I am going to sleep for '+str(long_poll_interval)+'s, see you soon')
        time.sleep(long_poll_interval)
        
        
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
            worksheet_name='Summary'
            #sort based on the date and the timestamp of 1st message of label
            spreadSheet.sort(worksheet_name, 2, 5000, 0, 50, [(0,1),(3,1)])
            last_count=current_count
            sheet_location_cache[worksheet_name].clear()
            logger.info(worksheet_name+' sorted')
        
        
        for store_id in asile_map.keys():
            current_count=len(asile_map.get(store_id))
            if current_count!=asile_last_count[store_id]:
                asile_last_count[store_id]=current_count
                #sort based on the date and the mission id and the ts of 1st message of label, as label may not generated for the aisle
                spreadSheet.sort(store_id, 2, 5000, 0, 50, [(0,1),(2,1),(3,1)])
                sheet_location_cache[store_id].clear()
                logger.info(store_id +' sorted')
        
        #persist the data before sleeping
        persistentData()   
        
        current_hour=datetime.datetime.now().hour      
        if current_hour in long_poll_hours:
            logger.info('I am going to sleep for '+str(long_poll_interval)+'s, see you soon')
            time.sleep(long_poll_interval)
        else:
            logger.info('I am going to sleep for '+str(poll_interval)+'s, see you soon')
            time.sleep(poll_interval)


   
    filter_type='msg'
    try:
        res=getEvents(filters,filter_type,payload)
    except:
        logger.info(traceback.format_exc())
        logger.info('I am going to sleep for '+str(long_poll_interval)+' before next try')
        time.sleep(long_poll_interval)
        continue
    result_list=res['value']
    
    
    if len(result_list)==0:
        continue# it needs to be here, as the processor request may get data earlier than the msg request
    
    if max_generate_date<res['max_date']:
        max_generate_date=res['max_date']
    updateLabels(result_list, filter_type,None)
    
    
    logger.debug('after msg request and before the processor request')
    logger.debug(filters)
    
    msg_set=res['msgs']
    set_len=len(msg_set)
    logger.info('msgs count is '+str(set_len))
    filter_type='processor'
    
    try:
        res=getEvents(filters,filter_type,payload)
    except:
        logger.info(traceback.format_exc())
        logger.info('I am going to sleep for '+str(long_poll_interval)+' before next try')
        time.sleep(long_poll_interval)
        continue
    result_list=res['value']
    updateLabels(result_list, filter_type,msg_set)
    
    #add the components which are not generated
    for pkey in summary_result.keys():
        label_map=summary_result.get(pkey)
        for component in components.keys():
            if component not in label_map.keys():
                label=Label(component)
                label_map[component]=label
        
        
    for pkey in store_result.keys():
        label_map=store_result.get(pkey)
        for component in components.keys():
            if component not in label_map.keys():
                label=Label(component)
                label_map[component]=label
    
    
    #push data to google sheet
    write2Spreadsheet()
  
    





