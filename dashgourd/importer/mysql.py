import os
from datetime import datetime, timedelta
from urlparse import urlparse
import MySQLdb
from pytz import timezone
import pytz
import json
from dashgourd.api.actions import ActionsApi
from dashgourd.api.imports import ImportApi
from dashgourd.api.helper import init_mongodb

class MysqlImporter(object):
     
    def __init__(self, mysql_uri, mongo_uri, mongo_dbname, tz=pytz.utc):
        result = urlparse(mysql_uri)
        
        self.mysql_conn = MySQLdb.connect(
            user=result.username,
            passwd=result.password,
            db=result.path[1:],
            host= result.hostname,
            port= result.port)
        
        mongodb = init_mongodb(mongo_uri, mongo_dbname)
        self.actions_api = ActionsApi(mongodb)
        self.import_api = ImportApi(mongodb)
        
        self.tz = tz

        self.date_format = '%Y-%m-%d %H:%M:%S'


    def get_daterange(self, query_name):
        start_date_utc = self.import_api.get_last_update(query_name)
        start_date_utc = start_date_utc.replace(tzinfo=pytz.utc)
        end_date = self.tz.localize(datetime.now().replace(minute=0, second=0, microsecond=0))
        start_date = start_date_utc.astimezone(self.tz)
        
        return {
            'start':start_date, 
            'end':end_date
        }

    
    def convert_daterange_for_query(self, daterange):
        return {
            'start': daterange['start'].strftime(self.date_format),
            'end': daterange['end'].strftime(self.date_format)
        }
      

    def import_users(self, query_name, query):
        if self.mysql_conn.open:  
            daterange = self.get_daterange(query_name)
            last_update = daterange['start']
            query_params = self.convert_daterange_for_query(daterange)
            
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query.format(**query_params))
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):
                data = cursor.fetchone()
                
                created_at = self.tz.localize(data['created_at'])
                if created_at > last_update:
                    last_update = created_at
                
                data['created_at'] = created_at.astimezone(pytz.utc)

                self.actions_api.create_user(data)

            self.import_api.set_last_update(query_name, last_update.astimezone(pytz.utc))
            cursor.close()        
    
    def import_profile(self, query_name, query):
        if self.mysql_conn.open:  
            daterange = self.get_daterange(query_name)
            last_update = daterange['start']
            query_params = self.convert_daterange_for_query(daterange)
            
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query.format(**query_params))
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):
                data = cursor.fetchone()
                
                _id = data['_id']
                del data['_id']

                created_at = self.tz.localize(data['created_at'])
                if created_at > last_update:
                    last_update = created_at
                del data['created_at']

                self.actions_api.update_profile(_id, data)

            self.import_api.set_last_update(query_name, last_update.astimezone(pytz.utc))
            cursor.close()  

    def import_actions(self, action_name, query_name, query, unique=False):
        if self.mysql_conn.open:

            daterange = self.get_daterange(query_name)
            last_update = daterange['start']
            query_params = self.convert_daterange_for_query(daterange)
            
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query.format(**query_params))
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):  
                data = cursor.fetchone()

                _id = data['_id']
                del data['_id']
                             
                data['name'] = action_name

                if 'meta' in data:
                    meta = data['meta']
                    del data['meta']
                    meta_data = json.loads(meta)
                    for key in meta_data:
                        if isinstance(meta_data[key], (int)):
                            meta_data[key] = long(meta_data[key])
                    data = dict(data.items() + meta_data.items())

                created_at = self.tz.localize(data['created_at'])
                if created_at > last_update:
                    last_update = created_at
                    
                data['created_at'] = created_at.astimezone(pytz.utc)   
                                            
                self.actions_api.insert_action(_id, data, unique)
            
            self.import_api.set_last_update(query_name, last_update.astimezone(pytz.utc))            
            cursor.close() 


    def import_abtests(self, abtest, query_name, query):
        if self.mysql_conn.open:
            daterange = self.get_daterange(query_name)
            last_update = daterange['start']
            query_params = self.convert_daterange_for_query(daterange)
            
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query.format(**query_params))
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):        
                data = cursor.fetchone()
                data['abtest'] = abtest
                
                _id = data['_id']
                del data['_id']
                
                created_at = self.tz.localize(data['created_at'])
                if created_at > last_update:
                    last_update = created_at
                del data['created_at'] 
                                                                                                   
                self.actions_api.tag_abtest(_id, data)

            self.import_api.set_last_update(query_name, last_update.astimezone(pytz.utc))            
            cursor.close() 
             

    def close(self):
        self.mysql_conn.close()