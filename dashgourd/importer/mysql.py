import os
from datetime import datetime, timedelta
from urlparse import urlparse
import MySQLdb
from dashgourd.api.actions import ActionsApi
from dashgourd.api.imports import ImportApi
from dashgourd.api.helper import init_mongodb

class MysqlImporter(object):
    """Imports data from MySQL to DashGourd.
    
    The importer can import users and actions.
    Users cannot be imported with actions yet.
    Actions can be imported if the user exists.
    Also make sure field names are labeled correctly.
    Users and Actions require a user_id attribute.
    
    MysqlImporter sends data directly to MongoDb using the 
    actions api. It does not use go through a web service api 
    endpoint. Seems inefficient for large amounts of data.
        
    Attributes:
        mysql_uri: MySQLdb connection string
        mongo_uri: MongoDb Connection string
        mongo_dbname: Name of dashgourd db
        date_format: MySQL timestamp string format
    """
     
    def __init__(self, mysql_uri, mongo_uri, mongo_dbname):
        
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
        
        self.date_format = '%Y-%m-%d %H:%M:%S'

    def get_daterange(self, query_name):
        """Gets date range for queries
        
        Args:
            query_name: Name of query to look up in db.
        
        Returns:
            date_range: Dictionary with start and end dates.
        """
        
        start_date = self.import_api.get_last_update(query_name)
        now = datetime.now()
        end_date = datetime(
            now.year, now.month, now.day, now.hour, 0, 0)     
        
        return {
            'start':start_date.strftime(self.date_format), 
            'end':end_date.strftime(self.date_format)
        }
    
    def import_users(self, query_name, query):
        """Imports users into DashGourd.
        
        The query should contain string format tokens for
        start and end dates. Use {start} and {end}. This is 
        required even if you plan import all data.
        
        The data will be inserted as is into the user collection.
        This method inserts new users and does not update them.
        
        Make sure one field is named `user_id`.
        
        `actions` is reserved for user actions
        
        Note that users are not inserted in batch. 
        They are inserted one at a time. This is to avoid 
        running out of RAM for large imports. But maybe this will 
        be too slow?
        
        Args:
            query_name: Name of query for tracking last update
            query: MySQL query to run
        """
            
        if self.mysql_conn.open:  
            date_range = self.get_daterange(query_name)
            last_update = datetime.strptime(date_range['start'], self.date_format)
                        
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query.format(**date_range))
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):
                data = cursor.fetchone()
                self.actions_api.create_user(data)
                
                if data['created_at'] > last_update:
                    last_update = data['created_at']
            
            self.import_api.set_last_update(query_name, last_update)
            cursor.close()        
        
    def import_actions(self, action_name, query_name, query):
        """Imports actions into DashGourd
        
        The data will be inserted into the embedded document list named
        `actions`.
        
        The data must include the following fields `user_id`, `name`, `created_at`.
        If the data does not contain those fields, then the api will fail silently
        and not insert that row.
        
        Args:
            query_name: Name of query so we can keep track of last update
            action_name: Action name
            query: MySQL query to run
        """
        
        if self.mysql_conn.open:
            date_range = self.get_daterange(query_name)
            last_update = datetime.strptime(date_range['start'], self.date_format)
            
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query.format(**date_range))
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):        
                data = cursor.fetchone()
                data['name'] = action_name
                
                user_id = data['user_id']
                del data['user_id']
                
                self.actions_api.insert_action(user_id, data)
            
                if data['created_at'] > last_update:
                    last_update = data['created_at']
            
            self.import_api.set_last_update(query_name, last_update)            
            cursor.close() 

    def import_abtests(self, abtest, query_name, query):
        """Imports abtests into DashGourd
        
        The data will be inserted into an object named `ab`
        
        The data must include the following fields `user_id`, `abtest`, `variation` 
        and `created_at`. Created at is only used to keep track of last update time.
        
        If the data does not contain those fields, then the api will fail silently
        and not insert that row.
        
        Args:
            query_name: Name of query
            abtest: Ab test name
            query: MySQL query to run
        """        
        if self.mysql_conn.open:
            date_range = self.get_daterange(query_name)
            last_update = datetime.strptime(date_range['start'], self.date_format)
            
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query.format(**date_range))
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):        
                data = cursor.fetchone()
                data['abtest'] = abtest
                
                user_id = data['user_id']
                del data['user_id']
                                
                created_at = data['created_at']
                del data['created_at']
                
                if created_at > last_update:
                    last_update = created_at
                                                    
                self.actions_api.tag_abtest(user_id, data)

            self.import_api.set_last_update(query_name, last_update)            
            cursor.close() 
             
    def close(self):
        """Closes MySQL connection
        
        When the connection is closed, the import methods
        will fail silently for now.
        """
        
        self.mysql_conn.close()