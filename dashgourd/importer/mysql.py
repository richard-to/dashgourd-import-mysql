import os
from datetime import datetime, timedelta
from urlparse import urlparse
import MySQLdb
from pytz import timezone
import pytz
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
    
    Need to determine timezone of timestamps in MySQL in case they 
    were not entered using UTC. This is because MongoDb stores timestamps 
    in UTC and will convert naive DateTime objects based on system timezone. 
    So it is important to convert the mysql timestamp to UTC manually.
    
    Attributes:
        mysql_uri: MySQLdb connection string
        mongo_uri: MongoDb Connection string
        mongo_dbname: Name of dashgourd db
        tz: pytz timezone of MySQL timezone. If none, assumes UTC.
    """
     
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
        """Gets date range for queries
        
        The date range method accounts for timezone differences in 
        mysql and system settings by converting to the stated timezone 
        set in the constructor, which is the timezone of the timestamps 
        stored in mysql. Ideally the mysql timestamp would be in UTC, but 
        that is not always the case.
        
        The last_update value retrieved from mongodb, which will be in UTC 
        format, so we need to convert this to the mysql timestamp.
        
        Args:
            query_name: Name of query to look up in db.
        
        Returns:
            date_range: Dictionary with start and end dates.
        """
        
        start_date_utc = self.import_api.get_last_update(query_name)
        start_date_utc = start_date_utc.replace(tzinfo=pytz.utc)
        end_date = self.tz.localize(datetime.now().replace(minute=0, second=0, microsecond=0))
        start_date = start_date_utc.astimezone(self.tz)
        
        return {
            'start':start_date, 
            'end':end_date
        }
    
    def convert_daterange_for_query(self, daterange):
        """Converts dict of datetime objects to mysql timestamp string
        
        Args:
            daterange: dict with start and end keys for string format.
        
        Returns:
            str_daterange: dict with start/end keys with datetime as mysql timestamp string
        """
        return {
            'start': daterange['start'].strftime(self.date_format),
            'end': daterange['end'].strftime(self.date_format)
        }
        
    def import_users(self, query_name, query):
        """Imports users into Dashgourd.
        
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
            daterange = self.get_daterange(query_name)
            last_update = daterange['start']
            query_params = self.convert_daterange_for_query(daterange)
            
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query.format(**query_params))
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):        
                data = cursor.fetchone()
                             
                data['name'] = action_name
                
                user_id = data['user_id']
                del data['user_id']
                
                created_at = self.tz.localize(data['created_at'])
                if created_at > last_update:
                    last_update = created_at
                    
                data['created_at'] = created_at.astimezone(pytz.utc)   
                                            
                self.actions_api.insert_action(user_id, data)
            
            self.import_api.set_last_update(query_name, last_update.astimezone(pytz.utc))            
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
            daterange = self.get_daterange(query_name)
            last_update = daterange['start']
            query_params = self.convert_daterange_for_query(daterange)
            
            cursor = self.mysql_conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query.format(**query_params))
            numrows = int(cursor.rowcount)
            
            for i in range(numrows):        
                data = cursor.fetchone()
                data['abtest'] = abtest
                
                user_id = data['user_id']
                del data['user_id']

                created_at = self.tz.localize(data['created_at'])
                if created_at > last_update:
                    last_update = created_at
                del data['created_at'] 
                                                                                                   
                self.actions_api.tag_abtest(user_id, data)

            self.import_api.set_last_update(query_name, last_update.astimezone(pytz.utc))            
            cursor.close() 
             
    def close(self):
        """Closes MySQL connection
        
        When the connection is closed, the import methods
        will fail silently for now.
        """
        
        self.mysql_conn.close()