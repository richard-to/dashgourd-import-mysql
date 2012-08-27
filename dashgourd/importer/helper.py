import os.path
import os
import subprocess
from dashgourd.importer.mysql import MysqlImporter
from pytz import timezone

def get_importer_from_env():

    return MysqlImporter(
        os.environ.get('MYSQL_URI'), 
        os.environ.get('MONGO_URI'), 
        os.environ.get('MONGO_DB'),
        timezone(os.environ.get('MYSQL_TZ')))
        
    
def import_users(query_name, query, importer=None):
    
    auto_close = False
    if importer is None:
        importer = get_importer_from_env()
        auto_close = True
    importer.import_users(query_name, query)
    
    if auto_close:
        importer.close()

def import_profile(query_name, query, importer=None):
    
    auto_close = False
    if importer is None:
        importer = get_importer_from_env()
        auto_close = True
    importer.import_profile(query_name, query)
    
    if auto_close:
        importer.close()
    

def import_actions(action_name, query, query_name=None, 
        unique=False, importer=None):
    
    if query_name is None:
        query_name = "{}_{}".format('action', action_name)
        
    auto_close = False
    if importer is None:
        importer = get_importer_from_env()
        auto_close = True

    importer.import_actions(action_name, query_name, query, unique)
   
    if auto_close:
        importer.close()
    
def import_abtests(abtest, query, query_name=None, importer=None):

    if query_name is None:
        query_name = "{}_{}".format('ab', abtest)
            
    auto_close = False
    if importer is None:
        importer = get_importer_from_env()
        auto_close = True

    importer.import_abtests(abtest, query_name, query)

    if auto_close:
        importer.close()
    
def run_scripts(location, args=[], whitelist=None):

    cmd_args = [0]
    cmd_args.extend(args)

    if os.path.isfile(location):
        path, filename = os.path.split(location)
        label, ext = os.path.splitext(filename)
        if whitelist is None or label in whitelist:
            cmd_args[0] = location 
            subprocess.call(cmd_args)          
    else:
        for dirname, dirnames, filenames in os.walk(location):

            for filename in filenames:
                script = os.path.join(dirname, filename)
                label, ext = os.path.splitext(filename)
                if whitelist is None or label in whitelist:
                    cmd_args[0] = script 
                    subprocess.call(cmd_args)
                    