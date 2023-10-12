# installer precip
# Copyright 2023 Johanna Roedenbeck
# Distributed under the terms of the GNU Public License (GPLv3)

from weecfg.extension import ExtensionInstaller

def loader():
    return PrecipInstaller()

class PrecipInstaller(ExtensionInstaller):
    def __init__(self):
        super(SNMPInstaller, self).__init__(
            version="0.8",
            name='precipmeter',
            description='WeeWX-Service to fetch and process disdrometer data',
            author="Johanna Roedenbeck",
            author_email="",
            data_services='user.precipmeter.PrecipData',
            archive_services='user.precipmeter.PrecipArchive',
            config={
              'DataBindings':{
                  'precip_binding':{
                      'database':'precip_sqlite',
                      'table_name':'archive',
                      'manager':'weewx.manager.DaySummaryManager',
                      'schema':'user.precipmeter.schema'}},
              'Databases':{
                  'precip_sqlite':{
                      'database_name':'precipmeter.sdb',
                      'database_type':'SQLite'}},
              'PrecipMeter': {
                  'data_binding':'precip_binding',
                  'weathercodes':'Parsivel2',
                  'visibility':'Parsivel2',
                  'Parsivel2': {
                      'enable':'True',
                      'prefix':'ott',
                      'type':'replace_me',
                      'host':'replace_me',
                      'port':'replace_me',
                      'model':'Ott-Parsivel2',
                      'telegram':'%13;%01;%02;%03;%07;%08;%34;%12;%10;%11;%18;/r/n'},
                  'Thies': {
                      'enable':'True',
                      'prefix':'thies',
                      'type':'replace_me',
                      'host':'replace_me',
                      'port':'replace_me',
                      'model':'Thies-LNM'}}
              },
            files=[('bin/user', ['bin/user/precipmeter.py'])]
            )
