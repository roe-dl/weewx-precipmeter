# installer precip
# Copyright 2023 Johanna Roedenbeck
# Distributed under the terms of the GNU Public License (GPLv3)

from weecfg.extension import ExtensionInstaller

def loader():
    return PrecipInstaller()

class PrecipInstaller(ExtensionInstaller):
    def __init__(self):
        super(SNMPInstaller, self).__init__(
            version="0.1",
            name='precipmeter',
            description='',
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
                  'Parsivel2': {
                      'host':'replace_me',
                      'telegram':'%13;%01;%02;%03;%07;%08;%34;%12;%10;%11;%18;/r/n'}}
              },
            files=[('bin/user', ['bin/user/precipmeter.py'])]
            )