#!/usr/bin/python3
# Precipmeter Service for WeeWX
# Copyright (C) 2023 Johanna Roedenbeck

"""

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""

VERSION = "0.1"

"""

    Radarreflektivität (Z)
    https://de.wikipedia.org/wiki/Reflektivität_(Radar)
    https://www.dwd.de/DE/leistungen/radarniederschlag/rn_info/download_niederschlagsbestimmung.pdf;jsessionid=A87B2F1098BF26972202EC007A15F634.live21072?__blob=publicationFile&v=4
    
    radar reflectivity factor (Z)
    https://en.wikipedia.org/wiki/DBZ_(meteorology)
    
    meteorologische Sichtweite MOR
    meteorological optical range MOR
    
"""

import threading 
import configobj
import time
import copy
import json

# deal with differences between python 2 and python 3
try:
    # Python 3
    import queue
except ImportError:
    # Python 2
    # noinspection PyUnresolvedReferences
    import Queue as queue

if __name__ == '__main__':

    import sys
    sys.path.append('/usr/share/weewx')
    
    def logdbg(x):
        print('DEBUG',x)
    def loginf(x):
        print('INFO',x)
    def logerr(x):
        print('ERROR',x)

else:

    try:
        # Test for new-style weewx logging by trying to import weeutil.logger
        import weeutil.logger
        import logging
        log = logging.getLogger("user.PrecipMeter")

        def logdbg(msg):
            log.debug(msg)

        def loginf(msg):
            log.info(msg)

        def logerr(msg):
            log.error(msg)

    except ImportError:
        # Old-style weewx logging
        import syslog

        def logmsg(level, msg):
            syslog.syslog(level, 'user.PrecipMeter: %s' % msg)

        def logdbg(msg):
            logmsg(syslog.LOG_DEBUG, msg)

        def loginf(msg):
            logmsg(syslog.LOG_INFO, msg)

        def logerr(msg):
            logmsg(syslog.LOG_ERR, msg)

import weewx
from weewx.engine import StdService
import weeutil.weeutil
import weewx.accum

ACCUM_SUM = { 'extractor':'sum' }
ACCUM_STRING = { 'accumulator':'firstlast','extractor':'last' }
ACCUM_LAST = { 'extractor':'last' }

# Ott Parsivel 2 
PARSIVEL = {
  #Nr,Beschreibung,Stellen,Form,Größe,Einheit,Gruppe
  # device information and identification
  (22,'Stationsname',10,'XXXXXXXXXX',None,'string',None),
  (23,'Stationsnummer',4,'XXXX',None,'string',None),
  (13,'Sensor Seriennummer',6,'123456','SNR','string',None),
  (14,'Versionsnummer Firmware Bootloader',6,'2.02.3',None,'string',None),
  (15,'Versionsnummer Firmware Firmware',6,'2.02.3',None,'string',None),
  ( 9,'Abfrageintervall',5,'00000','queryInterval','second','group_interval'),
  # device state
  (18,'Sensorstatus',1,'0','sensorState',None,None),
  (25,'Fehlercode',3,'000','errorCode',None,None),
  # date and time
  (19,'Datum/Uhrzeit Messbeginn',19,'00.00.0000_00:00:00',None,'string',None),
  (20,'Sensorzeit',8,'00:00:00',None,'string',None),
  (21,'Sensordatum',10,'00.00.0000',None,'string',None),
  # readings: present weather code
  ( 3,'Wettercode nach SYNOP wawa Tabelle 4680',2,'00','wawa','byte','group_data'),
  ( 4,'Wettercode nach SYNOP ww Tabelle 4677',2,'00','ww','byte','group_data'),
  ( 5,"Wettercode METAR/SPECI w'w' Tabelle 4678",5,'+RASN','METAR','string',None),
  ( 6,'Wettercode nach NWS Code',4,'RLS+','NWS','string',None),
  # readings 32 bit
  ( 1,'Regenintensität (32bit)',8,'0000.000','rainRate','mm_per_hour','group_rainrate'),
  ( 2,'Regenmenge akkumuliert (32bit)',7,'0000.00','rainAkku','mm','group_rain'),
  (24,'Regenmenge absolut (32bit)',7,'000.000','rainAbs','mm','group_rain'),
  ( 7,'Radarreflektivität (32bit)',6,'00.000','dBZ','db','group_db'),
  # readings 16 bit (not necessary if 32 bit readings can be used)
  (30,'Regenintensität (16bit) max 30 mm/h',6,'00.000',None,'mm_per_hour','group_rainrate'),
  (31,'Regenintensität (16bit) max 1200 mm/h',6,'0000.0',None,'mm_per_hour','group_rainrate'),
  (32,'Regenmenge akkumuliert (16bit)',7,'0000.00',None,'mm','group_rain'),
  (33,'Radarreflektivität (16bit)',5,'00.00',None,'db','group_db'),
  # other readings
  ( 8,'MOR Sichtweite im Niederschlag',5,'00000','visibility','meter','group_distance'),
  (10,'Signalamplitude des Laserbandes',5,'00000','signal','count','group_count'),
  (11,'Anzahl der erkannten und validierten Partikel',5,'00000','particle','count','group_count'),
  (12,'Temperatur im Sensorgehäuse',3,'000','housingTemp','degree_C','group_temperature'),
  (16,'Strom Sensorkopfheizung',4,'0.00','heatingCurrent','amp','group_amp'),
  (17,'Versorgungsspannung',4,'00.0','supplyVoltage','volt','group_volt'),
  (26,'Temperatur Leiterplatte',3,'000','circuitTemp','degree_C','group_temperature'),
  (27,'Temperatur im Sensorkopf rechts',3,'000','rightSensorTemp','degree_C','group_temperature'),
  (28,'Temperatur im Sensorkopf links',3,'000','leftSensorTemp','degree_C','group_temperature'),
  (34,'kinetische Energie',7,'000.000','energy','J/(m^2h)',''),
  (35,'Schneehöhen-Intensität (volumenäquivalent)',7,'0000.00','snowRate','mm_per_hour','group_rainrate'),
  # special data
  (60,'Anzahl aller erkannten Partikel',8,'00000000','particleCount','count','group_count'),
  (61,'Liste aller erkannten Partikel',13,'00.000;00.000',None,'mm;m/s',None),
  (90,'Feld N(d)',223,'00.000S',None,'log10(1/m^3 mm)',None),
  (91,'Feld v(d)',223,'00.000S',None,'meter_per_second',None),
  (93,'Rohdaten',4095,'000S',None,None,None)
}
PARSIVEL_WAWA = 3
PARSIVEL_WW = 4

##############################################################################
#    Database schema                                                         #
##############################################################################

exclude_from_summary = ['dateTime', 'usUnits', 'interval']

table = [('dateTime',             'INTEGER NOT NULL UNIQUE PRIMARY KEY'),
         ('usUnits',              'INTEGER NOT NULL'),
         ('interval',             'INTEGER NOT NULL')] 

def day_summaries(table):
    return [(e[0], 'scalar') for e in table
                 if e[0] not in exclude_from_summary and e[1]=='REAL'] 

schema = {
    'table': table,
    'day_summaries' : day_summaries(table)
    }

##############################################################################

def issqltexttype(x):
    """ Is this a string type in SQL? """
    if x is None: return None
    x = x.upper().split('(')[0].strip()
    return x in ('TEXT','CLOB','CHARACTER','VARCHAR','VARYING CHARACTER','NCHAR','NATIVE CHARACTER','NVARCHAR')
    
class PrecipThread(threading.Thread):

    def __init__(self, name, conf_dict, data_queue, query_interval):
    
        super(PrecipThread,self).__init__(name='PrecipMeter-'+name)

        self.telegram = conf_dict['telegram']
        self.telegram_list = conf_dict['loop']
        self.model = conf_dict.get('model','Ott-Parsivel2').lower()
        
        self.data_queue = data_queue
        self.query_interval = query_interval
        self.presentweather_list = []
        
        self.running = True

    def shutDown(self):
        """ request thread shutdown """
        self.running = False
        loginf("thread '%s': shutdown requested" % self.name)
        
    def presentweather(self, ts, ww, wawa):
        if ww is not None: ww = int(ww)
        if wawa is not None: wawa = int(wawa)
        # check if the actual weather code is different from the previous one
        if len(self.presentweather_list)==0:
            add = True
        else:
            add = (wawa!=self.presentweather_list[-1][3] or
                   ww!=self.presentweather_list[-1][2])
        #print(1,ts,ww,wawa,add)
        # add a new record or update the timestamp
        if add:
            self.presentweather_list.append([ts,ts,ww,wawa])
        else:
            self.presentweather_list[-1][1] = ts
        # remove the first element if it ends more than an hour ago
        if self.presentweather_list[0][1]<ts-3600:
            self.presentweather_list.pop(0)
        #print(4,self.presentweather_list)
        # Now we have a list of the weather codes of the last hour.
        if len(self.presentweather_list)<2:
            # The weather did not change during the last hour.
            return ww, wawa
        if (len(self.presentweather_list)==2 and 
            self.presentweather_list[0][2] and 
            self.presentweather_list[0][3]):
            # No weather condition at the beginning of the last hour,
            # then one weather condition.
            return ww, wawa
        # which weather how long?
        WW2 = {
            20: (50,51,52,53,54,55),
            21: (60,61,62,63,64,65),
            22: (70,71,72,73,74,75),
            23: (68,69),
            24: (56,57,66,67),
            25: (80,81,82),
            26: (85,86),
            27: (87,88,89,90),
            28: (41,42,43,44,45,46,47,48,49),
            29: (95,96,97,98,99)
        }
        WAWA2 = {
            20: (30,31,32,33,34,35),
            21: (40,41,42),
            22: (50,51,52,53,57,58),
            23: (60,61,62,63,67,68,43,44),
            24: (70,71,72,73,74,75,76,45,46),
            25: (54,55,56,64,65,66,47,48),
            26: (90,91,92,93,94,95,96)
        }
        wawa_dict = dict()
        ww_dict = dict()
        for ii in self.presentweather_list:
            if ii[0]>ts-3600:
                # time span
                duration = ii[1]-ii[0]
            else:
                # the last hour counts only
                # TODO: Ist das wirklich sinnvoll?
                duration = ii[1]-ts+3600
            ii_wawa = ii[3]
            for key,val in WAWA2.items():
                if ii_wawa in val:
                    ii_wawa = key
                    break
            if ii_wawa not in wawa_dict:
                wawa_dict[ii_wawa] = 0
            wawa_dict[ii_wawa] += duration
            ii_ww = ii[2]
            for key,val in WW2.items():
                if ii_ww in val:
                    ii_ww = key
                    break
            if ii_ww not in ww_dict:
                ww_dict[ii_ww] = 0
            ww_dict[ii_ww] += duration
        # One kind of weather only (not the same code all the time, but
        # always rain or always snow etc.)
        if len(wawa_dict)==1:
            return ww, wawa
        if len(ww_dict)==1:
            return ww, wawa
        # Is there actually some weather condition?
        if wawa or ww:
            # weather detected
            # TODO: detect showers
            return ww, wawa
        else:
            # The weather ended within the last hour. That means, the
            # weather code is 20...29.
            duration_since = self.presentweather_list[-1][1]-self.presentweather_list[-1][0]
            if 0 in wawa_dict:
                wawa_dict[0] -= duration_since
            if 0 in ww_dict:
                ww_dict[0] -= duration_since
            # sort weather conditions by time
            wawa_list = sorted(wawa_dict.items(),key=lambda x:x[1],reverse=True)
            ww_list = sorted(ww_dict.items(),key=lambda x:x[1],reverse=True)
            """
            # sum of time
            wawa_dur = sum([x[1] for x in wawa_list if x[1] is not None and x[1]!=0])
            wawa_dur0 = sum([x[1] for x in wawa_list if x[1] is not None and x[1]==0])
            ww_dur = sum([x[1] for x in ww_list if x[1] is not None and x[1]!=0])
            ww_dur0 = sum([x[1] for x in ww_list if x[1] is not None and x[1]==0])
            #
            if (wawa_dur<=wawa_dur0) and (ww_dur<=ww_dur0):
                return ww, wawa
            """
            return ww_list[0][0], wawa_list[0][0]
    
    def getRecord(self, ot):
    
        if __name__ == '__main__':
            print()
            print('-----',self.name,'-----',ot,'-----')

        reply = "200248;000.000;0000.00;00;-9.999;9999;000.00;025;15759;00000;0;\r\n"
        
        ts = time.time()
        ww = None
        wawa = None
        # record contains value tuples here.
        record = dict()
        if self.model=='ott-parsivel2':
            for ii in self.telegram_list:
                # if there are not enough fields within the data telegram
                # stop processing
                if not reply: break
                # split the first remaining field 
                # TODO: separator other than semikolon
                x = reply.split(';',1)
                try:
                    val = x[0]
                except LookupError:
                    val = ''
                try:
                    reply = x[1]
                except LookupError:
                    reply = ''
                # convert the field value string to the appropriate data type
                try:
                    if ii[0]==19:
                        # date and time
                        val = (...,'unixepoch','group_time')
                    elif ii[5]=='string':
                        # string
                        val = (str(val),None,None)
                    elif ii[7]=='INTEGER':
                        # counter, wawa, ww
                        val = (int(val),ii[5],ii[6])
                    elif ii[7]=='REAL':
                        # float
                        val = (float(val),ii[5],ii[6])
                    else:
                        print('error')
                    if ii[4]:
                        # ii[4] already includes prefix here.
                        record[ii[4]] = val
                    # remember weather codes
                    if ii[0]==PARSIVEL_WAWA: wawa = val[0]
                    if ii[0]==PARSIVEL_WW: ww = val[0]
                except (LookupError,ValueError,TypeError,ArithmeticError) as e:
                    pass
        try:
            ww, wawa = self.presentweather(ts, ww, wawa)
            if ww is not None: record['ww'] = (ww,'byte','group_data')
            if wawa is not None: record['wawa'] = (wawa,'byte','group_data')
        except (LookupError,ValueError,TypeError,ArithmeticError):
            pass
        self.put_data(record)
        
    def put_data(self, x):
        if x:
            if self.data_queue:
                try:
                    self.data_queue.put((self.name,x),
                                block=False)
                except queue.Full:
                    # If the queue is full (which should not happen),
                    # ignore the packet
                    pass
                except (KeyError,ValueError,LookupError,ArithmeticError) as e:
                    logerr("thread '%s': %s" % (self.name,e))

    def run(self):
        loginf("thread '%s' starting" % self.name)
        #try:
        if True:
            self.getRecord('once')
            while self.running:
                self.getRecord('loop')
                time.sleep(self.query_interval)
        #except Exception as e:
        #    logerr("thread '%s': %s" % (self.name,e))
        #finally:
        #    loginf("thread '%s' stopped" % self.name)


class PrecipData(StdService):

    def __init__(self, engine, config_dict):
        super(PrecipData,self).__init__(engine, config_dict)
        loginf("PrecipMeter service version %s" % VERSION)
        site_dict = weeutil.config.accumulateLeaves(config_dict.get('PrecipMeter',configobj.ConfigObj()))
        self.log_success = weeutil.weeutil.to_bool(site_dict.get('log_success',True))
        self.log_failure = weeutil.weeutil.to_bool(site_dict.get('log_failure',True))
        self.debug = weeutil.weeutil.to_int(site_dict.get('debug',0))
        if self.debug>0:
            self.log_success = True
            self.log_failure = True
        self.threads = dict()
        self.dbm = None
        self.archive_interval = 300
        if 'PrecipMeter' in config_dict:
            ct = 0
            for name in config_dict['PrecipMeter'].sections:
                dev_dict = weeutil.config.accumulateLeaves(config_dict['PrecipMeter'][name])
                if 'loop' in config_dict['PrecipMeter'][name]:
                    dev_dict['loop'] = config_dict['PrecipMeter'][name]['loop']
                if weeutil.weeutil.to_bool(dev_dict.get('enable',True)):
                    if self._create_thread(name,dev_dict):
                        ct += 1
            if ct>0 and __name__!='__main__':
                self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
                self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

    def _create_thread(self, thread_name, thread_dict):
        host = thread_dict.get('host')
        query_interval = thread_dict.get('query_interval',5)
        # IP address is mandatory.
        if not host:
            logerr("thread '%s': missing IP address" % thread_name) 
            return False
        loginf("thread %s, host %s, poll interval %s" % (thread_name,host,query_interval))
        # telegram config
        model = thread_dict.get('model','Ott-Parsivel2').lower()
        if model=='ott-parsivel2' and not 'loop' in thread_dict:
            # convert Ott Parsivel2 telegram configuration string to the
            # internal structure
            # Note: If that does not meet your needs, use a [loop]
            #       section instead to define the telegram structure
            #       and the observation types.
            # Ott telegram: %13;%01;%02;%03;%07;%08;%34;%12;%10;%11;%18;/r/n
            if 'telegram' not in thread_dict:
                """
                t = ""
                for ii in PARSIVEL:
                    if ii[4]:
                        t += "%%%02d;" % ii[0];
                t += '/r/n'
                thread_dict['telegram'] = t
                """
                thread_dict['telegram'] = "%13;%01;%02;%03;%07;%08;%34;%12;%10;%11;%18;/r/n"
            # parse config string
            t = []
            ct = None
            for ii in thread_dict['telegram']:
                if ct is not None:
                    if ii.isdigit():
                        ct.append(ii)
                    else:
                        nr = int(''.join(ct))
                        for jj in PARSIVEL:
                            if jj[0]==nr:
                                obstype = jj[4]
                                # TODO prefix
                                if 'prefix' in thread_dict and jj[4]:
                                    obstype = thread_dict['prefix']+jj[4][0].upper()+jj[4][1:]
                                else:
                                    obstype = jj[4]
                                if jj[6]=='group_count' or jj[0] in (PARSIVEL_WAWA,PARSIVEL_WW):
                                    obsdatatype = 'INTEGER'
                                elif jj[5]=='string':
                                    obsdatatype = 'VARCHAR(%d)' % jj[2]
                                else:
                                    obsdatatype = 'REAL'
                                t.append(jj[0:4]+(obstype,)+jj[5:]+(obsdatatype,))
                                break
                        ct = None
                elif ii=='%':
                    ct = []
            thread_dict['loop'] = t
        else:
            # another device than Ott Parsivel2 or special configuration
            # convert [loop] section to internal structure
            t = []
            for ii in thread_dict['loop']:
                obstype = thread_dict['loop'][ii].get('name')
                obsunit = thread_dict['loop'][ii].get('unit')
                obsgroup = thread_dict['loop'][ii].get('group')
                obsdatatype = thread_dict['loop'][ii].get('sql_datatype','REAL').upper()
                desc = thread_dict['loop'][ii].get('description','')
                if obsdatatype in ('REAL','INTEGER'):
                    obssize = 8
                elif obsdatatype[0:7]=='VARCHAR':
                    obssize = int(obsdatatype[8:])
                else:
                    obssize = 0
                t.append((ii,desc,obssize,'X'*obssize,obstype,obsunit,obsgroup,obsdatatype))
            thread_dict['loop'] = t
        if __name__=='__main__':
            print(json.dumps(thread_dict['loop'],indent=4,ensure_ascii=False))
        # create thread
        self.threads[thread_name] = dict()
        self.threads[thread_name]['queue'] = queue.Queue()
        self.threads[thread_name]['thread'] = PrecipThread(thread_name,thread_dict,self.threads[thread_name]['queue'],query_interval)
        self.threads[thread_name]['reply_count'] = 0
        # initialize observation types
        _accum = dict()
        for ii in thread_dict['loop']:
            obstype,obsunit,obsgroup,obsdatatype = ii[4:]
            if not obsgroup and obsunit:
                # if no unit group is given, try to find out
                for jj in weewx.units.MetricUnits:
                    if weewx.units.MetricUnits[jj]==obsunit:
                        obsgroup = jj
                        break
                if not obsgroup:
                    for jj in weewx.units.USUnits:
                        if weewx.units.USUnits[jj]==obsunit:
                            obsgroup = jj
                            break
            if obstype:
                if obsgroup:
                    weewx.units.obs_group_dict.setdefault(obstype,obsgroup)
                    if (obsgroup in ('group_deltatime',
                                     'group_time','group_count') and
                        obstype not in weewx.accum.accum_dict):
                        _accum[obstype] = ACCUM_LAST
                if issqltexttype(obsdatatype):
                    _accum[obstype] = ACCUM_STRING
                global table
                table.append((obstype,obsdatatype))
        # add accumulator entries
        if _accum:
            loginf ("accumulator dict for '%s': %s" % (thread_name,_accum))
            weewx.accum.accum_dict.maps.append(_accum)
        # start thread
        self.threads[thread_name]['thread'].start()
        return True
        
    def shutDown(self):
        """ shutdown threads """
        for ii in self.threads:
            try:
                self.threads[ii]['thread'].shutDown()
            except Exception:
                pass
        
    def _process_data(self, thread_name):
        # get collected data
        data = None
        ct = 0
        while True:
            try:
                data1 = self.threads[thread_name]['queue'].get(block=False)
            except queue.Empty:
                break
            else:
                data = data1
                ct += 1
        if data:
            data[1]['count'] = (ct,'count','group_count')
            return data[1]
        return None

    def new_loop_packet(self, event):
        for thread_name in self.threads:
            reply = self._process_data(thread_name)
            if reply:
                data = self._to_weewx(thread_name,reply,event.packet['usUnits'])
                # log 
                if self.debug>=3: 
                    logdbg("PACKET %s:%s" % (thread_name,data))
                # 'dateTime' and 'interval' must not be in data
                if 'dateTime' in data: del data['dateTime']
                if 'interval' in data: del data['interval']
                if 'count' in data: del data['count']
                # update loop packet with device data
                event.packet.update(data)
                # count records received from the device
                self.threads[thread_name]['reply_count'] += reply.get('count',(0,None,None))[0]

    def new_archive_record(self, event):
        for thread_name in self.threads:
            # log error if we did not receive any data from the device
            if self.log_failure and not self.threads[thread_name]['reply_count']:
                logerr("no data received from %s during archive interval" % thread_name)
            # log success to see that we are still receiving data
            if self.log_success and self.threads[thread_name]['reply_count']:
                loginf("%s records received from %s during archive interval" % (self.threads[thread_name]['reply_count'],thread_name))
            # reset counter
            self.threads[thread_name]['reply_count'] = 0

    def _to_weewx(self, thread_name, reply, usUnits):
        data = dict()
        for key in reply:
            #print('*',key)
            if key in ('time','interval','count','sysStatus'):
                pass
            elif key in ('interval','count','sysStatus'):
                data[key] = reply[key]
            else:
                try:
                    val = reply[key]
                    val = weewx.units.convertStd(val, usUnits)[0]
                except (TypeError,ValueError,LookupError,ArithmeticError) as e:
                    try:
                        val = reply[key][0]
                    except LookupError:
                        val = None
                data[key] = val
        return data

class PrecipArchive(StdService):

    def __init__(self, engine, config_dict):
        super(PrecipArchive,self).__init__(engine, config_dict)
        loginf("PrecipMeter archive version %s" % VERSION)
        site_dict = weeutil.config.accumulateLeaves(config_dict.get('PrecipMeter',config_dict))
        self.log_success = weeutil.weeutil.to_bool(site_dict.get('log_success',True))
        self.log_failure = weeutil.weeutil.to_bool(site_dict.get('log_failure',True))
        self.debug = weeutil.weeutil.to_int(site_dict.get('debug',0))
        if self.debug>0:
            self.log_success = True
            self.log_failure = True
        self.dbm = None
        self.archive_interval = 300
        if 'PrecipMeter' in config_dict:
            if __name__!='__main__':
                self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
                self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
            # init schema
            global schema
            schema = {
                'table':table,
                'day_summaries':day_summaries(table)}
            if __name__=='__main__':
                print('----------')
                print(schema)
                print('----------')
            # init database
            binding = config_dict['PrecipMeter'].get('data_binding','precip_binding')
            if binding in ('None','none'): binding = None
            if binding:
                binding_found = ( 
                    'DataBindings' in config_dict.sections and 
                    binding in config_dict['DataBindings'] and
                    'database' in config_dict['DataBindings'][binding]
                )
            else:
                binding_found = None
            self.dbm_init(engine,binding,binding_found)

    def shutDown(self):
        """ close database """
        try:
            self.dbm_close()
        except Exception:
            pass
        
    def new_loop_packet(self, event):
        """ process loop packet """
        if self.dbm:
            self.dbm_new_loop_packet(event.packet)

    def new_archive_record(self, event):
        """ process archive record """
        if self.dbm:
            self.dbm_new_archive_record(event.record)

    def dbm_init(self, engine, binding, binding_found):
        self.accumulator = None
        self.old_accumulator = None
        self.dbm = None
        if not binding: 
            loginf("no database storage configured")
            return
        if not binding_found: 
            logerr("binding '%s' not found in weewx.conf" % binding)
            return
        self.dbm = engine.db_binder.get_manager(data_binding=binding,
                                                     initialize=True)
        if self.dbm:
            loginf("Using binding '%s' to database '%s'" % (binding,self.dbm.database_name))
            # Back fill the daily summaries.
            _nrecs, _ndays = self.dbm.backfill_day_summary()
        else:
            loginf("no database access")
    
    def dbm_close(self):
        if self.dbm:
            self.dbm.close()
        
    def dbm_new_loop_packet(self, packet):
        """ Copyright (C) Tom Keffer """
        # Do we have an accumulator at all? If not, create one:
        if not self.accumulator:
            self.accumulator = self._new_accumulator(packet['dateTime'])

        # Try adding the LOOP packet to the existing accumulator. If the
        # timestamp is outside the timespan of the accumulator, an exception
        # will be thrown:
        try:
            self.accumulator.addRecord(packet, add_hilo=True)
        except weewx.accum.OutOfSpan:
            # Shuffle accumulators:
            (self.old_accumulator, self.accumulator) = \
                (self.accumulator, self._new_accumulator(packet['dateTime']))
            # Try again:
            self.accumulator.addRecord(packet, add_hilo=True)
        
    def dbm_new_archive_record(self, record):
        if self.dbm:
            self.dbm.addRecord(record,
                           accumulator=self.old_accumulator,
                           log_success=self.log_success,
                           log_failure=self.log_failure)
        
    def _new_accumulator(self, timestamp):
        """ Copyright (C) Tom Keffer """
        start_ts = weeutil.weeutil.startOfInterval(timestamp,
                                                   self.archive_interval)
        end_ts = start_ts + self.archive_interval

        # Instantiate a new accumulator
        new_accumulator = weewx.accum.Accum(weeutil.weeutil.TimeSpan(start_ts, end_ts))
        return new_accumulator

        
if __name__ == '__main__':

    conf_dict = configobj.ConfigObj("PrecipMeter.conf")

    if False:
    
        conf_dict['PrecipMeter']['Parsivel']['telegram'] = "%13;%01;%02;%03;%07;%08;%34;%12;%10;%11;%18;/r/n"
        q = queue.Queue()
        t = PrecipThread('Parsivel',conf_dict['PrecipMeter']['Parsivel'],q,5)
        t.start()

        try:
            while True:
                x = q.get(block=True)
                print(x)
        except (Exception,KeyboardInterrupt):
            pass

        print('xxxxxxxxxxxxx')
        t.shutDown()
        print('+++++++++++++')
        
    else:
    
        sv = PrecipData(None,conf_dict)
        
        try:
            while True:
                event = weewx.Event(weewx.NEW_LOOP_PACKET)
                event.packet = {'usUnits':weewx.METRIC}
                sv.new_loop_packet(event)
                if len(event.packet)>1:
                    print(event.packet)
        except Exception as e:
            print('**MAIN**',e.__class__.__name__,e)
        except KeyboardInterrupt:
            print()
            print('**MAIN** CTRL-C pressed')
            
        sv.shutDown()
    
