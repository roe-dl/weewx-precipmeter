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

VERSION = "0.8"

SIMULATE_ERRONEOUS_READING = False
TEST_LOG_THREAD = False

"""

    Radarreflektivität (Z)
    https://de.wikipedia.org/wiki/Reflektivität_(Radar)
    https://www.dwd.de/DE/leistungen/radarniederschlag/rn_info/download_niederschlagsbestimmung.pdf;jsessionid=A87B2F1098BF26972202EC007A15F634.live21072?__blob=publicationFile&v=4
    
    radar reflectivity factor (Z)
    https://en.wikipedia.org/wiki/DBZ_(meteorology)
    
    meteorologische Sichtweite MOR
    meteorological optical range MOR
    
    `sensorState` Parsivel2:
        0 - ok
        1 - dirty, but measurement is still possible
        2 - dirty, no measurement any more
        3 - laser defective
        
    self.presentweather_list elements are a list of:
    [0]  - start timestamp of the weather condition
    [1]  - end timestamp of the weather condition (updated each time,
           the same weather condition is reported as before)
    [2]  - ww value of the weather condition
    [3]  - wawa value of the weather conditon
    [4]  - if this weather condition is precipitation the start timestamp
           of the precipitation (If the weather condition changes
           during precipitation in intensity or kind, this value is
           not the same as [0].)
           if this weather condition is no precipitation the value is
           None
    [5]  - metar value of the weather condition
    [6]  - intsum
    [7]  - dursum
    [8]  - sum of rain rate readings received during this weather condition
    [9]  - count of rain rate readings received during this weather condition
    [10] - last value of accumulated or absolute rain received during this
           weather condition
    
    A short interruption of precipitation is defined as:
    * The interruption is shorter than 10 minutes AND
    * the interruption is shorter than the duration of precipitation.
    
    The WeeWX accumulator 'firstlast' as of version 4.10.2 converts
    all values to strings. So it is not suitable for lists.
    
"""

import threading 
import configobj
import time
import copy
import json
import select
import socket
import math
import sqlite3
import os.path
import traceback

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

def gettraceback(e):
    return ' - '.join(traceback.format_tb(e.__traceback__)).replace('\n',' ')

import weewx
from weewx.engine import StdService
import weeutil.weeutil
import weewx.accum
import weewx.xtypes
import weewx.units
import weewx.defaults

# Accumulators

ACCUM_SUM = { 'extractor':'sum' }
ACCUM_STRING = { 'accumulator':'firstlast','extractor':'last' }
ACCUM_LAST = { 'extractor':'last' }
ACCUM_MAX = { 'extractor':'max' }
ACCUM_NOOP = { 'accumulator':'firstlast','adder':'noop','extractor':'noop' }
ACCUM_HISTORY = ACCUM_NOOP

# Initialize default unit for the unit groups defined in this extension

for _,ii in weewx.units.std_groups.items():
    ii.setdefault('group_wmo_ww','byte')
    ii.setdefault('group_wmo_wawa','byte')
    ii.setdefault('group_wmo_W','byte')
    ii.setdefault('group_wmo_Wa','byte')
    ii.setdefault('group_rainpower','watt_per_meter_squared')

# Set the target unit groups for aggregation types defined by this extension
    
weewx.units.agg_group.setdefault('wmo_W1','group_wmo_W')
weewx.units.agg_group.setdefault('wmo_W2','group_wmo_W')
weewx.units.agg_group.setdefault('wmo_Wa1','group_wmo_Wa')
weewx.units.agg_group.setdefault('wmo_Wa2','group_wmo_Wa')

# Additional unit conversion formulae

weewx.defaults.defaults['Units']['StringFormats'].setdefault('millivolt',"%.0f")
weewx.defaults.defaults['Units']['Labels'].setdefault('millivolt',u" mV")
weewx.defaults.defaults['Units']['StringFormats'].setdefault('decivolt',"%.1f")
weewx.defaults.defaults['Units']['Labels'].setdefault('decivolt',u" dV")
weewx.defaults.defaults['Units']['StringFormats'].setdefault('milliamp',"%.0f")
weewx.defaults.defaults['Units']['Labels'].setdefault('milliamp',u" mA")

MILE_PER_METER = 1.0/weewx.units.METER_PER_MILE
weewx.units.conversionDict['meter'].setdefault('mile',lambda x: x*MILE_PER_METER)

if 'volt' not in weewx.units.conversionDict:
    weewx.units.conversionDict['volt'] = {}
weewx.units.conversionDict['volt'].setdefault('millivolt',lambda x: x*0.001)
weewx.units.conversionDict['volt'].setdefault('decivolt',lambda x: x*0.1)
if 'millivolt' not in weewx.units.conversionDict:
    weewx.units.conversionDict['millivolt'] = {}
weewx.units.conversionDict['millivolt'].setdefault('volt',lambda x: x*1000)
weewx.units.conversionDict['millivolt'].setdefault('decivolt',lambda x: x*0.01)
if 'decivolt' not in weewx.units.conversionDict:
    weewx.units.conversionDict['decivolt'] = {}
weewx.units.conversionDict['decivolt'].setdefault('volt',lambda x: x*0.1)
weewx.units.conversionDict['decivolt'].setdefault('millivolt',lambda x: x*100)

if 'amp' not in weewx.units.conversionDict:
    weewx.units.conversionDict['amp'] = {}
weewx.units.conversionDict['amp'].setdefault('milliamp',lambda x: x*1000)
if 'milliamp' not in weewx.units.conversionDict:
    weewx.units.conversionDict['milliamp'] = {}
weewx.units.conversionDict['milliamp'].setdefault('amp',lambda x: x*0.001)


##############################################################################
#    data telegrams                                                          #
##############################################################################

# Ott Parsivel 1 + 2 
# (not available for Parsivel 1: 34, 35, 60, 61)
# group_rainpower: 1 J/(m^2h) = 1 Ws/(m^2h) = 1/3600 W/m^2
PARSIVEL = [
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
  ( 3,'Wettercode nach SYNOP wawa Tabelle 4680',2,'00','wawa','byte','group_wmo_wawa'),
  ( 4,'Wettercode nach SYNOP ww Tabelle 4677',2,'00','ww','byte','group_wmo_ww'),
  ( 5,"Wettercode METAR/SPECI w'w' Tabelle 4678",5,'+RASN','METAR','string',None),
  ( 6,'Wettercode nach NWS Code',4,'RLS+','NWS','string',None),
  # readings 32 bit
  ( 1,'Regenintensität (32bit)',8,'0000.000','rainRate','mm_per_hour','group_rainrate'),
  ( 2,'Regenmenge akkumuliert (32bit)',7,'0000.00','rainAccu','mm','group_rain'),
  (24,'Regenmenge absolut (32bit)',7,'000.000','rainAbs','mm','group_rain'),
  ( 7,'Radarreflektivität (32bit)',6,'00.000','dBZ','dB','group_db'),
  # readings 16 bit (not necessary if 32 bit readings can be used)
  (30,'Regenintensität (16bit) max 30 mm/h',6,'00.000',None,'mm_per_hour','group_rainrate'),
  (31,'Regenintensität (16bit) max 1200 mm/h',6,'0000.0',None,'mm_per_hour','group_rainrate'),
  (32,'Regenmenge akkumuliert (16bit)',7,'0000.00',None,'mm','group_rain'),
  (33,'Radarreflektivität (16bit)',5,'00.00',None,'dB','group_db'),
  # other readings
  ( 8,'MOR Sichtweite im Niederschlag',5,'00000','MOR','meter','group_distance'),
  (10,'Signalamplitude des Laserbandes',5,'00000','signal','count','group_count'),
  (11,'Anzahl der erkannten und validierten Partikel',5,'00000','particle','count','group_count'),
  (34,'kinetische Energie',7,'000.000','energy','J/(m^2h)','group_rainpower'),
  (35,'Schneehöhen-Intensität (volumenäquivalent)',7,'0000.00','snowRate','mm_per_hour','group_rainrate'),
  # device monitoring data
  (12,'Temperatur im Sensorgehäuse',3,'000','housingTemp','degree_C','group_temperature'),
  (16,'Strom Sensorkopfheizung',4,'0.00','heatingCurrent','amp','group_amp'),
  (17,'Versorgungsspannung',4,'00.0','supplyVoltage','volt','group_volt'),
  (26,'Temperatur Leiterplatte',3,'000','circuitTemp','degree_C','group_temperature'),
  (27,'Temperatur im Sensorkopf rechts',3,'000','rightSensorTemp','degree_C','group_temperature'),
  (28,'Temperatur im Sensorkopf links',3,'000','leftSensorTemp','degree_C','group_temperature'),
  # special data
  (60,'Anzahl aller erkannten Partikel',8,'00000000','particleCount','count','group_count'),
  (61,'Liste aller erkannten Partikel',13,'00.000;00.000',None,'mm;m/s',None),
  (90,'Feld N(d)',223,'00.000S',None,'log10(1/m^3 mm)',None),
  (91,'Feld v(d)',223,'00.000S','particleSpeed','meter_per_second','group_speed'),
  (93,'Rohdaten',4095,'000S','raw','count','group_count')
]

THIES_READINGS = [
  #Nr,Beschreibung,Stellen,Form,Größe,Einheit,Gruppe
  # 1 STX
  # device information and identification
  ( 2,'Geräteadresse',2,'00',None,'string',None),
  ( 3,'Seriennummer',4,'NNNN','SNR','string',None),
  ( 4,'Software-Version',4,'N.NN',None,'string',None),
  ( 5,'Gerätedatum',8,'tt.mm.jj',None,'string',None),
  ( 6,'Gerätezeit zur Abfrage',8,'hh:mm:ss',None,'string',None),
  # readings (5 minutes averages)
  ( 7,'5-Minuten-Mittelwert SYNOP 4677',2,'NN',None,'byte','group_wmo_ww'),
  ( 8,'5-Minuten-Mittelwert SYNOP 4680',2,'NN',None,'byte','group_wmo_wawa'),
  ( 9,'5-Minuten-Mittelwert METAR 4678',5,'AAAAA',None,'string',None),
  (10,'5-Mintuen-Mittelwert Intensität',7,'NNN.NNN',None,'mm_per_hour','group_rainrate'),
  # readings (1 minute averages)
  (11,'1-Minuten-Wert SYNOP 4677',2,'NN','ww','byte','group_wmo_ww'),
  (12,'1-Minuten-Wert SYNOP 4680',2,'NN','wawa','byte','group_wmo_wawa'),
  (13,'1-Minuten-Wert METAR 4678',5,'NN','METAR','string',None),
  (14,'1-Minuten-Intensität alle Niederschläge',7,'NNN.NNN','precipRate','mm_per_hour','group_rainrate'),
  (15,'1-Minuten-Intensität flüssig',7,'NNN.NNN','rainRate','mm_per_hour','group_rainrate'),
  (16,'1-Minuten-Intensität fest',7,'NNN.NNN','snowRate','mm_per_hour','group_rainrate'),
  (17,'Niederschlagssumme',7,'NNNN.NN','rainAccu','mm','groupRain'),
  (18,'1-Minuten-Wert Sichtweite im Niederschlag',5,'NNNNN','MOR','meter','group_distance'),
  (19,'1-Minuten-Wert Radarreflektivität',4,'NN.N','dBZ','dB','group_db'),
  (20,'Qualitätsmaß',3,'NNN',None,'percent','group_percent'),
  (21,'1-Minuten-Wert maximaler Hageldurchmesser',3,'N.N',None,'mm',None),
]
THIES_STATE = [
  # 22...37 device state values
  (22,'Status Laser 0-an 1-aus',1,'N',None,'boolean','group_boolean'),
  (23,'Status statistisches Signal 0-ok 1-Fehler',1,'N',None,'boolean','group_boolean'),
  (24,'Status Lasertemperatur (analog) 0-ok 1-Fehler',1,'N',None,'boolean','group_boolean'),
  (25,'Status Lasertemperatur (digital) 0-ok 1-Fehler',1,'N',None,'boolean','group_boolean'),
  (26,'Status Laserstrom (analog) 0-ok 1-Fehler',1,'N',None,'boolean','group_boolean'),
  (27,'Status Laserstrom (digital) 0-ok 1-Fehler',1,'N',None,'boolean','group_boolean'),
  (28,'Status Sensorversorgung 0-ok 1-Fehler',1,'N',None,'boolean','group_boolean'),
  (29,'Status Glasheizung Laserkopf 0-ok 1-Warnung',1,'N',None,'boolean','group_boolean'),
  (30,'Status Glasheizung Empfangskopf 0-ok 1-Warnung',1,'N',None,'boolean','group_boolean'),
  (31,'Status Temperaturfühler 0-ok 1-Warnung',1,'N',None,'boolean','group_boolean'),
  (32,'Status Heizungsversorgung 0-ok 1-Warnung',1,'N',None,'boolean','group_boolean'),
  (33,'Status Heizung Gehäuse 0-ok 1-Warnung',1,'N',None,'boolean','group_boolean'),
  (34,'Status Heizung Kopf 0-ok 1-Warnung',1,'N',None,'boolean','group_boolean'),
  (35,'Status Heizung Bügel 0-ok 1-Warnung',1,'N',None,'boolean','group_boolean'),
  (36,'Status Regelausgang Laserleistung hoch 0-ok 1-Warnung',1,'N',None,'boolean','group_boolean'),
  (37,'Reserve Status',1,'N',None,'boolean','group_boolean'),
  # device monitoring data
  (38,'Innentemperatur',3,'NNN','housingTemp','degree_C','group_temperature'),
  (39,'Temperatur des Laser-Treibers',2,'NN',None,'degree_C','group_temperature'),
  (40,'Mittelwert des Laserstroms in 1/100 mA',4,'NNNN',None,None,None),
  (41,'Regel-Istspannung',4,'NNNN',None,'millivolt','group_volt'),
  (42,'Regelausgang',4,'NNNN',None,'millivolt','group_volt'),
  (43,'Spannung Sensorversorgung',3,'NNN','supplyVoltage','decivolt','group_volt'),
  (44,'Strom Glasheizung Laserkopf',3,'NNN','heatingCurrentLaser','milliamp','group_amp'),
  (45,'Strom Glasheizung Empfängerkopf',3,'NNN','heatingCurrentReceiver','milliamp','group_amp'),
  (46,'Außentemperatur',5,'NNN.N','outTemp','degree_C','group_temperature'),
  (47,'Spannung Heizungsversorgung',3,'NNN','heatingVoltage','decivolt','group_volt'),
  (48,'Strom Gehäuseheizung',4,'NNNN',None,'milliamp','group_amp'),
  (49,'Strom Kopfheizung',4,'NNNN',None,'milliamp','group_amp'),
  (50,'Strom Bügelheizung',4,'NNNN',None,'milliamp','group_amp'),
  # particle count
  (51,'Anzahl aller gemessenen Partikel',5,'particleCount','count','group_count'),
]
THIES_INTERNAL = [
  (52,'interne Daten',9,'00000.000',None,None,None),
  (53,'Partikelanzahl < minimale Geschwindigkeit',5,'NNNNN','particleCountTooSlow','count','group_count'),
  (54,'interne Daten',9,'00000.000',None,None,None),
  (55,'Partikelanzahl > maximale Geschwindigkeit',5,'NNNNN','particleCountTooFast','count','group_count'),
  (56,'interne Daten',9,'00000.000',None,None,None),
  (57,'Partikelanzahl < minimaler Durchmesser',5,'NNNNN','particleCountTooSmall','count','group_count'),
  (58,'interne Daten',9,'00000.000',None,None,None),
  # internal data 59...80
  (59,'Partikelanzahl kein Hydrometeor',5,'NNNNN',None,'count','group_count'),
  (60,'Gesamtvolumen (brutto) kein Hydrometeor',9,'',None,None,None),
  (61,'Partikelanzahl mit unbekannter Klassifizierung',5,'',None,'count','group_count'),
  (62,'Gesamtvolumen (brutto) unbekannte Klassifizierung',9,'',None,None,None),
  (63,'Partikelanzahl Klasse 1',5,'NNNNN',None,'count','group_count'),
  (64,'Gesamtvolumen (brutto) Klasse 1',9,'',None,None,None),
  (65,'Partikelanzahl Klasse 2',5,'NNNNN',None,'count','group_count'),
  (66,'Gesamtvolumen (brutto) Klasse 2',9,'',None,None,None),
  (67,'Partikelanzahl Klasse 3',5,'NNNNN',None,'count','group_count'),
  (68,'Gesamtvolumen (brutto) Klasse 3',9,'',None,None,None),
  (69,'Partikelanzahl Klasse 4',5,'NNNNN',None,'count','group_count'),
  (70,'Gesamtvolumen (brutto) Klasse 4',9,'',None,None,None),
  (71,'Partikelanzahl Klasse 5',5,'NNNNN',None,'count','group_count'),
  (72,'Gesamtvolumen (brutto) Klasse 5',9,'',None,None,None),
  (73,'Partikelanzahl Klasse 6',5,'NNNNN',None,'count','group_count'),
  (74,'Gesamtvolumen (brutto) Klasse 6',9,'',None,None,None),
  (75,'Partikelanzahl Klasse 7',5,'NNNNN',None,'count','group_count'),
  (76,'Gesamtvolumen (brutto) Klasse 7',9,'',None,None,None),
  (77,'Partikelanzahl Klasse 8',5,'NNNNN',None,'count','group_count'),
  (78,'Gesamtvolumen (brutto) Klasse 8',9,'',None,None,None),
  (79,'Partikelanzahl Klasse 9',5,'NNNNN',None,'count','group_count'),
  (80,'Gesamtvolumen (brutto) Klasse 9',9,'',None,None,None),
]
THIES_RAW = [
  (ii+81,'Niederschlagssprektrum',3,'NNN','raw%04d' % ii,'count','group_count') for ii in range(440)
]
THIES_AUX = [
  (521,'Temperatur',5,'NNN.N','outTemp','degree_C','group_temperature'),
  (522,'relative Luftfeuchte',5,'NNN.N','outHumidity','percent','group_percent'),
  (523,'Windgeschwindigkeit',4,'NN.N','windSpeed','meter_per_second','group_speed'),
  (524,'Windrichtung',3,'NNN','windDir','degree','group_direction')
]
THIES_AVG_4680 = [
  (56,'Mittelungszeitraum',2,'NN','avginterval','minute','group_deltatime'),
  (57,'mittlere Intensität',7,'NNN.NNN','rainRateAvg','mm_per_hour','group_rainrate'),
  (58,'maximale 1-Min-Intensität im Mittelungszeitraum',7,'NNN.NNN','rainRateMax','mm_per_hour','group_rainrate'),
  (59,'Maximalwert SYNOP 4680 im Mittelungszeitraum',2,'NN','wawaMax','byte','group_wmo_wawa'),
  (60,'1-Minuten-SYNOP 4680 -9 min.',2,'NN','wawa9','byte','group_wmo_wawa'),
  (61,'1-Minuten-SYNOP 4680 -8 min.',2,'NN','wawa8','byte','group_wmo_wawa'),
  (62,'1-Minuten-SYNOP 4680 -7 min.',2,'NN','wawa7','byte','group_wmo_wawa'),
  (63,'1-Minuten-SYNOP 4680 -6 min.',2,'NN','wawa6','byte','group_wmo_wawa'),
  (64,'1-Minuten-SYNOP 4680 -5 min.',2,'NN','wawa5','byte','group_wmo_wawa'),
  (65,'1-Minuten-SYNOP 4680 -4 min.',2,'NN','wawa4','byte','group_wmo_wawa'),
  (66,'1-Minuten-SYNOP 4680 -3 min.',2,'NN','wawa3','byte','group_wmo_wawa'),
  (67,'1-Minuten-SYNOP 4680 -2 min.',2,'NN','wawa2','byte','group_wmo_wawa'),
  (68,'1-Minuten-SYNOP 4680 -1 min.',2,'NN','wawa1','byte','group_wmo_wawa'),
  (69,'1-Minuten-SYNOP 4680 Sendezeitpunkt',2,'NN','wawa0','byte','group_wmo_wawa'),
]
THIES_CHKSUM = [
  (525,'Prüfsumme',2,'AA',None,'string',None)
  # 526 CRLF
  # 527 ETX
]
THIES = {
  4:THIES_READINGS + THIES_STATE + THIES_INTERNAL + THIES_RAW + THIES_CHKSUM,
  5:THIES_READINGS + THIES_STATE + THIES_INTERNAL + THIES_RAW + THIES_AUX + THIES_CHKSUM,
  6:THIES_READINGS + THIES_STATE + THIES_CHKSUM,
  7:THIES_READINGS + THIES_STATE + THIES_AUX + THIES_CHKSUM,
  8:THIES_READINGS + THIES_CHKSUM,
  9:THIES_READINGS + THIES_AUX + THIES_CHKSUM,
  10:THIES_READINGS + THIES_STATE + THIES_AUX + THIES_AVG_4680 + THIES_CHKSUM
}

#for ii in THIES: print(ii,sum([jj[2]+1 for jj in THIES[ii]])+4)

# "state after something" weather codes

WW2 = {
            20: (50,51,52,53,54,55),
            21: (60,61,62,63,64,65,58,59),
            22: (70,71,72,73,74,75),
            23: (68,69,79),
            24: (56,57,66,67),
            25: (80,81,82),
            26: (83,84,85,86),
            27: (87,88,89,90),
            28: (41,42,43,44,45,46,47,48,49),
            29: (17,95,96,97,98,99)
}
WAWA2 = {
            20: (30,31,32,33,34,35),
            21: (40,41,42),
            22: (50,51,52,53,57,58),
            23: (60,61,62,63,67,68,43,44),
            24: (70,71,72,73,74,75,76,45,46),
            25: (54,55,56,64,65,66,47,48),
            26: (90,91,92,93,94,95,96),
            # reserved values
            -1: (6,7,8,9,13,14,15,16,17,19,36,37,38,39,49,59,69,79,88,97,98)
}

WW2_REVERSED = { i:j for j,k in WW2.items() for i in k }
WAWA2_REVERSED = { i:j for j,k in WAWA2.items() for i in k }

# weather type

# Note: This is NOT a conversion table between WMO code and METAR code.
#       This is a helper table for WMO code postprocessing.

WW_TYPE = {
    # no significant weather | kein signifikantes Wetter
    'NP':(0,1,2,3),
    # in the vicinity        | in der Entfernung
    'VC':(9,14,15,16,40),
    # smoke, volcanic ash    | Rauch, Vulkanasche
    'FUVA':(4,),
    # haze                   | trockener Dunst
    'HZ':(5,),
    # mist                   | feuchter Dunst
    'BR':(10,),
    # fog                    | Nebel
    'FG':(11,12,41,42,43,44,45,46,47,48,49),
    # drizzle                | Sprühregen (Niesel)
    'DZ':(50,51,52,53,54,55),
    # rain                   | Regen
    'RA':(60,61,62,63,64,65,80,81,82,91,92),
    # snow                   | Schneefall
    'SN':(70,71,72,73,74,75,85,86,93,94),
    # ice pellets            | Eiskörner
    'PL':(79,),
    # snow grains            | Schneegriesel
    'SG':(77,),
    # drizzle and rain       | Sprühregen und Regen
    'RADZ':(58,59),
    # rain and snow          | Schneeregen
    'RASN':(68,69),
    # freezing drizzle       | gefrierender Sprühregen
    'FZDZ':(56,57),
    # freezing rain          | gefrierender Regen
    'FZRA':(66,67),
    # graupel                | Graupel
    'GS':(87,88),
    # hail                   | Hagel
    'GR':(89,90)
}
WAWA_TYPE = {
    # no significant weather | kein signifikantes Wetter
    'NP':(0,1,2,3),
    # mist                   | feuchter Dunst
    'BR':(10,),
    # fog                    | Nebel
    'FG':(30,31,32,33,34,35),
    # precipitation          | Niederschlag
    'UP':(40,41,42,80),
    # liquid precipitation   | flüssiger Niederschlag
    'LP':(43,44),
    # solid precipitation    | fester Niederschlag
    'SP':(45,46),
    # freezing precipitation | gefrierender Niederschlag
    'FZUP':(47,48),
    # drizzle                | Sprühregen (Niesel)
    'DZ':(50,51,52,53),
    # rain                   | Regen
    'RA':(60,61,62,63,80,81,82,83,84),
    # snow                   | Schneefall
    'SN':(70,71,72,73,85,86,87),
    # ice pellets            | Eiskörner
    'PL':(74,75,76),
    # snow grains            | Schneegriesel
    'SG':(77,),
    # ice crystals           | Eisnadeln
    'IC':(78,),
    # drizzle and rain       | Sprühregen und Regen
    'RADZ':(57,58),
    # rain and snow          | Schneeregen
    'RASN':(67,68),
    # freezing drizzle       | gefrierender Sprühregen
    'FZDZ':(54,55,56),
    # freezing rain          | gefrierender Regen
    'FZRA':(64,65,66),
    # hail                   | Hagel
    'GR':(90,),
    # tornado                | Tornado
    'tornado':(99,),
    # reserved               | reserviert
    'reserved': WAWA2[-1]
}

WW_TYPE_REVERSED = { i:j for j,k in WW_TYPE.items() for i in k }
WAWA_TYPE_REVERSED = { i:j for j,k in WAWA_TYPE.items() for i in k }

# precipitation intensity

WW_INTENSITY = [
    tuple(),
    (50,51,58,60,61,68,70,71,77,83,87,89,91,93), # light
    (52,53,59,62,63,69,72,73,77,84,88,90,92,94), # moderate
    (54,55,59,64,65,69,74,75,77,84,88,90,92,94), # heavy
    tuple()
]
WAWA_INTENSITY = [
    (40,50,60,70,80),                                  # unknown
    (41,43,45,47,51,54,57,61,64,67,71,74,77,81,85,89), # light
    (41,43,45,47,52,55,58,62,65,68,72,75,77,82,86,89), # moderate
    (42,44,46,48,53,56,58,63,66,68,73,76,77,83,87,89), # heavy
    (84,)                                              # extreme
]

WW_INTENSITY_REVERSED = { i:4-j for j,k in enumerate(reversed(WW_INTENSITY)) for i in k }
WAWA_INTENSITY_REVERSED = { i:4-j for j,k in enumerate(reversed(WAWA_INTENSITY)) for i in k }

# light    <[0]
# moderate >=[0] to <[1]
# heavy    >=[1]
WW_WAWA_INTENSITY_THRESHOLD = {
    # drizzle                | Sprühregen (Niesel)
    'DZ':(0.1,0.5),
    # drizzle and rain       | Sprühregen und Regen
    'RADZ':(2.5,10.0),
    # rain                   | Regen
    'RA':(2.5,10.0),
    # rain and snow          | Schneeregen
    'RASN':(2.5,10.0),
    # snow                   | Schneefall
    'SN':(1.0,4.0),
    # snow grains            | Schneegriesel
    'SG':(0.0,0.0),
    # graupel                | Graupel
    'GS':(1.0,1.0),
    # hail                   | Hagel
    'GR':(2.5,2.5)
}
METAR_INTENSITY_THRESHOLD = {
    # drizzle                | Sprühregen (Niesel)
    'DZ':(0.25,0.5),
    # drizzle and rain       | Sprühregen und Regen
    'RADZ':(2.5,7.6),
    # rain                   | Regen
    'RA':(2.5,7.6),
    # rain and snow          | Schneeregen
    'RASN':(2.5,7.6),
    # snow                   | Schneefall
    'SN':(1.25,2.5),
    # snow grains            | Schneegriesel
    'SG':(1.25,2.5),
    # graupel                | Graupel
    'GS':(1.25,2.5),
    # hail                   | Hagel
    'GR':(0.0,0.0)
}

# past weather VuB2 BUFR page 259

WA_WAWA = [
    (0,1,2,3,11,12,18,20,21,22,23,24,25,26), # no significant weather
    (4,5,10,27), # reduced visibility
    (28,29), # 
    (30,31,32,33,34,35), # fog
    (40,41,42), # precipitation
    (50,51,52,53,54,55,56), # drizzle
    (43,44,47,48,57,58,60,61,62,63,64,65,66), # rain
    (45,46,67,68,70,71,72,73,74,75,76,77,78), # snow
    (80,81,82,83,84,85,86,87,89), # shower or intermittent precipitation
    (90,91,92,93,94,95,96) # thunderstorm
]
WA_WW = [
    (0,1,2,3,13,14,15,16,18,19,76), # no significant weather
    (4,5,6,10,11,12,36,37,40), # reduced visibility
    (7,8,9,30,31,32,33,34,35,38,39), # 
    (28,41,42,43,44,45,46,47,48,49), # fog
    (24,), # precipitation
    (20,50,51,52,53,54,55,56,57), # drizzle
    (21,58,59,60,61,62,63,64,65,66,67), # rain
    (22,23,68,69,70,71,72,73,74,75,77,78,79), # snow
    (25,26,27,80,81,82,83,84,85,86,87,88,89,90), # shower
    (17,29,91,92,93,94,95,96,97,98,99) # thunderstorm
]

WA_WAWA_REVERSED = { i:j for j,k in enumerate(WA_WAWA) for i in k }
WA_WW_REVERSED = { i:j for j,k in enumerate(WA_WW) for i in k }

# ww to AWEKAS code
WW_AWEKAS = {
   0: 0,
   1: 0,
   2: 0,
   3: 0,
  45: 7, # fog
  80: 8, # rain showers
  81: 9, # heavy rain showers
  82: 9, # heavy rain showers
  58:10, # light rain
  59:11, # rain
  61:10, # light rain
  63:11, # rain
  65:12, # heavy rain
  79:13, # light snow
  71:13, # light snow
  73:14, # snow
  75:24, # heavy snow
  85:15, # light snow showers
  86:16, # snow showers
  68:17, # sleet
  69:17, # sleet
  83:17, # sleet
  84:17, # sleet
  68:17, # sleet
  69:17, # sleet
  87:18, # hail
  88:18, # hail
  89:18, # hail
  90:18, # hail
  95:19, # thunderstorm
  96:19, # thunderstorm
  97:19, # thunderstorm
  98:19, # thunderstorm
  99:19, # thunderstorm
  18:20, # storm
  56:21, # freezing rain
  57:21, # freezing rain
  66:21, # freezing rain
  67:21, # freezing rain
  51:23, # drizzle
  53:23, # drizzle
  55:23, # drizzle
}
# wawa to AWEKAS code
WAWA_AWEKAS = {
   0: 0,
  33: 7, # fog
  80: 8, # rain showers
  81: 8, # rain showers
  82: 8, # rain showers
  83: 9, # heavy rain showers
  84: 9, # heavy rain showers
  57:10, # light rain
  58:11, # rain
  60:10, # light rain
  61:10, # light rain
  62:11, # rain
  63:12, # heavy rain
  70:13, # light snow
  71:13, # light snow
  72:14, # snow
  73:24, # heavy snow
  85:15, # light snow showers
  86:16, # snow showers
  87:25, # heavy snow showers
  67:17, # sleet
  68:17, # sleet
  89:18, # hail
  90:19, # thunderstorm
  91:19, # thunderstorm
  92:19, # thunderstorm
  93:19, # thunderstorm
  94:19, # thunderstorm
  95:19, # thunderstorm
  96:19, # thunderstorm
  18:20, # storm
  99:20, # storm
  47:21, # freezing rain
  48:21, # freezing rain
  54:21, # freezing rain
  55:21, # freezing rain
  56:21, # freezing rain
  64:21, # freezing rain
  65:21, # freezing rain
  66:21, # freezing rain
  50:23, # drizzle
  51:23, # drizzle
  52:23, # drizzle
  53:23, # drizzle
}
# AWEKAS codes
# german description, English description, severity, icon
AWEKAS = [
    # 0 clear warning
    ('Warnung aufgehoben','clear warning',0,'clear-warning.svg'),
    # 1...6 cloud covering
    ('klar','clear',1,'clear-day.png'),
    ('heiter','sunny sky',2,'mostly-clear-day.png'),
    ('leicht bewölkt','partly cloudy',3,'partly-cloudy-day.png'),
    ('bewölkt','cloudy',4,'partly-cloudy-day.png'),
    ('stark bewölkt','heavy cloudy',5,'mostly-cloudy-day.png'),
    ('bedeckt','overcast sky',6,'cloudy.png'),
    # 7 fog
    ('Nebel','fog',7,'fog.png'),
    # 8... precipitation
    ('Regenschauer','rain showers',15,'rain.png'),
    ('schwere Regenschauer','heavy rain showers',16,'rain.png'),
    ('leichter Regen','light rain',9,'rain.png'),
    ('Regen','rain',10,'rain.png'),
    ('starker Regen','heavy rain',11,'rain.png'),
    ('leichter Schneefall','light snow',12,'snow.png'),
    ('Schneefall','snow',13,'snow.png'),
    ('leichte Schneeschauer','light snow showers',17,'snow.png'),
    ('Schneeschauer','snow showers',18,'snow.png'),
    ('Schneeregen','sleet',20,'sleet.png'),
    ('Hagel','hail',24,'hail.png'),
    # 19 thunderstorm
    ('Gewitter','thunderstorm',26,'thunderstorm.png'),
    # 20 storm
    ('Sturm','storm',27,'wind.png'),
    # 21 freezing precipitation
    ('gefrierender Regen','freezing rain',25,'freezingrain.png'),
    # 22 warning
    ('Warnung','warning',28,''),
    # 23...25 again precipitation
    ('Sprühregen','drizzle',8,'drizzle.png'),
    ('starker Schneefall','heavy snow',14,'snow.png'),
    ('starke Schneeschauer','heavy snow showers',19,'snow.png')
]

##############################################################################
#    Database schema                                                         #
##############################################################################

exclude_from_summary = ['dateTime', 'usUnits', 'interval','presentweatherTime']

table = [
    ('dateTime',             'INTEGER NOT NULL UNIQUE PRIMARY KEY'),
    ('usUnits',              'INTEGER NOT NULL'),
    ('interval',             'INTEGER NOT NULL'),
    ('ww',                   'INTEGER'),
    ('wawa',                 'INTEGER'),
    ('presentweatherWw',     'INTEGER'),
    ('hourPresentweatherW1', 'INTEGER'),
    ('hourPresentweatherW2', 'INTEGER'),
    ('presentweatherWawa',   'INTEGER'),
    ('hourPresentweatherWa1','INTEGER'),
    ('hourPresentweatherWa2','INTEGER'),
    ('presentweatherStart',  'INTEGER'),
    ('presentweatherTime',   'REAL'),
    ('precipitationStart',   'INTEGER'),
    ('frostIndicator',       'INTEGER')
]

def day_summaries(table):
    return [(e[0], 'scalar') for e in table
                 if e[0] not in exclude_from_summary and e[1]=='REAL'] 

schema = {
    'table': table,
    'day_summaries': day_summaries(table)
    }

# SQL VIEW to the weather condition codes history

weatherconditionschema = {
    'table': [
        ('dateTime',             'INTEGER NOT NULL UNIQUE PRIMARY KEY'),
        ('usUnits',              'INTEGER NOT NULL'),
        ('interval',             'INTEGER NOT NULL'),
        ('presentweatherStart',  'INTEGER'),
        ('precipitationStart',   'INTEGER'),
        ('presentweatherTime',   'REAL'),
        ('ww',                   'INTEGER'),
        ('wawa',                 'INTEGER'),
        ('METAR',                'INTEGER')
    ],
    'day_summaries': []
}

##############################################################################

def issqltexttype(x):
    """ Is this a string type in SQL? """
    if x is None: return None
    x = x.upper().split('(')[0].strip()
    return x in ('TEXT','CLOB','CHARACTER','VARCHAR','VARYING CHARACTER','NCHAR','NATIVE CHARACTER','NVARCHAR')

def is_ww_wawa_precipitation(ww, wawa):
    """ Does this weather code mean precipitation? """
    return (ww and ww>=50) or (wawa and wawa>=40)

def max_ww(ww_list):
    """ accumulate a list of table 4677 weather codes for significance
    
        This is done according to the rules of the meteorologists. See
        DWD VuB2 BUFR 0 20 003 page 44. It is the maximum of the codes 
        in the list except:
        - 17 preceeds 20 to 49
        - 28 preceeds 40
        Unfortunately those rules are not clear. 
        
        The conversion to a set before sorting makes sure, all values
        in the result are unique.
        
        Args:
            ww_list (iterator of int): list of weather ww codes
            
        Returns:
            int: most significant weather code
    """
    if not ww_list: return None
    ww = sorted(set(ww_list),key=lambda x:-1 if x is None else x)
    if ww[-1] is not None and ww[-1]<50:
        # If a code above 49 is in the list, there is no need to check
        # the special rules.
        if 17 in ww: 
            # As code 17 preceeds 28 there is no need to check the
            # code 28 rule, if 17 is in the list.
            if 18 not in ww and 19 not in ww:
                # There is no element that preceeds 17 in the list
                return 17
            if ww[-1] not in (18,19):
                # At this point the rule is not clear. The list
                # contains codes that preceed 18 and 19. At the
                # same time 17 preceeds those codes, but not
                # 18 and 19. 
                pass
        elif 28 in ww and 40 in ww:
            # 28 preceeds 40, but not 29 to 39
            idx = ww.index(28)
            # As the list is sorted, 28 cannot be the last element.
            # So there is at least one more element, and the following
            # statement cannot fail.
            if ww[idx+1]==40:
                # There is no code between 29 and 39 in the list.
                ww[idx],ww[idx+1] = ww[idx+1],ww[idx]
    return ww[-1]
    
def get_w1w2_from_ww(ww_list):
    """ get the past weather codes from a list of table 4677 weather codes

        Args:
            ww_list (iterator of int): list of weather ww codes
            
        Returns:
            int: W past weather code of the present weather
            int: W1 past weather code
            int: W2 past weather code
    """
    w_list = []
    last_w = None
    # loop through the list from now to the past
    for ww in reversed(ww_list):
        if ww is None:
            w = None
        elif ww==90:
            w = 8
        elif ww<30:
            w = None
        else:
            w = ww//10
        if last_w!=w:
            w_list.append(w)
        last_w = w
    if not w_list: return None,None,None
    # The present weather ist the first element in the list. We want
    # the past weather.
    w = sorted(set(w_list[1:]),key=lambda x:-1 if x is None else x)
    if not w: return w_list[0],None,None
    try:
        w1 = w[-1]
    except (LookupError,TypeError):
        w1 = None
    try:
        w2 = w[-2]
    except LookupError:
        w2 = None
    return w_list[0], w1, w2

def max_wawa(wawa_list):
    """ accumulate a list of table 4680 weather codes 
    
        Args:
            wawa_list (iterable): list if wawa weather codes
            
        Returns:
            int: most significant weather code
    """
    return max(wawa_list,key=lambda x:-1 if x is None else x,default=None)

def get_wa1wa2_from_wawa_or_ww(ww_list,obsgroup):
    """ get the past weather codes from a list weather codes

    
        Args:
            ww_list (iterator of int): list of weather ww codes
            
        Returns:
            int: W past weather code of the present weather
            int: W1 past weather code
            int: W2 past weather code
    """
    w_list = []
    last_w = None
    # loop through the list from now to the past
    for ww in reversed(ww_list):
        if obsgroup=='group_wmo_ww':
            w = WA_WW_REVERSED.get(ww)
        else:
            w = WA_WAWA_REVERSED.get(ww)
        if last_w!=w:
            w_list.append(w)
        last_w = w
    if not w_list: return None,None,None
    # The present weather ist the first element in the list. We want
    # the past weather.
    w = sorted(set(w_list[1:]),key=lambda x:-1 if x is None else x)
    if not w: return w_list[0],None,None
    try:
        w1 = w[-1]
    except (LookupError,TypeError):
        w1 = None
    try:
        w2 = w[-2]
    except LookupError:
        w2 = None
    return w_list[0], w1, w2

##############################################################################
#    XType extension to provide special aggregation types                    #
##############################################################################

# In general the maximum of ww or wawa is returned for the present weather
# of some kind of timespan. But there are some little exceptions. A special
# function is provided to handle the 'max' aggregation type of ww and wawa
# for this reason. 

class PrecipXType(weewx.xtypes.XType):

    def get_aggregate(self, obs_type, timespan, agg_type, db_manager, **option_dict):
        """ special aggregation for group_wmo_ww """
        obs_group = weewx.units.getUnitGroup(obs_type)
        if obs_group=='group_wmo_ww':
            if agg_type=='max':
                return self.get_ww_max(obs_type,timespan,db_manager,**option_dict)
            if agg_type in ('wmo_W1','wmo_W2','wmo_Wa1','wmo_Wa2'):
                return self.get_w(obs_type,timespan,agg_type,db_manager,**option_dict)
        if obs_group=='group_wmo_wawa':
            if agg_type=='max':
                return self.get_wawa_max(obs_type,timespan,db_manager,**option_dict)
            if agg_type in ('wmo_Wa1','wmo_Wa2'):
                return self.get_w(obs_type,timespan,agg_type,db_manager,**option_dict)
        raise weewx.UnknownAggregation("%s.%s" % (obs_type,agg_type))
        
    def get_ww_max(self, obs_type, timespan, db_manager, **option_dict):
        """ get most significant ww code of the timespan """
        start,stop,data = weewx.xtypes.get_series(obs_type,timespan,db_manager,**option_dict)
        ww = max_ww(data[0])
        logdbg('get_ww_max %s' % ww)
        return weewx.units.ValueTuple(ww,'byte','group_wmo_ww')
            
    def get_wawa_max(self, obs_type, timespan, db_manager, **option_dict):
        """ get most significant wawa code of the timespan """
        start,stop,data = weewx.xtypes.get_series(obs_type,timespan,db_manager,**option_dict)
        wawa = max_wawa(data[0])
        logdbg('get_wawa_max %s' % wawa)
        return weewx.units.ValueTuple(wawa,'byte','group_wmo_wawa')
        
    def get_w(self, obs_type, timespan, agg_type,db_manager, **option_dict):
        """ get past weather codes """
        # timespan to get the codes for
        duration = timespan.stop-timespan.start
        # get the list of present weather codes of the timespan in question
        start,stop,data = weewx.xtypes.get_series(obs_type,timespan,db_manager,**option_dict)
        # get past weather codes from the list of present weather codes
        if agg_type in ('wmo_W1','wmo_W2'):
            target_group = 'group_wmo_W'
            w, w1, w2 = get_w1w2_from_ww(data[0])
        elif agg_type in ('wmo_Wa1','wmo_Wa2'):
            target_group = 'group_wmo_Wa'
            w, w1, w2 = get_wa1wa2_from_wawa_or_ww(data[0],data[2])
        else:
            raise weewx.UnknownAggregation("%s.%s" % (obs_type,agg_type))
        # return result
        if duration<=3600 and w1==w:
            # If the timespan is up to one hour only, get no past weather
            # code that is the same as present weather
            return weewx.units.ValueTuple(None,'byte',target_group)
        if agg_type.endswith('1'):
            # W1 or Wa1
            return weewx.units.ValueTuple(w1,'byte',target_group)
        else:
            # W2 or Wa2
            return weewx.units.ValueTuple(w2,'byte',target_group)

##############################################################################
#    Thread to retrieve and process disdrometer data                         #
##############################################################################

class PrecipThread(threading.Thread):

    def __init__(self, name, conf_dict, data_queue, query_interval):
    
        super(PrecipThread,self).__init__(name='PrecipMeter-'+name)

        self.start_ts = time.time()
        self.telegram = conf_dict['telegram']
        self.telegram_list = conf_dict['loop']
        self.field_separator = conf_dict.get('field_separator',';')
        self.record_separator = conf_dict.get('record_separator','\r\n')
        self.model = conf_dict.get('model','Ott-Parsivel2').lower()
        self.set_weathercodes = conf_dict.get('weathercodes',name)==name
        self.set_visibility = conf_dict.get('visibility',name)==name
        self.set_precipitation = conf_dict.get('precipitation','-----')==name
        self.set_rainDur = conf_dict.get('rainDur','-----')==name
        self.set_awekas = name in weeutil.weeutil.option_as_list(conf_dict.get('AWEKAS',[]))
        self.prefix = conf_dict.get('prefix')
        # Precipitation or non-precipitation conditions lasting
        # less than self.error_limit are considered erroneous.
        self.error_limit = conf_dict.get('error_limit',60) # seconds
        
        self.data_queue = data_queue
        self.query_interval = query_interval
        self.device_interval = 60
        self.last_data_ts = time.time()+120
        
        # Intialize variables for AWEAKAS support
        self.last_awekas = None
        self.last_awekas_ct = 10
        self.sent_awekas = None

        self.db_fn = os.path.join(conf_dict['SQLITE_ROOT'],self.name)
        self.db_timeout = conf_dict.get('db_timeout',10)
        self.db_conn = None
        
        # list of present weather codes of the last hour, initialized
        # by the contents of the json file saved at thread stop
        self.presentweather_list = []
        self.next_presentweather_error = 0
        self.presentweather_lock = threading.Lock()
        try:
            with open(self.db_fn+'.json','rt') as file:
                self.presentweather_list = json.load(file)
        except FileNotFoundError:
            pass
        # delete outdated elements
        while len(self.presentweather_list)>0 and self.presentweather_list[0][1]<(time.time()-3600):
            del self.presentweather_list[0]
        
        self.next_obs_errors = dict()
        self.last_rain = None
        self.last_sensorState = None
        
        self.file = None
        self.socket = None
        # udp tcp restful usb none
        self.connection_type = conf_dict.get('type','none').lower()
        host = conf_dict.get('host')
        if host and self.connection_type in ('udp','tcp'): 
            host = socket.gethostbyname(host)
        self.host = host
        self.port = int(conf_dict.get('port'))
        
        self.running = True
        self.evt = threading.Event()
        
        if self.connection_type=='udp':
            # The device sends data by UDP.
            if self.port:
                loginf("thread '%s': UDP connection %s:%s" % (self.name,self.host,self.port))
            else:
                logerr("thread '%s': UDP configuration error" % self.name)
        elif self.connection_type=='tcp':
            # The device accepts TCP connections.
            if self.host and self.port:
                loginf("thread '%s': TCP connection to %s:%s" % (self.name,self.host,self.port))
            else:
                logerr("thread '%s': missing host and/or port for TCP connection" % self.name)
        elif self.connection_type in ('http','https','restful'):
            # The device has a restful interface.
            if self.host:
                loginf("thread '%s': HTTP(S) connection to %s" % (self.name,self.host))
            else:
                logerr("thread '%s': missing URL for HTTP(S) connection" % self.name)
        elif self.connection_type=='usb':
            # The device is connected by USB.
            if self.port:
                loginf("thread '%s': USB connection to %s" % (self.name,self.port))
            else:
                logerr("thread '%s': missing device for USB connection" % self.name)
        elif self.connection_type=='none':
            # simulator mode
            loginf("thread '%s': simulator mode, no real connection" % self.name)
        else:
            # no valid configuration
            logerr("thread '%s': unknown connection type '%s'" % (self.name,self.connection_type))

    def shutDown(self):
        """ Request thread shutdown. """
        self.running = False
        self.evt.set()
        loginf("thread '%s': shutdown requested" % self.name)
    
    def socket_open(self):
        """ Open connection to the device. """
        if __name__ == '__main__':
            print('socket_open()','start',self.connection_type)
        try:
            if self.connection_type=='udp':
                # UDP connection
                self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM | socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC)
                self.socket.bind(('',self.port))
            elif self.connection_type=='tcp':
                # TCP connection
                self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM | socket.SOCK_CLOEXEC)
                #select.select([self.socket],[self.socket],[self.socket])
                self.socket.connect((self.host,self.port))
            self.last_data_ts = time.time()+120
        except OSError as e:
            logerr("thread '%s': opening connection to %s:%s failed with %s %s, will be tried again" % (self.name,self.host,self.port,e.__class__.__name__,e))
            self.socket_close()
        if __name__ == '__main__':
            print('socket_open()','end',self.socket)
        
    
    def socket_close(self):
        """ Close connection to the device. """
        if self.socket:
            try:
                self.socket.close()
            except OSError as e:
                logerr("thread '%s': closing connection to %s:%s failed with %s %s",self.name,self.host,self.port,e.__class__.__name__,e)
            finally:
                self.socket = None
    
    def db_open(self):
        """ Open thread present weather database. """
        try:
            self.db_conn = sqlite3.connect(
                self.db_fn+'.sdb',
                timeout=self.db_timeout
            )
            cur = self.db_conn.cursor()
            reply = cur.execute('SELECT name FROM sqlite_master')
            rec = reply.fetchall()
            if rec and 'precipitation' in [ii[0] for ii in rec]:
                pass
                #reply = cur.execute('SELECT * FROM precipitation WHERE `start`>%d' % (time.time()-3600))
                #self.presentweather_list = reply.fetchall()
            else:
                cur.execute('CREATE TABLE precipitation(`start` INTEGER NOT NULL UNIQUE PRIMARY KEY,`stop` INTEGER NOT NULL,`ww` INTEGER,`wawa` INTEGER,`precipstart` INTEGER,`METAR` VARCHAR(5),`rainRate` REAL,`rain` REAL)')
                cur.execute('CREATE VIEW archive(`dateTime`,`usUnits`,`interval`,`presentweatherStart`,`precipitationStart`,`presentweatherTime`,`ww`,`wawa`,`METAR`,`rainRate`,`rain`) AS SELECT stop,17,(stop-start)/60,start,precipstart,stop-start,ww,wawa,METAR,rainRate,rain from precipitation order by stop')
            self.db_conn.commit()
            cur.close()
        except sqlite3.Error as e:
            logerr("thread '%s': SQLITE CREATE %s %s" % (self.name,e.__class__.__name__,e))
    
    def db_close(self):
        """ Close thread present weather database. """
        try:
            if self.db_conn:
                self.db_conn.close()
        except sqlite3.Error as e:
            logerr("thread '%s': SQLITE %s %s" % (self.name,e.__class__.__name__,e))
        finally:
            self.db_conn = None
    
    @staticmethod
    def is_el_precip(el):
        return is_ww_wawa_precipitation(el[2],el[3])
    
    def update_presentweather_list(self, ts, ww, wawa, metar, p_abs, p_rate):
        """ Maintain self.presentweather_list
            
            * Update the last element if the weather condition did not change.
            * Add a new element if the weather condition changed.
            * Check for erroneous readings and remove them from the list.
            * Remove outdated elements from the list.
            
            Args:
                ts (int): timestamp
                ww (int): new ww code
                wawa (int): new wawa code
                metar (str): new METAR code
                p_abs (float): accumulated precipitation
                p_rate (float): precipitation intensity
                
            Returns:
                int: start of precipitation timestamp or None
                updated self.presentweather_list
        """
        # A value of None is possible. Otherwise the value must be of
        # type int (or str in case of metar).
        if ww is not None: ww = int(ww)
        if wawa is not None: wawa = int(wawa)
        if metar is not None: metar = str(metar)
        # check for inconsistency between ww and wawa if both values are
        # present
        if ww is not None and wawa is not None:
            if (ww and not wawa) or (wawa and not ww):
                logerr('ww wawa inconsistency ww=%02d wawa=%02d' % (ww,wawa))
        # check if the actual weather code is different from the previous one
        if len(self.presentweather_list)==0:
            add = True
        else:
            add = (wawa!=self.presentweather_list[-1][3] or
                   ww!=self.presentweather_list[-1][2] or
                   metar!=self.presentweather_list[-1][5])
        # precipitation or not?
        is_precipitation = is_ww_wawa_precipitation(ww, wawa)
        # Check for values that appear only once. They will be considered
        # erroneous.
        if add and len(self.presentweather_list)>1:
            # There are at least 2 elements. 
            try:
                last_el = self.presentweather_list[-1]
                prev_el = self.presentweather_list[-2]
                if (PrecipThread.is_el_precip(last_el) and
                    not is_precipitation and
                    (wawa is not None or ww is not None) and
                    (last_el[1]-last_el[4])<self.error_limit):
                    # There was precipitation and now is not, and the
                    # precipitation lasted less than self.error_limit
                    # This is considered erroneous. We remove all
                    # the readings since the last non-precipitation
                    # reading.
                    if self.prefix:
                        # send negative rain duration value to
                        # compensate for the previously reported
                        # rain duration, now considered erroneous
                        rec = {
                            self.prefix+'RainDur':(
                                last_el[4]-last_el[1],
                                'second',
                                'group_deltatime'
                            )
                        }
                        if self.set_rainDur:
                            rec['rainDur'] = rec[self.prefix+'RainDur']
                        self.put_data(ts,rec)
                        #print('negative rainDur')
                    precipstart = last_el[4]
                    while (len(self.presentweather_list)>0 and
                           PrecipThread.is_el_precip(last_el) and
                           last_el[4]==precipstart):
                        loginf("thread '%s': discarded ww/wawa/w'w' %s/%s/%s lasting %s seconds" % (self.name,last_el[2],last_el[3],last_el[5],last_el[1]-last_el[0]))
                        # remove the last element
                        del self.presentweather_list[-1]
                        # If there are more elements in the list,
                        # point last_el to the new last element and
                        # remove that element from the database,
                        # as this is the new active element, and
                        # the active element is in memory only.
                        if len(self.presentweather_list)>0:
                            last_el = self.presentweather_list[-1]
                            try:
                                cur = self.db_conn.cursor()
                                cur.execute('DELETE FROM precipitation WHERE `start`=?',tuple((last_el[0],)))
                                self.db_conn.commit()
                                cur.close()
                            except sqlite3.Error as e:
                                logerr("thread '%s': SQLITE DELETE %s %s" % (self.name,e.__class__.__name__,e))
                            except LookupError:
                                pass
                        else:
                            last_el = None
                    # If there is still an element in the list
                    # (which should always be here), check if the
                    # actual weather code is different from
                    # the now last one of the list.
                    if last_el:
                        add = (wawa!=last_el[3] or
                               ww!=last_el[2] or
                               metar!=last_el[5])
                elif ((last_el[1]-last_el[0])<=max(self.device_interval,self.error_limit) and
                    (wawa is not None or ww is not None)):
                    # The last value appears only once.
                    if ((PrecipThread.is_el_precip(prev_el) and is_precipitation and not PrecipThread.is_el_precip(last_el)) or
                        (not PrecipThread.is_el_precip(prev_el) and not is_precipitation and PrecipThread.is_el_precip(last_el))):
                        # If there is one single reading of precipitation
                        # between readings of no precipitation consider this
                        # reading erroneous and remove it. The same applies
                        # for one single reading of no precipitation between
                        # readings of precipitation.
                        loginf("thread '%s': discarded ww/wawa %s/%s between %s/%s and %s/%s" % (self.name,last_el[2],last_el[3],prev_el[2],prev_el[3],ww,wawa))
                        add = False
                        if ww==prev_el[2] and wawa==prev_el[3]:
                            # It is the same weather code as that before 
                            # the single different one. So we can remove
                            # the record of the erroneous reading and
                            # use the previous one as actual one.
                            del self.presentweather_list[-1]
                            # If the removed element was precipitation,
                            # the element before (which is the last 
                            # and active element now) may be marked as
                            # short precipitation interruption. Remove
                            # that mark.
                            if not PrecipThread.is_el_precip(self.presentweather_list[-1]):
                                self.presentweather_list[-1][4] = None
                            # Now remove the last row from the database,
                            # as this is the active row again.
                            try:
                                cur = self.db_conn.cursor()
                                cur.execute('DELETE FROM precipitation WHERE `start`=?',tuple((self.presentweather_list[-1][0],)))
                                self.db_conn.commit()
                                cur.close()
                            except sqlite3.Error as e:
                                logerr("thread '%s': SQLITE DELETE %s %s" % (self.name,e.__class__.__name__,e))
                            except LookupError:
                                pass
                        else:
                            # The weather code is different from that before
                            # the erroneous reading. So we overwrite the
                            # record of the erroneous reading by the
                            # new data.
                            last_el[0] = int(ts-self.device_interval)
                            last_el[2] = ww
                            last_el[3] = wawa
                            last_el[4] = prev_el[4]
                            last_el[6] = None
                            last_el[7] = None
                            last_el[5] = metar
                            last_el[8] = p_rate
                            last_el[9] = 1
                            last_el[10] = p_abs
                    elif (PrecipThread.is_el_precip(prev_el) and
                          PrecipThread.is_el_precip(last_el) and
                          is_precipitation):
                        # 3 different elements of precipitation
                        if (prev_el[2] is not None and 
                            last_el[2] is not None and 
                            ww is not None):
                            # ww is present, so we use it
                            same = prev_el[2]==ww
                            prev_type = WW_TYPE_REVERSED[prev_el[2]]
                            last_type = WW_TYPE_REVERSED[last_el[2]]
                            now_type = WW_TYPE_REVERSED[ww]
                        elif (prev_el[3] is not None and
                              last_el[3] is not None and
                              wawa is not None):
                            # check for wawa instead
                            same = prev_el[3]==wawa
                            prev_type = WAWA_TYPE_REVERSED[prev_el[3]]
                            last_type = WAWA_TYPE_REVERSED[last_el[3]]
                            now_type = WAWA_TYPE_REVERSED[wawa]
                        else:
                            # neither ww nor wawa is present
                            same = False
                            prev_type = '--'
                            last_type = '--'
                            now_type = '--'
                        #if (same and 
                        #      (prev_type==last_type or 
                        #      (prev_type=='RADZ' and last_type in ('RA','DZ')) or
                        #      (prev_type in ('RA','DZ') and last_type=='RADZ'))
                        #   ):
                        if (same or
                            (last_type not in ('RA','DZ','RADZ') and prev_type in ('RA','DZ','RADZ') and now_type in ('RA','DZ','RADZ'))
                           ):
                            # The actual code is the same as the previous
                            # one. In between there is a code of the same
                            # type of precipitation, but of different 
                            # intensity, which lasted for a short time
                            # only.
                            # Test 2023-11-22: type of precipitation not
                            # checked any more
                            loginf("thread '%s': added ww/wawa %s/%s to %s/%s, new %s/%s" % (self.name,last_el[2],last_el[3],prev_el[2],prev_el[3],ww,wawa))
                            prev_el[1] = last_el[1]
                            prev_el[8] += last_el[8] # p_rate sum
                            prev_el[9] += last_el[9] # p_rate count
                            prev_el[10] = last_el[10] # last p_abs
                            del self.presentweather_list[-1]
                            add = not same
                            # Now remove the last row from the database,
                            # as this is the active row again.
                            try:
                                cur = self.db_conn.cursor()
                                cur.execute('DELETE FROM precipitation WHERE `start`=?',tuple((self.presentweather_list[-1][0],)))
                                self.db_conn.commit()
                                cur.close()
                            except sqlite3.Error as e:
                                logerr("thread '%s': SQLITE DELETE %s %s" % (self.name,e.__class__.__name__,e))
                            except LookupError:
                                pass
            except (LookupError,ValueError,TypeError,ArithmeticError):
                pass
        # add a new record or update the timestamp
        if add:
            # The weather code changed, so add a new record.
            if len(self.presentweather_list)>0:
                # There are already elements in the list. First determine 
                # start timestamp of precipitation if any
                was_precipitation = PrecipThread.is_el_precip(self.presentweather_list[-1])
                if is_precipitation and was_precipitation:
                    # precipitation continues, changed weather code only
                    precipstart = self.presentweather_list[-1][4]
                elif is_precipitation:
                    # precipitation with no precipitation before
                    precipstart = int(ts-self.device_interval)
                    # check for short precipitation interruption
                    # TODO: more than one "no precipitation" record after another
                    if (len(self.presentweather_list)>1 and
                        self.presentweather_list[-2][4]):
                        no_precip_duration = self.presentweather_list[-1][1]-self.presentweather_list[-1][0]
                        precip_duration = self.presentweather_list[-2][1]-self.presentweather_list[-2][4]
                        if (no_precip_duration<600 and
                            no_precip_duration<precip_duration):
                            precipstart = self.presentweather_list[-2][4]
                            self.presentweather_list[-1][4] = precipstart
                else:
                    # actually no precipitation
                    precipstart = None
                # average precipitation rate
                # Note: self.presentweather_list[-1][9] can be 0.
                try:
                    p_rate_avg = self.presentweather_list[-1][8]/self.presentweather_list[-1][9]
                except (LookupError,TypeError,ValueError,ArithmeticError):
                    p_rate_avg = None
                # total amount of precipitation while the weather cond. lasts
                try:
                    p_amount = self.presentweather_list[-1][10]-self.presentweather_list[-2][10]
                    if p_amount<0:
                        # TODO: if the max. is really 300, add 300 instead
                        # of raising an exception
                        raise ValueError('overflow')
                except (LookupError,TypeError,ValueError,ArithmeticError):
                    p_amount = None
                # save the last element to the database
                try:
                    cur = self.db_conn.cursor()
                    cur.execute('INSERT INTO precipitation VALUES (?,?,?,?,?,?,?,?)',tuple(self.presentweather_list[-1][:6]+[p_rate_avg,p_amount]))
                    self.db_conn.commit()
                    cur.close()
                except sqlite3.Error as e:
                    logerr("thread '%s': SQLITE INSERT %s %s" % (self.name,e.__class__.__name__,e))
                except LookupError:
                    pass
            else:
                # The list is empty. That means there is no information about 
                # the previous weather condition available.
                if is_precipitation:
                    # Precipitation is falling. As we do not know about
                    # the past we use the start of the actual weather
                    # condition as the start of the precipitation
                    precipstart = int(ts-self.device_interval)
                else:
                    # actually no precipitation
                    precipstart = None
            # Add the new record.
            if p_rate is None:
                sm = 0.0
                ct = 0.0
            else:
                sm = p_rate
                ct = 1.0
            self.presentweather_list.append([int(ts-self.device_interval),int(ts),ww,wawa,precipstart,metar,None,None,sm,ct,p_abs])
        else:
            # The weather code is the same as before, so update the end
            # timestamp.
            self.presentweather_list[-1][1] = int(ts)
            precipstart = self.presentweather_list[-1][4]
            try:
                self.presentweather_list[-1][8] += p_rate
                self.presentweather_list[-1][9] += 1.0
            except (LookupError,TypeError,ValueError,ArithmeticError):
                pass
            self.presentweather_list[-1][10] = p_abs
        # remove the first element if it ends more than an hour ago
        if self.presentweather_list[0][1]<(ts-3600):
            self.presentweather_list.pop(0)
        # Now we have a list of the weather codes of the last hour.
        if __name__ == '__main__' and TEST_LOG_THREAD:
            print('presentweather_list',self.presentweather_list)
        return precipstart
    
    def presentweather(self, ts):
        """ Postprocessing of ww and wawa.
            
            enhances ww and wawa and calculates `presentweatherStart`,
            `presentweatherTime`, and `precipitationStart`
            
            If the function is called from new_archive_record there may
            be weather conditions in the list which start after the
            archive period ended. Therefore the parameter ts is
            required.
            
            Args:
                ts (int): timestamp of the end of the timespan to process
            
            Returns:
                ww (int): postprocessed ww code
                wawa (int): postprocessed wawa code
                start (int): `presentweatherStart`
                elapsed (int): `presentweatherTime`
        """
        # start timestamp and duration of the current weather condition
        # (We do not care about the intensity of precipitation here.)
        # observation types `presentweatherStart` and `presentweatherTime`
        precip_duration = 0
        start = None
        start2x = None
        wwtype = None
        wawatype = None
        intsum = 0
        dursum = 0
        weather2x = None
        duration2x = ((None,None),(None,None))
        Wa_list = [0]*10
        try:
            dur_dict = {'ww':dict(), 'wawa':dict(), 'metar':dict()}
            for idx,ii in enumerate(self.presentweather_list):
                if ts and ii[0]>ts:
                    break
                if __name__ == '__main__' and TEST_LOG_THREAD:
                    print('idx',idx,'ii',ii)
                duration = ii[1]-ii[0]
                ww = ii[2]
                wawa = ii[3]
                # get weather type 
                wwtype1 = WW_TYPE_REVERSED.get(ii[2],ii[2]) 
                wawatype1 = WAWA_TYPE_REVERSED.get(ii[3],ii[3]) 
                # compare to the weather type of the previous timespan
                if wwtype1!=wwtype or wawatype1!=wawatype:
                    # weather type changed
                    wwtype = wwtype1
                    wawatype = wawatype1
                    start = ii[0]
                #
                if PrecipThread.is_el_precip(ii):
                    precip_duration += duration
                #
                if ii[4]:
                    # precipitation or short interruption of precipitation
                    if idx==0 and ii[4]!=ii[0]:
                        # If the precipitation started before the timespan
                        # of the first element of the list, initialize 
                        # dursum and intsum with the duration of the 
                        # precipitation before the first element in the list.
                        # As no information of the intensity is available,
                        # assume light intensity.
                        if ii[6] is None or ii[7] is None:
                            dursum = ii[0]-ii[4]
                            intsum = dursum
                        else:
                            dursum = ii[7]
                            intsum = ii[6]
                    # Why save intsum and dursum with the list element?
                    # Elements that end before 1 hour ago are removed
                    # from the list. If precipitation started more than
                    # 1 hour ago, intsum and dursum cannot be calculated
                    # from the beginning. So it is necessary to remember
                    # the sums.
                    ii[7] = dursum
                    ii[6] = intsum
                    if PrecipThread.is_el_precip(ii):
                        # Short interruptions of precipitation are not included
                        # in the intensity average.
                        if ii[2] is not None:
                            intensity = WW_INTENSITY_REVERSED.get(ii[2],0)
                        elif ii[3] is not None:
                            intensity = WAWA_INTENSITY_REVERSED.get(ii[3],0)
                        else:
                            intensity = 0
                        dursum += duration
                        intsum += duration*intensity
                        weather2x = ii
                        # prepare determining "state after precipition"
                        # weather code
                        ww2 = WW2_REVERSED.get(ii[2],ii[2])
                        wawa2 = WAWA2_REVERSED.get(ii[3],ii[3])
                        try:
                            metar2 = ii[5].replace('+','').replace('-','')
                        except AttributeError:
                            metar2 = ii[5]
                        dur_dict['ww'][ww2] = dur_dict['ww'].get(ww2,0)+duration
                        dur_dict['wawa'][wawa2] = dur_dict['wawa'].get(wawa2,0)+duration
                        dur_dict['metar'][metar2] = dur_dict['metar'].get(metar2,0)+duration
                        if __name__=='__main__' and TEST_LOG_THREAD:
                            print('     ','dur_dict',dur_dict)
                else:
                    # No precipitation and no short interruption of 
                    # precipitation
                    ii[7] = dursum
                    ii[6] = intsum
                    is2 = False
                    if dursum:
                        # average precipitation intensity during the last
                        # precipitation period
                        intensity_avg = intsum/dursum
                        # How intense or long the precipitation was?
                        # Intensity: 0 - unknown, 1 - light, 2 - moderate, 
                        #            3 - heavy
                        # no source for that rule
                        if intensity_avg>=2.5:
                            # heavy precipitation
                            if dursum>=150: is2 = True
                        elif intensity_avg>=1.5:
                            # moderate precipitation
                            if dursum>=300: is2 = True
                        elif intensity_avg>=0.5:
                            # light precipitation
                            if dursum>=450: is2 = True
                        else:
                            # unknown intensity
                            if dursum>=200: is2 = True
                    intsum = 0
                    dursum = 0
                    # If the precipitation intensity and duration before 
                    # suggest a "state after precipitation" return 
                    # weather code 20...29 otherwise 00.
                    # no source for that rule
                    # Note: Even if that precipitation period was too short
                    #       to set start2x, a previous end of precipitation
                    #       may have set start2x.
                    if is2:
                        # remember timestamp of start
                        start2x = ii[0]
                        # For wawa: Generally use the highest code reported
                        # within the last hour. If the duration of the 
                        # condition with the highest code is much shorter 
                        # than longest duration in the dict, then use the code
                        # that lasted longest. Nevertheless, thunderstorms
                        # and freezing precipitation take precedence over all.
                        max_wawa2_code = max(dur_dict['wawa'].items(),key=lambda x:x[0],default=(None,None))
                        max_wawa2_dur = max(dur_dict['wawa'].items(),key=lambda x:x[1],default=(None,None))
                        if (max_wawa2_code[0]!=max_wawa2_dur[0] and 
                            max_wawa2_code[0]!=26 and
                            max_wawa2_code[0]!=25):
                            if (max_wawa2_code[1]*3)<max_wawa2_dur[1]:
                                max_wawa2_code = max_wawa2_dur
                        # For ww: If both snow (22) and rain (21) or drizzle 
                        # (20) is in the dict, summarize them all together 
                        # in code 23.
                        if ((22 in dur_dict['ww'] and 
                             (21 in dur_dict['ww'] or 20 in dur_dict['ww'])) or 
                             23 in dur_dict['ww']):
                            dur_dict['ww'][23] = dur_dict['ww'].get(23,0)+dur_dict.pop(20,0)+dur_dict.pop(21,0)+dur_dict.pop(22,0)
                        # For ww: Use the code with the highest precedence
                        # or the longest duration
                        max_ww_dur = max(dur_dict['ww'].items(),key=lambda x:x[1],default=(None,None))
                        if 29 in dur_dict['ww']:
                            # thunderstorm is most important
                            max_ww_dur = (29,dur_dict['ww'][29])
                        elif 27 in dur_dict['ww']:
                            # hail shower is second important
                            max_ww_dur = (27,dur_dict['ww'][27])
                        elif 24 in dur_dict['ww']:
                            # freezing precipitation is third important
                            max_ww_dur = (24,dur_dict['ww'][24])
                        duration2x = (max_ww_dur, max_wawa2_code)
                        if __name__=='__main__' and TEST_LOG_THREAD:
                            print('     ','duration2x',duration2x,'dur_dict',dur_dict)
                    # re-initialize dur_dict
                    dur_dict = {'ww':dict(), 'wawa':dict(), 'metar':dict()}
                # past weather code
                try:
                    if ww is not None:
                        Wa_list[WA_WW_REVERSED[ww]] += duration
                    elif wawa is not None:
                        Wa_list[WA_WAWA_REVERSED[wawa]] += duration
                except LookupError:
                    # invalid code
                    pass
            if start:
                elapsed = ts-start if ts else self.presentweather_list[-1][1]-start
                start = int(start)
            else:
                elapsed = None
        except (LookupError,TypeError,ValueError,ArithmeticError):
            elapsed = None
            start = None
            start2x = None
        if len(self.presentweather_list)<2:
            # The weather did not change during the last hour.
            return ww, wawa, start, elapsed, Wa_list
        if (len(self.presentweather_list)==2 and 
            not self.presentweather_list[0][2] and 
            not self.presentweather_list[0][3]):
            # No significant weather at the beginning of the last hour,
            # then one significant weather condition.
            return ww, wawa, start, elapsed, Wa_list
        """
        # One kind of weather only (not the same code all the time, but
        # always rain or always snow etc.)
        if len(wawa_dict)<=1 and len(ww_dict)<=1:
            return ww, wawa, start, elapsed
        """
        # Is there actually some significant weather?
        if wawa or ww:
            # weather detected
            # TODO: detect showers
            return ww, wawa, start, elapsed, Wa_list
        elif elapsed>3600:
            # more than one hour no significant weather
            return ww, wawa, start, elapsed, Wa_list
        else:
            # The significant weather  ended within the last hour. That means, the
            # weather code is 20...29.
            if start2x and start2x>(ts-3600) and duration2x:
                return duration2x[0][0],duration2x[1][0],start,elapsed,Wa_list
            #if start2x and start2x>(ts-3600) and weather2x:
            #    return WW2_REVERSED.get(weather2x[2],ww),WAWA2_REVERSED.get(weather2x[3],wawa),start,elapsed
            return ww, wawa, start, elapsed, Wa_list
            
    def awekaspresentweather(self, awekas, record):
        """ set the AWEKASpresentweather observation type 
        
            To remove single different readings, send a precipitation code
            after at least 3 times precipitation codes. The same applies
            to non-precipitation codes.
        
            Args:
                awekas (int): new AWEKAS present weather code
                record (dict): LOOP record
                
            Returns:
                adds element `AWEKASpresentweather` 
        """
        if (awekas is not None and self.last_awekas is not None and
            ((awekas>=8 and self.last_awekas<8) or
            (awekas<8 and self.last_awekas>=8))):
            # was no precipitation and is now precipitation or
            # was precipitation and is now no precipitation
            self.last_awekas_ct = 0
        else:
            # preciptation state did not change
            self.last_awekas_ct += 1
            if self.last_awekas_ct>2:
                self.sent_awekas = awekas
        record['AWEKASpresentweather'] = (self.sent_awekas,'byte','group_data')
        self.last_awekas = awekas
    
    def getRecord(self, ot):
        """ fetch data from the device and decode it
        """
    
        if __name__ == '__main__' and TEST_LOG_THREAD:
            print()
            print('-----',self.name,'-----',ot,'-----',self.connection_type,'-----')

        # fetch data from device
        
        if self.connection_type in ('udp','tcp'):
            # UDP or TCP connection
            # In this case the device sends data by itself. We cannot 
            # control the interval. We have to process the data as they
            # arrive. The select() function pauses the thread until data
            # is available.
            if self.last_data_ts<time.time() and self.socket:
                # The last data arrived more than 10 minutes ago. May
                # be the connection is broken. Close it to force it
                # re-opened
                self.socket_close()
                logerr("thread '%s': no data for more than 10 minutes. Re-open socket." % self.name)
                self.evt.wait(self.query_interval)
            if not self.socket: 
                # If the socket is not opened, open it.
                self.socket_open()
            if not self.socket: 
                # The socket could not be opened. Wait and then return,
                # which means to try it again.
                self.evt.wait(self.query_interval)
                return
            reply = b''
            while self.running:
                # pause thread and wait for data from the device
                rlist, wlist, xlist = select.select([self.socket],[],[],self.query_interval)
                # If shutdown is requested, log and return
                if not self.running:
                    loginf("thread '%s': self.running==False getRecord() select() r %s w %s x %s" % (self.name,rlist,wlist,xlist))
                    return
                # No data received until timeout --> return
                if not rlist: 
                    return
                # get available data from the device
                try:
                    if self.connection_type=='udp':
                        # UDP connection
                        reply, source_addr = self.socket.recvfrom(8192)
                        if source_addr!=self.host: 
                            logerr("thread '%s': received data from %s but %s expected" %(self.name,source_addr,self.host))
                            return
                        break
                    else:
                        # TCP connection
                        x = self.socket.recv(8192)
                        reply += x
                        if b'\n' in reply: break
                except OSError as e:
                    logerr("thread '%s': error receiving data %s %s" % (self.name,e.__class__.__name__,e))
                    self.socket_close()
                    return
            # The very first telegram may be incomplete, so do not process it.
            if ot=='once': return
            # Convert bytes to ASCII string
            reply = reply.decode('ascii',errors='ignore')
        elif self.connection_type in ('restful','http','https'):
            # restful service
            # TODO
            pass
        elif self.connection_type=='usb':
            # The device is connected by USB
            if not self.file: 
                self.file = open(self.port,'rt')
                os.set_blocking(file.fileno(), False)
            if not self.file: return
            reply = ''
            while ('\n' not in reply) and self.running:
                rlist, wlist, xlist = select.select([self.file],[],[],5)
                if not rlist or not self.running: return
                reply += file.read()
            if ot=='once': return
        else:
            # simulator mode
            if ot=='once':
                # Initialization message
                reply = "Ott Parsivel2\r\n"
                self.device_interval = self.query_interval
            else:
                temp = int(round(25+2*math.sin((time.time()%30)/30*math.pi),0))
                since = int(time.time()-self.start_ts)
                if __name__ == '__main__' and TEST_LOG_THREAD:
                    print('///////////////////////',since,'///////////////////////')
                if SIMULATE_ERRONEOUS_READING:
                    # erroneous reading
                    self.rain_simulator = 0
                    if since==30:
                        loginf("Simulator: erroneous value ###########################################")
                        ww = 51
                        rainrate = 0.1
                    else:
                        ww = 0
                        rainrate = 0.0
                else:
                    # 30s no precipitation, then 90s rain, then again no
                    # precipitation
                    if since<30: self.rain_simulator = 0
                    if since>150 or since<30: 
                        ww = 0
                        rainrate = 0.0
                    elif since>120:
                        ww = 71
                        rainrate = 0.2
                        self.rain_simulator += 0.1
                    else:
                        ww = 53
                        rainrate = 0.1
                        self.rain_simulator += 0.25
                reply = "200248;%7.3f;%7.2f;%02d;-9.999;9999;000.00;%03d;15759;00000;0;\r\n" % (rainrate,self.rain_simulator,ww,temp)
        
        if not self.running: 
            loginf("thread '%s': self.running==False getRecord() after reading data" % self.name)
            return
        
        # process data
        
        if ((self.field_separator not in reply) or 
            (self.record_separator not in reply)):
            return
        ts = int(time.time())
        ww = None
        wawa = None
        metar = None
        p_abs = None
        p_rate = None
        # record contains value tuples here.
        record = dict()
        if (self.model.startswith('ott-parsivel') or 
            self.model in ('thies-lnm','generic')):
            # Thies LNM: initialize special values, process STX
            if self.model=='thies-lnm':
                deviceState = [None]*16
                if reply[0]==chr(2): reply = reply[1:]
            # OTT Parsivel: initialize special values
            if self.model.startswith('ott-parsivel'):
                p_count = None
            # If the remaining telegram string does not contain a field
            # separator any more, there is no field to process any more.
            if self.field_separator not in reply: reply = ''
            # Process telegram fields
            for ii in self.telegram_list:
                # thread stop requested
                if not self.running: 
                    loginf("thread '%s': self.running==False getRecord() for telegram_list loop" % self.name)
                    return
                # if there are not enough fields within the data telegram
                # stop processing
                if not reply: break
                # split the first remaining field 
                x = reply.split(self.field_separator,1)
                try:
                    val = x[0]
                except LookupError:
                    val = ''
                try:
                    reply = x[1]
                except LookupError:
                    reply = ''
                # not enough data
                # (for example if the connection starts inmidst of a
                # telegram)
                if val=='\r\n':
                    record = dict()
                    break
                # convert the field value string to the appropriate data type
                try:
                    if ii[0]==19 and self.model.startswith('ott-parsivel'):
                        # date and time
                        # TODO
                        val = (...,'unixepoch','group_time')
                    elif ii[0]==34 and self.model.startswith('ott-parsivel'):
                        # energy
                        # (According to the unit J/(m^2h) it is not energy
                        # but power.)
                        val = (float(val)/3600.0,'watt_per_meter_squared','group_rainpower')
                    elif ii[0]==61 and self.model.startswith('ott-parsivel'):
                        # list of all particles
                        # Note: If no. 60 does not precede no. 61, the count
                        #       of values is unknown to the driver.
                        if p_count is not None:
                            reply = val+self.field_separator+reply
                            val = []
                            for jj in range(p_count):
                                x = reply.split(self.field_separator,2)
                                try:
                                    val.append((float(x[0]),float(x[1])))
                                except (LookupError,ValueError,TypeError):
                                    val.append((None,None))
                                try:
                                    reply = x[2]
                                except LookupError:
                                    reply = ''
                                if not reply:
                                    break
                            val = (val,None,None)
                        else:
                            logerr('unknown length of field 61')
                            reply = ''
                    elif ii[5]=='string':
                        # string
                        val = weewx.units.ValueTuple(str(val),None,None)
                    elif ii[7]=='INTEGER':
                        # counter, wawa, ww
                        val = weewx.units.ValueTuple(int(val),ii[5],ii[6])
                    elif ii[7]=='REAL':
                        # float
                        val = weewx.units.ValueTuple(float(val),ii[5],ii[6])
                    else:
                        print('error')
                    # correct firmware error
                    if (self.model.startswith('ott-parsivel') and
                        ii[0]==4 and
                        val[0]==62):
                        val = weewx.units.ValueTuple(61,ii[5],ii[6])
                    # include reading in record
                    if ii[4]:
                        # ii[4] already includes prefix here.
                        record[ii[4]] = val
                    # remember weather codes
                    if ii[6]=='group_wmo_wawa': wawa = val[0]
                    if ii[6]=='group_wmo_ww': ww = val[0]
                    if self.model.startswith('ott-parsivel'):
                        if ii[0]==5: metar = val[0]
                    elif self.model=='thies-lnm':
                        if ii[4].endswith('METAR'): metar = val[0]
                    # additional processing 
                    if ((ii[0]==2 and self.model.startswith('ott-parsivel')) or
                        (ii[0]==17 and self.model=='thies-lnm')):
                        # rain
                        if self.last_rain is not None and self.prefix:
                            rain = val[0]-self.last_rain
                            if val[0]<self.last_rain:
                                rain += 300.0
                            record[self.prefix+'Rain'] = (rain,'mm','group_rain')
                        self.last_rain = val[0]
                    if self.model.startswith('ott-parsivel'):
                        # Ott-Hydromet Parsivel1+2
                        if ii[0]==18:
                            # sensor state
                            if self.last_sensorState is None or self.last_sensorState!=val[0]:
                                if val[0]>0:
                                    logerr("thread '%s': sensor error %s" % (self.name,val[0]))
                                elif self.last_sensorState is None or self.last_sensorState>0:
                                    loginf("thread '%s': sensor ok" % self.name)
                            self.last_sensorState = val[0]
                        elif ii[0]==9:
                            # data sending interval
                            self.device_interval = val[0]
                        elif ii[0]==1:
                            # rain intensity
                            p_rate = val[0]
                        elif ii[0]==2:
                            # rain accumulated
                            p_abs = val[0]
                        elif ii[0]==60:
                            p_count = val[0]
                    elif self.model=='thies-lnm':
                        # Thies LNM
                        if 22<=ii[0]<38: deviceState[ii[0]-22] = val[0]
                        if ii[0]==14: p_rate = val[0]
                        if ii[0]==17: p_abs = val[0]
                except (LookupError,ValueError,TypeError,ArithmeticError) as e:
                    # log the same error once in 300 seconds only
                    if ii[4] not in self.next_obs_errors:
                        self.next_obs_errors[ii[4]] = 0
                    if self.next_obs_errors[ii[4]]<time.time():
                        logerr("thread '%s': %s %s %s traceback %s" % (self.name,ii[4],e.__class__.__name__,e,gettraceback(e)))
                        self.next_obs_errors[ii[4]] = time.time()+300
            # Thies LNM: list of state values (no. 22 to 37)
            if self.model=='thies-lnm' and self.prefix:
                record[self.prefix+'DeviceError'] = (deviceState[0:7],'byte','group_data')
                record[self.prefix+'DeviceWarning'] = (deviceState[7:15],'byte','group_data')
        #elif self.model=='...'
        #    ...
        else:
            logerr("thread '%s': unknown model '%s'" % (self.name,self.model))
            self.shutDown()

        # If WeeWX requested to shutdown stop further processing and
        # return immediately.
        if not self.running: 
            loginf("thread '%s': self.running==False getRecord() after telegram_list loop" % self.name)
            return
        
        # update self.presentweather_list and set `...History` and `precipitationStart`
        if self.presentweather_lock.acquire():
            try:
                pstart = self.update_presentweather_list(ts, ww, wawa, metar, p_abs, p_rate)
                if record:
                    if self.prefix:
                        # history of present weather codes of the last hour
                        record[self.prefix+'History'] = (self.presentweather_list,'byte','group_data')
                    if self.set_weathercodes:
                        # start timestamp of the current precipitation
                        # independent of kind and intensity
                        record['precipitationStart'] = (pstart,'unix_epoch','group_time')
            except (LookupError,ValueError,TypeError,ArithmeticError) as e:
                # throttle the error logging frequency to once in 5 minutes
                if self.next_presentweather_error<time.time():
                    logerr("thread '%s': update present weather list %s %s traceback %s" % (self.name,e.__class__.__name__,e,gettraceback(e)))
                    if __name__ == '__main__':
                        self.next_presentweather_error = 0
                    else:
                        self.next_presentweather_error = time.time()+300
            finally:
                self.presentweather_lock.release()

        # `...RainDur`
        if record and self.prefix:
            # precipitation duration
            record[self.prefix+'RainDur'] = (
                self.device_interval if is_ww_wawa_precipitation(ww,wawa) else 0.0,
                'second',
                'group_deltatime')

        # AWEKAS support
        if record and self.set_awekas:
            if ww is not None:
                self.awekaspresentweather(WW_AWEKAS.get(ww),record)
            elif wawa is not None:
                self.awekaspresentweather(WAWA_AWEKAS.get(wawa),record)

        # Postprocess the present weather codes ww and wawa
        if record and self.set_weathercodes:
            try:
                # Postprocess readings and maintain thread database
                if self.presentweather_lock.acquire():
                    try:
                        ww, wawa, since, elapsed, wa_list = self.presentweather(ts)
                    finally:
                        self.presentweather_lock.release()
                    if ww is not None: 
                        record['ww'] = (ww,'byte','group_wmo_ww')
                        record['presentweatherWw'] = (ww,'byte','group_wmo_ww')
                    if wawa is not None: 
                        record['wawa'] = (wawa,'byte','group_wmo_wawa')
                        record['presentweatherWawa'] = (wawa,'byte','group_wmo_wawa')
                    if since: 
                        record['presentweatherStart'] = (since,'unix_epoch','group_time')
                    if elapsed is not None: 
                        record['presentweatherTime'] = (elapsed,'second','group_deltatime')
            except (LookupError,ValueError,TypeError,ArithmeticError) as e:
                # throttle the error logging frequency to once in 5 minutes
                if self.next_presentweather_error<time.time():
                    logerr("thread '%s': present weather %s %s traceback %s" % (self.name,e.__class__.__name__,e,gettraceback(e)))
                    if __name__ == '__main__':
                        self.next_presentweather_error = 0
                    else:
                        self.next_presentweather_error = time.time()+300

        # `visibility`
        if record and self.set_visibility and self.prefix:
            try:
                if (self.prefix+'MOR') in record: 
                    record['visibility'] = record[self.prefix+'MOR']
            except (LookupError,ValueError,TypeError,ArithmeticError) as e:
                pass

        # `rain` and `rainRate`
        if record and self.set_precipitation and self.prefix:
            # Generally the readings of `rain` and `rainRate` are not 
            # provided by this extension but by the driver that is set
            # up by the `station_type` key in the `[Station]` section
            # of weewx.conf. In case you want this extension to provide
            # `rain` and `rainRate` you can set up a `precipitation`
            # key in the `[PrecipMeter]` section and have it point to
            # the device subsection you want to get the readings from.
            try:
                if (self.prefix+'Rain') in record:
                    record['rain'] = record[self.prefix+'Rain']
                if (self.prefix+'RainRate') in record:
                    record['rainRate'] = record[self.prefix+'RainRate']
            except (LookupError,ValueError,TypeError,ArithmeticError):
                pass
        
        # `rainDur`
        if record and self.set_rainDur and self.prefix:
            try:
                if (self.prefix+'RainDur') in record:
                    record['rainDur'] = record[self.prefix+'RainDur']
            except (LookupError,ValueError,TypeError,ArithmeticError):
                pass
        
        # send record to queue for processing in the main thread
        
        if __name__ == '__main__' and TEST_LOG_THREAD:
            print(record)
        if ot=='loop':
            self.put_data(ts,record)
            self.last_data_ts = time.time()+600
        
    def put_data(self, ts, x):
        """ put data into the queue for further processing in PrecipData 
        
            Args:
                ts (int): timestamp of the data
                x (dict): data
                
            Returns:
                nothing
        """
        if x:
            if self.data_queue:
                try:
                    self.data_queue.put((self.name,x,ts),
                                block=False)
                except queue.Full:
                    # If the queue is full (which should not happen),
                    # ignore the packet
                    pass
                except (KeyError,ValueError,LookupError,ArithmeticError) as e:
                    logerr("thread '%s': %s %s traceback %s" % (self.name,e.__class__.__name__,e,gettraceback(e)))
                    
    def get_archive_record(self, timespan):
        """ get an archive record for timespan 
        
            Some readings included in the LOOP packets may be detected as
            erroneous and removed later. So process the actual list of 
            weather conditions. Do not accumulate the readings out of the 
            LOOP packets.
            
            Args:
                timespan (TimeSpan): archive period
                
            Returns:
                dict: dict of readings 
        """
        record = dict()
        ww_list = []
        wawa_list = []
        ww = None
        wawa = None
        since = None
        elapsed = None
        pstart = None
        if self.presentweather_lock.acquire():
            try:
                # get the ww and wawa codes during the archive period
                for el in self.presentweather_list:
                    if el[1]>timespan[0] and el[0]<=timespan[1]:
                        pstart = el[4]
                        if el[2] is not None:
                            ww_list.append(el[2])
                        if el[3] is not None:
                            wawa_list.append(el[3])
                # get the postprocessed values if necessary
                if self.set_weathercodes:
                    ww, wawa, since, elapsed, wa_list = self.presentweather(timespan[1])
            finally:
                self.presentweather_lock.release()
        # set the thread readings
        if self.prefix:
            record[self.prefix+'Ww'] = (max_ww(ww_list),'byte','group_wmo_ww')
            record[self.prefix+'Wawa'] = (max_wawa(wawa_list),'byte','group_wmo_wawa')
        # set the values out of the postprocessing
        if self.set_weathercodes:
            if ww is not None: 
                ww_list.append(ww)
                record['ww'] = (max_ww(ww_list),'byte','group_wmo_ww')
                record['presentweatherWw'] = (ww,'byte','group_wmo_ww')
            if wawa is not None:
                wawa_list.append(wawa) 
                record['wawa'] = (max_wawa(wawa_list),'byte','group_wmo_wawa')
                record['presentweatherWawa'] = (wawa,'byte','group_wmo_wawa')
            if since: 
                record['presentweatherStart'] = (since,'unix_epoch','group_time')
            if elapsed is not None: 
                record['presentweatherTime'] = (elapsed,'second','group_deltatime')
            record['precipitationStart'] = (pstart,'unix_epoch','group_time')
            self.pastweather(record,ww,wawa,wa_list)
        return record

    def pastweather(self, record, ww, wawa, wa_list):
        """ get W1, W2, Wa1, Wa2 """
        #WW2_WA = [5,6,7,7,None,8,8,8,3,9]
        #WAWA2_WA = [3,4,5,6,7,None,9,None,None,None]
        WA_W = [None,4,3,4,None,5,6,7,8,9]
        try:
            if ww is not None:
                wa0 = WA_WW_REVERSED[ww]
            elif wawa is not None:
                wa0 = WA_WAWA_REVERSED[wawa]
            else:
                return
        except (LookupError,TypeError):
            # invalid value
            return
        # What the actual weather code already expresses, we need not
        # repeat in past weather
        if wa0 is not None:
            wa_list[wa0] = 0
        # get Wa1 and Wa2
        # The higher the index, the higher the significance of the
        # weather condition. The value in the list is the duration 
        # of that weather condition during the last hour.
        wa1 = 0
        wa2 = 0
        for idx,val in enumerate(wa_list):
            if val>=60:
                wa2 = wa1
                wa1 = idx
        # get W1 and W2
        try:
            w1 = WA_W[wa1]
        except (LookupError,TypeError):
            w1 = None
        try:
            w2 = WA_W[wa2]
        except (LookupError,TypeError):
            w2 = None
        # set observations
        if ww is not None:
            record['hourPresentweatherW1'] = (w1,'byte','group_wmo_W')
            record['hourPresentweatherW2'] = (w2,'byte','group_wmo_W')
        if wawa is not None:
            record['hourPresentweatherWa1'] = (wa1,'byte','group_wmo_Wa')
            record['hourPresentweatherWa2'] = (wa2,'byte','group_wmo_Wa')
    
    def run(self):
        loginf("thread '%s' starting" % self.name)
        self.db_open()
        try:
            self.getRecord('once')
            while self.running:
                self.getRecord('loop')
                if self.connection_type in ('udp','tcp'):
                    if not self.socket:
                        self.getRecord('once')
                else:
                    if not self.running: break
                    self.evt.wait(self.query_interval)
        except Exception as e:
            logerr("thread '%s': %s %s" % (self.name,e.__class__.__name__,e))
            for ii in traceback.format_tb(e.__traceback__):
                for jj in ii.splitlines():
                    logerr("thread '%s': *** %s" % (self.name,jj.replace('\n',' ').strip()))
        finally:
            # remember the present weather codes of the last hour
            try:
                with open(self.db_fn+'.json','wt') as file:
                    json.dump(self.presentweather_list,file)
            except Exception as e:
                logerr("thread '%s': %s %s" % (self.name,e.__class__.__name__,e))
            # close socket and file descriptors
            if self.socket: 
                try:
                    self.socket_close()
                except Exception as e:
                    logerr("thread '%s': error closing socket %s %s" % (self.name,e.__class__.__name__,e))
            if self.file: 
                try:
                    self.file.close()
                except Exception as e:
                    logerr("thread '%s': error closing file %s %s" % (self.name,e.__class__.__name__,e))
            # close database connection
            try:
                self.db_close()
            except Exception as e:
                logerr("thread '%s': error closing database connection %s %s" % (self.name,e.__class__.__name__,e))
            # done
            loginf("thread '%s' stopped" % self.name)

##############################################################################
#    Service to receive and process weather condition data                   #
##############################################################################

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
        self.archive_interval = int(config_dict.get('StdArchive',configobj.ConfigObj()).get('archive_interval',300))
        sqlite_root = config_dict.get('DatabaseTypes',configobj.ConfigObj()).get('SQLite',configobj.ConfigObj()).get('SQLITE_ROOT','.')
        weewx.units.obs_group_dict.setdefault('ww','group_wmo_ww')
        weewx.units.obs_group_dict.setdefault('wawa','group_wmo_wawa')
        weewx.units.obs_group_dict.setdefault('presentweatherWw','group_wmo_ww')
        weewx.units.obs_group_dict.setdefault('hourPresentweatherW1','group_wmo_W')
        weewx.units.obs_group_dict.setdefault('hourPresentweatherW2','group_wmo_W')
        weewx.units.obs_group_dict.setdefault('presentweatherWawa','group_wmo_wawa')
        weewx.units.obs_group_dict.setdefault('hourPresentweatherWa1','group_wmo_Wa')
        weewx.units.obs_group_dict.setdefault('hourPresentweatherWa2','group_wmo_Wa')
        weewx.units.obs_group_dict.setdefault('presentweatherStart','group_time')
        weewx.units.obs_group_dict.setdefault('precipitationStart','group_time')
        weewx.units.obs_group_dict.setdefault('presentweatherTime','group_deltatime')
        weewx.units.obs_group_dict.setdefault('visibility','group_distance')
        weewx.units.obs_group_dict.setdefault('frostIndicator','group_boolean')
        weewx.units.obs_group_dict.setdefault('AWEKASpresentweather','group_data')
        # The accumulation for `ww`, `wawa`, `presentweatherStart`,
        # `precipitationStart`, and `presentweatherTime` is done within
        # the assigned thread because of special quality control.
        # Nevertheless it turned out that they need an accumulator anyway.
        weewx.accum.accum_dict.setdefault('ww',ACCUM_MAX)
        weewx.accum.accum_dict.setdefault('wawa',ACCUM_MAX)
        weewx.accum.accum_dict.setdefault('presentweatherWw',ACCUM_LAST)
        weewx.accum.accum_dict.setdefault('hourPresentweatherW1',ACCUM_LAST)
        weewx.accum.accum_dict.setdefault('hourPresentweatherW2',ACCUM_LAST)
        weewx.accum.accum_dict.setdefault('presentweatherWawa',ACCUM_LAST)
        weewx.accum.accum_dict.setdefault('hourPresentweatherWa1',ACCUM_LAST)
        weewx.accum.accum_dict.setdefault('hourPresentweatherWa2',ACCUM_LAST)
        weewx.accum.accum_dict.setdefault('presentweatherStart',ACCUM_LAST)
        weewx.accum.accum_dict.setdefault('precipitationStart',ACCUM_LAST)
        weewx.accum.accum_dict.setdefault('presentweatherTime',ACCUM_LAST)
        weewx.accum.accum_dict.setdefault('frostIndicator',ACCUM_MAX)
        weewx.accum.accum_dict.setdefault('AWEKASpresentweather',ACCUM_LAST)
        if 'PrecipMeter' in config_dict:
            ct = 0
            for name in config_dict['PrecipMeter'].sections:
                dev_dict = weeutil.config.accumulateLeaves(config_dict['PrecipMeter'][name])
                if 'loop' in config_dict['PrecipMeter'][name]:
                    dev_dict['loop'] = config_dict['PrecipMeter'][name]['loop']
                dev_dict['SQLITE_ROOT'] = sqlite_root
                if weeutil.weeutil.to_bool(dev_dict.get('enable',True)):
                    if self._create_thread(name,dev_dict):
                        ct += 1
            if ct>0 and __name__!='__main__':
                self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
                self.bind(weewx.END_ARCHIVE_PERIOD, self.end_archive_period)
                self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        # observation types to use in postprocessing the ww and wawa
        # readings
        self.obs_t5cm = site_dict.get('temp5cm')
        self.obs_t2m = site_dict.get('temp2m','outTemp')
        self.obs_s5cm = site_dict.get('soil5cm','soilTemp1')
        self.obs_windSpeed = site_dict.get('windSpeed','windSpeed')
        self.obs_windSpeed10 = site_dict.get('windSpeed10','windSpeed10')
        self.obs_windGust = site_dict.get('windGust','windGust')
        self.freezing_detection_source = site_dict.get('freezingPrecipDetectionSource','software')
        # Initialize variables for the special accumulators
        self.old_accum = dict()
        self.accum_start_ts = None
        self.accum_end_ts = None
        self.lightning_strike_ts = 0
        self.temp5cm_C = None
        self.temp2m_C = None
        self.soil5cm_C = None
        self.windGust = None
        self.windGust_ts = 0
        self.is_freezing = None
        # Initialize variables for AWEKAS support
        self.last_awekas = None
        self.current_awekas = None
        self.old_awekas = None
        # Register XType extension
        # Note: Prepend it to overwrite `max` for groups `group_wmo_ww` and
        #       `group_wmo_wawa`.
        self.xtype = PrecipXType()
        weewx.xtypes.xtypes.insert(0,self.xtype)

    def _create_thread(self, thread_name, thread_dict):
        """ Create device connection thread. """
        host = thread_dict.get('host')
        query_interval = thread_dict.get('query_interval',5)
        # IP address is mandatory.
        if not host:
            logerr("thread '%s': missing IP address" % thread_name) 
            return False
        loginf("thread %s, host %s, poll interval %s" % (thread_name,host,query_interval))
        # telegram config
        model = thread_dict.get('model','Ott-Parsivel2').lower()
        if (model in ('ott-parsivel','ott-parsivel1','ott-parsivel2') and 
            not 'loop' in thread_dict):
            # The prefix is 'ott' if no prefix is set by the user.
            if 'prefix' not in thread_dict:
                thread_dict['prefix'] = 'ott'
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
                                if jj[6] in ('group_count',
                                             'group_wmo_ww',
                                             'group_wmo_wawa',
                                             'group_boolean'):
                                    obsdatatype = 'INTEGER'
                                elif jj[5]=='string':
                                    obsdatatype = 'VARCHAR(%d)' % jj[2]
                                else:
                                    obsdatatype = 'REAL'
                                if nr in (90,91,93):
                                    width = 4 if nr==93 else 2
                                    for subfield in range((jj[2]+1)//len(jj[3])):
                                        if obstype:
                                            subobstype = '%s%0*d' % (obstype,width,subfield)
                                        else:
                                            subobstype = None
                                        t.append((jj[0],jj[1],len(jj[3]),jj[3],subobstype,)+jj[5:]+(obsdatatype,))
                                else:
                                    t.append(jj[0:4]+(obstype,)+jj[5:]+(obsdatatype,))
                                break
                        ct = None
                elif ii=='%':
                    ct = []
            thread_dict['loop'] = t
        elif model=='thies' and 'loop' not in thread_dict:
            # The prefix is 'thies' if the user did not set a prefix.
            if 'prefix' not in thread_dict:
                thread_dict['prefix'] = 'thies'
            if 'telegram' not in thread_dict:
                thread_dict['telegram'] = 4
            telegram = weeutil.weeutil.to_int(thread_dict['telegram'])
            t = []
            for ii in THIES[telegram]:
                if ii[4]:
                    if thread_dict['prefix']:
                        obstype = thread_dict['prefix']+ii[4][0].upper()+ii[4][1:]
                    else:
                        obstype = ii[4]
                else:
                    obstype = None
                if ii[6] in ('group_count',
                             'group_wmo_ww',
                             'group_wmo_wawa',
                             'group_boolean'):
                    obsdatatype = 'INTEGER'
                elif ii[5]=='string':
                    obsdatatype = 'VARCHAR(%d)' % ii[2]
                else:
                    obsdatatype = 'REAL'
                t.append(ii[0:4]+(obstype,)+ii[5:]+(obsdatatype,))
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
        self.threads[thread_name]['accum'] = dict()
        self.threads[thread_name]['prefix'] = thread_dict.get('prefix')
        # initialize observation types
        _accum = dict()
        # derived observation types
        if 'prefix' in thread_dict:
            # amount of rain during archive interval
            obstype = thread_dict['prefix']+'Rain'
            obsgroup = 'group_rain'
            weewx.units.obs_group_dict.setdefault(obstype,obsgroup)
            global table
            table.append((obstype,obsgroup))
            _accum[obstype] = ACCUM_SUM
            # precipitation duration
            obstype = thread_dict['prefix']+'RainDur'
            obsgroup = 'group_deltatime'
            weewx.units.obs_group_dict.setdefault(obstype,obsgroup)
            table.append((obstype,obsgroup))
            _accum[obstype] = ACCUM_SUM
            # present weather code history of the last hour
            # (for debugging purposes)
            # Warning: the `firstlast` accumulator converts all values
            #          to strings. So it cannot be used here.
            obstype = thread_dict['prefix']+'History'
            obsgroup = 'group_data'
            weewx.units.obs_group_dict.setdefault(obstype,obsgroup)
            _accum[obstype] = ACCUM_HISTORY
        # observation types that are readings from the device
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
                    if (obsgroup in ('group_deltatime','group_elapsed',
                                     'group_time','group_count') and
                        obstype not in weewx.accum.accum_dict):
                        _accum[obstype] = ACCUM_LAST
                    elif obsgroup in ('group_wmo_ww','group_wmo_wawa'):
                        _accum[obstype] = ACCUM_NOOP
                if (obstype.endswith('RainAccu') and
                    obstype not in weewx.accum.accum_dict):
                    _accum[obstype] = ACCUM_LAST
                if issqltexttype(obsdatatype):
                    _accum[obstype] = ACCUM_STRING
                table.append((obstype,obsdatatype))
        # add accumulator entries
        if _accum:
            loginf ("accumulator dict for '%s': %s" % (thread_name,_accum))
            weewx.accum.accum_dict.maps.append(_accum)
        # start thread
        self.threads[thread_name]['thread'].start()
        return True
        
    def shutDown(self):
        """ Shutdown threads. """
        # request shutdown
        for ii in self.threads:
            try:
                self.threads[ii]['thread'].shutDown()
            except:
                pass
        # wait at max 10 seconds for shutdown to complete
        timeout = time.time()+10
        for ii in self.threads:
            try:
                w = timeout-time.time()
                if w<=0: break
                self.threads[ii]['thread'].join(w)
                if self.threads[ii]['thread'].is_alive():
                    logerr("unable to shutdown thread '%s'" % self.threads[ii]['thread'].name)
            except:
                pass
        # report threads that are still alive
        _threads = [ii for ii in self.threads]
        for ii in _threads:
            try:
                if self.threads[ii]['thread'].is_alive():
                    logerr("unable to shutdown thread '%s'" % self.threads[ii]['thread'].name)
                del self.threads[ii]['thread']
                del self.threads[ii]['queue']
                del self.threads[ii]
            except:
                pass
        # remove XType extension
        weewx.xtypes.xtypes.remove(self.xtype)
        
    def _process_data(self, thread_name):
        """ Get and process data from the threads. """
        AVG_GROUPS = ('group_temperature','group_db','group_distance','group_volt')
        MAX_GROUPS = ('group_wmo_ww','group_wmo_wawa')
        SUM_GROUPS = ('group_deltatime',)
        # get collected data
        data = dict()
        ct = 0
        while True:
            try:
                # get the next packet
                reply = self.threads[thread_name]['queue'].get(block=False)
            except queue.Empty:
                # no more packets available so far
                break
            else:
                try:
                    self.presentweather(data[2],'ww',reply[1])
                except (LookupError,ValueError,TypeError,ArithmeticError):
                    pass
                try:
                    self.presentweather(data[2],'wawa',reply[1])
                except (LookupError,ValueError,TypeError,ArithmeticError):
                    pass
                # accumulate readings that arrived since the last LOOP
                # packet
                for key,val in reply[1].items():
                    if key in data:
                        # further occurances of the observation type
                        if key in ('presentweatherWw','presentweatherWawa','presentweatherStart','presentweatherTime','precipitationStart'):
                            data[key] = val
                        elif ((self.threads[thread_name].get('prefix') and
                            key==(self.threads[thread_name]['prefix']+'Rain')) or
                            val[2] in SUM_GROUPS):
                            # calculate rain amount during interval
                            try:
                                data[key] = (data[key][0]+val[0],val[1],val[2])
                            except ArithemticError:
                                pass
                        elif val[2] in AVG_GROUPS:
                            # average
                            try:
                                data[key] = ((data[key][0][0]+val[0],data[key][0][1]+1),val[1],val[2])
                            except ArithmeticError:
                                data[key] = val
                        elif val[2] in MAX_GROUPS:
                            # maximum
                            try:
                                if data[key][0]<val[0]:
                                    data[key] = val
                            except ArithmeticError:
                                pass
                        else:
                            data[key] = val
                    else:
                        # first occurance of this observation type
                        if val[2] in AVG_GROUPS:
                            data[key] = ((val[0],1),val[1],val[2])
                        else:
                            data[key] = val
                    # special accumulators
                    self.special_accumulator_add(thread_name,key,val)
                ct += 1
        if data:
            for key in data:
                if data[key][2] in AVG_GROUPS:
                    data[key] = (data[key][0][0]/data[key][0][1],data[key][1],data[key][2])
            data['count'] = (ct,'count','group_count')
            #print(data)
            #print('\n\n\n',data.get('AWEKASpresentweather'),'\n\n\n')
            return data
        return None
    
    def special_accumulator_add(self, thread_name, key, val):
        """ Add value to special accumulator. """
        # ignore None values
        if val[0] is None: return
        # history of the present weather
        if val[2]=='group_data' and key.endswith('History'):
            obs = (key,val[1],val[2])
            self.threads[thread_name]['accum'][obs] = val[0]
        
    def new_special_accumulator(self, timestamp):
        """ Initialize timespan for special accumulators. """
        self.accum_start_ts = weeutil.weeutil.startOfInterval(timestamp,
                                                   self.archive_interval)
        self.accum_end_ts = self.accum_start_ts + self.archive_interval

    def special_accumulator(self, obsunit, obsgroup, accum):
        """ Accumulator for group_data. 
        
            called from special_accumulators()
            
            Args:
                obsunit  : obs[1] from self.threads[thread_name]['accum']
                obsgroup : obs[2] from self.threads[thread_name]['accum']
                accum    : value of the accumulator
                           self.threads[thread_name]['accum'][obs]
            
            Returns: 
                the accumulated value
        """
        # For 'group_data' always the last reading is returned.
        if obsgroup=='group_data':
            return accum
        # The first element of accum is always out of the previous archive
        # interval. If it is the only element, no value is received during 
        # the actual archive interval. So return None.
        if len(accum)==1:
            return None
        # no accumulator for this observation type
        return None
    
    def special_accumulators(self, thread_name, thread_accum, timestamp):
        """ Process special accumulators. 
        
            Calculate the accumulated values, store them to self.old_accum
            and re-initialize thread_accum for the next ARCHIVE interval.
            
            thread_name  - name of the thread the readings came from
            thread_accum - self.threads[thread_name]['accum']
            timestamp    - the timestamp of the LOOP packet
        """
        for obs in thread_accum:
            # accumulate values and set archive value
            try:
                if __name__=='__main__':
                    print('accumulator',obs,thread_accum[obs])
                # get the accumulated reading
                val = self.special_accumulator(obs[1],obs[2],thread_accum[obs])
                if val is not None:
                    self.old_accum[obs[0]] = val
            except (ValueError,TypeError,LookupError,ArithmeticError) as e:
                if self.log_failure:
                    logerr("accumulator %s %s %s %s traceback %s" % (thread_name,obs,e.__class__.__name__,e,gettraceback(e)))
            # re-initialize accumulator
            if obs[2]=='group_data':
                # remove the value for group_data
                thread_accum[obs] = None
            else:
                # remember the last value for group_wmo_ww and group_wmo_wawa
                try:
                    last_val = thread_accum[obs][-1]
                except LookupError:
                    last_val = None
                thread_accum[obs] = [last_val]
    
    def frostindicator(self):
        """ frost indicator according to DWD VuB 2 BUFR page 257
        
            This rule is published by the German Weather Service. I hope,
            other weather services use the same rule.
        
            air temp 2m | air temp 5cm | soil temp 5cm | frost indicator
            ------------|--------------|---------------|----------------
                <0°C    |    <0°C      |     any       |     yes
                any     |     any      |    <-0,5°C    |     yes
                other   |    other     |     other     |     no
              failure   |    >=0°C     |   failure     |   failure
               >=0°C    |   failure    |   failure     |   failure
              failure   |   failure    |   failure     |   failure
              
             Returns:
                 boolean: frost indicator
        """
        try:
            if not self.obs_t5cm:
                # no observation type for air temperature 5cm configured
                # That does not comply with the rules of the German Weather Service,
                # but we need some reasonable result.
                if self.temp2m_C is not None and self.temp2m_C<0.0:
                    return True
            # air temperature 2m and 5cm is below 0°C
            if (self.temp2m_C is not None and self.temp2m_C<0.0 and
                self.temp5cm_C is not None and self.temp5cm_C<0.0):
                return True
            # soil temperature 5cm is below -0.5°C
            if (self.soil5cm_C is not None and self.soil5cm_C<-0.5):
                return True
            # failure indicators
            if (self.temp2m_C is None and self.soil5cm_C is None and
                self.temp5cm_C is not None and self.temp5cm_C>=0.0):
                return None
            if (self.temp2m_C is not None and self.temp2m_C>=0.0 and
                self.temp5cm_C is None and self.soil5cm_C is None):
                return None
            if (self.temp2m_C is None and self.temp5cm_C is None and 
                self.soil5cm_C is None):
                return None
            # no frost
            return False
        except (TypeError,ValueError,ArithmeticError):
            # unexpected error is considered failure, too
            return None

    def presentweather(self, ts, obstype, record):
        """ Postprocess ww and wawa. 
        
            Do such postprocessing that is not possible within the device
            thread, because it requires additional information from the 
            archive record. Changes record[obstype] if appropriate.
            Applies to 'ww' and 'wawa'.
            
        """
        if obstype not in record: return
        # According to the standards a thunderstorm ends when the last
        # lightning strike appeared more than 10 minutes ago.
        if self.lightning_strike_ts<(ts-600):
            self.lightning_strike_ts = 0
        val = record[obstype]
        val_val = val[0]
        if obstype=='ww' and val[2]=='group_wmo_ww':
            # Note:
            # - 17 preceeds 20 to 49
            # - 28 preceeds 40
            # otherwise the higher preceeds the lower

            # thunderstorm
            if self.lightning_strike_ts:
                if val_val==79:
                    val_val = 96
                elif val_val>=50 and val_val<=90:
                    val_val = 95
                elif val_val<17 or 20<=val_val<=49:
                    val_val = 17
            # freezing rain or drizzle
            if self.is_freezing:
                if val_val in (50,51):
                    val_val = 56
                elif val_val in (52,53,54,55):
                    val_val = 57
                elif val_val in (60,61,58):
                    val_val = 66
                elif val_val in (62,63,64,65,59):
                    val_val = 67
                elif val_val in (20,21):
                    val_val = 24
            # wind gust
            if val_val<18 and self.windGust:
                val_val = 18
            # new value
            record[obstype] = (val_val,val[1],val[2])
        elif obstype=='wawa' and val[2]=='group_wmo_wawa':
            # thunderstorm
            if self.lightning_strike_ts:
                if val_val==89:
                    val_val = 93
                elif val_val>=40 and val_val<90:
                    val_val = 92
                else:
                    val_val = 90
            # freezing rain or drizzle
            if self.is_freezing:
                if val_val in (51,52,53,61,62,63):
                    val_val += 3
                elif val_val==57:
                    val_val = 64
                elif val_val==58:
                    val_val = 66
                elif val_val in (21,22,23):
                    val_val = 25
            # wind gust
            if val_val<18 and self.windGust:
                val_val = 18
            # new value
            record[obstype] = (val_val,val[1],val[2])
    
    
    def new_loop_packet(self, event):
        """ Process LOOP event. """
        timestamp = event.packet.get('dateTime',time.time())
        for thread_name in self.threads:
            # if the LOOP packet belongs to a new archive interval, calculate
            # the accumulated values
            if self.accum_end_ts and timestamp>self.accum_end_ts:
                self.special_accumulators(thread_name,self.threads[thread_name]['accum'],timestamp)
            # get readings that newly arrived since the last LOOP event 
            reply = self._process_data(thread_name)
            # if new data is available process them and update the LOOP packet
            if reply:
                # There may be more than one source for `AWEKASpresentweather`.
                # If so, one may provide precipiation, another one cloud
                # coverage. If there is already a higher value in the
                # packet, discard the value from this thread.
                try:
                    if 'AWEKASpresentweather' in reply:
                        new_awekas = reply.pop('AWEKASpresentweather')[0]
                        if self.is_freezing and new_awekas in (8,9,10,11,12,23):
                            # freezing precipitation
                            new_awekas = 21
                        awekas1 = AWEKAS[self.current_awekas][2] if self.current_awekas is not None else -1
                        awekas2 = AWEKAS[new_awekas][2] if new_awekas is not None else -1
                        if new_awekas:
                            logdbg('AWEKAS vgl %s %s %s %s' % (self.current_awekas,new_awekas,awekas1,awekas2))
                        if awekas2>awekas1:
                            self.current_awekas = new_awekas
                except (LookupError,ValueError,TypeError,ArithmeticError) as e:
                    pass
                    logerr('AWEKAS %s %s' % (e.__class__.__name__,e))
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
        # if the LOOP packet belongs to a new archive interval, initialize
        # the new archive timespan
        if not self.accum_end_ts or timestamp>self.accum_end_ts:
            self.new_special_accumulator(timestamp)
        # remember thunderstorm
        if 'lightning_strike_count' in event.packet and event.packet['lightning_strike_count']>0:
            self.lightning_strike_ts = event.packet.get('dateTime',time.time())
        # remember wind gust
        # Rule to detect wind gusts strong enough to set ww/wawa to 18
        # according to the BUFR specification found in VuB 2 BUFR page 258:
        # If the 1 minute average of wind speed is >=10.5 m/s and the
        # 1 minute average of the wind speed is by at least 8.0 m/s higher
        # than the 10 minutes average of wind speed, then this is a wind
        # gust worth ww/wawa 18.
        # As there is an observation type `windGust` this observation type
        # is used instead of the 1 minute average as the 1 minute average
        # is not available in WeeWX.
        if self.obs_windGust in event.packet:
            try:
                # get actual wind gust speed reading
                windGust = weewx.units.convert(weewx.units.as_value_tuple(event.packet,self.obs_windGust),'meter_per_second')[0]
                # Check if there is the 10 minutes average observation type
                # `windSpeed10` is available. If not use `windSpeed` instead.
                if self.obs_windSpeed10 in event.packet and event.packet[self.obs_windSpeed10] is not None:
                    windSpeed = self.obs_windSpeed10
                else:
                    windSpeed = self.obs_windSpeed
                # get wind speed reading
                windSpeed = weewx.units.convert(weewx.units.as_value_tuple(event.packet,windSpeed),'meter_per_second')[0]
                # current time
                windGust_ts = event.packet.get('dateTime',time.time())
                #
                #logdbg('wind %s %s %s' % (windGust,windSpeed,windGust_ts))
                # apply rule
                if (windSpeed and windGust and 
                    windGust>=10.5 and 
                    (windGust-windSpeed)>=8.0):
                    # wind gust condition met
                    self.windGust = True
                    self.windGust_ts = windGust_ts
                elif windGust_ts>(self.windGust_ts+300):
                    # no wind gust condition for more than 5 minutes
                    self.windGust = False
            except (TypeError,ValueError,ArithmeticError,LookupError):
                pass
        # AWEKAS
        if self.lightning_strike_ts:
            self.current_awekas = 19
    
    def end_archive_period(self, event):
        """ Process end of archive period event. 
        
            called when all LOOP packets of the archive interval are
            processed, but before the first LOOP packet of the new
            archive interval
        """
        self.old_awekas = self.current_awekas
        self.current_awekas = None

    def new_archive_record(self, event):
        """ Process new archive record event. """
        ts = event.record.get('dateTime',time.time())
        interval = event.record.get('interval',self.archive_interval/60)*60
        timespan = (ts-interval,ts)
        for thread_name in self.threads:
            # log error if we did not receive any data from the device
            if self.log_failure and not self.threads[thread_name]['reply_count']:
                logerr("no data received from %s during archive interval" % thread_name)
            # log success to see that we are still receiving data
            if self.log_success and self.threads[thread_name]['reply_count']:
                loginf("%s records received from %s during archive interval" % (self.threads[thread_name]['reply_count'],thread_name))
            # reset counter
            self.threads[thread_name]['reply_count'] = 0
            # get readings that are not accumulated from the LOOP packets
            # but by the thread itself
            # Note: This is done because some readings sent within the 
            #       LOOP packets get fixed or removed afterwards due
            #       to quality control.
            try:
                reply = self.threads[thread_name]['thread'].get_archive_record(timespan)
                if reply:
                    try:
                        self.presentweather(ts,'ww',reply)
                    except (LookupError,ValueError,TypeError,ArithmeticError):
                        pass
                    try:
                        self.presentweather(ts,'wawa',reply)
                    except (LookupError,ValueError,TypeError,ArithmeticError):
                        pass
                    data = self._to_weewx(thread_name,reply,event.record['usUnits'])
                    event.record.update(data)
                    logdbg(data)
            except Exception as e:
                #except (LookupError,ValueError,TypeError,ArithmeticError) as e:
                logerr("Error reading archive record from thread '%s': %s %s traceback %s" % (thread_name,e.__class__.__name__,e,gettraceback(e)))
        # special accumulators
        event.record.update(self.old_accum)
        self.old_accum = dict()
        # thunderstorm
        if 'lightning_strike_count' in event.record and event.record['lightning_strike_count']>0:
            self.lightning_strike_ts = event.record.get('dateTime',time.time())
        else:
            self.lightning_strike_ts = 0
        # air temperature 5cm
        try:
            self.temp5cm_C = weewx.units.convert(weewx.units.as_value_tuple(event.record,self.obs_t5cm),'degree_C')[0]
        except (LookupError,ValueError,TypeError,ArithmeticError):
            self.temp5cm_C = None
        # air temperature 2m
        try:
            self.temp2m_C = weewx.units.convert(weewx.units.as_value_tuple(event.record,self.obs_t2m),'degree_C')[0]
        except (LookupError,ValueError,TypeError,ArithmeticError):
            self.temp2m_C = None
        # soil temperature 5cm
        try:
            self.soil5cm_C = weewx.units.convert(weewx.units.as_value_tuple(event.record,self.obs_s5cm),'degree_C')[0]
        except (LookupError,ValueError,TypeError,ArithmeticError):
            self.soil5cm_C = None
        # frost indicator
        if self.freezing_detection_source.lower()=='software':
            # calculated by this extension
            self.is_freezing = self.frostindicator()
            event.record['frostIndicator'] = weeutil.weeutil.to_int(self.is_freezing)
        elif self.freezing_detection_source.lower()=='hardware':
            # provided by some other source
            self.is_freezing = event.record.get('frostIndicator')
        elif self.freezing_detection_source.lower()=='none':
            # freezing precipitation detection not performed
            self.is_freezing = None
        else:
            # from some device handled by this extension, set within
            # new_loop_packet() while handling the appropriate thread,
            # nothing to do here
            pass
        # debugging output
        if self.debug>1:
            logdbg('temp5cm %s°C, temp2m %s°C, soil5cm %s°C isfreezing %s' % (self.temp5cm_C,self.temp2m_C,self.soil5cm_C,self.is_freezing))
        # AWEKAS
        if self.old_awekas!=self.last_awekas:
            event.record['AWEKASpresentweather'] = self.old_awekas
            self.last_awekas = self.old_awekas
            loginf('new AWEKAS code %s' % self.old_awekas)

    def _to_weewx(self, thread_name, reply, usUnits):
        data = dict()
        for key in reply:
            #print('*',key)
            if key in ('time','interval','count'):
                pass
            elif key in ('interval','count'):
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

##############################################################################
#    Service to save data to a database                                      #
##############################################################################

# This service is intended to save the readings gathered by this extension
# to a separate database. So the user need not add columns to the core 
# database of WeeWX to store that readings. The service uses the database 
# interface provided by WeeWX.

class PrecipArchive(StdService):
    """ Store PrecipMeter data to a separate database """

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
        self.archive_interval = int(config_dict.get('StdArchive',configobj.ConfigObj()).get('archive_interval',300))
        if 'PrecipMeter' in config_dict:
            if __name__!='__main__':
                self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
                self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
            # init schema
            global schema
            global table
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
            try:
                self.dbm_new_loop_packet(event.packet)
            except Exception as e:
                logerr('new_loop_packet %s %s' % (e.__class__.__name__,e))

    def new_archive_record(self, event):
        """ process archive record """
        if self.dbm:
            try:
                self.dbm_new_archive_record(event.record)
            except Exception as e:
                logerr('saving to database: %s %s' % (e.__class__.__name__,e))

    def dbm_init(self, engine, binding, binding_found):
        """ open or create database """
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
        """ close database access """
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
        """ add new archive record and update daily summary """
        logdbg("dbm_new_archive_record frostIndicator %s %s" % ('frostIndicator' in record,record.get('frostIndicator')))
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

##############################################################################
        
if __name__ == '__main__':

    def print_record(record):
        if False:
            print(record)
        else:
            for ii in ('ottHistory','ottWawa','wawa','precipitationStart','presentweatherWawa','presentweatherStart','presentweatherTime','ottRainDur','rainDur'):
                if ii in record:
                    print('%-20s: %s' % (ii,record[ii]))
                else:
                    print('%-20s: not in record' % ii)
    
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
        
    elif True:
    
        sv = PrecipData(None,conf_dict)
        
        try:
            while True:
                for i in range(5):
                    event = weewx.Event(weewx.NEW_LOOP_PACKET)
                    event.packet = {'usUnits':weewx.METRIC}
                    if i==4:
                        sv.accum_end_ts = time.time()-1
                    sv.new_loop_packet(event)
                    if len(event.packet)>1:
                        print('=== LOOP ===================================================')
                        print_record(event.packet)
                        #print(type(event.packet.get('ottHistory')))
                        print('============================================================')
                    time.sleep(10)
                sv.end_archive_period(dict())
                event = weewx.Event(weewx.NEW_ARCHIVE_RECORD)
                event.record = {'usUnits':weewx.METRIC}
                sv.new_archive_record(event)
                print('=== ARCHIVE ================================================')
                print_record(event.record)
                if 'ottHistory' in event.record:
                    wawa_list = []
                    ww_list =[]
                    for ii in event.record['ottHistory']:
                        ww_list.append(ii[2])
                        wawa_list.append(ii[3])
                    print('max(ww)',max_ww(ww_list))
                    print('W1W2',get_w1w2_from_ww(ww_list))
                    print('max(wawa)',max_wawa(wawa_list))
                    print('Wa1Wa2',get_wa1wa2_from_wawa_or_ww(wawa_list,'group_wmo_wawa'))
                print('============================================================')
                #break
        except Exception as e:
            print('**MAIN**',e.__class__.__name__,e)
        except KeyboardInterrupt:
            print()
            print('**MAIN** CTRL-C pressed')
            
        sv.shutDown()
    
    else:
        
        obs = 'ww'
        
        conf_dict['temp5cm'] = 'extraTemp1'
        sv = PrecipData(None,conf_dict)
        sv.temp5cm_C = 1.0
        sv.lightning_strike_ts = time.time()
        
        for i in range(50,100):
            record = {obs:(i,'byte','group_wmo_'+obs)}
            sv.presentweather(obs,record)
            print(i,record[obs])

