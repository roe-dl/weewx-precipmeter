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

VERSION = "0.4"

SIMULATE_ERRONEOUS_READING = False

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
    [0] - start timestamp of the weather condition
    [1] - end timestamp of the weather condition (updated each time,
          the same weather condition is reported as before)
    [2] - ww value of the weather condition
    [3] - wawa value of the weather conditon
    [4] - if this weather condition is precipitation the start timestamp
          of the precipitation (If the weather condition changes
          during precipitation in intensity or kind, this value is
          not the same as [0].)
          if this weather condition is no precipitation the value is
          None
    [5] - metar value of the weather condition
    [6] - intsum
    [7] - dursum
    
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
ACCUM_MAX = { 'extractor':'max' }
ACCUM_NOOP = { 'accumulator':'firstlast','adder':'noop','extractor':'noop' }

for _,ii in weewx.units.std_groups.items():
    ii.setdefault('group_wmo_ww','byte')
    ii.setdefault('group_wmo_wawa','byte')
    ii.setdefault('group_rainpower','watt_per_meter_squared')

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
            20: (50,51,52,53,54,55,58,59),
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
    ('presentweatherStart',  'INTEGER'),
    ('presentweatherTime',   'REAL'),
    ('precipitationStart',   'INTEGER')
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
        self.prefix = conf_dict.get('prefix')
        
        self.data_queue = data_queue
        self.query_interval = query_interval
        self.device_interval = 60
        self.last_data_ts = time.time()+120

        self.db_fn = os.path.join(conf_dict['SQLITE_ROOT'],self.name)
        self.db_conn = None
        
        # list of present weather codes of the last hour, initialized
        # by the contents of the json file saved at thread stop
        self.presentweather_list = []
        self.next_presentweather_error = 0
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
            self.db_conn = sqlite3.connect(self.db_fn+'.sdb')
            cur = self.db_conn.cursor()
            reply = cur.execute('SELECT name FROM sqlite_master')
            rec = reply.fetchall()
            if rec and 'precipitation' in [ii[0] for ii in rec]:
                pass
                #reply = cur.execute('SELECT * FROM precipitation WHERE `start`>%d' % (time.time()-3600))
                #self.presentweather_list = reply.fetchall()
            else:
                cur.execute('CREATE TABLE precipitation(`start` INTEGER NOT NULL UNIQUE PRIMARY KEY,`stop` INTEGER NOT NULL,`ww` INTEGER,`wawa` INTEGER,`precipstart` INTEGER,`METAR` VARCHAR(5))')
                cur.execute('CREATE VIEW archive(`dateTime`,`usUnits`,`interval`,`presentweatherStart`,`precipitationStart`,`presentweatherTime`,`ww`,`wawa`,`METAR`) AS SELECT stop,16,(stop-start)/60,start,precipstart,stop-start,ww,wawa,METAR from precipitation order by stop')
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
    
    def presentweather(self, ts, ww, wawa, metar):
        """ Postprocessing of ww and wawa.
            
            enhances ww and wawa and calculates `presentweatherStart`,
            `presentweatherTime`, and `precipitationStart`
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
                   ww!=self.presentweather_list[-1][2])
        # precipitation or not?
        is_precipitation = is_ww_wawa_precipitation(ww, wawa)
        # Check for values that appear only once. They will be considered
        # erroneous.
        if add and len(self.presentweather_list)>1:
            # There are at least 2 elements. 
            try:
                last_el = self.presentweather_list[-1]
                prev_el = self.presentweather_list[-2]
                if ((last_el[1]-last_el[0])<=self.device_interval and
                    (wawa is not None or ww is not None)):
                    # The last value appears only once.
                    if ((PrecipThread.is_el_precip(prev_el) and is_precipitation and not PrecipThread.is_el_precip(last_el)) or
                        (not PrecipThread.is_el_precip(prev_el) and not is_precipitation and PrecipThread.is_el_precip(last_el))):
                        # If there is one single reading of precipitation
                        # between readings of no precipitation consider this
                        # reading erroneous and remove it. The same applies
                        # for one single reading of no precipitation between
                        # readings of precipitation.
                        loginf("thread %s: discarded ww/wawa %s/%s between %s/%s and %s/%s" % (self.name,last_el[2],last_el[3],prev_el[2],prev_el[3],ww,wawa))
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
                # save the last element to the database
                try:
                    cur = self.db_conn.cursor()
                    cur.execute('INSERT INTO precipitation VALUES (?,?,?,?,?,?)',tuple(self.presentweather_list[-1][:6]))
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
            self.presentweather_list.append([int(ts-self.device_interval),int(ts),ww,wawa,precipstart,metar,None,None])
        else:
            # The weather code is the same as before, so update the end
            # timestamp.
            self.presentweather_list[-1][1] = int(ts)
            precipstart = self.presentweather_list[-1][4]
        # remove the first element if it ends more than an hour ago
        if self.presentweather_list[0][1]<(ts-3600):
            self.presentweather_list.pop(0)
        # Now we have a list of the weather codes of the last hour.
        if __name__ == '__main__':
            print('presentweather_list',self.presentweather_list)
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
        try:
            for idx,ii in enumerate(self.presentweather_list):
                if __name__ == '__main__':
                    print('idx',idx,'ii',ii)
                duration = ii[1]-ii[0]
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
                #if is_ww_wawa_precipitation(ii[2],ii[3]):
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
                    #if is_ww_wawa_precipitation(ii[2],ii[3]):
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
                        start2x = ii[0]
            if start:
                elapsed = self.presentweather_list[-1][1]-start
                start = int(start)
            else:
                elapsed = None
        except (LookupError,TypeError,ValueError,ArithmeticError):
            elapsed = None
            start = None
            start2x = None
        if len(self.presentweather_list)<2:
            # The weather did not change during the last hour.
            return ww, wawa, start, elapsed, precipstart
        if (len(self.presentweather_list)==2 and 
            not self.presentweather_list[0][2] and 
            not self.presentweather_list[0][3]):
            # No significant weather at the beginning of the last hour,
            # then one significant weather condition.
            return ww, wawa, start, elapsed, precipstart
        """
        # One kind of weather only (not the same code all the time, but
        # always rain or always snow etc.)
        if len(wawa_dict)<=1 and len(ww_dict)<=1:
            return ww, wawa, start, elapsed, precipstart
        """
        # Is there actually some significant weather?
        if wawa or ww:
            # weather detected
            # TODO: detect showers
            return ww, wawa, start, elapsed, precipstart
        elif elapsed>3600:
            # more than one hour no significant weather
            return ww, wawa, start, elapsed, precipstart
        else:
            # The significant weather  ended within the last hour. That means, the
            # weather code is 20...29.
            if start2x and start2x>(ts-3600) and weather2x:
                return WW2_REVERSED.get(weather2x[2],ww),WAWA2_REVERSED.get(weather2x[3],wawa),start,elapsed,precipstart
            return ww, wawa, start, elapsed, precipstart
    
    def getRecord(self, ot):
    
        if __name__ == '__main__':
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
                if __name__ == '__main__':
                    print('///////////////////////',since,'///////////////////////')
                if SIMULATE_ERRONEOUS_READING:
                    # erroneous reading
                    self.rain_simulator = 0
                    if since==30:
                        loginf("Simulator: erroneous value ###########################################")
                        ww = 51
                    else:
                        ww = 0
                else:
                    # 30s no precipitation, then 90s rain, then again no
                    # precipitation
                    if since<30: self.rain_simulator = 0
                    if since>120 or since<30: 
                        ww = 0
                    else:
                        ww = 53
                        self.rain_simulator += 0.25
                reply = "200248;000.000;%7.2f;%02d;-9.999;9999;000.00;%03d;15759;00000;0;\r\n" % (self.rain_simulator,ww,temp)
        
        if not self.running: 
            loginf("thread '%s': self.running==False getRecord() after reading data" % self.name)
            return
        
        # process data
        
        if ((self.field_separator not in reply) or 
            (self.record_separator not in reply)):
            return
        ts = time.time()
        ww = None
        wawa = None
        metar = None
        # record contains value tuples here.
        record = dict()
        if (self.model.startswith('ott-parsivel') or 
            self.model in ('thies-lnm','generic')):
            # Thies LNM: initialize special values, process STX
            if self.model=='thies-lnm':
                deviceState = [None]*16
                if reply[0]==chr(2): reply = reply[1:]
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
                    elif self.model=='thies-lnm':
                        # Thies LNM
                        if 22<=ii[0]<38: deviceState[ii[0]-22] = val[0]
                except (LookupError,ValueError,TypeError,ArithmeticError) as e:
                    # log the same error once in 300 seconds only
                    if ii[4] not in self.next_obs_errors:
                        self.next_obs_errors[ii[4]] = 0
                    if self.next_obs_errors[ii[4]]<time.time():
                        logerr("thread '%s': %s %s %s" % (self.name,ii[4],e.__class__.__name__,e))
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

        if not self.running: 
            loginf("thread '%s': self.running==False getRecord() after telegram_list loop" % self.name)
            return
            
        if record and self.prefix:
            # history of present weather codes of the last hour
            record[self.prefix+'History'] = (self.presentweather_list,'byte','group_data')

        if record and self.set_weathercodes:
            try:
                ww, wawa, since, elapsed, pstart = self.presentweather(ts, ww, wawa, metar)
                if ww is not None: 
                    record['ww'] = (ww,'byte','group_wmo_ww')
                if wawa is not None: 
                    record['wawa'] = (wawa,'byte','group_wmo_wawa')
                if since: 
                    record['presentweatherStart'] = (since,'unix_epoch','group_time')
                if elapsed is not None: 
                    record['presentweatherTime'] = (elapsed,'second','group_deltatime')
                record['precipitationStart'] = (pstart,'unix_epoch','group_time')
            except (LookupError,ValueError,TypeError,ArithmeticError) as e:
                if self.next_presentweather_error<time.time():
                    logerr("thread '%s': present weather %s %s" % (self.name,e.__class__.__name__,e))
                    if __name__ == '__main__':
                        self.next_presentweather_error = 0
                    else:
                        self.next_presentweather_error = time.time()+300
        if record and self.set_visibility and self.prefix:
            try:
                if (self.prefix+'MOR') in record: 
                    record['visibility'] = record[self.prefix+'MOR']
            except (LookupError,ValueError,TypeError,ArithmeticError) as e:
                pass
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
            except (LookupError,ValueError,TypeError,ArithmeticError) as e:
                pass
        
        # send record to queue for processing in the main thread
        
        if __name__ == '__main__':
            print(record)
        if ot=='loop':
            self.put_data(record)
            self.last_data_ts = time.time()+600
        
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
        weewx.units.obs_group_dict.setdefault('presentweatherStart','group_time')
        weewx.units.obs_group_dict.setdefault('precipitationStart','group_time')
        weewx.units.obs_group_dict.setdefault('presentweatherTime','group_deltatime')
        weewx.units.obs_group_dict.setdefault('visibility','group_distance')
        weewx.accum.accum_dict.setdefault('ww',ACCUM_MAX)
        weewx.accum.accum_dict.setdefault('wawa',ACCUM_MAX)
        weewx.accum.accum_dict.setdefault('presentweatherStart',ACCUM_LAST)
        weewx.accum.accum_dict.setdefault('precipitationStart',ACCUM_LAST)
        weewx.accum.accum_dict.setdefault('presentweatherTime',ACCUM_LAST)
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
            self.obs_t5cm = site_dict.get('temp5cm')
        # Initialize variables for the special accumulators
        self.old_accum = dict()
        self.accum_start_ts = None
        self.accum_end_ts = None
        self.lightning_strike_ts = 0
        self.temp5cm_C = None

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
            # present weather code history of the last hour
            # (for debugging purposes)
            obstype = thread_dict['prefix']+'History'
            obsgroup = 'group_data'
            weewx.units.obs_group_dict.setdefault(obstype,obsgroup)
            _accum[obstype] = ACCUM_NOOP
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
        
    def _process_data(self, thread_name):
        """ Get and process data from the threads. """
        AVG_GROUPS = ('group_temperature','group_db','group_distance','group_volt')
        MAX_GROUPS = ('group_wmo_ww','group_wmo_wawa')
        # get collected data
        data = dict()
        ct = 0
        while True:
            try:
                # get the next packet
                data1 = self.threads[thread_name]['queue'].get(block=False)
            except queue.Empty:
                # no more packets available so far
                break
            else:
                # accumulate readings that arrived since the last LOOP
                # packet
                for key,val in data1[1].items():
                    if key in data:
                        # further occurances of the observation type
                        if (self.threads[thread_name].get('prefix') and
                            key==(self.threads[thread_name]['prefix']+'Rain')):
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
            return data
        return None
    
    def special_accumulator_add(self, thread_name, key, val):
        """ Add value to special accumulator. """
        # present weather
        if val[2] in ('group_wmo_ww','group_wmo_wawa'):
            obs = (key,val[1],val[2])
            if obs not in self.threads[thread_name]['accum']:
                self.threads[thread_name]['accum'][obs] = [None]
            self.threads[thread_name]['accum'][obs].append(val[0])
        # history of the present weather
        if val[2]=='group_data':
            obs = (key,val[1],val[2])
            self.threads[thread_name]['accum'][obs] = val[0]
        
    def new_special_accumulator(self, timestamp):
        """ Initialize timespan for special accumulators. """
        self.accum_start_ts = weeutil.weeutil.startOfInterval(timestamp,
                                                   self.archive_interval)
        self.accum_end_ts = self.accum_start_ts + self.archive_interval

    def special_accumulator(self, obsunit, obsgroup, accum):
        """ Accumulator for ww, wawa and group_data. 
        
            called from special_accumulators()
            
            obsunit  - obs[1] from self.threads[thread_name]['accum']
            obsgroup - obs[2] from self.threads[thread_name]['accum']
            accum    - value of the accumulator
                       self.threads[thread_name]['accum'][obs]
            
            returns the accumulated value
        """
        # For 'group_data' always the last reading is returned.
        if obsgroup=='group_data':
            return accum
        # The first element of accum is always out of the previous archive
        # interval. If it is the only element, no value is received during 
        # the actual archive interval. So return None.
        if len(accum)==1:
            return None
        # accumulator for ww and wawa
        # The propability of error is about 3% according to the specification.
        # Therefore erroneous readings are quite frequent. For this reason
        # one single value of precipitation between values of no precipitation
        # is considered erroneous. The same applies for one single value
        # of no precipitation between values of precipitation.
        if obsgroup in ('group_wmo_ww','group_wmo_wawa'):
            min_precip = 40 if obsgroup=='group_wmo_wawa' else 50
            _accum = []
            for idx,val1 in enumerate(accum):
                try:
                    val2 = accum[idx+1]
                    val3 = accum[idx+2]
                    if val1 is None:
                        v1 = True
                    else:
                        v1 = val1>=min_precip 
                    v2 = val2>=min_precip
                    v3 = val3>=min_precip
                    if ((v2 and (v1 or v3)) or
                        (not v2 and (not v1 or not v3)) or
                        (val1 is None)):
                        _accum.append(val2)
                except LookupError:
                    break
                except TypeError:
                    pass
            return max(_accum)
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
                    logerr("accumulator %s %s %s %s" % (thread_name,obs,e.__class__.__name__,e))
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

    def presentweather(self, obstype, record):
        """ Postprocess ww and wawa. 
        
            Do such postprocessing that is not possible within the device
            thread, because it requires additional information from the 
            archive record. Changes record[obstype] if appropriate.
            Applies to 'ww' and 'wawa'.
            
        """
        if obstype not in record: return
        ts = record.get('dateTime',time.time())
        # According to the standards a thunderstorm ends when the last
        # lightning strike appeared more than 10 minutes ago.
        if self.lightning_strike_ts<(ts-600):
            self.lightning_strike_ts = 0
        val = record[obstype]
        if obstype=='ww' and val[2]=='group_wmo_ww':
            # thunderstorm
            if self.lightning_strike_ts:
                if val[0]==79:
                    record[obstype] = (96,val[1],val[2])
                elif val[0]>=50 and val[0]<=90:
                    record[obstype] = (95,val[1],val[2])
                elif val[0]<17:
                    record[obstype] = (17,val[1],val[2])
            # freezing rain or drizzle
            if self.temp5cm_C is not None and self.temp5cm_C<0:
                if val[0] in (50,51,58):
                    record[obstype] = (56,val[1],val[2])
                elif val[0] in (52,53,54,55,59):
                    record[obstype] = (57,val[1],val[2])
                elif val[0] in (60,61):
                    record[obstype] = (66,val[1],val[2])
                elif val[0] in (62,63,64,65):
                    record[obstype] = (67,val[1],val[2])
                elif val[0] in (20,21):
                    record[obstype] = (24,val[1],val[2])
        elif obstype=='wawa' and val[2]=='group_wmo_wawa':
            # thunderstorm
            if self.lightning_strike_ts:
                if val[0]==89:
                    record[obstype] = (93,val[1],val[2])
                elif val[0]>=40 and val[0]<90:
                    record[obstype] = (92,val[1],val[2])
                else:
                    record[obstype] = (90,val[1],val[2])
            # freezing rain or drizzle
            if self.temp5cm_C is not None and self.temp5cm_C<0:
                if val[0] in (51,52,53,61,62,63):
                    record[obstype] = (val[0]+3,val[1],val[2])
                elif val[0] in (21,22,23):
                    record[obstype] = (25,val[1],val[2])
    
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
                try:
                    self.presentweather('ww',reply)
                except (LookupError,ValueError,TypeError,ArithmeticError):
                    pass
                try:
                    self.presentweather('wawa',reply)
                except (LookupError,ValueError,TypeError,ArithmeticError):
                    pass
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
        # thunderstorm
        if 'lightning_strike_count' in event.packet and event.packet['lightning_strike_count']>0:
            self.lightning_strike_ts = event.packet.get('dateTime',time.time())
    
    def end_archive_period(self, event):
        """ Process end of archive period event. 
        
            called when all LOOP packets of the archive interval are
            processed, but before the first LOOP packet of the new
            archive interval
        """
        pass

    def new_archive_record(self, event):
        """ Process new archive record event. """
        for thread_name in self.threads:
            # log error if we did not receive any data from the device
            if self.log_failure and not self.threads[thread_name]['reply_count']:
                logerr("no data received from %s during archive interval" % thread_name)
            # log success to see that we are still receiving data
            if self.log_success and self.threads[thread_name]['reply_count']:
                loginf("%s records received from %s during archive interval" % (self.threads[thread_name]['reply_count'],thread_name))
            # reset counter
            self.threads[thread_name]['reply_count'] = 0
        # special accumulators
        event.record.update(self.old_accum)
        self.old_accum = dict()
        # thunderstorm
        if 'lightning_strike_count' in event.record and event.record['lightning_strike_count']>0:
            self.lightning_strike_ts = event.record.get('dateTime',time.time())
        else:
            self.lightning_strike_ts = 0
        # Bodentemperatur
        try:
            self.temp5cm_C = weewx.units.convert(event.record[self.obs_t5cm],'degree_C')[0]
        except (LookupError,ValueError,TypeError,ArithmeticError):
            self.temp5cm_C = None

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
                        print(event.packet)
                        print(type(event.packet['ottHistory']))
                        print('============================================================')
                    time.sleep(10)
                sv.end_archive_period(dict())
                event = weewx.Event(weewx.NEW_ARCHIVE_RECORD)
                event.record = {'usUnits':weewx.METRIC}
                sv.new_archive_record(event)
                print('=== ARCHIVE ================================================')
                print(event.record)
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

