# weewx-precipmeter
WeeWX service to fetch data from Ott Parsivel 2

## Prerequisites

### Hardware

#### General

In general you need:
* a disdrometer or present weather sensor
* a converter from RS485 to whatever your PC understands
  (RS485-to-ethernet converter or RS485-to-USB converter)
* a 24V DC power supply (I recommend using a model including an accumulator
  for uninterupted supply)
* in case you want to use WLAN a WLAN adapter
* an electric cabinet for outdoor usage to put the power supply and the
  converter(s) in
* cable glands, wires etc.

#### Parts list

For example I used the following components:

item | amount | description | manufacturer |
----:|-------:|-------------|--------------|
01   | 1 pc.  | laser disdrometer Parsivel<sup>2</sup> | Ott Hydromet Fellbach GmbH |
02   | 1 pc.  | Com-Server++ 58665 | Wiesemann & Theis GmbH |
03   | 1 pc.  | power supply APU230V.24V-6A/20Ah *) | Rinck Electronics Germany GmbH |
04   | 1 pc.  | electric cabinet AX | Rittal |
05   | 2.33 m  | pipe 2" | |

*) If the grid power is more reliable at your location than at mine,
   you can order that power supply with a smaller accumulator.

### Software

* WeeWX (of course)
* SQLite3
* python3-configobj
* python3-requests (if the device offers a restful service)
* python3-serial (if the device is connected by USB or serial)
* python3-simplejson



## Installation instructions

1) download

   ```
   wget -O weewx-precipmeter.zip https://github.com/roe-dl/weewx-precipmeter/archive/master.zip
   ```

2) run the installer

   ```
   sudo wee_extension --install weewx-precipmeter.zip
   ```

3) edit configuration in weewx.conf

   Before using this extension you have to set up which devices
   to be queried and which variables to be fetched. See
   section "Configuration" for details.

   **Caution!** If you want to save the readings to a separate 
   database and have it created properly, you have to edit
   the configuration file before you **first** start WeeWX
   after installing the extension. 

   If you want to add additional variables afterwards you have to 
   extend the database schema manually by using the
   `wee_database` utility. This is **not** done automatically.

5) restart weewx

   ```
   sudo /etc/init.d/weewx stop
   sudo /etc/init.d/weewx start
   ```

## Configuration

It is possible to configure more than one device. 

### General options

* `enable`: If True or omitted, retrieve data from that device.
  If False, that subsection is not used. (optional)
* `log_success`: If True, log successful operation. 
  If omitted, global options apply. (optional)
* `log_failure`: If True, log unsuccessful operation. 
  If omitted, global options apply. (optional)
* `data_binding`: data binding to use for storage
* `weathercodes`: device to get present weather codes from
  (use the section name of the device configuration section)
* `visibility`: device to get `visbility` reading from
  (use the section name of the device configuration section)
* `precipitation`: Generally the readings of `rain` and `rainRate` are not 
  provided by this extension but by the driver that is set
  up by the `station_type` key in the `[Station]` section
  of weewx.conf. In case you want this extension to provide
  `rain` and `rainRate` you can set up `precipitation`
  to point to the device subsection of the device you want to get 
  the readings from.
  Default is not to provide `rain` and `rainRate`.


### Connection configuration

* `host`: host name or IP address of the device to get data from
* `port`: port number
* `timeout`: request timeout (optional, default is 0.5s)
* `retries`: request retries (0 is no retries) (optional, default is
   no retries)
* `query_interval`: query interval (optional, default 5s)

### Authentication configuration


### Device configuration

* `model`: device model (actually `Ott-Parsivel1` or `Ott-Parsivel2`)
* `prefix`: observation type name prefix (default `ott`)
* `telegram`: telegram configuration string as set up in the device
  (Instead of this key a `[[[loop]]]` sub-subsection can be used 
  to define the observation types measured by the device.)

See [WeeWX Customization Guide](http://www.weewx.com/docs/customizing.htm#units)
for a list of predefined units and unit groups.

The observation types are automatically registered with WeeWX.


### Accumulators

Accumulators define how to aggregate the readings during the
archive interval.
This extension tries to set up reasonable accumulators. If
they do not work for you, you can set up accumulators manually
in the `[Accumulator]` section of `weewx.conf`.
See [WeeWX Accumulators wiki page](https://github.com/weewx/weewx/wiki/Accumulators)
for how to set up accumulators in WeeWX.

The accumulator `firstlast` does not work for numeric values of this
extension. The reason is that the database schema within this extension
includes all numeric values in the list of daily summeries tables. But
WeeWX let you have an observation type either with a daily summeries
table or the `firstlast` accumulator, not both.

### Example configuration

```
...

[DataBindings]
    ...
    # additional section for an extra database to store the disdrometer data
    # optional!
    [[preicp_binding]]
        database = precip_sqlite
        table_name = archive
        manager = weewx.manager.DaySummaryManager
        schema = user.precipmeter.schema

[Databases]
    ...
    # additional section for an extra database to store disdrometer data
    # optional!
    [[precip_sqlite]]
        database_name = precipmeter.sdb
        database_type = SQLite

[Engine]
    [[Services]]
        data_services = ..., user.precipmeter.PrecipData
        archive_services = ..., user.precipmeter.PrecipArchive

[PrecipMeter]
    data_binding = precip_binding
    weathercodes = Parsivel
    visibility = Parsivel
    [[Parsivel]]
        model = Ott-Parsivel2
        prefix = ott
        telegram = "%13;%01;%02;%03;%07;%08;%34;%12;%10;%11;%18;/r/n"
        type = tcp # udp tcp restful usb none
        host = replace_me
        port = replace_me

```

## Observation types

### General observation types

* `ww`: present weather code according to WMO table 4677, enhanced by
  this extension
* `wawa`: present weather code according to WMO table 4680, enhanced by
  this extension
* `presentweatherStart`: timestamp of the beginning of the present weather
* `presentweatherTime`: time elapsed since last change of the present weather
* `visibility`: visibility, derived from `MOR`

To convert the present weather code into a symbol or icon
see [weewx-DWD](https://github.com/roe-dl/weewx-DWD).

The observation types `ww`, `wawa`, `presentweatherStart` and
`presentweatherTime` are derived from the device that is set
by the `weathercodes` key. The observation type `visibility`
is derived from the device that is set by the `visibility`
key. If the configuration keys `weathercodes` or `visibility`
are omitted or point to an unknown subsection, the respective
observation types are omitted.

### Ott Hydromet Parsivel and Parsivel<sup>2</sup>

Those observation type names are prepended by the prefix defined in
`weewx.conf`. Default is `ott`.

* `SNR`: serial number of the device
* `queryInterval`: 
* `sensorState`: 0 - ok, 1 - dirty but measurement is still possible,
  2 - dirty, no measurement possible, 3 - laser defective
* `errorCode`:
* `wawa`: present weather code according to WMO table 4680
* `ww`: present weather code according to WMO table 4677
* `METAR`: present weather code according to WMO table 4678
* `NWS`: present weather code according to NWS
* `rainRate`: rain rate
* `rainAkku`: accumulated rain since power-on
* `rainAbs`: absolute amount of rain
* `dBZ`: radar reflectivity factor
* `MOR`: meteorological optical range (visibility)


## How to set up Ott Parsivel<sup>2</sup>?

* Open the front cover
* Connect the PC to the Parsivel<sup>2</sup> by an USB wire
* Start a terminal application on the PC
  - macOS: `screen`
  - Windows: 
* Use the commands as described in the Parsivel<sup>2</sup> manual

## References

### Ott Hydromet

#### English

* [OTT Parsivel<sup>2</sup>](https://www.ott.com/en-uk/products/meteorological-sensors-26/ott-parsivel2-laser-weather-sensor-2392/)
* [manual OTT Parsivel<sup>2</sup>](https://www.ott.com/en-uk/products/download/operating-instructions-present-weather-sensor-ott-parsivel2-with-screen-heating-1/)

#### German

* [OTT Parsivel<sup>2</sup>](https://www.ott.com/de-de/produkte/meteorologie-29/ott-parsivel2-niederschlagsbestimmung-97/)
* [Bedienanleitung OTT Parsivel<sup>2</sup>](https://www.ott.com/de-de/produkte/download/bedienungsanleitung-present-weather-sensor-ott-parsivel2-mit-glasscheibenheizung-1/)

### Thies Clima

* [Thies laser precipitation monitor](https://www.thiesclima.com/en/Products/Precipitation-measuring-technology-Electrical-devices/?art=774)
* [Thies Laser-Niederschlags-Monitor](https://www.thiesclima.com/de/Produkte/Niederschlag-Messtechnik-Elektrische-Geraete/?art=774)

### WeeWX

* [WeeWX website](https://www.weewx.com)
* [WeeWX information in german](https://www.woellsdorf-wetter.de/software/weewx.html)
* [WeeWX customization guide](https://www.weewx.com/docs/customizing.htm)
  (See this guide for using the observation types in skins.)
* [WeeWX accumulators](https://github.com/weewx/weewx/wiki/Accumulators)
  (This extension tries to set up reasonable accumulators for the
  observation types, but if you want them different or if they do not
  work appropriately, you can define them in `weewx.conf`)
* [Calculation in templates](https://github.com/weewx/weewx/wiki/calculate-in-templates)
