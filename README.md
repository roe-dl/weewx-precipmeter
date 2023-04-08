# weewx-precipmeter
WeeWX service to fetch data from Ott Parsivel 2

## Prerequisites


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


### General options

* `enable`: If True or omitted, retrieve data from that device.
  If False, that subsection is not used. (optional)
* `log_success`: If True, log successful operation. 
  If omitted, global options apply. (optional)
* `log_failure`: If True, log unsuccessful operation. 
  If omitted, global options apply. (optional)

### Connection configuration

* `host`: host name or IP address of the device to get data from
  (mandatory)
* `port`: port number (mandatory, standard 161)
* `timeout`: request timeout (optional, default is 0.5s)
* `retries`: request retries (0 is no retries) (optional, default is
   no retries)
* `query_interval`: query interval (optional, default 5s)

### Authentication configuration


### Variables configuration


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

### Ott Hydromet Parsivel<sup>2</sup>

Those observation type names are prepended by the prefix defined in
`weewx.conf`. Default is `ott`.

* `SNR`: serial number of the device
* `queryInterval`: 
* `sensorState`:
* `errorCode`:
* `wawa`: present weather code according to WMO table 4680
* `ww`: present weather code according to WMO table 4677
* `METAR`: present weather code according to WMO table 4678
* `NWS`: present weather code according to NWS
* `rainRate`: rain rate
* `rainAkku`: accumulated rain
* `rainAbs`: absolute amount of rain
* `dBZ`: radar reflectivity factor
* `MOR`: meteorological optical range (visibility)


## References


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
