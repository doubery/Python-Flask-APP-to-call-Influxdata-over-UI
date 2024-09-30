# This is a function to get some values of the InfluxDB
# The wanted timeranges and "hop" times, are set by the userinterface in app.py

import datetime
import csv
from influxdb_client import InfluxDBClient
from dateutil import tz

# define the function myfunction
def myfunction(datum_start, datum_stop, day_time, hop_time, station, measurement):

    header = ['Zeit', 'Station', 'Sensor', 'Messwert', 'Tageszeit']

    dburl               = "http://< your influxdb server ip >:8086"
    dbtoken             = "< your influx token >"
    dborg               = '< your influx organisation >'
    dbbucket            = '"< your influx database >"'

    client = InfluxDBClient(url=dburl, token=dbtoken, org=dborg)

    # if the user called station data
    if 'Station_' in station:
        dbmeasurement       = '"' + station + '"'                # '"Station_1"'
        csvname             = dbmeasurement.replace('"','')
        dbapplication       = csvname.replace('Station_', '')
        int_dbapplication       = int(dbapplication)
        if int_dbapplication < 10:
            dbapplication       = '"1000' + dbapplication + '"'
        elif int_dbapplication < 100 and dbapplication >= 10:
            dbapplication       = '"100' + dbapplication + '"'
        elif int_dbapplication < 1000 and dbapplication >= 100:
            dbapplication       = '"10' + dbapplication + '"'
        elif int_dbapplication < 10000 and dbapplication >= 1000:
            dbapplication       = '"1' + dbapplication + '"'
        dbfield             = '"' + measurement + '"'           # '"dBAmax"'
    
    # if the user called sensebox data
    elif 'Sensebox_' in station:
        dbmeasurement       = '"' + station + '"'                # '"Sensebox_1142"'
        csvname             = dbmeasurement.replace('"','')
        dbapplication       = csvname.replace('Sensebox_', '')
        dbapplication       = '"' + dbapplication + '"'
        dbfield             = '"' + measurement + '"'           # '"Lautstärke"'

    dbdate_start        = datum_start # '2022-02-07'
    dbdate_stop         = datum_stop  # '2022-02-08'

    # if no stop date is set, set it to start-date
    if (dbdate_stop == '' ):
        dbdate_stop = dbdate_start

    # these are the fixed times of the RLS-19
    addtime             = ":00.000Z"
    dbtime_day_start    = '06:00' + addtime     #startet Messwertrecherche um 06:00
    dbtime_day_stop     = '18:00' + addtime     #endet Messwertrecherche um 22:00
    dbtime_eve_start    = '18:00' + addtime     #startet Messwertrecherche um 18:00
    dbtime_eve_stop     = '22:00' + addtime     #endet Messwertrecherche um 22:00
    dbtime_night_start  = '22:00' + addtime     #startet Messwertrecherche um 22:00
    dbtime_night_stop   = '06:00' + addtime     #endet Messwertrecherche um 06:00

    dbday_start         = dbdate_start + 'T' + dbtime_day_start
    dbday_stop          = dbdate_stop + 'T' + dbtime_day_stop
    dbeve_start         = dbdate_start + 'T' + dbtime_eve_start
    dbeve_stop          = dbdate_stop + 'T' + dbtime_eve_stop
    dbnight_start       = dbdate_start + 'T' + dbtime_night_start
    dbnight_stop        = dbdate_stop + 'T' + dbtime_night_stop

    dbinterv            = hop_time #'20m'
    tageszeit           = day_time #'Tag u. Nacht'

    # debug info of called data
    print(dbday_start, dbday_stop, tageszeit, dbinterv, dbmeasurement, dbapplication, dbfield)

    results = []

    # get the day and night values
    if (tageszeit == 'Tag u. Nacht'):
        # get one day
        if (dbdate_start == dbdate_stop):
            queryday = 'from(bucket: ' + dbbucket + ')\
            |> range(start: ' + dbday_start + ', stop: ' + dbday_stop + ')\
            |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
            |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
            |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
            |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
            |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
            resultday = client.query_api().query(org=dborg, query=queryday)
            for table in resultday:
                for record in table.records:
                    time = record.get_time()
                    time = tz.resolve_imaginary(time)
                    time = time.strftime("%Y-%m-%d %H:%M:%S")
                    value = record.get_value()
                    value = str(value)
                    value = value.replace('.', ',')
                    if(value != "None"):
                        results.append((time, record.get_measurement(), record.get_field(), value, "Tag"))

            date_time_stop_obj = datetime.datetime.strptime(dbdate_stop, '%Y-%m-%d')
            new_dbnight_stop = date_time_stop_obj + datetime.timedelta(days = 1)
            new_dbnight_stop = new_dbnight_stop.strftime("%Y-%m-%d")
            new_dbnight_stop = new_dbnight_stop + 'T' + dbtime_night_stop
            querynight = 'from(bucket: ' + dbbucket + ')\
            |> range(start: ' + dbnight_start + ', stop: ' + new_dbnight_stop + ')\
            |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
            |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
            |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
            |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
            |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
            resultnight = client.query_api().query(org=dborg, query=querynight)
            for table in resultnight:
                for record in table.records:
                    time = record.get_time()
                    time = tz.resolve_imaginary(time)
                    time = time.strftime("%Y-%m-%d %H:%M:%S")
                    value = record.get_value()
                    value = str(value)
                    value = value.replace('.', ',')
                    if(value != "None"):
                        results.append((time, record.get_measurement(), record.get_field(), value, "Nacht"))

        # if more days are called calculate the wanted days
        elif (dbdate_start != dbdate_stop):
            date_time_start_obj = datetime.datetime.strptime(dbdate_start, '%Y-%m-%d')
            date_time_stop_obj = datetime.datetime.strptime(dbdate_stop, '%Y-%m-%d')
            delta_day = date_time_stop_obj - date_time_start_obj
            delta_day = delta_day.days
            delta_night = date_time_stop_obj - date_time_start_obj
            delta_night = delta_night.days
            i = 0
            while (i <= delta_day):
                new_dbday_start = date_time_start_obj + datetime.timedelta(days = i)
                new_dbday_start = new_dbday_start.strftime("%Y-%m-%d")
                new_dbday_start = new_dbday_start + 'T' + dbtime_day_start
                new_dbday_stop = date_time_start_obj + datetime.timedelta(days = i)
                new_dbday_stop = new_dbday_stop.strftime("%Y-%m-%d")
                new_dbday_stop = new_dbday_stop + 'T' + dbtime_day_stop
                queryday = 'from(bucket: ' + dbbucket + ')\
                |> range(start: ' + new_dbday_start + ', stop: ' + new_dbday_stop + ')\
                |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
                |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
                |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
                |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
                |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
                resultday = client.query_api().query(org=dborg, query=queryday)
                for table in resultday:
                    for record in table.records:
                        time = record.get_time()
                        time = tz.resolve_imaginary(time)
                        time = time.strftime("%Y-%m-%d %H:%M:%S")
                        value = record.get_value()
                        value = str(value)
                        value = value.replace('.', ',')
                        if(value != "None"):
                            results.append((time, record.get_measurement(), record.get_field(), value, "Tag"))

                new_dbnight_start = date_time_start_obj + datetime.timedelta(days = i)
                new_dbnight_start = new_dbnight_start.strftime("%Y-%m-%d")
                new_dbnight_start = new_dbnight_start + 'T' + dbtime_night_start
                new_dbnight_stop = date_time_start_obj + datetime.timedelta(days = i + 1)
                new_dbnight_stop = new_dbnight_stop.strftime("%Y-%m-%d")
                new_dbnight_stop = new_dbnight_stop + 'T' + dbtime_night_stop
                print(new_dbnight_start, new_dbnight_stop)
                querynight = 'from(bucket: ' + dbbucket + ')\
                |> range(start: ' + new_dbnight_start + ', stop: ' + new_dbnight_stop + ')\
                |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
                |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
                |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
                |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
                |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
                resultday = client.query_api().query(org=dborg, query=querynight)
                for table in resultday:
                    for record in table.records:
                        time = record.get_time()
                        time = tz.resolve_imaginary(time)
                        time = time.strftime("%Y-%m-%d %H:%M:%S")
                        value = record.get_value()
                        value = str(value)
                        value = value.replace('.', ',')
                        if(value != "None"):
                            results.append((time, record.get_measurement(), record.get_field(), value, "Nacht"))
                i = i + 1

    # get the day, night and evening values
    if (tageszeit == 'Tag u. Abend u. Nacht'):
        # check if only one day is called
        if (dbdate_start == dbdate_stop):
            queryday = 'from(bucket: ' + dbbucket + ')\
            |> range(start: ' + dbday_start + ', stop: ' + dbday_stop + ')\
            |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
            |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
            |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
            |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
            |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
            resultday = client.query_api().query(org=dborg, query=queryday)
            for table in resultday:
                for record in table.records:
                    time = record.get_time()
                    time = tz.resolve_imaginary(time)
                    time = time.strftime("%Y-%m-%d %H:%M:%S")
                    value = record.get_value()
                    value = str(value)
                    value = value.replace('.', ',')
                    if(value != "None"):
                        results.append((time, record.get_measurement(), record.get_field(), value, "Tag"))

            date_time_stop_obj = datetime.datetime.strptime(dbdate_stop, '%Y-%m-%d')
            queryeve = 'from(bucket: ' + dbbucket + ')\
            |> range(start: ' + dbeve_start + ', stop: ' + dbeve_stop + ')\
            |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
            |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
            |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
            |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
            |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
            resultday = client.query_api().query(org=dborg, query=queryeve)
            for table in resultday:
                for record in table.records:
                    time = record.get_time()
                    time = tz.resolve_imaginary(time)
                    time = time.strftime("%Y-%m-%d %H:%M:%S")
                    value = record.get_value()
                    value = str(value)
                    value = value.replace('.', ',')
                    if(value != "None"):
                        results.append((time, record.get_measurement(), record.get_field(), value, "Abend"))

            new_dbnight_stop = date_time_stop_obj + datetime.timedelta(days = 1)
            new_dbnight_stop = new_dbnight_stop.strftime("%Y-%m-%d")
            new_dbnight_stop = new_dbnight_stop + 'T' + dbtime_night_stop
            querynight = 'from(bucket: ' + dbbucket + ')\
            |> range(start: ' + dbnight_start + ', stop: ' + new_dbnight_stop + ')\
            |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
            |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
            |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
            |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
            |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
            resultnight = client.query_api().query(org=dborg, query=querynight)
            for table in resultnight:
                for record in table.records:
                    time = record.get_time()
                    time = tz.resolve_imaginary(time)
                    time = time.strftime("%Y-%m-%d %H:%M:%S")
                    value = record.get_value()
                    value = str(value)
                    value = value.replace('.', ',')
                    if(value != "None"):
                        results.append((time, record.get_measurement(), record.get_field(), value, "Nacht"))

        # if more days are called calculate the wanted days
        elif (dbdate_start != dbdate_stop):
            date_time_start_obj = datetime.datetime.strptime(dbdate_start, '%Y-%m-%d')
            date_time_stop_obj = datetime.datetime.strptime(dbdate_stop, '%Y-%m-%d')
            delta_day = date_time_stop_obj - date_time_start_obj
            delta_day = delta_day.days
            delta_night = date_time_stop_obj - date_time_start_obj
            delta_night = delta_night.days
            i = 0
            while (i <= delta_day):
                new_dbday_start = date_time_start_obj + datetime.timedelta(days = i)
                new_dbday_start = new_dbday_start.strftime("%Y-%m-%d")
                new_dbday_start = new_dbday_start + 'T' + dbtime_day_start
                new_dbday_stop = date_time_start_obj + datetime.timedelta(days = i)
                new_dbday_stop = new_dbday_stop.strftime("%Y-%m-%d")
                new_dbday_stop = new_dbday_stop + 'T' + dbtime_day_stop
                queryday = 'from(bucket: ' + dbbucket + ')\
                |> range(start: ' + new_dbday_start + ', stop: ' + new_dbday_stop + ')\
                |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
                |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
                |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
                |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
                |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
                resultday = client.query_api().query(org=dborg, query=queryday)
                for table in resultday:
                    for record in table.records:
                        time = record.get_time()
                        time = tz.resolve_imaginary(time)
                        time = time.strftime("%Y-%m-%d %H:%M:%S")
                        value = record.get_value()
                        value = str(value)
                        value = value.replace('.', ',')
                        if(value != "None"):
                            results.append((time, record.get_measurement(), record.get_field(), value, "Tag"))

                new_dbeve_start = date_time_start_obj + datetime.timedelta(days = i)
                new_dbeve_start = new_dbeve_start.strftime("%Y-%m-%d")
                new_dbeve_start = new_dbeve_start + 'T' + dbtime_eve_start
                new_dbeve_stop = date_time_start_obj + datetime.timedelta(days = i)
                new_dbeve_stop = new_dbeve_stop.strftime("%Y-%m-%d")
                new_dbeve_stop = new_dbeve_stop + 'T' + dbtime_eve_stop
                queryeve = 'from(bucket: ' + dbbucket + ')\
                |> range(start: ' + new_dbeve_start + ', stop: ' + new_dbeve_stop + ')\
                |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
                |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
                |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
                |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
                |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
                resultday = client.query_api().query(org=dborg, query=queryeve)
                for table in resultday:
                    for record in table.records:
                        time = record.get_time()
                        time = tz.resolve_imaginary(time)
                        time = time.strftime("%Y-%m-%d %H:%M:%S")
                        value = record.get_value()
                        value = str(value)
                        value = value.replace('.', ',')
                        if(value != "None"):
                            results.append((time, record.get_measurement(), record.get_field(), value, "Abend"))

                new_dbnight_start = date_time_start_obj + datetime.timedelta(days = i)
                new_dbnight_start = new_dbnight_start.strftime("%Y-%m-%d")
                new_dbnight_start = new_dbnight_start + 'T' + dbtime_night_start
                new_dbnight_stop = date_time_start_obj + datetime.timedelta(days = i + 1)
                new_dbnight_stop = new_dbnight_stop.strftime("%Y-%m-%d")
                new_dbnight_stop = new_dbnight_stop + 'T' + dbtime_night_stop
                queryday = 'from(bucket: ' + dbbucket + ')\
                |> range(start: ' + new_dbnight_start + ', stop: ' + new_dbnight_stop + ')\
                |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
                |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
                |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
                |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
                |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
                resultday = client.query_api().query(org=dborg, query=queryday)
                for table in resultday:
                    for record in table.records:
                        time = record.get_time()
                        time = tz.resolve_imaginary(time)
                        time = time.strftime("%Y-%m-%d %H:%M:%S")
                        value = record.get_value()
                        value = str(value)
                        value = value.replace('.', ',')
                        if(value != "None"):
                            results.append((time, record.get_measurement(), record.get_field(), value, "Nacht"))
                i = i + 1


    # get the evening and night values
    if (tageszeit == 'Abend u. Nacht'):
        # if only one day is called
        if (dbdate_start == dbdate_stop):
            date_time_stop_obj = datetime.datetime.strptime(dbdate_stop, '%Y-%m-%d')
            queryeve = 'from(bucket: ' + dbbucket + ')\
            |> range(start: ' + dbeve_start + ', stop: ' + dbeve_stop + ')\
            |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
            |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
            |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
            |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
            |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
            resultday = client.query_api().query(org=dborg, query=queryeve)
            for table in resultday:
                for record in table.records:
                    time = record.get_time()
                    time = tz.resolve_imaginary(time)
                    time = time.strftime("%Y-%m-%d %H:%M:%S")
                    value = record.get_value()
                    value = str(value)
                    value = value.replace('.', ',')
                    if(value != "None"):
                        results.append((time, record.get_measurement(), record.get_field(), value, "Abend"))

            new_dbnight_stop = date_time_stop_obj + datetime.timedelta(days = 1)
            new_dbnight_stop = new_dbnight_stop.strftime("%Y-%m-%d")
            new_dbnight_stop = new_dbnight_stop + 'T' + dbtime_night_stop
            querynight = 'from(bucket: ' + dbbucket + ')\
            |> range(start: ' + dbnight_start + ', stop: ' + new_dbnight_stop + ')\
            |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
            |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
            |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
            |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
            |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
            resultnight = client.query_api().query(org=dborg, query=querynight)
            for table in resultnight:
                for record in table.records:
                    time = record.get_time()
                    time = tz.resolve_imaginary(time)
                    time = time.strftime("%Y-%m-%d %H:%M:%S")
                    value = record.get_value()
                    value = str(value)
                    value = value.replace('.', ',')
                    if(value != "None"):
                        results.append((time, record.get_measurement(), record.get_field(), value, "Nacht"))

        # if more days are called calculate the wanted days
        elif (dbdate_start != dbdate_stop):
            date_time_start_obj = datetime.datetime.strptime(dbdate_start, '%Y-%m-%d')
            date_time_stop_obj = datetime.datetime.strptime(dbdate_stop, '%Y-%m-%d')
            delta_day = date_time_stop_obj - date_time_start_obj
            delta_day = delta_day.days
            delta_night = date_time_stop_obj - date_time_start_obj
            delta_night = delta_night.days
            i = 0
            while (i <= delta_day):
                new_dbeve_start = date_time_start_obj + datetime.timedelta(days = i)
                new_dbeve_start = new_dbeve_start.strftime("%Y-%m-%d")
                new_dbeve_start = new_dbeve_start + 'T' + dbtime_eve_start
                new_dbeve_stop = date_time_start_obj + datetime.timedelta(days = i)
                new_dbeve_stop = new_dbeve_stop.strftime("%Y-%m-%d")
                new_dbeve_stop = new_dbeve_stop + 'T' + dbtime_eve_stop
                queryeve = 'from(bucket: ' + dbbucket + ')\
                |> range(start: ' + new_dbeve_start + ', stop: ' + new_dbeve_stop + ')\
                |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
                |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
                |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
                |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
                |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
                resultday = client.query_api().query(org=dborg, query=queryeve)
                for table in resultday:
                    for record in table.records:
                        time = record.get_time()
                        time = tz.resolve_imaginary(time)
                        time = time.strftime("%Y-%m-%d %H:%M:%S")
                        value = record.get_value()
                        value = str(value)
                        value = value.replace('.', ',')
                        if(value != "None"):
                            results.append((time, record.get_measurement(), record.get_field(), value, "Abend"))

                new_dbnight_start = date_time_start_obj + datetime.timedelta(days = i)
                new_dbnight_start = new_dbnight_start.strftime("%Y-%m-%d")
                new_dbnight_start = new_dbnight_start + 'T' + dbtime_night_start
                new_dbnight_stop = date_time_start_obj + datetime.timedelta(days = i + 1)
                new_dbnight_stop = new_dbnight_stop.strftime("%Y-%m-%d")
                new_dbnight_stop = new_dbnight_stop + 'T' + dbtime_night_stop
                queryday = 'from(bucket: ' + dbbucket + ')\
                |> range(start: ' + new_dbnight_start + ', stop: ' + new_dbnight_stop + ')\
                |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
                |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
                |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
                |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
                |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
                resultday = client.query_api().query(org=dborg, query=queryday)
                for table in resultday:
                    for record in table.records:
                        time = record.get_time()
                        time = tz.resolve_imaginary(time)
                        time = time.strftime("%Y-%m-%d %H:%M:%S")
                        value = record.get_value()
                        value = str(value)
                        value = value.replace('.', ',')
                        if(value != "None"):
                            results.append((time, record.get_measurement(), record.get_field(), value, "Nacht"))
                i = i + 1


    # get the day and evening values
    if (tageszeit == 'Tag u. Abend'):
        # if only one day is called
        if (dbdate_start == dbdate_stop):
            queryday = 'from(bucket: ' + dbbucket + ')\
            |> range(start: ' + dbday_start + ', stop: ' + dbday_stop + ')\
            |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
            |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
            |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
            |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
            |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
            resultday = client.query_api().query(org=dborg, query=queryday)
            for table in resultday:
                for record in table.records:
                    time = record.get_time()
                    time = tz.resolve_imaginary(time)
                    time = time.strftime("%Y-%m-%d %H:%M:%S")
                    value = record.get_value()
                    value = str(value)
                    value = value.replace('.', ',')
                    if(value != "None"):
                        results.append((time, record.get_measurement(), record.get_field(), value, "Tag"))

            date_time_stop_obj = datetime.datetime.strptime(dbdate_stop, '%Y-%m-%d')
            queryeve = 'from(bucket: ' + dbbucket + ')\
            |> range(start: ' + dbeve_start + ', stop: ' + dbeve_stop + ')\
            |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
            |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
            |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
            |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
            |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
            resultday = client.query_api().query(org=dborg, query=queryeve)
            for table in resultday:
                for record in table.records:
                    time = record.get_time()
                    time = tz.resolve_imaginary(time)
                    time = time.strftime("%Y-%m-%d %H:%M:%S")
                    value = record.get_value()
                    value = str(value)
                    value = value.replace('.', ',')
                    if(value != "None"):
                        results.append((time, record.get_measurement(), record.get_field(), value, "Abend"))

        # if more days are called calculate the wanted days
        elif (dbdate_start != dbdate_stop):
            date_time_start_obj = datetime.datetime.strptime(dbdate_start, '%Y-%m-%d')
            date_time_stop_obj = datetime.datetime.strptime(dbdate_stop, '%Y-%m-%d')
            delta_day = date_time_stop_obj - date_time_start_obj
            delta_day = delta_day.days
            delta_night = date_time_stop_obj - date_time_start_obj
            delta_night = delta_night.days
            i = 0
            while (i <= delta_day):
                new_dbday_start = date_time_start_obj + datetime.timedelta(days = i)
                new_dbday_start = new_dbday_start.strftime("%Y-%m-%d")
                new_dbday_start = new_dbday_start + 'T' + dbtime_day_start
                new_dbday_stop = date_time_start_obj + datetime.timedelta(days = i)
                new_dbday_stop = new_dbday_stop.strftime("%Y-%m-%d")
                new_dbday_stop = new_dbday_stop + 'T' + dbtime_day_stop
                queryday = 'from(bucket: ' + dbbucket + ')\
                |> range(start: ' + new_dbday_start + ', stop: ' + new_dbday_stop + ')\
                |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
                |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
                |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
                |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
                |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
                resultday = client.query_api().query(org=dborg, query=queryday)
                for table in resultday:
                    for record in table.records:
                        time = record.get_time()
                        time = tz.resolve_imaginary(time)
                        time = time.strftime("%Y-%m-%d %H:%M:%S")
                        value = record.get_value()
                        value = str(value)
                        value = value.replace('.', ',')
                        if(value != "None"):
                            results.append((time, record.get_measurement(), record.get_field(), value, "Tag"))

                new_dbeve_start = date_time_start_obj + datetime.timedelta(days = i)
                new_dbeve_start = new_dbeve_start.strftime("%Y-%m-%d")
                new_dbeve_start = new_dbeve_start + 'T' + dbtime_eve_start
                new_dbeve_stop = date_time_start_obj + datetime.timedelta(days = i)
                new_dbeve_stop = new_dbeve_stop.strftime("%Y-%m-%d")
                new_dbeve_stop = new_dbeve_stop + 'T' + dbtime_eve_stop
                queryeve = 'from(bucket: ' + dbbucket + ')\
                |> range(start: ' + new_dbeve_start + ', stop: ' + new_dbeve_stop + ')\
                |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
                |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
                |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
                |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
                |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
                resultday = client.query_api().query(org=dborg, query=queryeve)
                for table in resultday:
                    for record in table.records:
                        time = record.get_time()
                        time = tz.resolve_imaginary(time)
                        time = time.strftime("%Y-%m-%d %H:%M:%S")
                        value = record.get_value()
                        value = str(value)
                        value = value.replace('.', ',')
                        if(value != "None"):
                            results.append((time, record.get_measurement(), record.get_field(), value, "Abend"))

                i = i + 1


    # get night values
    elif (tageszeit == 'Nacht'):
        # if only one night is called
        if (dbdate_start == dbdate_stop):
            date_time_stop_obj = datetime.datetime.strptime(dbdate_stop, '%Y-%m-%d')
            new_dbnight_stop = date_time_stop_obj + datetime.timedelta(days = 1)
            new_dbnight_stop = new_dbnight_stop.strftime("%Y-%m-%d")
            new_dbnight_stop = new_dbnight_stop + 'T' + dbtime_night_stop
            querynight = 'from(bucket: ' + dbbucket + ')\
            |> range(start: ' + dbnight_start + ', stop: ' + new_dbnight_stop + ')\
            |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
            |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
            |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
            |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
            |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
            resultnight = client.query_api().query(org=dborg, query=querynight)
            for table in resultnight:
                for record in table.records:
                    time = record.get_time()
                    time = tz.resolve_imaginary(time)
                    time = time.strftime("%Y-%m-%d %H:%M:%S")
                    value = record.get_value()
                    value = str(value)
                    value = value.replace('.', ',')
                    if(value != "None"):
                        results.append((time, record.get_measurement(), record.get_field(), value, "Nacht"))
        
        # if more days are called calculate the wanted days
        elif (dbdate_start != dbdate_stop):
            date_time_start_obj = datetime.datetime.strptime(dbdate_start, '%Y-%m-%d')
            date_time_stop_obj = datetime.datetime.strptime(dbdate_stop, '%Y-%m-%d')
            delta_nights = date_time_stop_obj - date_time_start_obj
            delta_nights = delta_nights.days
            new_dbnight_stop = date_time_stop_obj - datetime.timedelta(delta_nights -1)#2022-10-02
            i = 0
            print(delta_nights)
            while (i <= delta_nights):

                new_dbnight_start = date_time_start_obj + datetime.timedelta(days = i)
                new_dbnight_start = new_dbnight_start.strftime("%Y-%m-%d")
                new_dbnight_start = new_dbnight_start + 'T' + dbtime_night_start
                new_dbnight_stop = date_time_start_obj + datetime.timedelta(days = i + 1)
                new_dbnight_stop = new_dbnight_stop.strftime("%Y-%m-%d")
                new_dbnight_stop = new_dbnight_stop + 'T' + dbtime_night_stop
                queryday = 'from(bucket: ' + dbbucket + ')\
                |> range(start: ' + new_dbnight_start + ', stop: ' + new_dbnight_stop + ')\
                |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
                |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
                |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
                |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
                |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
                resultday = client.query_api().query(org=dborg, query=queryday)
                for table in resultday:
                    for record in table.records:
                        time = record.get_time()
                        time = tz.resolve_imaginary(time)
                        time = time.strftime("%Y-%m-%d %H:%M:%S")
                        value = record.get_value()
                        value = str(value)
                        value = value.replace('.', ',')
                        if(value != "None"):
                            results.append((time, record.get_measurement(), record.get_field(), value, "Nacht"))
                i = i + 1



    # get day vales
    elif (tageszeit == 'Tag'):
        # if only one day was called
        if (dbdate_start == dbdate_stop):
            queryday = 'from(bucket: ' + dbbucket + ')\
            |> range(start: ' + dbday_start + ', stop: ' + dbday_stop + ')\
            |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
            |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
            |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
            |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
            |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
            resultday = client.query_api().query(org=dborg, query=queryday)
            for table in resultday:
                for record in table.records:
                    time = record.get_time()
                    time = tz.resolve_imaginary(time)
                    time = time.strftime("%Y-%m-%d %H:%M:%S")
                    value = record.get_value()
                    value = str(value)
                    value = value.replace('.', ',')
                    if(value != "None"):
                        results.append((time, record.get_measurement(), record.get_field(), value, "Tag"))

        # if more days are called calculate the wanted days
        elif (dbdate_start != dbdate_stop):
            date_time_start_obj = datetime.datetime.strptime(dbdate_start, '%Y-%m-%d')
            date_time_stop_obj = datetime.datetime.strptime(dbdate_stop, '%Y-%m-%d')
            delta_day = date_time_stop_obj - date_time_start_obj
            delta_day = delta_day.days
            i = 0
            while (i <= delta_day):
                new_dbday_start = date_time_start_obj + datetime.timedelta(days = i)
                new_dbday_start = new_dbday_start.strftime("%Y-%m-%d")
                new_dbday_start = new_dbday_start + 'T' + dbtime_day_start
                new_dbday_stop = date_time_start_obj + datetime.timedelta(days = i)
                new_dbday_stop = new_dbday_stop.strftime("%Y-%m-%d")
                new_dbday_stop = new_dbday_stop + 'T' + dbtime_day_stop
                queryday = 'from(bucket: ' + dbbucket + ')\
                |> range(start: ' + new_dbday_start + ', stop: ' + new_dbday_stop + ')\
                |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
                |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
                |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
                |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
                |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
                resultday = client.query_api().query(org=dborg, query=queryday)
                for table in resultday:
                    for record in table.records:
                        time = record.get_time()
                        time = tz.resolve_imaginary(time)
                        time = time.strftime("%Y-%m-%d %H:%M:%S")
                        value = record.get_value()
                        value = str(value)
                        value = value.replace('.', ',')
                        if(value != "None"):
                            results.append((time, record.get_measurement(), record.get_field(), value, "Tag"))
                i = i + 1

    # get evening values
    elif (tageszeit == 'Abend'):
        # if only one evening is called
        if (dbdate_start == dbdate_stop):
            queryeve = 'from(bucket: ' + dbbucket + ')\
            |> range(start: ' + dbeve_start + ', stop: ' + dbeve_stop + ')\
            |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
            |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
            |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
            |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
            |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
            resultday = client.query_api().query(org=dborg, query=queryeve)
            for table in resultday:
                for record in table.records:
                    time = record.get_time()
                    time = tz.resolve_imaginary(time)
                    time = time.strftime("%Y-%m-%d %H:%M:%S")
                    value = record.get_value()
                    value = str(value)
                    value = value.replace('.', ',')
                    if(value != "None"):
                        results.append((time, record.get_measurement(), record.get_field(), value, "Abend"))

        # if more days are called calculate the wanted days
        elif (dbdate_start != dbdate_stop):
            #Berechnung der Anzahl an Tagen welche aus der DB geholt werden sollen
            date_time_start_obj = datetime.datetime.strptime(dbdate_start, '%Y-%m-%d')
            date_time_stop_obj = datetime.datetime.strptime(dbdate_stop, '%Y-%m-%d')
            delta_day = date_time_stop_obj - date_time_start_obj
            delta_day = delta_day.days
            i = 0
            while (i <= delta_day):
                new_dbeve_start = date_time_start_obj + datetime.timedelta(days = i)
                new_dbeve_start = new_dbeve_start.strftime("%Y-%m-%d")
                new_dbeve_start = new_dbeve_start + 'T' + dbtime_eve_start
                new_dbeve_stop = date_time_start_obj + datetime.timedelta(days = i)
                new_dbeve_stop = new_dbeve_stop.strftime("%Y-%m-%d")
                new_dbeve_stop = new_dbeve_stop + 'T' + dbtime_eve_stop
                queryeve = 'from(bucket: ' + dbbucket + ')\
                |> range(start: ' + new_dbeve_start + ', stop: ' + new_dbeve_stop + ')\
                |> filter(fn: (r) => r["_measurement"] == ' + dbmeasurement + ')\
                |> filter(fn: (r) => r["Serialnumber"] == ' + dbapplication + ')\
                |> filter(fn: (r) => r["_field"] == ' + dbfield + ')\
                |> aggregateWindow(every: ' + dbinterv + ', fn: mean)\
                |> keep(columns: ["_time", "_measurement", "_field", "_value"])'
                resultday = client.query_api().query(org=dborg, query=queryeve)
                for table in resultday:
                    for record in table.records:
                        time = record.get_time()
                        time = tz.resolve_imaginary(time)
                        time = time.strftime("%Y-%m-%d %H:%M:%S")
                        value = record.get_value()
                        value = str(value)
                        value = value.replace('.', ',')
                        if(value != "None"):
                            results.append((time, record.get_measurement(), record.get_field(), value, "Abend"))
                i = i + 1

    # prepare the called time ranges to a simple format for filename
    if " u. " in day_time:
        day_time = day_time.replace(' u. ', '-')
    
    # save the vlaues to .csv file
    with open('/var/www/html/webapp/downloads/messdaten_' + csvname + '_' + datum_start + '_' + datum_stop + '_' + day_time + '_' + hop_time + '_' + measurement + '.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)

        # write the header
        writer.writerow(header)

        # write the data
        writer.writerows(results)
    # return feedback for the function
    return("erstellt")



# debug data
#datum_start = '2022-10-01'
#datum_stop = '2022-10-03'
#day_time = 'Abend u. Nacht'
#hop_time = '1h'
#station = 'Station_1'
#measurement = 'dBAavg'

# test the function
#print(myfunction(datum_start, datum_stop, day_time, hop_time, station, measurement))
