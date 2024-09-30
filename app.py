# This is a Python WebApp to call Influx-Data
# with defined Ranges of a User over the UI

from flask import Flask, request, render_template, send_file, redirect, request
from influxdbdata import myfunction

dict_field = {
        "dBAavg":"Durchschnitt Umgebungslautstärke",
        "dBAmin":"Minimum Umgebungslautstärke",
        "dBAmax":"Lautstärke",
        "temp":"Temperatur",
        "feucht":"rel. Luftfeuchte",
        "druck":"Luftdruck",
        "fein10":"PM10",
        "fein25":"PM2.5",
}

app = Flask(__name__)

# app route for calling ip/domain or /home
@app.route("/")
@app.route("/home", methods =["GET", "POST"])
def home():
    global filename

    # check if the wanted data are filled out if not, use default values
    if request.method == "POST":
        datum_start = request.form.get("datum_start")
        if datum_start == '':
            return render_template('home.html')
        datum_stop = request.form.get("datum_stop")
        if datum_stop == '':
            datum_stop = datum_start
        day_time = request.form.get("day_time")
        hop_time = request.form.get("hop_time")
        station = request.form.get("station")
        measurement = request.form.get("measurement")

        # if the user want sensebox data, the names of the variables are not the same like Station-Data, so use the dict to translate
        if 'Sensebox_' in station:
            measurement = dict_field[measurement]

        # call the myfunction of influxdbdata_sensebox to get the wanted measurements
        if myfunction(datum_start, datum_stop, day_time, hop_time, station, measurement) == 'erstellt':
            csvname = station.replace('"','')
            if " u. " in day_time:
                day_time = day_time.replace(' u. ', '-')
            # create the filename
            filename = 'messdaten_' + csvname + '_' + datum_start + '_' + datum_stop + '_' + day_time + '_' + hop_time + '_' + measurement + '.csv'
            # route direct to download to send the csv file
            return redirect("/download")
    # route to home to select next dataranges
    return render_template('home.html')

# app route for calling download
@app.route('/download')
def downloadFile ():
  # get the file created at /home route
  path = '/var/www/html/webapp/downloads/' + filename + ''
  # send the file to the client
  return send_file(path, as_attachment=True)

# run the app
app.run(host="yor server ip", port=8089)

# run server
if __name__ == "__main__":
    from waitress import serve
    serve(app, host="yor server ip", port=8089)
