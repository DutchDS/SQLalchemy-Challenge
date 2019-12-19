import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

import datetime as dt
from datetime import datetime, timedelta

import pandas as pd

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
            f" <br/>"
        f"Precipication in the last year of available data: <br/>"
        f"/api/v1.0/precipitation<br/>"
        f" <br/>"
        f"Weather Stations in database: <br/>"
        f"/api/v1.0/stations<br/>"
        f" <br/>"
        f"Rainfall for the most active station over the last year of availeble data: <br/>"
        f"/api/v1.0/tobs<br/>"
        f" <br/>"
        f"Change start date. End date will be last date in database. Min/Avg/Max averages collected over available years in DB.<br>"
        f"/api/v1.0/2017-05-05<br/>"
        f" <br/>"
        f"Change start and end date. Min/Avg/Max averages collected over available years in DB. <br/>"
        f"/api/v1.0/range/2017-02-05/2017-02-12"
    )

#################################################
# Flask Route 1: Rainfall
#################################################

@app.route("/api/v1.0/precipitation")
def precipitation():

    # Create our session (link) from Python to the DB
    session = Session(engine) 
    # Get last date and find date one year prior
    get_last_date = session.query(func.max(Measurement.date))
    session.close()

    for d in get_last_date:
        max_date = d[0]

    temp_date = datetime.strptime(max_date, '%Y-%m-%d') - dt.timedelta(days=365)
    min_date = temp_date.date()
    
    # Create another session (link) from Python to the DB
    session = Session(engine)
    """Return a list of precipitation by date in the Database"""
    # Query average precipitation from multiple stations by date
    results = session.query(Measurement.date, func.avg(Measurement.prcp)).\
        filter(Measurement.date >= min_date).filter(Measurement.date <= max_date).\
        group_by(Measurement.date).all()
    session.close()

    # move results into a list
    all_rainfall = []
    for date, avg_1 in results:
        rainfall_dict = {}
        rainfall_dict["date"] = date
        rainfall_dict["avg_1"] = avg_1
        all_rainfall.append(rainfall_dict)

    return jsonify(all_rainfall)

#################################################
# Flask Route 2: Stations
#################################################

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of weather stations"""
    # Query all stations
    result_stations = session.query(Station.station,Station.name).all()
    session.close()

    # Move results into a list
    all_stations = []
    for station, name in result_stations:
        station_dict = {}
        station_dict["station"] = station
        station_dict["name"] = name
        all_stations.append(station_dict)

    return jsonify(all_stations)

#################################################
# Flask Route 3: Temperatures
#################################################

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of average temperatures for the past year"""
    # Get last date and find date one year prior
    get_last_date = session.query(func.max(Measurement.date))
    for d in get_last_date:
        max_date = d[0]

    temp_date = datetime.strptime(max_date, '%Y-%m-%d') - dt.timedelta(days=365)
    min_date = temp_date.date()
    
    # Get most active station
    sel = [Measurement.station,func.count(Measurement.tobs)]
    most_active = session.query(*sel).group_by(Measurement.station).order_by(func.count(Measurement.tobs).desc()).limit(1).all()
    for m in most_active:
        act_station = m.station
        print(act_station)

    # Query temperatures
    result_tobs = session.query(Measurement.date, func.avg(Measurement.tobs)).\
        filter(Measurement.date >= min_date).filter(Measurement.date <= max_date).\
        filter(Measurement.station == act_station).\
        group_by(Measurement.date).all()
    session.close()

    # Move results into a list
    all_temps = []
    for date, avg_1 in result_tobs:
        temp_dict = {}
        temp_dict["date"] = date
        temp_dict["temp"] = avg_1
        all_temps.append(temp_dict)

    return jsonify(all_temps)

#################################################
# Definitions for route 4 and 5
#################################################

def daily_normals(date):
    session = Session(engine)

    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    results_daily_normal = session.query(*sel).filter(func.strftime("%m-%d", Measurement.date) == date).all()

    session.close()

    return results_daily_normal

def get_min_max_avg(s_date, e_date):

    date_range = []
        
    start_date = s_date
    end_date = e_date

    var_date = start_date
    date_range.append(var_date)

    # Add all dates requested to a list
    while var_date < end_date:
        loc_date = datetime.strptime(var_date, "%Y-%m-%d")
        modified_date = loc_date + timedelta(days=1)
        var_date = datetime.strftime(modified_date, "%Y-%m-%d")
        date_range.append(var_date)

    # Stip off the year and save a list of %m-%d strings
    strip_range = []
    for d in date_range:
        strip_string = d[5:]
        strip_range.append(strip_string)

    # Get dataframe ready with all dates within the selected range
    df_results = pd.DataFrame(date_range)
    df_results = df_results.rename(columns={0:"Date"})
    df_results = df_results.set_index("Date")

    list_min = []
    list_avg = []
    list_max = []
     # Loop through the list of %m-%d strings and calculate the normals for each date
    for s in strip_range:
        daily = daily_normals(s)
        for d in daily:
            list_min_s = d[0]
            list_min.append(list_min_s)
            list_avg_s = d[1]
            list_avg.append(list_avg_s)
            list_max_s = d[2]
            list_max.append(list_max_s)
    df_results['Minimum'] = list_min
    df_results['Average'] = list_avg
    df_results['Maximum'] = list_max

    return df_results

#######################################################################
# Flask Route 4: Average Rainfall - Start Date onwards
#######################################################################

@app.route("/api/v1.0/<date>")
def start_date_only(date):

    """Return a list of minimum temperature, the average temperature, and the max temperature for a given start date until the last date in the database"""
    start_date = date
    print(start_date) 

     # Get last date and find date one year prior
    session = Session(engine)
    get_last_date = session.query(func.max(Measurement.date))
    for d in get_last_date:
        end_date = d[0]
        print(end_date)
    session.close()

    df_results = get_min_max_avg(start_date, end_date)

    return df_results.to_json()

#######################################################################
# Flask Route 5: Average Rainfall - Start and End date
#######################################################################

@app.route("/api/v1.0/range/<s_date>/<e_date>")
def start_and_end_date(s_date, e_date):

    """Return a list of minimum temperature, the average temperature, and the max temperature for a given start date"""
    start_date = s_date
    end_date = e_date
    print(start_date)
    print(end_date)
    
    # Use the start and end date to retrieve min, avg and max temperatures for date range given for the same days in prior years
    df_results = get_min_max_avg(start_date, end_date)
    
    return df_results.to_json()

#######################################################################
# Run the app
#######################################################################

if __name__ == '__main__':
    app.run(debug=True)