# University of California Irvine School of Continuing Education
# Data Analytics & Visualization Boot Camp Module 10 Challenge
# Erik Arbelo-Nguyễn

import datetime as dt
from flask import Flask, jsonify
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

# Database

# Create Engine
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflect Existing Database
Base = automap_base()

# Reflect Tables
Base.prepare(autoload_with=engine)

# Save References
Measurement = Base.classes.measurement
Station = Base.classes.station

# Flask Setup

app = Flask(__name__)

# Flask Routes

@app.route("/")
def index():
    """List all available api routes."""

    routes = "<h1>Welcome!</h1><br/>" \
             "Below are the avaialable api routes.<br/>" \
             "Date parameters should be in 'MMDDYYYY' format.<br/>" \
             "This dataset is from 01/01/2010 - 08/23/2017.<br/><br/>" \
             "/api/v1.0/precipitation<br/>" \
             "/api/v1.0/stations<br/>" \
             "/api/v1.0/tobs<br/>" \
             "/api/v1.0/{start date}<br/>" \
             "/api/v1.0/{start date}/{end date}"

    return routes

@app.route("/api/v1.0/precipitation")
def precipitation():
    """Route for handling precipitation api."""

    # Create Session from Python
    session = Session(engine)

    # Get the Date
    one_year_date = twelve_month_date()

    # Query to Retrieve the Date & Precipitation Scores
    precip_data = session. \
        query(Measurement.date, Measurement.prcp). \
        filter(Measurement.date >= one_year_date).all()

    session.close()

    # Store Records
    precip_list = []
    for date, prcp in precip_data:
        precip_list.append(
            {date: prcp}
        )

    return jsonify(precip_list)

@app.route("/api/v1.0/stations")
def stations():
    """Route for handling stations api."""

    # Create Session from Python
    session = Session(engine)

    # Perform Query to Retrieve Stations
    all_stations = session. \
        query(Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation).all()

    session.close()

    # Store Data
    station_list = []
    for station, name, latitude, longitude, elevation in all_stations:
        station_dict = {
            'station': station,
            'name': name,
            'latitude': latitude,
            'longitude': longitude,
            'elevation': elevation
        }
        station_list.append(station_dict)

    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
    """Route for handling temperature api."""

    # Create Session from Python
    session = Session(engine)

    # Query Most Active Station
    most_active = session. \
        query(Measurement.station, func.count(Measurement.station)).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).first()[0]

    # Calculate One Year Date
    one_year_date = twelve_month_date()

    # Query Most Active Station
    temp_data = session. \
        query(Measurement.date, Measurement.tobs). \
        filter(Measurement.station == most_active). \
        filter(Measurement.date >= one_year_date).all()

    session.close()

    # Store Data
    temp_list = []
    for date, temp in temp_data:
        temp_list.append(
            {date: temp}
        )

    final_dict = {most_active: temp_list}

    return jsonify(final_dict)

@app.route("/api/v1.0/<start>")
def single_date(start):
    """Route for handling min, max, and avg for dates greater or equal to start."""

    # Convert Start Date
    if date_is_valid(start):
        start_date = dt.datetime.strptime(start, "%m%d%Y").strftime('%Y-%m-%d')

        # Create Session from Python
        session = Session(engine)

        # Query Aggregate Temperatures
        agg_temps = session.query(
                Measurement.date,
                func.min(Measurement.tobs),
                func.avg(Measurement.tobs),
                func.max(Measurement.tobs)
            ). \
            filter(Measurement.date >= start_date). \
            group_by(Measurement.date).all()

        session.close()

        # Store Data
        agg_list = []
        for date, tmin, tavg, tmax in agg_temps:
            agg_dict = {
                'date': date,
                'TMIN': tmin,
                'TAVG': tavg,
                'TMAX': tmax
            }
            agg_list.append(agg_dict)

        return jsonify(agg_list)
    else:
        return "Error: Start date entered is not valid or is outside of the dataset.<br/>" \
               f"Start Date: {start}"


@app.route("/api/v1.0/<start>/<end>")
def date_range(start, end):
    """Route for handling min, max, and avg for dates between start and end."""

    # Convert Start and End Dates
    if date_is_valid(start):
        if date_is_valid(end):
            start_date = dt.datetime.strptime(start, "%m%d%Y").strftime('%Y-%m-%d')
            end_date = dt.datetime.strptime(end, "%m%d%Y").strftime('%Y-%m-%d')

            # Create Session from Python
            session = Session(engine)

            # Query Aggregate Temperatures
            agg_temps = session.query(
                    Measurement.date,
                    func.min(Measurement.tobs),
                    func.avg(Measurement.tobs),
                    func.max(Measurement.tobs)
                ). \
                filter(Measurement.date >= start_date). \
                filter(Measurement.date <= end_date). \
                group_by(Measurement.date).all()

            session.close()

            if len(agg_temps) == 0:
                return "Error: Start date is greater than end date.<br/>" \
                       f"Start date: {start_date}<br/>" \
                       f"End date: {end_date}"

            # Store Data
            agg_list = []
            for date, tmin, tavg, tmax in agg_temps:
                agg_dict = {
                    'date': date,
                    'TMIN': tmin,
                    'TAVG': tavg,
                    'TMAX': tmax
                }
                agg_list.append(agg_dict)

            return jsonify(agg_list)
        else:
            return "Error: End date entered is not valid or is outside of the dataset.<br/>" \
                   f"End Date: {end}"
    else:
        return "Error: Start date entered is not valid or is outside of the dataset.<br/>" \
               f"Start Date: {start}"


# Helper Methods

def twelve_month_date():
    """Returns the most recent date in the dataset minus 12 months."""

    # Create Session from Python
    session = Session(engine)

    # Get Most Recent Date
    recent_date = session. \
        query(Measurement.date).order_by(Measurement.date.desc()).first()[0]

    # Calculate One Year Date
    one_year_date = (dt.datetime.strptime(recent_date, "%Y-%m-%d") - dt.timedelta(days=365)).strftime('%Y-%m-%d')

    session.close()

    return one_year_date

def date_is_valid(date):
    """Verifies input date is correct date and if available in dataset."""

    session = Session(engine)

    try:
        dt.datetime.strptime(date, "%m%d%Y").strftime('%Y-%m-%d')

    except ValueError:
        session.close()
        return False

    else:
        # Query Measurement Table
        val_date = dt.datetime.strptime(date, "%m%d%Y").strftime('%Y-%m-%d')
        result = session.query(Measurement.date). \
            filter(Measurement.date == val_date).all()

        if len(result) == 0:
            session.close()
            return False

        session.close()
        return True

if __name__ == "__main__":
    app.run(debug=True)