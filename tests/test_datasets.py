from trane.datasets.load_functions import (
    load_bike,
    load_covid,
    load_flight,
    load_youtube,
)


def test_load_covid():
    df = load_covid()
    for col in [
        "Province/State",
        "Country/Region",
        "Lat",
        "Long",
        "Date",
        "Confirmed",
        "Deaths",
        "Recovered",
    ]:
        assert col in df.columns
    assert len(df) >= 17136
    assert df["Date"].dtype == "datetime64[ns]"


def test_load_flight():
    airlines_df, airport_df, flights_df = load_flight()
    for col in [
        "IATA_CODE",
        "AIRPORT",
        "CITY",
        "STATE",
        "COUNTRY",
        "LATITUDE",
        "LONGITUDE",
    ]:
        assert col in airport_df.columns
    for col in ["IATA_CODE", "AIRLINE"]:
        assert col in airlines_df.columns
    for col in [
        "DATE",
        "DAY_OF_WEEK",
        "AIRLINE",
        "FLIGHT_NUMBER",
        "TAIL_NUMBER",
        "ORIGIN_AIRPORT",
        "DESTINATION_AIRPORT",
        "SCHEDULED_DEPARTURE_HOUR",
        "SCHEDULED_TIME",
        "ELAPSED_TIME",
        "DEPARTURE_DELAY",
        "ARRIVAL_DELAY",
        "CANCELLED",
        "CANCELLATION_REASON",
        "AIR_SYSTEM_DELAY",
        "SECURITY_DELAY",
        "AIRLINE_DELAY",
        "LATE_AIRCRAFT_DELAY",
        "WEATHER_DELAY",
    ]:
        assert col in flights_df.columns

    assert flights_df["DATE"].dtype == "datetime64[ns]"


def test_load_bike():
    df = load_bike()
    for col in [
        "date",
        "hour",
        "usertype",
        "gender",
        "tripduration",
        "temperature",
        "from_station_id",
        "dpcapacity_start",
        "to_station_id",
        "dpcapacity_end",
    ]:
        assert col in df.columns
    assert df["date"].dtype == "datetime64[ns]"


def test_load_youtube():
    df = load_youtube()
    for col in [
        "trending_date",
        "channel_title",
        "category_id",
        "views",
        "likes",
        "dislikes",
        "comment_count",
    ]:
        assert col in df.columns
    assert df["trending_date"].dtype == "datetime64[ns]"
