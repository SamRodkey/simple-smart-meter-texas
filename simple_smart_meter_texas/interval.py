"""module containing code for 15 minute interval data from Smart Meter Texas """
import os
import re
import requests

from urllib.parse import urljoin

from time import sleep

from datetime import date, datetime, timedelta

from collections import OrderedDict

from . import logger

from .config import BASE_URL, DEFAULT_ESID_ENV_VARIABLE

INTERVAL_URL = urljoin(
    BASE_URL,
    "api/adhoc/intervalsynch",
)
"""URL used to make interval data requests using Texas Smart Meter HTTP API"""

MAX_DAYS_PER_REQUEST = 30
"""maximum days worth of data for each individual request to Texas Smart Meter HTTP API"""

SLEEP_TIME_PER_REQUEST = 1.0
"""adds pause between requests in a string of requests to prevent saturating Smart Meter Texas HTTP API with requests"""

DATA_TYPE_LOOKUP = {
    "C": "net_consumed_kwh",
    "G": "net_generated_kwh",
}
"""lookup dictionary which translates the 'RT' value in the response entry into its corresponding data column in the final daily record dictionary output"""


def _request_interval_data_over_date_range(
    start_date: date,
    end_date: date,
    token: str,
    esid: str,
) -> dict:
    """helper method executes the request to the API directly and returns
        the raw JSON response

    Parameters
    ----------
    start_date : datetime.date
        first day in date range to request data
    end_date : datetime.date
        last day in date range to request data
    token : str
        session token to use for authentication to HTTP API
    esid : str
        subscriber ID to request data  HTTP API

    Returns
    -------
    dict
        dictionary containing raw JSON returned from HTTP request

    Raises
    ------
    ValueError
        if date arguments are inconsistent, invalid, or are not supported
        by the Smart Meter Texas API.
    requests.exceptions.HTTPError
        if an error response is returned from the Smart Meter Texas HTTP API.
    """

    # check to make sure end date is after start date
    if end_date < start_date:
        raise ValueError(
            f"Specified end_date '{end_date.isoformat()}' is before start_date!"
        )
    # ensure that end date is not after today
    if end_date > date.today():
        raise ValueError(
            f"Specified end_date '{end_date.isoformat()}' cannot be after today"
        )

    # ensure that date range is less than or equal to MAX_DAYS_PER_REQUEST
    if (end_date - start_date) > timedelta(days=(MAX_DAYS_PER_REQUEST - 1)):
        raise ValueError(
            f"Specified date range is longer than limit of {MAX_DAYS_PER_REQUEST:d} days of data per request to Smart Meter Texas HTTP API."
        )

    logger.debug(
        f"Submitting POST request to {INTERVAL_URL:s} for dates between {start_date} and {end_date} for ESID {esid:s}..."
    )

    # submit POST request to get 15-minute interval data over range of dates
    resp = requests.post(
        INTERVAL_URL,
        headers={
            f"Authorization": f"Bearer {token:s}",
        },
        json={
            "startDate": start_date.strftime("%m/%d/%Y"),
            "endDate": end_date.strftime("%m/%d/%Y"),
            "reportFormat": "JSON",
            "ESIID": [esid],
            "versionDate": None,
            "readDate": None,
            "versionNum": None,
            "dataType": None,
        },
    )

    logger.debug(
        f"Recieved response for POST request to {INTERVAL_URL:s} for dates between {start_date} and {end_date} for ESID {esid:s}."
    )

    # raise exception if request doesn't work
    if resp.status_code != 200:
        raise requests.exceptions.HTTPError(
            f"POST Request to {INTERVAL_URL:s} returned status code {resp.status_code:d} : {resp.json() if resp.json() else ''}"
        )

    logger.info(
        f"Recieved response containing interval data for dates between {start_date} and {end_date} for ESID {esid:s}."
    )

    return resp.json()


def _parse_value_array_string(values: str) -> list[float]:
    """helper method to parse the string of interval values returned from
        Smart Meter Texas API calls

    Parameters
    ----------
    values : str
        string containing comma separated values for each 15 minute period

    Returns
    -------
    list[float]
        list of values contained in input string
    """
    return [
        float(re.search(r"^[\+-]?[\d\.]+", v).group(0))
        if re.search(r"^[\+-]?[\d\.]+", v)
        else float("nan")
        for v in values.split(",")
        if "-" in v
    ]


def _parse_response_for_daily_record_dicts(resp: dict) -> list[dict]:
    """helper method to parse the response returned from the interval data request
        and turn it into a list dictionaries containing the data key/values from
        each day

    Parameters
    ----------
    resp : dict
        raw JSON response from interval data request

    Returns
    -------
    list[dict]
        list of daily data dictionaries

    Raises
    ------
    KeyError
        if response from Smart Meter Texas API is mal-formed or missing fields
    """

    # create empty ordered dictionary to store each daily record dictionary by date
    daily_records_by_date = OrderedDict()

    # verify response structure meets expectations
    if "data" not in resp:
        raise KeyError("Expected key `data` not found in response dictionary!")

    if "energyData" not in resp.get("data"):
        raise KeyError("Expected key `energyData` not found in `data` dictionary!")

    # loop over list of data dictionaries in `energyData` portion of response
    for entry in resp.get("data").get("energyData"):
        # get datetime for current entry at midnight
        entry_datetime = datetime.strptime(entry.get("DT"), "%m/%d/%Y")

        # get corresponding date for current entry
        entry_date = entry_datetime.date()

        # if current date has no data associated with it yet, initialize an empty record dict
        if entry_date not in daily_records_by_date:
            daily_records_by_date[entry_date] = {
                "start_timestamp": [
                    entry_datetime + timedelta(hours=hours, minutes=minutes)
                    for hours in range(24)
                    for minutes in (0, 15, 30, 45)
                ],
                "end_timestamp": [
                    entry_datetime + timedelta(hours=hours, minutes=minutes)
                    for hours in range(24)
                    for minutes in (15, 30, 45, 60)
                ],
                "net_consumed_kwh": [float("nan") for i in range(24 * 4)],
                "net_generated_kwh": [float("nan") for i in range(24 * 4)],
            }

        # get current entry daily data dictionary
        entry_daily_record = daily_records_by_date[entry_date]

        # if data is missing or empty, skip this entry and deliver a warning
        if "RD" not in entry or not entry.get("RD"):
            logger.warning(
                f"Skipping entry for {entry_date.isoformat():s} due to missing 'RD' values!"
            )
            continue

        # parse to extract list of float values contained within string
        values = _parse_value_array_string(entry.get("RD"))

        # if an unexpected number of values is found, skip this entry and deliver a warning
        if len(values) != (24 * 4):
            logger.warning(
                f"Skipping entry on {entry_date.isoformat():s} which contains unexpected number of numeric values : {len(values):d}"
            )
            continue

        # if data type code is missing or empty, skip this entry and deliver a warning
        if "RT" not in entry or not entry.get("RT"):
            logger.warning(
                f"Skipping entry for {entry_date.isoformat():s} due to missing 'RT' value!"
            )
            continue

        # if data type code is not expected, skip this entry and deliver a warning
        if entry.get("RT") not in DATA_TYPE_LOOKUP:
            logger.warning(
                f"Skipping entry for {entry_date.isoformat():s} due to invalid or unexpected 'RT' value : {entry.get('RT'):s}"
            )
            continue

        logger.debug(
            f"Successfully parsed values for {entry_date.isoformat():s} for entry type `{entry.get('RT'):s}`"
        )

        # add the appropriate data key and associated values to daily record dictionary
        entry_daily_record |= {DATA_TYPE_LOOKUP[entry.get("RT")]: values}

    # return a sorted list of data dictionaries for each day
    return list(daily_records_by_date.values())


def _iterate_over_interval_date_subranges(
    start_date: date,
    end_date: date,
) -> tuple[date, date]:
    """helper method which generates iterates over a larger range of dates by using
        smaller date sub-intervals, returns pairs of start/end dates for each sub interval
        as a tuple

    Parameters
    ----------
    start_date : datetime.date
        overall start date for entire date range
    end_date : datetime.date
        overall end date for entire date range

    Returns
    -------
    tuple[date, date]
        tuple of start and end date for each sub interval of dates in the
        overall date interval

    Yields
    ------
    Iterator[tuple[date, date]]
        pairs of start and end dates for each date sub-interval
    """

    # get date range start as ordinal integer
    range_start_ordint = start_date.toordinal()

    # get date range stop as ordinal integer
    range_stop_ordint = end_date.toordinal() + 1

    current_subrange_start = range_start_ordint

    current_subrange_stop = min(
        current_subrange_start + MAX_DAYS_PER_REQUEST,
        range_stop_ordint,
    )

    while current_subrange_start < current_subrange_stop:
        # calculate first date in subrange
        current_subrange_start_date = date.fromordinal(current_subrange_start)

        # calculate last day in subrange, one less than stop date
        current_subrange_end_date = date.fromordinal(current_subrange_stop - 1)

        logger.debug(
            f"Current interval data date subrange : {current_subrange_start_date.isoformat()} - {current_subrange_end_date.isoformat()}"
        )

        # return current date subrange
        yield (
            current_subrange_start_date,
            current_subrange_end_date,
        )

        # calculate values for next iteration
        current_subrange_start = current_subrange_stop

        current_subrange_stop = min(
            current_subrange_start + MAX_DAYS_PER_REQUEST,
            range_stop_ordint,
        )


def get_interval_data_over_date_range(
    start_date: date,
    end_date: date,
    token: str,
    esid: str = os.environ.get(DEFAULT_ESID_ENV_VARIABLE, None),
) -> list[dict]:
    f"""returns 15-minute interval energy usage data using the Smart Meter Texas HTTP API
        for a specified ESID. The data will be returned as a list of daily data dictionaries
        for each day between the input `start_date` and `end_date`.

    Parameters
    ----------
    start_date : datetime.date
        first date to request interval data
    end_date : date
        last date to request interval data
    token : str
        session token to use for authentication to Smart Meter Texas HTTP API
    esid : str, optional
        subscriber ID to request data via Smart Meter Texas HTTP API, if not specified will use
        the value of the environment variable {os.get(DEFAULT_ESID_ENV_VARIABLE):s}.

    Returns
    -------
    list[dict]
        list of daily data dictionaries dates between the specified start and end date
        with arrays 15-minute energy data for the specified ESID
    """

    daily_data_dicts = []

    for sub_start_date, sub_end_date in _iterate_over_interval_date_subranges(
        start_date,
        end_date,
    ):
        daily_data_dicts.extend(
            _parse_response_for_daily_record_dicts(
                _request_interval_data_over_date_range(
                    sub_start_date,
                    sub_end_date,
                    token,
                    esid,
                )
            )
        )

        logger.info(
            f"Completed retrieval of daily data between {sub_start_date.isoformat():s} and {sub_end_date.isoformat():s} ."
        )

        if sub_end_date != end_date:
            logger.info(f"Sleeping for {SLEEP_TIME_PER_REQUEST} seconds...")
            sleep(SLEEP_TIME_PER_REQUEST)

    logger.info(
        f"Completed retrieval of all daily data between {start_date.isoformat():s} and {end_date.isoformat():s} ."
    )

    return daily_data_dicts


def get_interval_dataframe_over_date_range(
    start_date: date,
    end_date: date,
    token: str,
    esid: str = os.environ.get(DEFAULT_ESID_ENV_VARIABLE, None),
):
    f"""returns 15-minute interval energy usage data using the Smart Meter Texas HTTP API
        for a specified ESID. The data will be returned as a pandas DataFrame
        with alla available data between the input `start_date` and `end_date`.

    Parameters
    ----------
    start_date : datetime.date
        first date to request interval data
    end_date : date
        last date to request interval data
    token : str
        session token to use for authentication to Smart Meter Texas HTTP API
    esid : str, optional
        subscriber ID to request data via Smart Meter Texas HTTP API, if not specified will use
        the value of the environment variable {os.get(DEFAULT_ESID_ENV_VARIABLE):s}.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing all 15-minute energy data for the specified
        ESID over the range of dates between the specified start and end date

    Raises
    ------
    ImportError
        _description_
    """

    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Need to have pandas installed")

    return pd.concat(
        pd.DataFrame(day)
        for day in get_interval_data_over_date_range(
            start_date,
            end_date,
            token,
            esid=esid,
        )
    )
