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
    # TODO: add docstring
    if end_date < start_date:
        raise ValueError(
            f"Specified end_date '{end_date.isoformat()}' is before start_date!"
        )

    if end_date > date.today():
        raise ValueError(
            f"Specified end_date '{end_date.isoformat()}' cannot be after today"
        )

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
    """_summary_

    Parameters
    ----------
    values : str
        _description_

    Returns
    -------
    list[float]
        _description_
    """
    # TODO: add docstring
    return [
        float(re.search(r"^[\+-]?[\d\.]+", v).group(0))
        if re.search(r"^[\+-]?[\d\.]+", v)
        else float("nan")
        for v in values.split(",")
        if "-" in v
    ]


def _parse_response_for_daily_record_dicts(resp: dict) -> list[dict]:
    # TODO: add docstring
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
    # TODO: add docstring
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

        yield (
            current_subrange_start_date,
            current_subrange_end_date,
        )

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
):
    # TODO: add docstring
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
    # TODO: add docstring
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
