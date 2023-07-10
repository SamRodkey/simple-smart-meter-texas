import pytest
import requests_mock

from math import isnan

from requests.exceptions import HTTPError
from datetime import datetime, date, timedelta

from simple_smart_meter_texas import _check_for_pandas
from simple_smart_meter_texas.interval import (
    _parse_response_for_daily_record_dicts,
    _iterate_over_interval_date_subranges,
    INTERVAL_URL,
    MAX_DAYS_PER_REQUEST,
    _request_interval_data_over_date_range,
    get_interval_data_over_date_range,
    get_interval_dataframe_over_date_range,
)


@pytest.fixture
def example_interval_response():
    """simulated Smart Meter Texas API response

    Returns
    -------
    dict
        example response taken from Page 23 in Smart Meter API Documentation (https://www.smartmetertexas.com/Smart_Meter_Texas_Interface_Guide.pdf)
    """

    return {
        "data": {
            "trans_id": "123",
            "esiid": "1008901012126195372100",
            "energyData": [
                {
                    "DT": "07/01/2019",
                    "RevTS": "07/02/2019 00:20:05",
                    "RT": "C",
                    "RD": ".198-A,.141-A,.183-A,.109-A,.176-A,.12-A,.148-A,.181-A,,,,,.156-A,.132-A,.16-A,.154-A,.132-A,.197-A,.147-A,.148-A,.169-A,.135-A,.139-A,.109-A,.243-A,.125-A,.093-A,.117-A,.146-A,.103-A,.115-A,.18-A,.133-A,.165-A,.152-A,.123-A,.126-A,.161-A,.153-A,.183-A,.209-A,.185-A,.136-A,.142-A,.153-A,.151-A,.198-A,.158-A,.191-A,.18-A,.193-A,.204-A,.241-A,.24-A,.243-A,.226-A,.233-A,.231-A,.24-A,.275-A,.245-A,.254-A,.235-A,.232-A,.231-A,.297-A,.23-A,.366-A,.295-A,.313-A,.39-A,.316-A,.317-A,.362-A,.387-A,.383-A,.328-A,.395-A,.396-A,.395-A,.393-A,.388-A,.382-A,.365-A,.353-A,.289-A,.286-A,.251-A,.245-A,.279-A,.255-A,.248-A,.294-A,.24-A,.238-A,.275-A,.242-A,.292-A,.242-A,.23-A",
                },
                {
                    "DT": "07/02/2019",
                    "RevTS": "07/03/2019 02:39:13",
                    "RT": "C",
                    "RD": ".212-A,.192-A,.172-A,.196-A,.201-A,.151-A,.209-A,.149-A,,,,,.151-A,.173-A,.212-A,.248-A,.295-A,.166-A,.208-A,.184-A,.188-A,.201-A,.196-A,.155-A,.16-A,.177-A,.156-A,.146-A,.163-A,.135-A,.178-A,.152-A,.165-A,.168-A,.211-A,.154-A,.184-A,.186-A,.21-A,.188-A,.155-A,.2-A,.166-A,.15-A,.183-A,.18-A,.155-A,.169-A,.192-A,.175-A,.192-A,.205-A,.247-A,.213-A,.218-A,.244-A,.229-A,.206-A,.233-A,.239-A,.238-A,.221-A,.202-A,.172-A,.217-A,.213-A,.196-A,.224-A,.201-A,.178-A,.218-A,.216-A,.209-A,.228-A,.201-A,.407-A,.311-A,.247-A,.246-A,.286-A,.25-A,.232-A,.233-A,.186-A,.191-A,.21-A,.262-A,.161-A,.198-A,.161-A,.144-A,.179-A,.174-A,.208-A,.168-A,.167-A,.157-A,.169-A,.184-A,.203-A",
                },
                {
                    "DT": "07/03/2019",
                    "RevTS": "07/04/2019 04:21:07",
                    "RT": "C",
                    "RD": ".18-A,.173-A,.17-A,.153-A,.131-A,.137-A,.124-A,.123-A,,,,,.13-A,.144-A,.123-A,.159-A,.113-A,.162-A,.153-A,.114-A,.156-A,.138-A,.155-A,.134-A,.116-A,.127-A,.122-A,.108-A,.098-A,.123-A,.128-A,.086-A,.132-A,.199-A,.162-A,.171-A,.175-A,.164-A,.157-A,.189-A,.173-A,.191-A,.186-A,.167-A,.154-A,.169-A,.186-A,.153-A,.18-A,.19-A,.155-A,.156-A,.179-A,.185-A,.205-A,.18-A,.207-A,.182-A,.221-A,.18-A,.208-A,.175-A,.192-A,.171-A,.186-A,.187-A,.196-A,.19-A,.207-A,.185-A,.225-A,.208-A,.26-A,.265-A,.254-A,.27-A,.254-A,.282-A,.228-A,.236-A,.332-A,.395-A,.4-A,.297-A,.267-A,.289-A,.292-A,.28-A,.234-A,.246-A,.244-A,.306-A,.286-A,.263-A,.294-A,.244-A,.297-A,.235-A,.224-A,.272-A",
                },
            ],
        }
    }


def valiate_example_interval_response(results):
    """test helper method to validate the properties and structure of the response
        containing 15-minute interval energy consumption data

    Parameters
    ----------
    results : dict

    Raises
    ------
        AssertionError
            if any test fails
    """

    # assert we got a list of entries for each day
    assert isinstance(results, list)

    # assert we got three entries in list
    assert len(results) == 3

    # loop through each day's data
    for daily_result in results:
        # assert we got a dictionary
        assert isinstance(daily_result, dict)

        # assert dictionary has all keys
        assert set(daily_result) == set(
            [
                "start_timestamp",
                "end_timestamp",
                "net_consumed_kwh",
                "net_generated_kwh",
            ]
        )

        # assert we got a list for value associated with key `start_timestamp`
        assert isinstance(daily_result.get("start_timestamp"), list)

        # assert we got 24 hours worth of data every 15 minutes
        assert len(daily_result.get("start_timestamp")) == 24 * 4

        # assert that all elements in list are datetime
        assert all(isinstance(v, datetime) for v in daily_result.get("start_timestamp"))

        # assert we got a list for value associated with key `end_timestamp`
        assert isinstance(daily_result.get("end_timestamp"), list)

        # assert that all elements in list are datetime
        assert all(isinstance(v, datetime) for v in daily_result.get("end_timestamp"))

        # assert we got 24 hours worth of data every 15 minutes
        assert len(daily_result.get("end_timestamp")) == 24 * 4

        # assert we got a list for value associated with key `net_consumed_kwh`
        assert isinstance(daily_result.get("net_consumed_kwh"), list)

        # assert that all elements in list are floats
        assert all(isinstance(v, float) for v in daily_result.get("net_consumed_kwh"))
        assert not any(isnan(v) for v in daily_result.get("net_consumed_kwh"))

        # assert we got 24 hours worth of data every 15 minutes
        assert len(daily_result.get("net_consumed_kwh")) == 24 * 4

        # assert we got a list for value associated with key `net_generated_kwh`
        assert isinstance(daily_result.get("net_generated_kwh"), list)

        # assert that all elements in list are floats
        assert all(isinstance(v, float) for v in daily_result.get("net_generated_kwh"))
        assert all(isnan(v) for v in daily_result.get("net_generated_kwh"))

        # assert we got 24 hours worth of data every 15 minutes
        assert len(daily_result.get("net_generated_kwh")) == 24 * 4


def test_date_iteration():
    """unit test to verify _iterate_over_interval_date_subranges"""

    # single day subrange
    assert list(
        _iterate_over_interval_date_subranges(
            date.fromisoformat("2023-10-01"),
            date.fromisoformat("2023-10-01"),
        )
    ) == [
        (
            date.fromisoformat("2023-10-01"),
            date.fromisoformat("2023-10-01"),
        )
    ]

    # full subrange
    assert list(
        _iterate_over_interval_date_subranges(
            date.fromisoformat("2023-10-01"),
            date.fromisoformat("2023-10-30"),
        )
    ) == [
        (
            date.fromisoformat("2023-10-01"),
            date.fromisoformat("2023-10-30"),
        )
    ]

    # one full plus one partial subrange
    assert list(
        _iterate_over_interval_date_subranges(
            date.fromisoformat("2023-10-01"),
            date.fromisoformat("2023-10-31"),
        )
    ) == [
        (
            date.fromisoformat("2023-10-01"),
            date.fromisoformat("2023-10-30"),
        ),
        (
            date.fromisoformat("2023-10-31"),
            date.fromisoformat("2023-10-31"),
        ),
    ]

    # two full plus one partial subrange
    assert list(
        _iterate_over_interval_date_subranges(
            date.fromisoformat("2023-10-01"),
            date.fromisoformat("2023-12-20"),
        )
    ) == [
        (
            date.fromisoformat("2023-10-01"),
            date.fromisoformat("2023-10-30"),
        ),
        (
            date.fromisoformat("2023-10-31"),
            date.fromisoformat("2023-11-29"),
        ),
        (
            date.fromisoformat("2023-11-30"),
            date.fromisoformat("2023-12-20"),
        ),
    ]

    # assert all subintervals are at or below our limit on number of days per request
    # when we request a large range of data
    assert all(
        len(i) <= MAX_DAYS_PER_REQUEST
        for i in _iterate_over_interval_date_subranges(
            date.fromisoformat("2021-01-01"),
            date.fromisoformat("2023-01-01"),
        )
    )


def test_parsing(example_interval_response):
    """unit test to verify _parse_response_for_daily_record_dicts"""
    # parse our example response dictionary
    results = _parse_response_for_daily_record_dicts(example_interval_response)

    valiate_example_interval_response(results)

    with pytest.raises(KeyError):
        _parse_response_for_daily_record_dicts({})

    with pytest.raises(KeyError):
        _parse_response_for_daily_record_dicts({"data": {}})


def test_request_interval_data_over_date_range(example_interval_response):
    """unit test to verify _request_interval_data_over_date_range"""

    with requests_mock.Mocker() as m:
        m.post(INTERVAL_URL, json=example_interval_response)

        # test nominal case
        assert (
            _request_interval_data_over_date_range(
                date.fromisoformat("2019-07-01"),
                date.fromisoformat("2019-07-03"),
                "faketoken",
                esid="1008901012126195372100",
            )
            == example_interval_response
        )

        # test that bad/invalid  arguments throw value errors
        with pytest.raises(ValueError):
            _request_interval_data_over_date_range(
                date.fromisoformat("2019-07-01"),
                date.fromisoformat("2019-05-01"),  # end date is before start_date
                "faketoken",
                esid="1008901012126195372100",
            )

        with pytest.raises(ValueError):
            _request_interval_data_over_date_range(
                date.fromisoformat("2019-07-01"),
                date.today() + timedelta(days=3),  # end_date in future
                "faketoken",
                esid="1008901012126195372100",
            )

        with pytest.raises(ValueError):
            _request_interval_data_over_date_range(
                date.fromisoformat("2019-12-01"),
                date.fromisoformat("2019-12-31"),  # end_date is more than limit in days
                "faketoken",
                esid="1008901012126195372100",
            )

    with requests_mock.Mocker() as m:
        m.post(INTERVAL_URL, status_code=401, json=example_interval_response)

        with pytest.raises(HTTPError):
            result = _request_interval_data_over_date_range(
                date.fromisoformat("2019-07-01"),
                date.fromisoformat("2019-07-03"),
                "faketoken",
                esid="1008901012126195372100",
            )


def test_get_interval_data_over_date_range(example_interval_response):
    """end-to-end test to verify get_interval_data_over_date_range"""
    with requests_mock.Mocker() as m:
        # define response to post request to interval url
        m.post(INTERVAL_URL, json=example_interval_response)

        results = get_interval_data_over_date_range(
            date.fromisoformat("2019-07-01"),
            date.fromisoformat("2019-07-03"),
            "faketoken",
            esid="1008901012126195372100",
        )

    valiate_example_interval_response(results)


def test_get_interval_dataframe_over_date_range(example_interval_response):
    """end-to-end test to verify get_interval_dataframe_over_date_range"""
    assert _check_for_pandas()

    import pandas as pd

    with requests_mock.Mocker() as m:
        # define response to post request to interval url
        m.post(INTERVAL_URL, json=example_interval_response)

        results_df = get_interval_dataframe_over_date_range(
            date.fromisoformat("2019-07-01"),
            date.fromisoformat("2019-07-03"),
            "faketoken",
            esid="1008901012126195372100",
        )

    # assert we got a pandas DataFrame object
    assert isinstance(results_df, pd.DataFrame)

    # assert we have expected number of rows for 3 days of entries
    assert results_df.shape == (96 * 3, 4)

    # assert the dataframe has all of the column names we expect
    assert set(results_df.columns) == set(
        [
            "start_timestamp",
            "end_timestamp",
            "net_consumed_kwh",
            "net_generated_kwh",
        ]
    )
