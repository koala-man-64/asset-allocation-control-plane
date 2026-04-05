"""
Utility functions for working with Alpha Vantage API responses.

Alpha Vantage returns JSON objects with nested structures for time
series data and financial reports.  The helpers in this module
transform those raw responses into more convenient pandas
``DataFrame`` objects.  They also provide basic merging of new data
with existing datasets so that incremental updates can be applied
without re‑fetching the entire history.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd


def _extract_series_key(response_json: Dict[str, Any]) -> Optional[str]:
    """Find the key in a response that contains time series data.

    Alpha Vantage uses different naming conventions for various
    endpoints.  For example, daily stock prices are under
    ``"Time Series (Daily)"``, FX data under ``"Time Series FX (Daily)"``,
    crypto currency data under ``"Time Series (Digital Currency Daily)"``,
    and technical indicators under ``"Technical Analysis: SMA"``.  This
    helper searches for the first key that appears to contain series
    values.

    Parameters
    ----------
    response_json : dict
        The JSON object returned from the API call.

    Returns
    -------
    Optional[str]
        The name of the series key if found, otherwise ``None``.
    """
    for key in response_json.keys():
        if "Series" in key or "Analysis" in key:
            return key
    return None


def parse_time_series(response_json: Dict[str, Any]) -> pd.DataFrame:
    """Convert a time series JSON response into a pandas DataFrame.

    The DataFrame will have a datetime index named ``timestamp`` and
    columns corresponding to each price or indicator in the series.
    Column names are derived by removing numeric prefixes such as
    ``"1. open"`` -> ``"open"``.  Values are converted to floats
    whenever possible.  The index is sorted in ascending order.

    Parameters
    ----------
    response_json : dict
        Raw JSON returned by Alpha Vantage for a time series endpoint.

    Returns
    -------
    pandas.DataFrame
        A DataFrame indexed by timestamp with one column per field.

    Raises
    ------
    ValueError
        If no time series key can be found in the response.
    """
    series_key = _extract_series_key(response_json)
    if not series_key:
        raise ValueError("No time series data found in response")

    series_data = response_json[series_key]
    records: List[Dict[str, Any]] = []
    for timestamp, values in series_data.items():
        record: Dict[str, Any] = {"timestamp": pd.to_datetime(timestamp)}
        for key, value in values.items():
            # Remove numeric prefix "1. open" -> "open"
            col_name = key.split(". ", 1)[-1]
            # Attempt to convert to float; leave string if not numeric
            try:
                record[col_name] = float(value)
            except (TypeError, ValueError):
                record[col_name] = value
        records.append(record)

    df = pd.DataFrame.from_records(records)
    df.sort_values(by="timestamp", inplace=True)
    df.set_index("timestamp", inplace=True)
    return df


def parse_financial_reports(response_json: Dict[str, Any], report_type: str = "annualReports") -> pd.DataFrame:
    """Parse a financial statement response into a DataFrame.

    Company fundamentals such as income statements, balance sheets and
    cash flow statements are returned as lists under ``annualReports``
    and ``quarterlyReports`` keys.  This helper converts one of those
    lists into a DataFrame indexed by fiscal date ending.

    Parameters
    ----------
    response_json : dict
        Raw JSON returned by Alpha Vantage for a financial statement
        endpoint.

    report_type : {'annualReports', 'quarterlyReports'}, optional
        Which set of reports to parse.  Defaults to
        ``'annualReports'``.

    Returns
    -------
    pandas.DataFrame
        A DataFrame where each row represents a single report and the
        index is ``fiscalDateEnding``.

    Raises
    ------
    ValueError
        If the specified report type is not present in the response.
    """
    if report_type not in response_json:
        raise ValueError(f"Report type '{report_type}' not found in response")
    reports: List[Dict[str, Any]] = response_json[report_type]
    if not reports:
        return pd.DataFrame()
    df = pd.DataFrame.from_records(reports)
    if "fiscalDateEnding" in df.columns:
        df["fiscalDateEnding"] = pd.to_datetime(df["fiscalDateEnding"])
        df.set_index("fiscalDateEnding", inplace=True)
    return df


def merge_time_series(existing: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
    """Merge new time series rows into an existing DataFrame.

    This function concatenates two DataFrames, drops duplicate
    timestamps and sorts the result.  It is useful when updating
    historical data with newly fetched points: simply read your
    existing dataset, parse the latest response with
    :func:`parse_time_series` and call this function to obtain a
    unified DataFrame.  The original input objects are not modified.

    Parameters
    ----------
    existing : pandas.DataFrame
        The DataFrame containing previously stored data.  The index
        must be a ``DatetimeIndex``.

    new_data : pandas.DataFrame
        The newly fetched data to merge.  The index must also be a
        ``DatetimeIndex``.

    Returns
    -------
    pandas.DataFrame
        A new DataFrame with all unique rows from ``existing`` and
        ``new_data``, sorted by the index.
    """
    combined = pd.concat([existing, new_data])
    # Drop duplicate index entries, keeping the last occurrence
    combined = combined[~combined.index.duplicated(keep="last")]
    combined.sort_index(inplace=True)
    return combined