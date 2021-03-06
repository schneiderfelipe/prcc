#!/usr/bin/python3

import os
import re
import time
import logging
import datetime
from timeit import default_timer as timer
from functools import lru_cache
from collections import OrderedDict

import numpy as np
import pandas as pd
import pandas_datareader
import pandas_datareader.data as web
import pystore
import unidecode
import requests_cache

logging.getLogger(__name__).addHandler(logging.NullHandler())

storage_path = os.path.expanduser("~/.prcc")
pystore.set_path(storage_path)
collection = pystore.store("data").collection("all")

requests_cache.core.install_cache(
    os.path.join(storage_path, "cache"), "sqlite", expire_after=86400
)

_last_api_call = 0.0
_b3_indices = {
    # Índices Amplos
    "ibovespa": "IBOV",
    "ibrx100": "IBRX",
    "ibrx50": "IBXL",
    "ibra": "IBRA",
    # Índices de Governança
    "igcx": "IGCX",
    "itag": "ITAG",
    "igct": "IGCT",
    "igc-nm": "IGNM",
    # Índices de Segmento
    "idiv": "IDIV",
    "mlcx": "MLCX",
    "smll": "SMLL",
    "ivbx2": "IVBX",
    # Índices de Sustentabilidade
    "ico2": "ICO2",
    "ise": "ISEE",
    # Índices Setoriais
    "ifnc": "IFNC",
    "imob": "IMOB",
    "util": "UTIL",
    "icon": "ICON",
    "iee": "IEEX",
    "imat": "IMAT",
    "indx": "INDX",
    # Outros Índices
    "bdrx": "BDRX",
    "ifix": "IFIX",
}


def extract_datareader(tickers, data_source="av-daily-adjusted", pause=None):
    """
    Retrieve daily data with web.DataReader.

    Parameters
    ----------
    tickers : str or list of strs
        Ticker or tickers to extract
    data_source : str, optional
        The data source ("quandl", "av-daily-adjusted", "iex", "fred", "ff", etc.)
    pause : float, optional
        Time, in seconds, to pause between consecutive queries of chunks

    Warns
    -----
    On remote data error or invalid ticker. If using AlphaVantage API, extra
    pause is made in those cases (see notes).

    Yields
    ------
    ticker : str
        Short name for the extract data
    data : dataframe
        A dataframe with all extract data
    metadata : dict
        Extra information about the extract data

    Notes
    -----
    Index is forced to datetime.

    Attempt is made not to go over AlphaVantage API limits (maximum of five
    calls per minute for non-premium accounts). This means that some pauses
    are made, but they are not on the conservative side so be aware of remote
    data errors and try again latter.

    Examples
    --------
    >>> for ticker, data, metadata in extract_datareader("^BVSP"):
    ...     print(ticker, metadata)
    ^BVSP {'price_column': 'adjusted close'}
    >>> for ticker, data, metadata in extract_datareader(["PETR4.SAO", "ITUB4.SAO"], pause=1.0):
    ...     print(ticker, metadata)
    PETR4.SAO {'price_column': 'adjusted close'}
    ITUB4.SAO {'price_column': 'adjusted close'}
    >>> for ticker, data, metadata in extract_datareader("BCB/11", data_source="quandl"):
    ...     print(ticker, metadata)
    BCB/11 {'price_column': 'Value'}

    """
    global _last_api_call

    if isinstance(tickers, str):
        tickers = [tickers]

    if pause is None:
        if data_source == "av-daily-adjusted":
            pause = 12.0  # max. 5 calls per minute for non-premium accounts
        else:
            pause = 1.0
    extra_pause = 0.25 * pause

    if data_source == "quandl":
        metadata = {"price_column": "Value"}
    else:
        metadata = {"price_column": "adjusted close"}
    for ticker in tickers:
        end = timer()
        if end - _last_api_call > pause:
            _last_api_call = end
        else:
            time_interval = end - _last_api_call + np.random.uniform(0.0, extra_pause)
            logging.info(f"Waiting {time_interval:.3f} seconds.")

            time.sleep(time_interval)
            _last_api_call = timer()

        logging.info(f"Attempting to retrieve {ticker}.")

        try:
            # TODO: support start_date and end_date
            data = web.DataReader(ticker, data_source)
        except ValueError as e:
            logging.warning(e)

            time_interval = np.random.uniform(0.0, 2.0 * pause)
            logging.info(f"Waiting {time_interval:.3f} extra seconds.")

            time.sleep(time_interval)
            continue
        except pandas_datareader._utils.RemoteDataError as e:
            logging.warning(f"Remote data error{e} for {ticker}.")

            time_interval = np.random.uniform(0.0, 2.0 * pause)
            logging.info(f"Waiting {time_interval:.3f} extra seconds.")

            time.sleep(time_interval)
            requests_cache.get_cache().remove_old_entries(datetime.datetime.now())
            continue
        data.index = pd.to_datetime(data.index)

        yield ticker, data, metadata


@lru_cache(maxsize=32)
def extract_infofundos(io):
    """
    Extract data from a Excel file in InfoFundos format.

    Only variable data is returned, i.e. constant columns are removed and
    stored as metadata (see examples).

    Index is forced to datetime.

    Parameters
    ----------
    io : str, file descriptor, pathlib.Path, ExcelFile or xlrd.Book
        Excel source for extraction

    Yields
    ------
    item : str
        Short name for the extracted data
    data : dataframe
        A dataframe with all data extracted
    metadata : dict
        Extra information about the extracted data

    Examples
    --------
    >>> for item, data, metadata in extract_infofundos("examples/cotas1561656396620.xlsx"):
    ...     print(item, metadata)
    BRASIL PLURAL DEBENTURES INCENTIVADAS 45 {'price_column': 'Cota', 'code': 35785, 'description': 'CREDITO PRIVADO FUNDO DE INVESTIMENTO EM COTAS DE FUNDO DE INVESTIMENTO MULTIMERCADO'}
    CA INDOSUEZ VITESSE {'price_column': 'Cota', 'code': 13228, 'description': 'FUNDO DE INVESTIMENTO RENDA FIXA CREDITO PRIVADO'}
    DEVANT DEBENTURES INCENTIVADAS {'price_column': 'Cota', 'code': 40284, 'description': 'FUNDO DE INVESTIMENTO RENDA FIXA CREDITO PRIVADO'}
    TARPON GT {'price_column': 'Cota', 'code': 34259, 'description': 'FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO EM ACOES'}

    """
    dataframe = pd.read_excel(io, sheet_name="Cotas", index_col=2, skipfooter=1).dropna(
        axis=0, how="all"
    )

    spaces_pattern = re.compile(r"\s+")
    split_pattern = re.compile(r"FUNDO|CREDITO")
    for item, group in dataframe.groupby("Fundo"):
        logging.info(f"Extracting {item}.")

        # https://stackoverflow.com/a/20210048/4039050
        data = group.loc[:, (group != group.iloc[0]).any()]
        data.index = pd.to_datetime(data.index)

        # https://stackoverflow.com/a/2633310/4039050
        item = unidecode.unidecode(item)
        # https://stackoverflow.com/a/1546244/4039050
        item = spaces_pattern.sub(" ", item)

        item = item.replace(" FI ", " FUNDO DE INVESTIMENTO ")
        item = item.replace(
            "FUNDO DE INVESTIMENTO EM ACOES-BDR NIVEL I",
            "BDR NIVEL I FUNDO DE INVESTIMENTO EM ACOES",
        )

        logging.info(f"Expanded name to {item}.")

        match = split_pattern.search(item)
        if match:
            pos = match.start()
            item, description = item[:pos].strip(), item[pos:].strip()
        else:
            description = ""

        metadata = {
            "price_column": "Cota",
            "code": int(group["Código"].iloc[-1]),
            "description": description,
        }
        yield item, data, metadata


def import_objects(objects, source, overwrite=True, *args, **kwargs):
    """
    Store objects using PyStore.

    Parameters
    ----------
    objects : str or list of strs
        List of items to extract and store
    source : str
        Source key for extraction (see examples below)
    overwrite : bool, optional
        Whether to overwrite existing data

    Notes
    -----
    Values are tested against the database before updating. Data already
    up-to-dated is not downloaded.

    Objects can be an index name, in which case all tickers in the index are
    considered.

    Extra arguments are passed directly to the extraction function, which
    depends on source.

    Examples
    --------
    >>> import_objects(["examples/cotas1561656396620.xlsx"], "infofundos")
    >>> item = collection.item("TARPON GT")
    >>> item.metadata["code"]
    34259
    >>> item.tail()  # doctest: +NORMALIZE_WHITESPACE
                    Cota  Variação    Captação   Resgate           PL  Cotistas
    Data
    2019-06-18  6.778209  0.002867   390487.16      0.00  80448095.55    2710.0
    2019-06-19  6.907007  0.019002  1058349.86      0.00  81976755.80    2775.0
    2019-06-21  6.892222 -0.002141  1058349.86  43026.86  83583126.05    2845.0
    2019-06-24  6.926969  0.005042   365520.69  40098.89  84329940.18    2917.0
    2019-06-25  6.845344 -0.011784   652833.24      0.00  83989058.53    2998.0

    >>> import_objects("PETR4.SAO", "av-daily-adjusted")
    >>> item = collection.item("PETR4.SAO").to_pandas().truncate(after="2019-07-12")
    >>> item.tail()  # doctest: +NORMALIZE_WHITESPACE
                 open   high    low  close  adjusted close    volume  dividend amount  split coefficient
    index
    2019-07-05  27.27  27.59  27.13  27.40           27.40  27414700              0.0                1.0
    2019-07-08  27.50  27.72  27.44  27.65           27.65  25318200              0.0                1.0
    2019-07-10  28.00  28.27  27.97  28.07           28.07  50715800              0.0                1.0
    2019-07-11  28.20  28.51  28.16  28.40           28.40  48206900              0.0                1.0
    2019-07-12  28.54  28.74  28.41  28.53           28.53  40908900              0.0                1.0

    Importing tickers from indices:

    >>> import_objects("ICO2", "av-daily-adjusted", pause=1.0)
    >>> import_objects(["PETR4.SAO", "ICO2"], "av-daily-adjusted", pause=1.0)

    Importing tickers from Quandl:

    >>> import_objects("BCB/11", "quandl")
    >>> item = collection.item("BCB/11").to_pandas().truncate(after="2019-07-23")
    >>> item.tail()  # doctest: +NORMALIZE_WHITESPACE
                  Value
    Date
    2019-07-17  0.02462
    2019-07-18  0.02462
    2019-07-19  0.02462
    2019-07-22  0.02462
    2019-07-23  0.02462

    """
    if isinstance(objects, str):
        objects = [objects]

    if source == "infofundos":
        extract_func = lambda s: extract_infofundos(s, *args, **kwargs)
    else:
        extract_func = lambda s: extract_datareader(s, source, *args, **kwargs)

    available_items = set(collection.list_items())
    for obj in objects:
        if obj.lower() in _b3_indices:
            import_objects(get_index(obj), source, overwrite, *args, **kwargs)
            continue

        if source != "infofundos" and obj in available_items:
            # here, obj == item
            old_item = collection.item(obj)
            old_data = old_item.to_pandas()

            if old_data.index[-1].date() == datetime.datetime.today().date():
                logging.info(f"{obj} is already up-to-date.")

                continue

        for item, data, metadata in extract_func(obj):
            logging.info(f"Importing {item}.")

            if source != "infofundos" and item in available_items:
                logging.info(f"Updating old data for {item}.")

                old_metadata = old_item.metadata
                old_metadata.update(metadata)
                metadata = old_metadata

                data = pd.concat([old_data[~old_data.index.isin(data.index)], data])

            collection.write(item, data, metadata=metadata, overwrite=overwrite)


def export_objects(objects):
    """
    Retrieve prices for objects from PyStore.

    Parameters
    ----------
    objects : str or list of strs
        Item names to export

    Returns
    -------
    data : dataframe
        Daily price data with as many columns as objects

    Warns
    -----
    On file-not-found errors.

    Notes
    -----
    Prices equal zero are considered data error and are assigned `numpy.nan`.

    Objects can be an index name, in which case all tickers in the index are
    considered.

    Examples
    --------
    >>> data = export_objects(["CA INDOSUEZ VITESSE", "TARPON GT"])
    >>> data.tail()  # doctest: +NORMALIZE_WHITESPACE
                CA INDOSUEZ VITESSE  TARPON GT
    Data
    2019-06-18            22.420314   6.778209
    2019-06-19            22.426387   6.907007
    2019-06-21            22.431954   6.892222
    2019-06-24            22.437576   6.926969
    2019-06-25            22.443196   6.845344
    >>> data = export_objects("PETR4.SAO").truncate(after="2019-07-12")
    >>> data.tail()  # doctest: +NORMALIZE_WHITESPACE
                PETR4.SAO
    index
    2019-07-05      27.40
    2019-07-08      27.65
    2019-07-10      28.07
    2019-07-11      28.40
    2019-07-12      28.53
    >>> data = export_objects(["PETR4.SAO", "TARPON GT"]).truncate(after="2019-07-12")
    >>> data.tail(20)
                PETR4.SAO  TARPON GT
    2019-06-13      27.18   6.736261
    2019-06-14      27.06   6.782966
    2019-06-17      27.11   6.758832
    2019-06-18      27.45   6.778209
    2019-06-19      27.52   6.907007
    2019-06-21      28.28   6.892222
    2019-06-24      28.25   6.926969
    2019-06-25      27.51   6.845344
    2019-06-26      27.67        NaN
    2019-06-27      27.23        NaN
    2019-06-28      27.41        NaN
    2019-07-01      27.26        NaN
    2019-07-02      26.82        NaN
    2019-07-03      27.18        NaN
    2019-07-04      27.39        NaN
    2019-07-05      27.40        NaN
    2019-07-08      27.65        NaN
    2019-07-10      28.07        NaN
    2019-07-11      28.40        NaN
    2019-07-12      28.53        NaN

    Exporting from Quandl:

    >>> data = export_objects("BCB/11").truncate(after="2019-07-23")
    >>> data.tail()  # doctest: +NORMALIZE_WHITESPACE
                 BCB/11
    Date
    2019-07-17  0.02462
    2019-07-18  0.02462
    2019-07-19  0.02462
    2019-07-22  0.02462
    2019-07-23  0.02462

    Exporting tickers from indices:

    >>> data = export_objects("ICO2")
    >>> len(data.columns)  # 29 if KLBN11.SAO had data available
    28
    >>> data = export_objects(["KROT3.SAO", "ICO2"]).truncate(after="2019-07-19")
    >>> len(data.columns)
    29
    >>> "KROT3.SAO" in data.columns and "ITUB4.SAO" in data.columns
    True
    >>> data.tail()  # doctest: +ELLIPSIS
                KROT3.SAO  ITUB4.SAO  BBDC4.SAO  ...
    index                                        ...
    2019-07-15      11.66      36.36      37.84  ...
    2019-07-16      11.75      36.50      37.85  ...
    2019-07-17      12.00      36.40      37.65  ...
    2019-07-18      12.60      37.37      38.47  ...
    2019-07-19      12.60      36.40      37.60  ...
    <BLANKLINE>
    [5 rows x 29 columns]

    When exporting indices, ticker redundancy is avoided:

    >>> data = export_objects(["PETR4.SAO", "ICO2", "CIEL3.SAO"])  # Both PETR4.SAO and CIEL3.SAO are already in ICO2
    >>> len(data.columns)
    28

    """
    if isinstance(objects, str):
        objects = [objects]

    prices = []
    columns = []
    for obj in objects:
        if obj.lower() in _b3_indices:
            index_data = export_objects(
                [ticker for ticker in get_index(obj) if ticker not in columns]
            )
            prices.append(index_data)

            columns.extend(index_data.columns)
            continue
        elif obj in columns:
            continue

        logging.info(f"Exporting {obj}.")

        try:
            item = collection.item(obj)
        except FileNotFoundError as e:
            logging.warning(e)

            continue
        prices.append(item.to_pandas()[item.metadata["price_column"]])
        columns.append(obj)

    data = pd.concat(prices, axis="columns").replace(
        0.0, np.nan
    )  # "there's no free lunch"
    data.columns = columns

    return data


@lru_cache(maxsize=32)
def get_index(name):
    """
    Get a list of tickers for an index.

    Parameters
    ----------
    name : str
        Index name. See notes for supported indices

    Returns
    -------
    tickers : list of strs
        Tickers in the requested index

    Notes
    -----
    The following indices are available (case-insensitive):

    - Índices Amplos
      - Ibovespa
      - IBrX100
      - IBrX50
      - IBrA
    - Índices de Governança
      - IGCX
      - ITAG
      - IGCT
      - IGC-NM
    - Índices de Segmento
      - IDIV
      - MLCX
      - SMLL
      - IVBX2
    - Índices de Sustentabilidade
      - ICO2
      - ISE
    - Índices Setoriais
      - IFNC
      - IMOB
      - UTIL
      - ICON
      - IEE
      - IMAT
      - INDX
    - Outros Índices
      - BDRX
      - IFIX


    Examples
    --------
    >>> get_index("ibovespa")[:5]
    ['ITUB4.SAO', 'VALE3.SAO', 'BBDC4.SAO', 'PETR4.SAO', 'B3SA3.SAO']
    >>> get_index("bdrx")[:5]
    ['AAPL34.SAO', 'MSFT34.SAO', 'AMZO34.SAO', 'FBOK34.SAO', 'VISA34.SAO']
    >>> len(get_index("ibrx100"))
    100
    """
    # A tabela real se encontra dentro de um frame (url abaixo) do link indicado acima.
    url = f"http://bvmf.bmfbovespa.com.br/indices/ResumoCarteiraTeorica.aspx?Indice={_b3_indices[name.lower()]}&idioma=pt-br"

    dataframe = (
        pd.read_html(url, decimal=",", thousands=".", index_col=0)[0]
        .iloc[:-1]
        .sort_values("Part. (%)", ascending=False, kind="mergesort")
    )
    dataframe.index = dataframe.index + ".SAO"
    dataframe["Part. (%)"] /= 100.0

    return list(dataframe.index)
