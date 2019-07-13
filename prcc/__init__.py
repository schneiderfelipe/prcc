#!/usr/bin/python3

import os
import re
from collections import OrderedDict

import numpy as np
import pandas as pd
import pandas_datareader.data as web
import pystore
import requests_cache

storage_path = os.path.expanduser("~/.prcc")
pystore.set_path(storage_path)
collection = pystore.store("data").collection("all")
requests_cache.core.install_cache(os.path.join(storage_path, "cache"), "sqlite")


# TODO: make an example with Quandl
def extract_datareader(objects, data_source="av-daily-adjusted"):
    """
    Retrieve daily data with web.DataReader.

    Index is forced to datetime.

    Parameters
    ----------
    objects : str or list of strs
        Object or objects to extract
    data_source : str, optional
        The data source ("quandl", "av-daily-adjusted", "iex", "fred", "ff", etc.)

    Yields
    ------
    item : str
        Name for the extract data
    data : dataframe
        A dataframe with all extract data
    metadata : dict
        Extra information about the extract data

    Examples
    --------
    >>> for item, data, metadata in extract_datareader("^BVSP"):
    ...     print(item, metadata)
    ^BVSP {'price_column': 'adjusted close'}
    >>> for item, data, metadata in extract_datareader(["PETR4.SAO", "ITUB4.SAO"]):
    ...     print(item, metadata)
    PETR4.SAO {'price_column': 'adjusted close'}
    ITUB4.SAO {'price_column': 'adjusted close'}

    """
    if isinstance(objects, str):
        objects = [objects]

    metadata = {"price_column": "adjusted close"}
    for item in objects:
        # TODO: support start and end dates
        data = web.DataReader(item, data_source)
        data.index = pd.to_datetime(data.index)

        yield item, data, metadata


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
        Name for the extracted data
    data : dataframe
        A dataframe with all data extracted
    metadata : dict
        Extra information about the extracted data

    Examples
    --------
    >>> for item, data, metadata in extract_infofundos("examples/cotas1561656396620.xlsx"):
    ...     print(item, metadata)
    BRASIL PLURAL DEBÊNTURES INCENTIVADAS 45 {'price_column': 'Cota', 'code': 35785, 'description': 'CRÉDITO PRIVADO FI EM COTAS DE FI MULTIMERCADO'}
    CA INDOSUEZ VITESSE {'price_column': 'Cota', 'code': 13228, 'description': 'FUNDO DE INVESTIMENTO RENDA FIXA CRÉDITO PRIVADO'}
    DEVANT DEBÊNTURES INCENTIVADAS {'price_column': 'Cota', 'code': 40284, 'description': 'FUNDO DE INVESTIMENTO RENDA FIXA CRÉDITO PRIVADO'}
    TARPON GT {'price_column': 'Cota', 'code': 34259, 'description': 'FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO EM AÇÕES'}

    """
    dataframe = pd.read_excel(io, sheet_name="Cotas", index_col=2, skipfooter=1).dropna(
        axis=0, how="all"
    )

    pattern = re.compile(r"FUNDO|CRÉDITO")
    for item, group in dataframe.groupby("Fundo"):
        # https://stackoverflow.com/a/20210048/4039050
        data = group.loc[:, (group != group.iloc[0]).any()]
        data.index = pd.to_datetime(data.index)

        match = pattern.search(item)
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


# TODO: make an example with Quandl
def import_objects(objects, source, overwrite=True):
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
    >>> item = collection.item("PETR4.SAO")
    >>> item.tail()  # doctest: +NORMALIZE_WHITESPACE
                 open   high    low  close  adjusted close    volume  dividend amount  split coefficient
    index
    2019-07-05  27.27  27.59  27.13  27.40           27.40  27414700              0.0                1.0
    2019-07-08  27.50  27.72  27.44  27.65           27.65  25318200              0.0                1.0
    2019-07-10  28.00  28.27  27.97  28.07           28.07  50715800              0.0                1.0
    2019-07-11  28.20  28.51  28.16  28.40           28.40  48206900              0.0                1.0
    2019-07-12  28.54  28.74  28.41  28.63           28.63  37796100              0.0                1.0

    """
    if isinstance(objects, str):
        objects = [objects]

    if source == "infofundos":
        extract_func = extract_infofundos
    else:
        extract_func = lambda s: extract_datareader(s, source)

    for obj in objects:
        for item, data, metadata in extract_func(obj):
            # TODO: update data instead of simply overwrite
            # if item exists:
            #     metadata = current_metadata.update(metadata)
            #     data = current_data.update(data)
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

    Notes
    -----
    Prices equal zero are considered data error and are assigned `numpy.nan`.

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
    >>> data = export_objects("PETR4.SAO")
    >>> data.tail()  # doctest: +NORMALIZE_WHITESPACE
                PETR4.SAO
    index
    2019-07-05      27.40
    2019-07-08      27.65
    2019-07-10      28.07
    2019-07-11      28.40
    2019-07-12      28.63
    >>> data = export_objects(["PETR4.SAO", "TARPON GT"])
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
    2019-07-12      28.63        NaN

    """
    if isinstance(objects, str):
        objects = [objects]

    prices = []
    for obj in objects:
        item = collection.item(obj)
        prices.append(item.to_pandas()[item.metadata["price_column"]])

    data = pd.concat(prices, axis="columns").replace(
        0.0, np.nan
    )  # "there's no free lunch"
    data.columns = objects

    return data
