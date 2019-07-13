#!/usr/bin/python3

import os
import re
from collections import OrderedDict

import numpy as np
import pandas as pd
import pystore

storage_path = os.path.expanduser("~/.prcc")
pystore.set_path(storage_path)
collection = pystore.store("data").collection("all")


def extract_infofundos(io):
    """
    Extract data from a Excel file in InfoFundos format.

    Only variable data is returned, i.e. constant columns are removed and
    stored as metadata (see examples).

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
    dataframe = pd.read_excel(
        io, sheet_name="Cotas", index_col=2, skip_footer=1
    ).dropna(axis=0, how="all")

    pattern = re.compile(r"FUNDO|CRÉDITO")
    for item, group in dataframe.groupby("Fundo"):
        # https://stackoverflow.com/a/20210048/4039050
        data = group.loc[:, (group != group.iloc[0]).any()]

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


def import_objects(objects, source, overwrite=True):
    """
    Store objects using PyStore.

    Parameters
    ----------
    objects : list of str
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
    >>> item.head()  # doctest: +NORMALIZE_WHITESPACE
                    Cota  Variação  Captação  Resgate          PL  Cotistas
    Data
    2015-08-04  2.258242       NaN       0.0      0.0  9502613.39      10.0
    2015-08-05  2.244789 -0.005957       0.0      0.0  9446004.04      10.0
    2015-08-06  2.224238 -0.009155       0.0      0.0  9359525.55      10.0
    2015-08-07  2.193907 -0.013637       0.0      0.0  9231892.92      10.0
    2015-08-10  2.193513 -0.000180       0.0      0.0  9230235.39      10.0
    >>> item = collection.item("CA INDOSUEZ VITESSE")
    >>> item.metadata["code"]
    13228
    >>> item.head()  # doctest: +NORMALIZE_WHITESPACE
                     Cota  Variação     Captação  Resgate           PL  Cotistas
    Data
    2011-08-16  10.000000       NaN  18600000.00      0.0  18600000.00       3.0
    2011-08-17  10.004863  0.000486  17608433.86      0.0  36217480.54      10.0
    2011-08-18  10.009643  0.000478    600016.53      0.0  36834800.68      12.0
    2011-08-19  10.014469  0.000482         0.00      0.0  36852558.51      12.0
    2011-08-22  10.019299  0.000482    300000.00      0.0  37170334.04      13.0

    """
    if source == "infofundos":
        extract_func = extract_infofundos

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
    objects : list of str
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
    >>> data.tail()
                CA INDOSUEZ VITESSE  TARPON GT
    Data                                      
    2019-06-18            22.420314   6.778209
    2019-06-19            22.426387   6.907007
    2019-06-21            22.431954   6.892222
    2019-06-24            22.437576   6.926969
    2019-06-25            22.443196   6.845344

    """
    prices = []
    for obj in objects:
        item = collection.item(obj)
        prices.append(item.to_pandas()[item.metadata["price_column"]])

    data = pd.concat(prices, axis="columns").replace(
        0.0, np.nan
    )  # "there's no free lunch"
    data.columns = objects

    return data
