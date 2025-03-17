import json
from data import generate_dataset
from operator import ne
from urllib import response
import polars as pl
import pandas as pd
import numpy as np
import os
import logging
import requests
import sys
# from tqmd import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO)

# Add the `src` folder to Python's search path
src_path = "/Users/bradkittrell/Projects/imdb/IMDB_Fun/src"
sys.path.append(src_path)

with open("/Users/bradkittrell/Projects/imdb/IMDB_Fun/src/data/api_keys.json", "r") as json_file:
    config = json.load(json_file)

omdb_key = config['OMDB_API_KEY']


def normalized_yearly_directors(start_year: int, end_year: int, director_ids: list[str] = None):
    """_summary_

    Args:
        start_year (int): _description_
        end_year (int): _description_
        director_ids (list[str], optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """

    names = generate_dataset.load_names(None)
    names = names.filter((pl.col("nconst").is_in(director_ids)))

    directors = generate_dataset.directors_greater_n_films(3)

    data = pl.DataFrame({
        "director": names['primaryName'].to_list(),
        "nconst": names['nconst'].to_list(),
        "deathYear": names['deathYear'].to_list(),
        "birthYear": names['birthYear'].to_list(),
        "year": [list(range(start_year, end_year + 1))] * len(director_ids),
    })
    data = data.explode("year")
    # drop locations where
    data = data.join(directors, left_on="nconst",
                     right_on="nconst", how="left")
    data = data.filter((pl.col("year") >= pl.col(
        "first_year")) & (pl.col("year") <= pl.col("last_year")))
    return data
