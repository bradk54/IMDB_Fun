from operator import ne
from urllib import response
import polars as pl
import pandas as pd
import numpy as np
import os
import logging
import requests
# from tqmd import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO)

## ** Load Data Functions **##


def load_names(path: str) -> pl.DataFrame:
    """
    Load the names dataset from the processed data folder.

    Parameters:
    path (str): The file path to the names dataset.

    Returns:
    pl.DataFrame: A DataFrame containing the names data.
    """
    if not path:
        # If no path is provided, use the default static path
        path = "/Users/bradkittrell/Projects/imdb/IMDB_Fun/Data/processed/name.parquet"
        logging.info(f"No path provided. Using default path: {path}")

    if not os.path.exists(path):
        logging.error(f"File not found: {path}")
        raise FileNotFoundError(f"File not found: {path}")

    try:
        # Read the parquet file from the path
        names = pl.read_parquet(path)
        logging.info(f"Successfully loaded data from {path}")
    except Exception as e:
        logging.error(f"Error reading the parquet file: {e}")
        raise

    # Filter the DataFrame to include only rows where birthYear is greater than 1899
    names = names.filter(pl.col("birthYear") > 1899)
    logging.info(
        "Filtered names data to include only rows where birthYear > 1899")

    if "__index_level_0__" in names.columns:
        names = names.drop("__index_level_0__")
        logging.info("Dropped __index_level_0__ column")

    return names


def load_title_basics(path: str) -> pl.DataFrame:
    """
    Load the title basics dataset from the processed data folder.

    Parameters:
    path (str): The file path to the title basics dataset.

    Returns:
    pl.DataFrame: A DataFrame containing the title basics data.
    """
    if not path:
        # If no path is provided, use the default static path
        path = "/Users/bradkittrell/Projects/imdb/IMDB_Fun/Data/processed/title_basics_movies.parquet"
        logging.info(f"No path provided. Using default path: {path}")

    if not os.path.exists(path):
        logging.error(f"File not found: {path}")
        raise FileNotFoundError(f"File not found: {path}")

    try:
        # Read the parquet file from the path
        title_basics = pl.read_parquet(path)
        logging.info(f"Successfully loaded data from {path}")
    except Exception as e:
        logging.error(f"Error reading the parquet file: {e}")
        raise

    # Filter the DataFrame to include only rows where startYear is greater than 1899
    title_basics = title_basics.filter(pl.col("startYear") > 1899)
    if "__index_level_0__" in title_basics.columns:
        title_basics = title_basics.drop("__index_level_0__")
        logging.info("Dropped __index_level_0__ column")
    return title_basics


def load_ratings_imdb(path: str) -> pl.DataFrame:
    """
    Load the ratings dataset from the processed data folder.

    Parameters:
    path (str): The file path to the ratings dataset.

    Returns:
    pl.DataFrame: A DataFrame containing the ratings data.
    """
    if not path:
        # If no path is provided, use the default static path
        path = "/Users/bradkittrell/Projects/imdb/IMDB_Fun/Data/processed/title_ratings.parquet"
        logging.info(f"No path provided. Using default path: {path}")

    if not os.path.exists(path):
        logging.error(f"File not found: {path}")
        raise FileNotFoundError(f"File not found: {path}")

    try:
        # Read the parquet file from the path
        ratings = pl.read_parquet(path)
        logging.info(f"Successfully loaded data from {path}")
    except Exception as e:
        logging.error(f"Error reading the parquet file: {e}")
        raise

    if "__index_level_0__" in ratings.columns:
        ratings = ratings.drop("__index_level_0__")
        logging.info("Dropped __index_level_0__ column")

    return ratings


def load_crew(path: str) -> pl.DataFrame:
    """
    Load the crew dataset from the processed data folder.

    Parameters:
    path (str): The file path to the crew dataset.

    Returns:
    pl.DataFrame: A DataFrame containing the crew data.
    """
    if not path:
        # If no path is provided, use the default static path
        path = "/Users/bradkittrell/Projects/imdb/IMDB_Fun/Data/processed/title_crew.parquet"
        logging.info(f"No path provided. Using default path: {path}")

    if not os.path.exists(path):
        logging.error(f"File not found: {path}")
        raise FileNotFoundError(f"File not found: {path}")

    try:
        # Read the parquet file from the path
        crew = pl.read_parquet(path)
        logging.info(f"Successfully loaded data from {path}")
    except Exception as e:
        logging.error(f"Error reading the parquet file: {e}")
        raise

    if "__index_level_0__" in crew.columns:
        crew = crew.drop("__index_level_0__")
        logging.info("Dropped __index_level_0__ column")

    return crew


def load_principals(path: str) -> pl.DataFrame:
    """
    Load the principals dataset from the processed data folder.

    Parameters:
    path (str): The file path to the principals dataset.

    Returns:
    pl.DataFrame: A DataFrame containing the principals data.
    """
    if not path:
        # If no path is provided, use the default static path
        path = "/Users/bradkittrell/Projects/imdb/IMDB_Fun/Data/processed/title_principals.parquet"
        logging.info(f"No path provided. Using default path: {path}")

    if not os.path.exists(path):
        logging.error(f"File not found: {path}")
        raise FileNotFoundError(f"File not found: {path}")

    try:
        # Read the parquet file from the path
        principals = pl.read_parquet(path)
        logging.info(f"Successfully loaded data from {path}")
    except Exception as e:
        logging.error(f"Error reading the parquet file: {e}")
        raise

    if "__index_level_0__" in principals.columns:
        principals = principals.drop("__index_level_0__")
        logging.info("Dropped __index_level_0__ column")

    return principals


## ** External data from OMDB API **##

def fetch_movie_data(imdb_film_id: str, api_key: str):
    """


    Args:
        title (_type_): _description_
        api_key (_type_): _description_

    Returns:
        _type_: _description_
    """

    url = f"http://www.omdbapi.com/?i={imdb_film_id}&plot=full&apikey={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def load_cached_omdb_data(CSV_FILE: str = None) -> pd.DataFrame:
    """Load the previously saved responses from CSV."""
    if not CSV_FILE:
        # If no path is provided, use the default static path
        CSV_FILE = "/Users/bradkittrell/Projects/imdb/IMDB_Fun/Data/external/omdb.csv"
        logging.info(f"No path provided. Using default path: {CSV_FILE}")
    if os.path.exists(CSV_FILE):
        # Ensure IMDb IDs are treated as strings
        return pd.read_csv(CSV_FILE, dtype=str)
    static_cols = ['Title', 'Year', 'Rated', 'Released', 'Runtime', 'Genre', 'Director',
                   'Writer', 'Actors', 'Plot', 'Language', 'Country', 'Awards', 'Poster',
                   'Ratings', 'Metascore', 'imdbRating', 'imdbVotes', 'imdbID', 'Type',
                   'DVD', 'BoxOffice', 'Production', 'Website', 'Response', 'RatingSource',
                   'RatingValue']
    return pd.DataFrame(columns=static_cols)


def response_to_df(response):
    """
    Helper function designed to convert the response from the OMDB API into a DataFrame.

    Args:
        response (_type_): _description_

    Returns:
        _type_: _description_
    """
    _ = pd.DataFrame(response)
    _['RatingSource'] = _["Ratings"].apply(lambda x: x.get('Source'))
    _['RatingValue'] = _["Ratings"].apply(lambda x: x.get('Value'))

    return _


def process_omdb_ids(api_key: str, imdb_ids: list[str], CSV_FILE: str = None) -> pd.DataFrame:
    """Check cached repository for existing data and fetch new data if necessary."""
    df_cache = load_cached_omdb_data()

    # Convert existing IMDb IDs to a set for fast lookup
    cached_ids = set(df_cache["imdbID"].tolist())
    counter = 0
    null_counter = 0
    new_data = []
    for imdb_id in imdb_ids:
        if imdb_id not in cached_ids:  # Fetch only if not in cache
            counter += 1
            data = fetch_movie_data(imdb_film_id=imdb_id, api_key=api_key)
            # verify type
            if data is None:
                logging.error(f"Failed to fetch data for {imdb_id}")
                null_counter += 1
                if null_counter > 100:
                    logging.error(
                        f"Null counter exceeded limit for {imdb_id}. Stopping fetch.")
                    break
                continue
            else:
                # reset null counter to 0
                null_counter = 0

            # Check valid response
            # convert to date frame and
            if "Ratings" in data:
                new_data.append(response_to_df(data))
            else:
                new_data.append(pd.DataFrame(data))

            if counter % 10 == 0:
                df_new = pd.concat(new_data, ignore_index=True)
                df_cache_updated = pd.concat(
                    [df_cache, df_new], ignore_index=True)
                if not CSV_FILE:
                    CSV_FILE = "/Users/bradkittrell/Projects/imdb/IMDB_Fun/Data/external/omdb.csv"
                    logging.info(
                        f"No path provided. Using default path: {CSV_FILE}")
                print(f"Updated cache with {len(new_data)} new entries.")
                df_cache_updated.to_csv(
                    CSV_FILE, index=False)  # Save updated cache

    # # Convert new data to DataFrame and append to cache
    # if new_data:
    #     df_new = pd.concat(new_data, ignore_index=True)
    #     df_cache = pd.concat([df_cache, df_new], ignore_index=True)
    #     if not CSV_FILE:
    #         CSV_FILE = "/Users/bradkittrell/Projects/imdb/IMDB_Fun/Data/external/omdb.csv"
    #         logging.info(f"No path provided. Using default path: {CSV_FILE}")
    #     df_cache.to_csv(CSV_FILE, index=False)  # Save updated cache

    return df_cache_updated


def select_films_by_director(directors: list[str]) -> pl.DataFrame:
    """
    Select films by a list of directors.

    Args:
        directors (list[str]): List of director names.

    Returns:
        pl.DataFrame: DataFrame containing films directed by the specified directors.
    """
    names = load_names(None)
    film_crew = load_crew(None)
    films = load_title_basics(None)

    return selected_films


def directors_greater_n_films(n: int) -> pl.DataFrame:
    """_summary_
    Assuming only films greater than 1 hour to 5 hrs

    Args:
        n (int): _description_

    Returns:
        pl.DataFrame: _description_
    """
    names = load_names(None)
    film_crew = load_crew(None)
    films = load_title_basics(None)

    dir_counts = films.join(film_crew, on="tconst").filter(
        (pl.col("runtimeMinutes") > 60) & (pl.col("runtimeMinutes") < 60*5))
    dir_counts = dir_counts.group_by("directors").agg([pl.count('directors').alias('num_films'),
                                                       pl.min('startYear').alias(
                                                           'first_year'),
                                                       pl.max('startYear').alias('last_year')])
    dir_counts = dir_counts.join(names, left_on="directors", right_on="nconst")

    dir_counts = dir_counts.select(
        ["directors", "num_films", "primaryName", "birthYear", "deathYear",  'first_year', 'last_year']).filter(pl.col("num_films") > n).with_columns(pl.col("directors").alias("nconst")).drop("directors")
    return dir_counts


def clean_imdb_director_films(n_films: int) -> pl.DataFrame:
    """
    Select films directed by directors with more than n films.
    Args:
        n_films (int): Number of films.
    Returns:
        pl.DataFrame: DataFrame containing films directed by directors with more than n films.
    """
    n_dirs = directors_greater_n_films(n_films)
    n_dirs_list = n_dirs["nconst"].to_list()
    # read films d ata
    films = load_title_basics(None)
    film_crew = load_crew(None)

    films = films.join(film_crew, on="tconst").filter(
        (pl.col("runtimeMinutes") > 60) & (pl.col("runtimeMinutes") < 60*5)).filter(pl.col("directors").is_in(n_dirs_list))

    films = films.join(n_dirs, left_on="directors", right_on="nconst")
    # prep  ratings
    rating = load_ratings_imdb(None)
    films = films.join(rating, on="tconst")

    return films.drop("isAdult", "endYear", "titleType")
