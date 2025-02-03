import polars as pl
import os
import logging

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
