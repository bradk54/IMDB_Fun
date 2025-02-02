import polars as pl


def load_names(path: str) -> pl.DataFrame:
    """
    Load the names dataset from the processed data folder.

    """

    if path:
        return pl.read_parquet(path)
    else:
        # use static path
        path = "/Users/bradkittrell/Projects/imdb/IMDB_Fun/Data/processed/name.parquet"

    names = pl.read_parquet(path)
    names = names.filter(pl.col("birthYear") > 1899)
    return names
