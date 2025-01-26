def load_names():
    """
    Load the names dataset from the processed data folder.

    """
    return pl.read_parquet('data/processed/names.parquet')
