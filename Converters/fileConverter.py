import pandas as pd
from pathlib import Path

def convert_file(file_path: str, target_extension: str):
    """
    Convert a CSV file to Parquet or a Parquet file to CSV.

    Parameters
    ----------
    file_path : str
        Path to the input file.
    target_extension : str
        Target extension ('csv' or 'parquet').

    Returns
    -------
    str
        Path to the newly created file.
    """

    path = Path(file_path)
    target_extension = target_extension.lower()

    if target_extension not in ["csv", "parquet"]:
        raise ValueError("target_extension must be 'csv' or 'parquet'")

    # Load the file
    if path.suffix == ".csv":
        df = pd.read_csv(path)
    elif path.suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        raise ValueError("Input file must be .csv or .parquet")

    # Create new file path
    new_path = path.with_suffix(f".{target_extension}")

    # Save with new format
    if target_extension == "csv":
        df.to_csv(new_path, index=False)
    else:
        df.to_parquet(new_path, index=False)

    return str(new_path)