import json
import pandas as pd
import warnings


def save_renaming_scheme(data: dict, file_path):
    """
    Saves a renaming scheme dictionary to a JSON file.

    Args:
        data (dict): The renaming scheme to save, typically mapping original column names to new ones.
        file_path (str): Path to the output JSON file.

    Returns:
        None
    """
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


def create_empty_renaming_scheme(df: pd.DataFrame):
    """
    Creates an empty renaming scheme from a DataFrame's columns.
    Each column name is mapped to an empty string, indicating no renaming yet.

    Args:
        df (pd.DataFrame): The DataFrame whose columns will be used.

    Returns:
        dict: A dictionary with column names as keys and empty strings as values.
    """
    empty_dict = {col: '' for col in df.columns}
    return empty_dict


def apply_renaming_dict(df: pd.DataFrame, renaming_scheme: dict, logging=True):
    """
    Applies a renaming scheme to a DataFrame's columns.

    If a value in the renaming scheme is None or an empty string, the original column name is retained.
    Logs any keys in the renaming scheme that do not match existing columns.

    Args:
        df (pd.DataFrame): The DataFrame to rename.
        renaming_scheme (dict): Dictionary mapping original column names to new names.
        logging (bool): Whether to print warnings for unmatched keys. Default is True.

    Returns:
        pd.DataFrame: A new DataFrame with renamed columns.
    """
    # Ensure that None or empty string values are replaced with the original key
    cleaned_scheme = {
        k: (v if v is not None and v != "" else k)
        for k, v in renaming_scheme.items()
    }

    # Filter valid and invalid renames
    valid_renames = {old: new for old, new in cleaned_scheme.items() if old in df.columns}
    invalid_renames = {old: new for old, new in cleaned_scheme.items() if old not in df.columns}

    # Optional logging
    if logging and invalid_renames:
        print(f"A total of {len(invalid_renames)} renames could not be applied, "
              f"since they are not present in the data:\n{list(invalid_renames.keys())}")

    # Apply renaming
    return df.rename(columns=valid_renames)


def warn_if_fill_order_matters(df, columns):
    for col in columns:
        series = df[col]
        if series.isna().any():
            # Find indices of non-NaN values
            non_na = series.notna()
            # Check for any NaNs between two non-NaNs
            first = non_na.idxmax()
            last = len(series) - 1 - non_na[::-1].idxmax()
            if series[first:last].isna().any():
                warnings.warn(
                    f"Column '{col}' contains NaN values between non-NaN values. "
                    "The order of bfill/ffill may influence the result."
                )
