import os
from abc import ABC, abstractmethod
from .utils import *
class BaseReader(ABC):
    """
    Abstract base class for JSON readers.

    Provides a common interface and shared functionality for reading and processing
    JSON data into structured formats. Subclasses must implement the `read_json` method
    """
    @abstractmethod
    def read_json(self, data):
        """
        Abstract method to read JSON data and return structured outputs.

        Args:
            data (dict or list): The JSON data to be read.

        Returns:
            Tuple of DataFrames: Typically (hf_df, lf_df, hfblock_df), representing
            high-frequency data, low-frequency data, and block event data.
        """
        pass

    def read_json_with_rename(self, data, hf_rename_path, lf_rename_path,
                              hfblock_rename_path, base_path=None):
        """
        Reads JSON data and applies renaming schemes from provided JSON files.

        Args:
            data (dict or list): The JSON data to be read.
            hf_rename_path (str): Path to the renaming scheme for high-frequency data.
            lf_rename_path (str): Path to the renaming scheme for low-frequency data.
            hfblock_rename_path (str): Path to the renaming scheme for block event data.
            base_path (str): Path to where the parent directory of where the renaming schemas are saved

        Returns:
            Tuple of DataFrames: Renamed (hf_df, lf_df, hfblock_df).

        """

        if base_path is None:
            base_path = os.path.dirname(__file__)

        hf_rename_path = os.path.join(base_path, hf_rename_path)
        lf_rename_path = os.path.join(base_path, lf_rename_path)
        hfblock_rename_path = os.path.join(base_path, hfblock_rename_path)

        hf_df, lf_df, hfblock_df = self.read_json(data)
        with open(hf_rename_path) as f2:
            hf_rename = json.load(f2)
        with open(lf_rename_path) as f3:
            lf_rename = json.load(f3)
        with open(hfblock_rename_path) as f4:
            hfblock_rename = json.load(f4)
        for f in [f2, f3, f4]:
            f.close()

        hf_df = apply_renaming_dict(hf_df, hf_rename)
        lf_df = apply_renaming_dict(lf_df, lf_rename)
        hfblock_df = apply_renaming_dict(hfblock_df, hfblock_rename)

        print(f"Used renaming scheme defined in {hf_rename_path} for HFData")
        print(f"Used renaming scheme defined in {lf_rename_path} for LFData")
        print(f"Used renaming scheme defined in {hfblock_rename_path} for HFBlockEvents")

        return hf_df, lf_df, hfblock_df

    def get_renaming_structure(self, data, base_path=None, hf_schema_path="hf_df_empty.json", lf_schema_path="lf_df_empty.json",
                               hfblock_schema_path="hfblock_df_empty.json"):
        """
        Generates and saves empty renaming schemes based on the structure of the input data.

        Args:
            data (dict or list): The JSON data to be read.
            base_path (str): Path to the directory where the renaming schemes are to be saved, defaults directory of this script
            hf_schema_path (str): Path to save the empty renaming scheme for high-frequency data.
            lf_schema_path (str): Path to save the empty renaming scheme for low-frequency data.
            hfblock_schema_path (str): Path to save the empty renaming scheme for block event data.

        Returns:
            None
        """
        hf_df, lf_df, hfblock_df = self.read_json(data)

        if base_path is None:
            base_path = os.path.dirname(__file__)

        hf_schema_path = os.path.join(base_path, hf_schema_path)
        lf_schema_path = os.path.join(base_path, lf_schema_path)
        hfblock_schema_path = os.path.join(base_path, hfblock_schema_path)

        save_renaming_scheme(create_empty_renaming_scheme(hf_df), hf_schema_path)
        save_renaming_scheme(create_empty_renaming_scheme(lf_df), lf_schema_path)
        save_renaming_scheme(create_empty_renaming_scheme(hfblock_df), hfblock_schema_path)
        print(f"Saved empty renaming scheme for HFData under {hf_schema_path}")
        print(f"Saved empty renaming scheme for LFData under {lf_schema_path}")
        print(f"Saved empty renaming scheme for HFBlockEvents under {hfblock_schema_path}")
