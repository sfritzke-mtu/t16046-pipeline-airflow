import json
import pandas as pd
from .utils import warn_if_fill_order_matters

from .base_reader import BaseReader


class V3Reader(BaseReader):
    """
    Concrete implementation of BaseReader for V3 JSON format of Siemens EDGE data.

    Parses JSON data structured with 'Payload', 'HFData', 'LFData', 'HFBlockEvent',
    and 'HFTimestamp' entries. Converts them into structured pandas DataFrames
    for further processing.
    """

    def read_json(self, data):
        """
        Reads and parses V3 JSON data into structured DataFrames.

        Extracts and processes:
        - High-frequency data (HFData)
        - Low-frequency data (LFData), pivoted to wide format
        - Block event data (HFBlockEvent), merged with timestamps (HFTimestamp)

        Args:
            data (dict): The JSON object containing 'Payload' and 'Header' sections.

        Returns:
            tuple: A tuple of three pandas DataFrames:
                - hf_df: High-frequency data with signal addresses as columns.
                - lf_df: Low-frequency data pivoted to wide format.
                - hfblock_df: Block event data merged with timestamp information.
        """
        payload_data = data['Payload']
        dictionaries_hf = [entry['HFData'] for entry in payload_data if 'HFData' in entry]
        hf_data = [item for sublist in dictionaries_hf for item in sublist]
        column_names = [f"{signal['Address']}" for signal in data['Header'][
            'SignalListHFData']]
        hf_df = pd.DataFrame(data=hf_data, columns=column_names)

        dictionaries_hfblock = [entry['HFBlockEvent'] for entry in payload_data if
                                'HFBlockEvent' in entry]  # Check if we need to get HFCallEvent as well
        hfblock_data = [item for item in dictionaries_hfblock]
        hfblock_df = pd.DataFrame(hfblock_data)

        dictionaries_lf = [entry['LFData'] for entry in payload_data if 'LFData' in entry]
        lf_data = [item for sublist in dictionaries_lf for item in sublist]
        for item in lf_data:
            item['address'] = item['address'].split('/')[-1]
        lf_df = pd.DataFrame(lf_data)
        # lf to wide
        lf_df = lf_df.rename(columns={"HfProbeCounter": "HFProbeCounter"})
        lf_df = lf_df.pivot(index=['timestamp', 'HFProbeCounter'], columns='address',
                            values='value')
        lf_df.reset_index(inplace=True)
        lf_df.columns.name = None
        lf_df.columns = lf_df.columns.astype(str)

        columns_to_fill = lf_df.columns
        warn_if_fill_order_matters(lf_df, columns_to_fill)
        lf_df[columns_to_fill] = lf_df[columns_to_fill].bfill()
        lf_df[columns_to_fill] = lf_df[columns_to_fill].ffill()

        dictionaries_hftimestamps = [entry['HFTimestamp'] for entry in payload_data if 'HFTimestamp' in entry]
        for entry in payload_data:
            if 'HfTimestamp' in entry:
                print("found")
        # hftimestamps_df = pd.DataFrame(dictionaries_hftimestamps)
        # hftimestamps_df = hftimestamps_df.rename(columns={"HfProbeCounter": "HFProbeCounter"})
        hfblock_df = hfblock_df.rename(columns={"HfProbeCounter": "HFProbeCounter"})
        # hfblock_df = hfblock_df.merge(hftimestamps_df, on="HFProbeCounter", how="left")

        return hf_df, lf_df, hfblock_df

# if __name__ == '__main__':
#     with open("../new_edge_test.json") as file:
#         data = json.load(file)
#     reader = V3Reader()
#     hf, lf, hfblock_df = reader.read_json(data)
#     print(reader.get_renaming_structure(data))