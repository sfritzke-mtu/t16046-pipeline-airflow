import json
import pandas as pd

from .base_reader import BaseReader


class V2Reader(BaseReader):
    """
    Concrete implementation of BaseReader for V2 JSON format of Siemens EDGE data.

    Parses JSON data structured with 'Payload', 'HFData', 'LFData', and 'HFBlockEvent' entries.
    Converts them into structured pandas DataFrames for further processing.
    """

    def read_json(self, data):
        """
        Reads and parses V2 JSON data into structured DataFrames.

        Extracts and processes:
        - High-frequency data (HFData)
        - Low-frequency data (LFData), pivoted to wide format
        - Block event data (HFBlockEvent)

        Args:
            data (dict): The JSON object containing 'Payload' and 'Header' sections.

        Returns:
            tuple: A tuple of three pandas DataFrames:
                - hf_df: High-frequency data with signal addresses as columns.
                - lf_df: Low-frequency data pivoted to wide format.
                - hfblock_df: Block event data.

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
        # hfblock_df['nc_row_gcode'] = hfblock_df['GCode'].str.extract('(^N\d{1,})')
        # hfblock_df['sequential_number_nc_row'] = hfblock_df['nc_row_gcode'].str.extract('(\d{1,})')

        dictionaries_lf = [entry['LFData'] for entry in payload_data if 'LFData' in entry]
        lf_data = [item for sublist in dictionaries_lf for item in sublist]
        lf_df = pd.DataFrame(lf_data)
        # lf to wide
        lf_df = lf_df.pivot(index=['timestamp', 'HfProbeCounter'], columns='address',
                            values='value')
        lf_df.reset_index(inplace=True)
        lf_df.columns.name = None
        lf_df.columns = lf_df.columns.astype(str)

        # 'combine' values with bfill to one cycle and remove NAs
        columns_to_fill = lf_df.drop(columns=['timestamp', 'HfProbeCounter', 'recordingServoCounter']).columns
        lf_df[columns_to_fill] = lf_df[columns_to_fill].bfill()
        lf_df = lf_df[lf_df['recordingServoCounter'].notna()]

        return hf_df, lf_df, hfblock_df


if __name__ == '__main__':
    with open("../../transformations_to_parquet/LENCBN4908_SCHAUFEL_1_FINI_BL_1_FILEID_1544.json") as file:
        data = json.load(file)
    reader = V2Reader()
    hf, lf, hfblock_df = reader.read_json(data)
    print(reader.get_renaming_structure(data))