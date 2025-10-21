import json
import os.path
import pandas as pd

from mtu.converter.version_detector import check_version, create_reader

#with open(
#        "./data/MT_Analytics_Test_98f2580e-9537-4565-8746-96e3379c978e_20240515-121736_B4_F1.json") as file:
#    data = json.load(file)

def convert(data):
    version = check_version(data)
    print(f"Detected version: {version}")
    reader = create_reader(data)
    # reader.get_renaming_structure(data, base_path=os.path.dirname(__file__), hf_schema_path="hf_schema_suzan.json", lf_schema_path="lf_schema_suzan.json", hfblock_schema_path="hfblock_schema_suzan.json")

    # hf_df, lf_df, hfblock_df = reader.read_json_with_rename(
    #     data, "example_hf_df_rename.json", "example_lf_df_rename.json", "example_hfblock_df_rename.json", base_path=os.path.dirname(__file__)
    # )

    hf_df, lf_df, hfblock_df = reader.read_json_with_rename(
        data, "hf_schema_suzan.json", "lf_schema_suzan.json", "hfblock_schema_suzan.json", base_path=os.path.dirname(__file__)
    )

    alldat_df = pd.merge(hf_df, hfblock_df,
                    left_on='cycle', right_on='HFProbeCounter', how='left')

    lf_df['HFProbeCounter'] = lf_df['HFProbeCounter'].astype(int)
    alldat_df = pd.merge(alldat_df, lf_df,
                    on='HFProbeCounter', how='left')
    alldat_df['machine_rot_offset_x'] = 0
    alldat_df['machine_trans_offset_z'] = 0

    #Serial-Nr. Fix-Test-Daten 
    alldat_df["serialn"] = "LENCBH4711"
    alldat_df["maschine"] = "49514580"
    alldat_df["material"] = "32A4302"
    alldat_df["operation"] = "0100"
    alldat_df["ordern"] = "120088241"
    alldat_df["block"] = "4"
    alldat_df["schaufel"] = "3"
    alldat_df["befehl"] = "Schlichten"

    #alldat_df.to_parquet("output.parquet")

    #print("done")
    return alldat_df


