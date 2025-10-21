import pandas as pd

def setDuration(df_input : pd, sampling_frequency = 500):
    dt = 1 / (sampling_frequency * 60)

    print("Calculate process duration")
    process_duration = dt * (df_input['cycle'].iat[-1] - df_input['cycle'].iat[0])  # in minutes

    df_input['process_duration'] = process_duration