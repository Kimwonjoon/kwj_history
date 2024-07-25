import pandas as pd

def top():

    df = pd.read_parquet('~/data/parquet')
    fdf = df[df['dt'] == '2024-07-12']
    sdf = fdf.sort_values(by='cnt', ascending=False).head(10)
    ddf = sdf.drop(columns=['dt'])

    r = ddf.to_string(index = False)
    return r
