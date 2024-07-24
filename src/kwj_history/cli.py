import pandas as pd
import sys
import argparse
def cnt(q):
    df = read_parquet()
    df = pd.read_parquet('~/tmp/history.parquet')
    fdf = df[df['cmd'].str.contains(q)]
    cnt = fdf['cnt'].sum()
    #print(cnt)
    return cnt

def read_parquet(path='~/tmp/history.parquet'):
    df = pd.read_parquet(path)
    return df

#def read_parquet(path):
#    df = pd.read_parquet(path)
#    return df

def top_df(num):
    df = pd.read_parquet('~/data/parquet')
    result = df.head(num).to_string(index=False)
    return result

def day_df(d):
    df = pd.read_parquet('~/data/parquet')
    result = df[df['dt'] == d].to_string(index=False)
    return result

def double_df(num,d):
    df = pd.read_parquet('~/data/parquet')
    df2 = df[df['dt'] == d].sort_values(by='cnt', ascending = False).head(num)
    df2 = df2.drop(columns = ['dt']).to_string(index=False)
    return df2

def query():
    parser = argparse.ArgumentParser()

    parser.add_argument("-s", "--search", help="hist -s <ls>",type=str, action='store')
    parser.add_argument("-t", "--top", help="hist -t <int>",type=int,action='store')
    parser.add_argument("-d", "--day", help="hist -d <YYYY-MM-DD>", type=str,action='store')

    args = parser.parse_args()
    if args.search:
        q = args.search
        i = cnt(q)
        print(f'{q} 사용 횟수는 {i}회 입니다.')
    elif args.top and args.day:
        r = double_df(args.top, args.day)
        print(r)
    elif args.top:
        r = top_df(args.top)
        print(r)
    elif args.day:
        r = day_df(args.day)
        print(r)
