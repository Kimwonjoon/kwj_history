import pandas as pd
import sys
import argparse
from tabulate import tabulate
def cnt(q):
    df = read_parquet()
    df = pd.read_parquet('~/tmp/history.parquet')
    fdf = df[df['cmd'] == q]
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
    df2 = df2.drop(columns = ['dt'])
    return df2

def query():
#    import pandas as pd
    parser = argparse.ArgumentParser()

    parser.add_argument("-s", "--search", help="hist -s <ls>",type=str, action='store')
    parser.add_argument("-t", "--top", help="hist -t <int>",type=int,action='store')
    parser.add_argument("-d", "--day", help="hist -d <YYYY-MM-DD>", type=str,action='store')
    parser.add_argument("-p", "--pretty", help="hist -p",action='store_true')

    args = parser.parse_args()
    if args.search:
        q = args.search
        i = cnt(q)
        print(f'{q} 사용 횟수는 {i}회 입니다.')
    elif args.top and args.day:
        r = double_df(args.top, args.day)
        if args.pretty:
            print(tabulate(r, tablefmt="pipe", showindex="always", headers="keys"))
        else:
            print(r)
    elif args.top:
        r = top_df(args.top)
        print(r)
    elif args.day:
        r = day_df(args.day)
        print(r)

def hello_msg():
    return "hello"

def cmd():
    msg = hello_msg()
    print(msg)

    parser = argparse.ArgumentParser(
                    prog='ProgramName',
                    description='What the program does',
                    epilog='Text at the bottom of help')
    parser.add_argument('filename')           # positional argument
    parser.add_argument('-c', '--count')      # option that takes a value
    parser.add_argument('-v', '--verbose', action='store_true')  # on/off flag

    args = parser.parse_args()
    print(args.filename, args.count, args.verbose)

    if True:
        print("verbose ON")
    else:
        print("verbose OFF")
