import math

from typing import Tuple, List

import pandas as pd 
import yfinance as yf


URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def download_sp500() -> None:
    sp500 = pd.read_html(URL)[0]
    sp500.to_csv('data/sp500list.csv', sep=';')
    print("List of S&P 500 companies downloaded!")


def get_sp500_symbols()->List[str]:
    sp500 = pd.read_csv('data/sp500list.csv', sep=';')
    return sp500['Symbol'].values


def get_sp500_symbols_bloom(column:str = 'Symbol')->List[str]:
    sp500 = pd.read_csv('data/SP500_list_bloom.txt', sep='\t')
    return sp500[column].values


def download_sp500_quotes_per_company()->None:
    symbols = get_sp500_symbols_bloom()
    symbols_string = ' '.join(symbols)
    data = yf.download(tickers=symbols_string, period='1y', interval='1d')
    data.to_csv('data/prices_sp500_per_company.csv', sep=';')
    data['Adj Close'].to_csv('data/ADJ_prices_sp500_per_company.csv', sep=';')


def calculate_var(s: pd.Series, T: int, Gp: float)->Tuple[float]:
    mean_chg = s.pct_change().mean()
    std = s.pct_change().std()
    last_price = s.iloc[-1]
    var_price = last_price * (mean_chg * T - std * Gp * math.sqrt(T))
    var_pct = var_price/last_price

    return mean_chg, std, last_price, var_price, var_pct


def get_sp500_vars()->pd.DataFrame:
    symbls = get_sp500_symbols_bloom('Symbol')
    comp_names = get_sp500_symbols_bloom('name')
    d_sym_name = {k:v for k,v in zip(symbls, comp_names)}

    sp500 = pd.read_csv('data/ADJ_prices_sp500_per_company.csv', sep=';')
    last_td = sp500.iloc[-1]['Date']
    symbols = [i for i in sp500.columns if i!='Date']

    res = []
    T = 1

    varOpts = {'95': 1.65, '99': 2.33}
    vOpt = '99'

    for sym in symbols:
        s = sp500[sym]    
        m, std, price, var_price, var_pct = calculate_var(s, T, varOpts[vOpt])
        row = {'as_for_date': last_td,'symbol':sym, 'company_name': d_sym_name[sym], 'adj_price': price,'mean_price_chg': m, 'std_price_chg':std, 
                f'var_{vOpt}_price_{T}d': var_price, f'var{vOpt}_pct_{T}d': var_pct}
        res.append(row)
    
    return pd.DataFrame(res).sort_values(f'var{vOpt}_pct_{T}d')


def main()->None:
    df = get_sp500_vars()
    with pd.ExcelWriter('data/res_sp500_vars.xlsx') as w:
        df.to_excel(w)


if __name__=='__main__':
    main()