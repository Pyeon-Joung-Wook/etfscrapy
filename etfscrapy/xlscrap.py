import xlwings as xw
import pandas as pd
import requests
import io
import re
import asyncio
from functools import partial
from bs4 import BeautifulSoup


def etf_alloc(*etfs):
    
    def _name(df):
        name = df.index.name
        if name.lower() != 'region':
            return name
        elif df.index.str.contains('america|asia|europe|africa|middle', case=False).any():
            return name
        else:
            return 'Market Tier'
    
    def _df(df, etf):
        return pd.DataFrame({etf.upper():df.dropna().Percentage.str.rstrip('%').astype('float')})

    
    async def get_tables(etf):
        url = 'https://etfdb.com/etf/' + etf
        read_html_partial = partial(pd.read_html, attrs={'class':'chart base-table'}, index_col=0, flavor=['lxml', 'bs4'])
        tables = await loop.run_in_executor(None, read_html_partial, url)
        return {_name(df):_df(df, etf) for df in tables}


    async def main():
        fts = [asyncio.ensure_future(get_tables(etf)) for etf in etfs]
        return await asyncio.gather(*fts)
    
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    
    # 다음 코드를 주피터에서 돌리려면, tornado를 downgrade 해야함
    # pip install tornado==4.5.3
    res = loop.run_until_complete(main())
    loop.close()
    
    etfs = {
        k:pd.concat([dic[k] for dic in res], axis=1, sort='False').fillna(0) for k in res[0]
    }
    
    return pd.concat(etfs, axis=0)


def get():
    wb = xw.Book.caller()
    wb.sheets[0].range("B2").value = etf_alloc('spy','acwi','mtum','xle')


if __name__ == '__main__':
    get()
