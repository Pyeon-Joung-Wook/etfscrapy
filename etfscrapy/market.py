import pandas as pd
import io
import asyncio
from functools import partial
import yahoo_fin.stock_info as yf


def market_data(*tickers, start=None, end=None):

    async def _data(ticker):
        get_data_partial = partial(yf.get_data, start_date=start, end_date=end)
        data = await loop.run_in_executor(None, get_data_partial, ticker)
        return ticker.upper(), data
    
    async def main():
        fts = [asyncio.ensure_future(_data(ticker)) for ticker in tickers]
        return await asyncio.gather(*fts)
    
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    
    try:
        # 다음 코드를 주피터에서 돌리려면, tornado를 downgrade 해야함
        # pip install tornado==4.5.3
        res = loop.run_until_complete(main())        
        out = pd.concat(dict(res), axis=1, sort=True).fillna(method='ffill')
    
    except Exception as ex:
        out = ex
    
    finally:
        loop.close()
    
    cols = [t.upper() for t in tickers]
    out = out[list(cols)]
    #ut.columns = out.columns.str.upper()
    return out