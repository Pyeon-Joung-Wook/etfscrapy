


ETFDB_BASE_URL = 'https://etfdb.com/etf/'
DIR_ETFDB = 'etfdb'



def etf_alloc(*etfs, at=None, cwd=None):
    #cwd = os.getcwd()
    dir_target = os.path.join(cwd, DIR_ETFDB, at)

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


    def _tables(etf):
        file = os.path.join(dir_target, etf + '.html')
        tables = pd.read_html(file, attrs={'class':'chart base-table'}, index_col=0)
        return {_name(df):_df(df, etf) for df in tables}


    if not os.path.exists(dir_target):
        os.makedirs(dir_target)

    wb.sheets['xlwings.conf'].range('A14').value = 0
    wb.sheets['xlwings.conf'].range('A15').value = len(etfs)        
    for etf in etfs:
        file = os.path.join(dir_target, etf.upper() + '.html')
        if not os.path.exists(file):
            urlretrieve(ETFDB_BASE_URL + etf, file)
        wb.sheets['xlwings.conf'].range('A14').value += 1

    tables = []
    for etf in etfs:
        tables.append(_tables(etf))

    tables_dict = {
        k:pd.concat([dic[k] for dic in tables], axis=1, sort='False').fillna(0) for k in tables[0]
    }

    return pd.concat(tables_dict, axis=0)  