from pyttkits import kits
import requests
import os
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class WindClient:

    def __init__(self, cached_dir='/data/public//raw/cne/wind/cache', url='http://172.17.0.1:5000'):
        self.url = url
        self.cached_dir = cached_dir

    def _make_post(self, path, data):
        response = requests.post(self.url + path, json=data)
        return response

    def cache_request(self, path, data, to_cache=False):
        reqhash = kits.make_hash(sorted([(k, v) for k, v in data.items()], key=lambda x: x[0]))
        cache_file = f'{self.cached_dir}/{reqhash}.json.gz'
        logger.debug(f'cache_file: {cache_file}')
        if os.path.exists(cache_file):
            rspdata =  kits.read_json(cache_file)
        else:
            result = self._make_post(path, data)
            if result.status_code != 200:
                raise Exception(f'Error: {result.text}')
            res = result.json()
            is_ok = res.get('ok', False)
            if not is_ok:
                raise Exception(f'Error: {res}')
            if to_cache:
                kits.save_json(res, cache_file)
            rspdata = res
        return rspdata

    def wsd(self, symbols, fields, begin_date, end_date, options='', to_cache=False):
        data = {
            'cmd': 'wsd',
            'codes': symbols,
            'fields': fields,
            'beginTime': begin_date,
            'endTime': end_date,
            'options': options
        }
        return self.cache_request('/post_data', data, to_cache)


    def wset(self, table_name, options='', to_cache=False):
        data = {
            'cmd': 'wset',
            'tableName': table_name,
            'options': options
        }
        return self.cache_request('/post_data', data, to_cache)

    def get_symbols(self, date='', include_BJ=False):
        import pandas as pd
        if not date:
            date = datetime.now().strftime('%Y%m%d')

        data = self.wset('sectorconstituent', f'date={date};sectorid=a001010100000000', to_cache=True)
        # logger.debug(f"data: {data}")

        df = pd.DataFrame(json.loads(data['data']))

        names = df['wind_code'].tolist()
        if not include_BJ:
            names = [name for name in names if not name.endswith('.BJ')]

        return names

    def get_daily_field(self, field, date, start_date=None):
        import pandas as pd
        today = datetime.now().strftime('%Y%m%d')
        to_cache = True
        if date >= today:
            to_cache = False
        symbols = self.get_symbols(date)
        if start_date is None:
            start_date = date
        data = self.wsd(symbols, field, start_date, date, to_cache=to_cache)
        # logger.debug(f'data: {data}')
        df = pd.DataFrame(json.loads(data['data']))
        if start_date == date:
            df = df.iloc[:, -1]
        else:
            df.index = pd.to_datetime(df.index, unit='ms').strftime('%Y%m%d')
        return df


def test_client(date):
    cl = WindClient()

    # symbols = cl.get_symbols()

    # df = cl.get_daily_field(date='20241016', field='close')
    # print(df)
    # pcls_df = cl.get_daily_field(date='20241018', start_date='20241018', field='pre_close')
    # print(pcls_df)


    # df = cl.get_daily_field(date='20241019', field='close')
    # print(df)

    df = cl.get_daily_field(date=date, field='close')
    print(df)

def main():
    from pyttkits import kits, file as etfile
    kits.init_logging('debug')
    args = {'__subcmd__': []}
    args.update(kits.make_sub_cmd(test_client))
    args = kits.make_args(args)
    kits.run_cmds(args)

if __name__ == '__main__':
    main()
