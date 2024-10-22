from datetime import datetime
import logging

def test():
    from WindPy import w

    #w.start() # 默认命令超时时间为120秒
    w.start()
    # 如需设置超时时间可以加入waitTime参数，例如waitTime=60,即设置命令超时时间为60秒

    result = w.isconnected() # 判断WindPy是否已经登录成功
    print(result) # True

    def func(*args, **kwargs):
        print(args, kwargs)

    print('pre_close:')
    data = w.wsd("603817.SH,301168.SZ", "pre_close")
    print(data)

    print('close')
    data = w.wsd("603817.SH,301168.SZ", "close")
    print(data)

    print('open')
    data = w.wsd("603817.SH,301168.SZ", "open")
    print(data)

    w.stop()

class MockWindPy:

    def __init__(self):
        pass

    def wsd(self, codes, fields, begin_time=None, end_time=None, options=''):
        if fields == '':
            segs = 'open,high,low,close,volume,amt'.split(',')
        else:
            segs = fields.split(',')

        if begin_time is None and end_time is None:
            begin_time = ''
            end_time = ''

        if begin_time == '' and end_time == '':
            begin_time = datetime.now().strftime('%Y-%m-%d')
            end_time = datetime.now().strftime('%Y-%m-%d')

        dates_range = pd.date_range(begin_time, end_time)
        df = pd.DataFrame(columns=segs, index=dates_range)
        return data

    def start(self):
        pass

    def stop(self):
        pass

class WindServer:

    def __init__(self):
        #from MockWindPy import w ; self.w = MockWindPy()
        from WindPy import w ; self.w = w
        self.w.start()

    def __del__(self):
        self.w.stop()

    def get_data(self, code, field):
        data = self.w.wsd(code, field)
        return data

    def handle_request(self, json_data):
        logging.info('json_data: %s' % json_data)
        response = {
            'ok': True,
        }
        try:
            cmd = json_data.get('cmd')
        except Exception as e:
            response['ok'] = False
            response['error'] = str(e)
            return response

        if cmd == 'wsd':
            codes = json_data.get('codes', None)
            fields = json_data.get('fields', '')
            options = json_data.get('options', '')
            begin_time = json_data.get('beginTime', None)
            end_time = json_data.get('endTime', None)

            logging.info('codes: %s, fields: %s, begin_time: %s, end_time: %s' % (codes, fields, begin_time, end_time))

            if begin_time is None and end_time is None:
                begin_time = ''
                end_time = ''

            if codes is None or fields is None:
                response['ok'] = False
                response['error'] = 'codes or fields is None'
                return response

            rsp, df = self.w.wsd(codes, fields, begin_time, end_time, options, usedf=True)

            if rsp != 0:
                response['ok'] = False
                response['error_no'] = str(rsp)
                response['error'] = str(df)
                return response

            df_data = df.to_json()
            response['data'] = df_data
            response['wind_ret'] = str(rsp)
        elif cmd == 'wset':
            tableName = json_data.get('tableName', None)
            options = json_data.get('options', None)
            if tableName is None:
                response['ok'] = False
                response['error'] = 'tableName is None'
                return response

            if options is None:
                response['ok'] = False
                response['error'] = 'options is None'
                return response

            # self.w.wset("sectorconstituent","sectorid=a001010100000000;field=wind_code")
            rsp, df = self.w.wset(tableName, options, usedf=True)
            if rsp != 0:
                response['ok'] = False
                response['error_no'] = str(rsp)
                response['error'] = str(df)
                return response
            df_data = df.to_json()
            response['data'] = df_data
            response['wind_ret'] = str(rsp)

        else:
            response['ok'] = False
            response['error'] = 'Unknown cmd: %s' % cmd

        return response

__context__ = {}

def get_wind_server():
    global __context__
    if 'wind_server' not in __context__:
        __context__['wind_server'] = WindServer()
    return __context__['wind_server']

from flask import Flask, request

app = Flask(__name__)

file_handler = logging.FileHandler('app.log')

# Set the log level
file_handler.setLevel(logging.DEBUG)

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Set flush behavior to ensure logs are written immediately
class FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

# Use the custom handler to flush logs immediately
flush_file_handler = FlushFileHandler('app.log')
flush_file_handler.setLevel(logging.DEBUG)
flush_file_handler.setFormatter(formatter)

# Add the handler to the app's logger
# app.logger.addHandler(flush_file_handler)
# app.logger.setLevel(logging.DEBUG)


@app.route("/")
def hello_world():
    app.logger.warn('hello_world')
    logging.warn('hello_world')
    return "<p>Hello, World!</p>"

@app.route("/post_data", methods=['POST'])
def post_data():
    # print('post_data')
    if request.method == 'POST':
        data = request.get_json()
        wind_server = get_wind_server()
        logging.info('data: %s' % data)
        response = wind_server.handle_request(data)
        return response
    else:
        return json.dumps({'ok': False, 'error': 'Only support POST method'})


def context_init():
    app.logger.info('context_init')



def main():
    logging.basicConfig(level=logging.DEBUG)
    app.logger.setLevel(logging.DEBUG)

    with app.app_context():
       wind = get_wind_server()

    app.run(debug=True)

if __name__ == "__main__":
    main()
