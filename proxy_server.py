from flask import Flask, current_app
from trans4redis import RedisClient
from setting import API_HOST, API_PORT, API_THREADED, IS_DEV

__all__ = ['app']

app = Flask(__name__)
if IS_DEV:
    app.debug = True


def get_conn():
    """
    get redis client object
    :return:
    """
    if not hasattr(current_app, 'redis'):
        current_app.redis = RedisClient()
    return current_app.redis


@app.route('/')
def index():
    """
    get home page, you can define your own templates
    :return:
    """
    return '<h2>Welcome to Proxy Pool System</h2>'


@app.route('/random')
def get_proxy():
    """
    get a random proxy
    :return: get a random proxy
    """
    conn = get_conn()
    return str(conn.random())


@app.route('/all')
def get_proxy_all():
    """
    get a random proxy
    :return: get a random proxy
    """
    conn = get_conn()
    proxies = conn.all()
    proxies_string = ''
    if proxies:
        for proxy in proxies:
            proxies_string += str(proxy) + '\n'

    return proxies_string


@app.route('/count')
def get_count():
    """
    get the count of proxies
    :return: count, int
    """
    conn = get_conn()
    return str(conn.count())


if __name__ == '__main__':
    app.run(host=API_HOST, port=API_PORT, threaded=API_THREADED)
