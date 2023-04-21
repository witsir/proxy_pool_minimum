import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import queue
import re
from pprint import pprint

from fake_headers import Headers
import requests
from retrying import RetryError, retry
# import urllib3

from models import Proxy
from setting import GET_TIMEOUT
from init_urls import init_urls
from trans4redis import RedisClient
from handle_log import get_logger

# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = get_logger('Getter')


def _get_proxies_base64_in(html):
    """
    handle some string encoded by base64 in a html
    """
    lst_base64_in = re.findall(r'window\.atob\("(\S+)"\)[\D,\s]*?(\d{1,5})', html)
    for item in lst_base64_in:
        ip = base64.b64decode(item[0]).decode('utf-8')
        port = item[1]
        proxy = Proxy(ip=ip, port=port)
        logger.info('put new proxy %s', proxy)


def _combine_url(url, item):
    """
    fix suffix of a hrel is same as prefix of a url
    """
    item = item.strip('/')
    url = url.strip('/')
    i = item.find('/')
    j = url.rfind('/')
    str1 = item[:i]
    str2 = url[j + 1:]
    if str1 == str2:
        url = url[:j]
    new_url = url + '/' + item
    return new_url


def from_page_list(url, html, urls_q):
    """
    general method to get urls from page
    """
    pattern = re.compile(r'href="([^"]+)".*?>.*?2.*?<[\D,\s]*?href="([^"]+)".*?>.*?3.*?<[\D,\s]*?href="([^"]+)"')
    m_list = pattern.findall(html)
    if m_list:
        m_tuple = m_list[0]
        for item in m_tuple:
            new_url = _combine_url(url, item)
            logger.info('put new url %s', new_url)
            urls_q.put(new_url)
    else:
        return None


def get_proxies_html(html, redis_trans):
    """
    get list of proxies from html
    """
    pattern = re.compile(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\D+(\d{1,5})')
    lst = pattern.findall(html)
    for item in lst:
        proxy = Proxy(ip=item[0], port=item[1])
        redis_trans.add(proxy)
        logger.info(f'put new proxy {proxy}')


def get_proxies_from_json_txt(json_txt, redis_trans):
    """
    get list of proxies from json
    """
    lines = json_txt.strip().split('\n')
    for line in lines:
        pre_proxy = json.loads(line)
        proxy = Proxy(ip=pre_proxy['host'], port=pre_proxy['port'])
        redis_trans.add(proxy)
        logger.info(f'put new proxy {proxy}')


def get_proxies_from_json(json_data, redis_trans):
    """
    get list of proxies from json
    """
    dict_data = json.loads(json_data)
    key = list(dict_data.keys())[0]
    list_data = dict_data[key]
    for item in list_data:
        proxy = Proxy(ip=item['ip'], port=item['port'])
        redis_trans.add(proxy)
        logger.info(f'put new proxy {proxy}')


@retry(stop_max_attempt_number=3, retry_on_result=lambda x: x is None, wait_fixed=20000)
def fetch(url, **kwargs):
    """
    get html entities from url
    """
    headers = Headers(headers=True).generate()
    default_dict = {'timeout': GET_TIMEOUT, 'verify': False, 'headers': headers}
    kwargs = {**default_dict, **kwargs}

    try:
        logger.info('fetching %s', url)
        response = requests.get(url, **kwargs)
        if response.status_code == 200:
            response.encoding = 'utf-8'
            with open('test.html', 'w', encoding='utf-8') as f:
                f.write(response.text)
            return response.text
    except (requests.ConnectionError, requests.ReadTimeout):
        return None


def process_init_url(url, parsetype, next_type, urls_q, trans_redis):
    """
    process initial url
    """
    try:
        if parsetype == 'html':
            if next_type == 'list':
                html = fetch(url)
                from_page_list(url, html, urls_q)
                logger.info('put new url %s', url)
                get_proxies_html(html, trans_redis)
            if next_type == 'one_page':
                logger.info('put new url %s', url)
                get_proxies_html(url, trans_redis)
            if next_type == 'click_page':
                html = fetch(url)
                suffix_urls = re.findall(r'href="(/dayProxy/ip/\d{4}.html)"[\D,\s]*?href="(/dayProxy/ip/\d{4}.html)"', html)[0]
                for suffix_url in suffix_urls:
                    url_new = _combine_url(url, suffix_url)
                    logger.info('put new url %s', url_new)
                    urls_q.put(url_new)
        if parsetype == 'html_with_base64':
            html = fetch(url)
            logger.info('put new url %s', url)
            _get_proxies_base64_in(html)
        if parsetype == 'json_txt':
            json_txt = fetch(url)
            if json_txt:
                logger.info('put new url %s', url)
                get_proxies_from_json_txt(json_txt, trans_redis)
        if parsetype == 'json':
            json_data = fetch(url)
            if json_data:
                logger.info('put new url %s', url)
                get_proxies_from_json(json_data, trans_redis)
    except RetryError:
        logger.error('fetch %s failed', url)


class Getter(object):
    def __init__(self):
        self.urls_queue = queue.Queue()
        self.redis = RedisClient()
        self.initial_urls = init_urls

    def process_init_urls(self, initial_urls, trans_redis):
        """
        build some Thread pools for processing initial urls
        """
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures_ = [executor.submit(process_init_url,
                                       url_item['url'],
                                       url_item['parsetype'],
                                       url_item.get('next_type', None),
                                       self.urls_queue, trans_redis) for url_item in initial_urls]
            as_completed(futures_)
            logger.info('all initial urls processed')

    def __call__(self, *args, **kwargs):
        self.process_init_urls(self.initial_urls, self.redis)
        while True:
            try:
                url = self.urls_queue.get(timeout=4)
                html = fetch(url)
                if html:
                    get_proxies_html(html, self.redis)
            except queue.Empty:
                logger.info('queue is empty')
                break


if __name__ == '__main__':
    getter = Getter()
    getter()