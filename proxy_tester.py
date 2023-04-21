import asyncio
import aiohttp
import requests
from fake_headers import Headers

from models import Proxy
from trans4redis import RedisClient
from handle_log import get_logger
from setting import TEST_TIMEOUT, TEST_BATCH, TEST_URL, TEST_VALID_STATUS, TEST_ANONYMOUS
from aiohttp import ClientProxyConnectionError, ServerDisconnectedError, ClientOSError, ClientHttpProxyError, \
    ContentTypeError, ClientResponseError
from asyncio import TimeoutError
EXCEPTIONS = (
    ClientProxyConnectionError,
    ConnectionRefusedError,
    TimeoutError,
    ServerDisconnectedError,
    ClientOSError,
    ClientHttpProxyError,
    AssertionError
)

logger = get_logger('Tester')


class Tester(object):
    """
    tester for testing proxies in queue
    """

    def __init__(self):
        """
        self.loop = asyncio.get_event_loop()
        init redis
        """
        self.redis = RedisClient()
        self.loop = asyncio.new_event_loop()
        self.url = 'https://httpbin.org/ip'

    def get_origin_ip(self):
        response = requests.get(self.url, timeout=TEST_TIMEOUT)
        resp_json = response.json()
        self.origin_ip = resp_json['origin']

    async def test(self, proxy: Proxy):
        """
        test single proxy
        :param proxy: Proxy object
        :return:
        """
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            try:
                headers = Headers(headers=True).generate()
                logger.debug(f'testing {proxy}')
                # if TEST_ANONYMOUS is True, make sure that
                # the proxy has the effect of hiding the real IP
                if TEST_ANONYMOUS:
                    async with session.get(self.url,
                                           proxy=f'http://{proxy}',
                                           timeout=TEST_TIMEOUT,
                                           headers=headers) as response:
                        resp_json = await response.json()
                        anonymous_ip = resp_json['origin']
                    assert self.origin_ip != anonymous_ip
                    assert proxy.ip == anonymous_ip
                async with session.get(TEST_URL,
                                       proxy=f'http://{proxy}',
                                       timeout=TEST_TIMEOUT,
                                       allow_redirects=False, headers=headers) as response:
                    if response.status in TEST_VALID_STATUS:
                        self.redis.max(proxy)
                        logger.debug(f'proxy {proxy} is valid, set max score')
                    else:
                        self.redis.decrease(proxy)
                        logger.debug(f'proxy {proxy} is invalid, decrease score')
            except (ContentTypeError, ClientResponseError, AttributeError) as e:
                logger.debug(f'proxy {proxy} is invalid, decrease score \n {e}')
            except EXCEPTIONS:
                self.redis.decrease(proxy)
                logger.debug(f'proxy {proxy} is invalid, decrease score')

    def __call__(self, *args, **kwargs):
        """
        test main method
        :return:
        """
        asyncio.set_event_loop(self.loop)
        if TEST_ANONYMOUS:
            self.get_origin_ip()
        # event loop of aiohttp
        logger.info('stating tester...')
        count = self.redis.count()
        logger.debug(f'{count} proxies to test')
        cursor = 0
        while True:
            logger.debug(f'testing proxies use cursor {cursor}, count {TEST_BATCH}')
            cursor, proxies = self.redis.batch(cursor, count=TEST_BATCH)
            if proxies:
                tasks = [self.loop.create_task(self.test(proxy)) for proxy in proxies]
                self.loop.run_until_complete(asyncio.wait(tasks))
            if not cursor:
                break


if __name__ == '__main__':
    tester = Tester()
    tester()

    # tester.get_origin_ip()
