import sys

import redis
from base_exception import PoolEmptyException
from models import Proxy, is_valid_proxy, convert_proxy_or_proxies
from setting import REDIS_CONNECTION_STRING, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB, REDIS_KEY, PROXY_SCORE_MAX, PROXY_SCORE_MIN, \
    PROXY_SCORE_INIT
from random import choice
from typing import List
from handle_log import get_logger


REDIS_CLIENT_VERSION = redis.__version__
IS_REDIS_VERSION_2 = REDIS_CLIENT_VERSION.startswith('2.')
logger = get_logger('redis_client')


class RedisClient(object):
    """
    redis connection client of proxypool
    """

    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=REDIS_DB,
                 connection_string=REDIS_CONNECTION_STRING, **kwargs):
        """
        init redis client
        :param host: redis host
        :param port: redis port
        :param password: redis password
        :param connection_string: redis connection_string
        """
        # if set connection_string, just use it
        if connection_string:
            self.db = redis.StrictRedis.from_url(connection_string, decode_responses=True, **kwargs)
        else:
            self.db = redis.StrictRedis(
                host=host, port=port, password=password, db=db, decode_responses=True, **kwargs)

    def add(self, proxy: Proxy, score=PROXY_SCORE_INIT) -> int:
        """
        add proxy and set it to init score
        :param proxy: proxy, ip:port, like 8.8.8.8:88
        :param score: int score
        :return: result
        """
        if not is_valid_proxy(f'{proxy}'):
            logger.warn(f'invalid proxy {proxy}, throw it')
        if not self.exists(proxy):
            if IS_REDIS_VERSION_2:
                return self.db.zadd(REDIS_KEY, score, str(proxy))
            return self.db.zadd(REDIS_KEY, {str(proxy): score})

    def random(self) -> Proxy:
        """
        get random proxy
        firstly try to get proxy with max score
        if not exists, try to get proxy by rank
        if not exists, raise error
        :return: proxy, like 8.8.8.8:8
        """
        # try to get proxy with max score
        proxies = self.db.zrangebyscore(
            REDIS_KEY, PROXY_SCORE_MAX, PROXY_SCORE_MAX)
        if len(proxies):
            return convert_proxy_or_proxies(choice(proxies))
        # else get proxy by rank
        proxies = self.db.zrevrange(
            REDIS_KEY, PROXY_SCORE_MIN, PROXY_SCORE_MAX)
        if len(proxies):
            return convert_proxy_or_proxies(choice(proxies))
        # else raise error
        raise PoolEmptyException

    def decrease(self, proxy: Proxy):
        """
        decrease score of proxy, if small than PROXY_SCORE_MIN, delete it
        :param proxy: proxy
        :return: new score
        """
        if IS_REDIS_VERSION_2:
            self.db.zincrby(REDIS_KEY, str(proxy), -1)
        else:
            self.db.zincrby(REDIS_KEY, -1, str(proxy))
        score = self.db.zscore(REDIS_KEY, str(proxy))
        logger.info(f'{proxy} score decrease 1, current {score}')
        if score <= PROXY_SCORE_MIN:
            logger.info(f'{proxy} current score {score}, remove')
            self.db.zrem(REDIS_KEY, str(proxy))

    def exists(self, proxy: Proxy) -> bool:
        """
        if proxy exists
        :param proxy: proxy
        :return: if exists, bool
        """
        return not self.db.zscore(REDIS_KEY, str(proxy)) is None

    def max(self, proxy: Proxy) -> int:
        """
        set proxy to max score
        :param proxy: proxy
        :return: new score
        """
        logger.info(f'{proxy} is valid, set to {PROXY_SCORE_MAX}')
        if IS_REDIS_VERSION_2:
            return self.db.zadd(REDIS_KEY, PROXY_SCORE_MAX, str(proxy))
        return self.db.zadd(REDIS_KEY, {str(proxy): PROXY_SCORE_MAX})

    def count(self) -> int:
        """
        get count of proxies
        :return: count, int
        """
        return self.db.zcard(REDIS_KEY)

    def all(self) -> List[Proxy]:
        """
        get all proxies
        :return: list of proxies
        """
        return convert_proxy_or_proxies(self.db.zrangebyscore(REDIS_KEY, PROXY_SCORE_MIN, PROXY_SCORE_MAX))

    def batch(self, cursor, count) -> (int, List[Proxy]):
        """
        get batch of proxies
        :param cursor: scan cursor
        :param count: scan count
        :return: list of proxies
        """
        cursor, proxies = self.db.zscan(REDIS_KEY, cursor, count=count)
        return cursor, convert_proxy_or_proxies([i[0] for i in proxies])


if __name__ == '__main__':
    conn = RedisClient()
    zset = conn.db.zrangebyscore('proxies:universal', '-inf', '+inf', withscores=True)
    for member, score in zset:
        print(member, score)
        conn.db.zadd('proxies:universal', {member: 10})