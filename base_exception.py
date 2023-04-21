class PoolEmptyException(Exception):
    def __str__(self):
        """
        proxypool is empty
        :return:
        """
        return repr('no proxy in proxypool')
