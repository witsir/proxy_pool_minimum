import time
import multiprocessing
from proxy_server import app
from proxy_getter import Getter
from proxy_tester import Tester
from setting import APP_PROD_METHOD_GEVENT, APP_PROD_METHOD_MEINHELD, APP_PROD_METHOD_TORNADO, CYCLE_GETTER, \
    CYCLE_TESTER, API_HOST, \
    API_THREADED, API_PORT, ENABLE_SERVER, IS_PROD, APP_PROD_METHOD, \
    ENABLE_GETTER, ENABLE_TESTER, IS_WINDOWS
from handle_log import get_logger

logger = get_logger('Scheduler')

if IS_WINDOWS:
    multiprocessing.freeze_support()

tester_process, getter_process, server_process = None, None, None


class Scheduler(object):
    """
    scheduler
    """

    def run_tester(self, cycle=CYCLE_TESTER):
        """
        run tester
        """
        if not ENABLE_TESTER:
            logger.info('tester not enabled, exit')
            return
        tester = Tester()
        loop = 0
        while True:
            logger.debug(f'tester loop {loop} start...')
            tester()
            loop += 1
            time.sleep(cycle)

    def run_getter(self, cycle=CYCLE_GETTER):
        """
        run getter
        """
        if not ENABLE_GETTER:
            logger.info('getter not enabled, exit')
            return
        getter = Getter()
        loop = 0
        while True:
            logger.debug(f'getter loop {loop} start...')
            getter()
            loop += 1
            time.sleep(cycle)

    def run_server(self):
        """
        run server for api
        """
        if not ENABLE_SERVER:
            logger.info('server not enabled, exit')
            return
        if IS_PROD:
            if APP_PROD_METHOD == APP_PROD_METHOD_GEVENT:
                try:
                    from gevent.pywsgi import WSGIServer
                except ImportError as e:
                    logger.exception(e)
                else:
                    http_server = WSGIServer((API_HOST, API_PORT), app)
                    http_server.serve_forever()

            elif APP_PROD_METHOD == APP_PROD_METHOD_TORNADO:
                try:
                    from tornado.wsgi import WSGIContainer
                    from tornado.httpserver import HTTPServer
                    from tornado.ioloop import IOLoop
                except ImportError as e:
                    logger.exception(e)
                else:
                    http_server = HTTPServer(WSGIContainer(app))
                    http_server.listen(API_PORT)
                    IOLoop.instance().start()

            elif APP_PROD_METHOD == APP_PROD_METHOD_MEINHELD:
                try:
                    import meinheld
                except ImportError as e:
                    logger.exception(e)
                else:
                    meinheld.listen((API_HOST, API_PORT))
                    meinheld.run(app)

            else:
                logger.error("unsupported APP_PROD_METHOD")
                return
        else:
            app.run(host=API_HOST, port=API_PORT, threaded=API_THREADED)

    def run(self):
        global tester_process, getter_process, server_process
        try:
            logger.info('starting proxypool...')
            if ENABLE_TESTER:
                tester_process = multiprocessing.Process(target=self.run_tester)
                tester_process.start()
                logger.info(f'starting tester, pid {tester_process.pid}...')

            if ENABLE_GETTER:
                getter_process = multiprocessing.Process(target=self.run_getter)
                getter_process.start()
                logger.info(f'starting getter, pid {getter_process.pid}...')

            if ENABLE_SERVER:
                server_process = multiprocessing.Process(target=self.run_server)
                server_process.start()
                logger.info(f'starting server, pid {server_process.pid}...')
            #
            tester_process and tester_process.join()
            getter_process and getter_process.join()
            server_process and server_process.join()
        except KeyboardInterrupt:
            logger.info('received keyboard interrupt signal')
            tester_process and tester_process.terminate()
            getter_process and getter_process.terminate()
            server_process and server_process.terminate()
        finally:
            # must call join method before calling is_alive
            tester_process and tester_process.join()
            getter_process and getter_process.join()
            server_process and server_process.join()
            logger.info(
                f'tester is {"alive" if tester_process.is_alive() else "dead"}')
            logger.info(
                f'getter is {"alive" if getter_process.is_alive() else "dead"}')
            logger.info(
                f'server is {"alive" if server_process.is_alive() else "dead"}')
            logger.info('proxy terminated')


if __name__ == '__main__':
    scheduler = Scheduler()
    scheduler.run()
