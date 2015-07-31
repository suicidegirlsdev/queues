"""
Backend for redis.

Requires redis.py from the redis source (found in client-libraries/python).
"""

from queues.backends.base import BaseQueue
from queues import InvalidBackend, QueueException
import os
import math
from urlparse import urlparse

try:
    import redis
except ImportError:
    raise InvalidBackend("Unable to import redis.")

REDIS_URL = DB = None

try:
    from django.conf import settings
    REDIS_URL = getattr(settings, 'REDIS_URL', None)
    TIMEOUT = getattr(settings, 'QUEUE_REDIS_TIMEOUT', None)
except:
    REDIS_URL = os.environ.get('REDIS_URL', None)
    TIMEOUT = os.environ.get('QUEUE_REDIS_TIMEOUT', None)

if not REDIS_URL:
    raise InvalidBackend("REDIS_URL not set.")

redis_url = urlparse(REDIS_URL)

# Pop DB number from query string.
path = redis_url.path[1:]
path = path.split('?', 2)[0]
DB = int(path or 0)

host = redis_url.hostname
password = redis_url.password
port = redis_url.port

if not host:
    raise InvalidBackend("REDIS_URL is missing host (url format should be redis://username:password@hostname:port/db_number).")

try:
    port = int(port)
except ValueError:
    raise InvalidBackend("Port portion of REDIS_URL should be an integer.")


def _get_connection(host=host, port=port, db=DB, password=password, timeout=TIMEOUT):
    kwargs = {'host' : host, 'port' : port}
    if DB:
        kwargs['db'] = DB
    if password:
        kwargs['password'] = password
    if timeout:
        kwargs['timeout'] = float(timeout)
    try:
        # Try using the "official" redis kwargs
        return redis.Redis(**kwargs)
    except TypeError, e:
        # Possibly 'timeout' caused an issue...
        if 'timeout' not in kwargs:
            raise
        # Try using Andy McCurdy's library
        kwargs['socket_timeout'] = kwargs.pop('timeout')
        return redis.Redis(**kwargs)


class Queue(BaseQueue):
    def __init__(self, name, connection=None):
        try:
            self.name = name
            self.backend = 'redis'
            self._connection = connection or _get_connection()
        except redis.RedisError, e:
            raise QueueException, "%s" % e

    def read(self, block=False, timeout=0):
        try:
            if block:
                # Redis requires an integer, so round a float UP to the nearest
                # int (0.1 -> 1).
                try:
                    m = self._connection.blpop(self.name, timeout=int(math.ceil(timeout)))[1]
                except TypeError:
                    m = None
            else:
                m = self._connection.lpop(self.name)
            if m is None:
                raise QueueException('Queue is empty')
            return m
        except redis.RedisError, e:
            raise QueueException(str(e))

    def write(self, value):
        try:
            resp = self._connection.rpush(self.name, value)
            if resp in ('OK', 1):
                return True
            else:
                return False
        except redis.RedisError, e:
            raise QueueException, "%s" % e

    def __len__(self):
        try:
            return self._connection.llen(self.name)
        except redis.RedisError, e:
            raise QueueException, "%s" % e

    def __repr__(self):
        return "<Queue %s>" % self.name


def create_queue():
    """This isn't required, so we noop.  Kept here for swapability."""
    return True


def delete_queue(name):
    """Delete a queue"""
    try:
        resp = _get_connection().delete(name)
        if resp and resp == 1:
            return True
        else:
            return False
    except redis.RedisError, e:
        raise QueueException, "%s" % e


def get_list():
    return _get_connection().keys('*')
