import socket
import functools
from .packages.urllib3.response import HTTPResponse
from .packages.urllib3.util import Timeout as TimeoutSauce
from .packages.urllib3.exceptions import MaxRetryError
from .packages.urllib3.exceptions import TimeoutError
from .packages.urllib3.exceptions import SSLError as _SSLError
from .packages.urllib3.exceptions import HTTPError as _HTTPError
from .packages.urllib3.exceptions import ProxyError as _ProxyError
from .exceptions import ConnectionError, Timeout, SSLError, ProxyError


def remap_exception(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except socket.error as sockerr:
            raise ConnectionError(sockerr)

        except MaxRetryError as e:
            raise ConnectionError(e)

        except _ProxyError as e:
            raise ProxyError(e)

        except (_SSLError, _HTTPError) as e:
            if isinstance(e, _SSLError):
                raise SSLError(e)
            elif isinstance(e, TimeoutError):
                raise Timeout(e)
            else:
                raise
    return wrapper


class UploadStream(object):

    def __init__(self, adapter, request, timeout=None, verify=True, cert=None, proxies=None, assertHostName=None):
        self._adapter = adapter
        self._request = request
        self._timeout = timeout
        self._verify = verify
        self._cert = cert
        self._proxies = proxies

        self._connection = None
        self._lowLevelConnection = None
        self._assertHostName = assertHostName

    @remap_exception
    def open(self):
        self._connection = self._adapter.get_connection(self._request.url, self._proxies)

        self._adapter.cert_verify(self._connection, self._request.url, self._verify, self._cert, self._assertHostName)
        url = self._adapter.request_url(self._request, self._proxies)
        self._adapter.add_headers(self._request)

        timeout = TimeoutSauce(connect=self._timeout, read=self._timeout)

        try:
            if hasattr(self._connection, 'proxy_pool'):
                self._connection = self._connection.proxy_pool

            self._lowLevelConnection = self._connection._get_conn(timeout=timeout)
            self._lowLevelConnection.putrequest(self._request.method, url, skip_accept_encoding=True)

            for header, value in self._request.headers.items():
                self._lowLevelConnection.putheader(header, value)

            self._lowLevelConnection.endheaders()

        except socket.error as sockerr:
            raise ConnectionError(sockerr)

        except MaxRetryError as e:
            raise ConnectionError(e)

        except _ProxyError as e:
            raise ProxyError(e)

        except (_SSLError, _HTTPError) as e:
            if isinstance(e, _SSLError):
                raise SSLError(e)
            elif isinstance(e, TimeoutError):
                raise Timeout(e)
            else:
                raise

    @remap_exception
    def close(self):
        # nothing to close
        if not self._connection:
            return

        r = self._lowLevelConnection.getresponse()
        resp = HTTPResponse.from_httplib(r,
            pool=self._connection,
            connection=self._lowLevelConnection,
            preload_content=False,
            decode_content=False
        )

        r = self._adapter.build_response(self._request, resp)
        # consume whatever server sent back
        r.content
        self._connection.close()
        return r

    @remap_exception
    def write(self, chunk):
        self._lowLevelConnection.send(chunk)

