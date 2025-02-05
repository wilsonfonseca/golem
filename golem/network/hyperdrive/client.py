import json
import logging
import math
from ipaddress import AddressValueError, ip_address
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import collections

import requests
from requests import HTTPError

from golem_messages import helpers as msg_helpers
from twisted.internet.defer import inlineCallbacks
from twisted.web.client import readBody
from twisted.web.http_headers import Headers

from golem.core.golem_async import AsyncHTTPRequest
from golem.resource.client import IClient, ClientOptions

log = logging.getLogger(__name__)


def to_hyperg_peer(host: str, port: int) -> Dict[str, Tuple[str, int]]:
    return {'TCP': (host, port)}


# TODO: Change 'Optional[Union[int, float]]' hint to 'Union[int, float]' and
#       remove the 'isinstance' check when HyperdriveResourceManager is removed
def round_timeout(value: Optional[Union[int, float]]) -> int:
    if not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"Invalid timeout: {value}")
    return int(math.ceil(value))


class HyperdriveClient(IClient):
    """
    Enables communication between Golem and the Hyperdrive service.
    """

    CLIENT_ID = 'hyperg'
    VERSION = 1.1
    DEFAULT_ENDPOINT = 'api'
    HEADERS = {'content-type': 'application/json'}

    def __init__(self, port, host, timeout=None):
        super().__init__()
        # connection / read timeout
        self.timeout = timeout
        # API destination address
        self._url = f'http://{host}:{port}'

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.CLIENT_ID} at {self._url}>'

    @classmethod
    def build_options(cls, **kwargs):
        return HyperdriveClientOptions(
            cls.CLIENT_ID, cls.VERSION, options=kwargs)

    def id(self, client_options=None, *args, **kwargs):
        return self._request(command='id')

    def addresses(self):
        response = self._request(command='addresses')
        addresses = response['addresses']

        for proto, entry in addresses.items():
            addresses[proto] = (entry['address'], entry['port'])

        return addresses

    def add(self, files, client_options=None, **kwargs):
        response = self._request(
            command='upload',
            id=kwargs.get('id'),
            files=files,
            timeout=round_timeout(client_options.timeout)
        )
        return response['hash']

    def restore(self, content_hash, client_options=None, **kwargs):
        response = self._request(
            command='upload',
            id=kwargs.get('id'),
            hash=content_hash,
            timeout=round_timeout(client_options.timeout)
        )
        return response['hash']

    def get(self, content_hash, client_options=None, **kwargs):
        path = kwargs['filepath']
        params = self._download_params(content_hash, client_options, **kwargs)
        response = self._request(**params)
        return [(path, content_hash, response['files'])]

    @classmethod
    def _download_params(cls, content_hash, client_options, **kwargs):
        path = kwargs['filepath']
        peers, size, timeout = None, None, None

        if client_options:
            size = client_options.get(cls.CLIENT_ID, cls.VERSION, 'size')
            if size:
                timeout = msg_helpers.maximum_download_time(size).seconds
            else:
                timeout = None
            peers = client_options.peers

        return dict(
            command='download',
            hash=content_hash,
            dest=path,
            peers=peers or [],
            size=size,
            timeout=timeout
        )

    def cancel(self, content_hash):
        response = self._request(
            command='cancel',
            hash=content_hash
        )
        return response['hash']

    def _request(
            self,
            endpoint: str = DEFAULT_ENDPOINT,
            **data
    ) -> Dict:
        if endpoint and endpoint[0] == '/':
            endpoint = endpoint[1:]

        response = requests.post(url=f'{self._url}/{endpoint}',
                                 headers=self.HEADERS,
                                 data=json.dumps(data),
                                 timeout=self.timeout)

        try:
            response.raise_for_status()
        except HTTPError:
            if response.text:
                raise HTTPError('Hyperdrive HTTP {} error: {}'.format(
                    response.status_code, response.text), response=response)
            raise

        if response.content:
            return json.loads(response.content.decode('utf-8'))
        return dict()


class HyperdriveAsyncClient(HyperdriveClient):

    RAW_HEADERS = Headers({'Content-Type': ['application/json']})
    ENCODING = 'utf-8'

    def add_async(
            self,
            files: Dict[str, str],
            client_options: ClientOptions,
            **kwargs
    ):
        params = dict(
            command='upload',
            id=kwargs.get('id'),
            files=files,
            timeout=round_timeout(client_options.timeout))

        return self._async_request(
            params=params,
            parser=lambda res: res['hash'])

    def restore_async(
            self,
            content_hash: str,
            client_options: ClientOptions,
            **kwargs
    ):
        params = dict(
            command='upload',
            id=kwargs.get('id'),
            hash=content_hash,
            timeout=round_timeout(client_options.timeout))

        return self._async_request(
            params=params,
            parser=lambda res: res['hash'])

    def get_async(
            self,
            content_hash: str,
            filepath: str,
            client_options: ClientOptions,
            **kwargs
    ):
        params = self._download_params(
            content_hash,
            client_options,
            filepath=filepath,
            **kwargs)

        return self._async_request(
            params=params,
            parser=lambda res: [(filepath, content_hash, res['files'])])

    def cancel_async(
            self,
            content_hash: str,
            **kwargs
    ):
        params = dict(
            command='cancel',
            id=kwargs.get('id'),
            hash=content_hash)

        return self._async_request(
            params=params,
            parser=lambda response: response['hash'])

    def resources_async(
            self,
    ):
        return self._async_request(
            endpoint='resources',
            method=b'GET')

    def resource_async(
            self,
            content_hash: str,
    ):
        return self._async_request(
            endpoint=f'resources/{content_hash}',
            method=b'GET')

    @inlineCallbacks
    def _async_request(
            self,
            endpoint: str = HyperdriveClient.DEFAULT_ENDPOINT,
            method: bytes = b'POST',
            params: Optional[Dict] = None,
            parser: Optional[Callable[[Dict], Any]] = None,
    ):
        body = None

        if endpoint and endpoint[0] == '/':
            endpoint = endpoint[1:]
        if params:
            body = json.dumps(params).encode(self.ENCODING)

        uri = f'{self._url}/{endpoint}'.encode(self.ENCODING)
        response = yield AsyncHTTPRequest.run(
            method,
            uri=uri,
            headers=self.RAW_HEADERS,
            body=body
        )

        try:
            response_body = yield readBody(response)
        except Exception:  # pylint: disable=broad-except
            raise HTTPError(response.decode(self.ENCODING))

        decoded = response_body.decode(self.ENCODING)
        deserialized = json.loads(decoded)

        if parser:
            return parser(deserialized)
        return deserialized


class HyperdriveClientOptions(ClientOptions):

    max_peers = 8

    def filtered(self,
                 client_id: str = HyperdriveClient.CLIENT_ID,
                 version: float = HyperdriveClient.VERSION,
                 verify_peer: Optional[Callable] = None,
                 **_kwargs) -> Optional['HyperdriveClientOptions']:

        opts = super().filtered(client_id, version)

        if not opts:
            pass

        elif opts.version < 1.0:
            log.warning('Resource client: incompatible version: %s',
                        opts.version)

        elif not isinstance(opts.options, dict):
            log.warning('Resource client: invalid type: %s; dict expected',
                        type(opts.options))

        elif not isinstance(opts.options.get('peers'), collections.Iterable):
            log.warning('Resource client: peers not provided')

        else:
            opts.options['peers'] = self.filter_peers(opts.options['peers'],
                                                      verify_peer)
            return opts

    @classmethod
    def filter_peers(cls,
                     peers: Iterable,
                     verify_peer: Optional[Callable] = None) -> List:
        result = list()

        for peer in peers:
            entry = cls.filter_peer(peer, verify_peer)
            if entry:
                result.append(entry)
            if len(result) == cls.max_peers:
                break

        return result

    @classmethod
    def filter_peer(cls, peer, verify_peer: Optional[Callable] = None) \
            -> Optional[Dict]:

        if not isinstance(peer, dict):
            return None

        new_entry = dict()

        for protocol, entry in peer.items():
            try:
                ip_str = entry[0]
                port = int(entry[1])

                ip_address(ip_str)  # may raise an exception

                if protocol != 'TCP':
                    raise ValueError('protocol {} is invalid'.format(protocol))
                if not 0 < port < 65536:
                    raise ValueError('port {} is invalid'.format(port))
                if verify_peer and not verify_peer(ip_str, port):
                    raise ValueError('peer not accepted')

            except (ValueError, TypeError, AddressValueError) as err:
                log.debug('Resource client: %r (%s)', err, peer)
            else:
                new_entry[protocol] = (ip_str, port)

        return new_entry
