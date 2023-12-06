"""
This module provides classes for managing HTTP requests and rate limiting.
"""
from abc import ABC
import asyncio
from functools import wraps
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional, TypeVar, Union
import aiohttp
from aiohttp import (
    ClientError,
    ClientResponseError,
    ClientConnectionError,
    ClientPayloadError,
)
from ..db.db_manager import DatabaseManager

RequestorType = TypeVar("RequestorType", bound="Requestor")

class Requestor(ABC):
    """
    This class provides methods for managing HTTP requests.
    """
    def __init__(
        self,
        db_manager: DatabaseManager,
        endpoint: str,
        required_params: Callable[[], bool],
        request_func: Callable[[Dict], Coroutine[Any, Any, Optional[Any]]],
        default_return_value: Any,
    ) -> None:
        self.__dbm = db_manager
        self.endpoint = endpoint
        self.params = {}
        self.__required_params = required_params
        self.__request_func = request_func
        self.__async_tasks: List[Coroutine] = []
        self._cached_only: bool = False
        self._return_data: bool = True
        self._save: bool = True
        self._delete_from_db: bool = False
        self._default_return_value = default_return_value

    def cached_only(
        self: RequestorType, cached_only: bool = True
    ) -> RequestorType:
        self._cached_only = cached_only
        return self

    def return_data(self: RequestorType, return_data: bool) -> RequestorType:
        self._return_data = return_data
        return self

    def save(self: RequestorType, save: bool) -> RequestorType:
        self._save = save
        return self

    def delete_from_db(self: RequestorType) -> RequestorType:
        if not self.__required_params():
            raise RuntimeError("Required params not set")

        self.__async_tasks.append(self.__dbm.delete_encoded(self.endpoint, self.params))
        return self
    
    async def request_only(self) -> None:
        if await self.__dbm.contains_encoded(self.endpoint, self.params):
            return
        await self.request()

    async def request(self) -> Union[List, Dict]:
        """Request order book for symbol and save them to cache"""
        if not self.__required_params():
            raise RuntimeError("Required params not set")

        if self.__async_tasks:
            for t in self.__async_tasks:
                await t
            self.__async_tasks = []

        resp = await self.__dbm.fetch_encoded(
            self.endpoint,
            self.__request_func,
            self.params,
            self._cached_only,
            self._save,
        )
        if not self._return_data or not resp:
            return self._default_return_value
        else:
            return resp


class RateLimitContext:
    def __init__(self, period: int, max_calls: int, safety_margin: float = 0.25, max_concurrency: int = 24):
        assert period > 0, "period must be > 0"
        assert max_calls > 0, "max_calls must be > 0"
        assert max_concurrency > 0, "max_concurrency must be > 0"
        
        self._rate = max_calls / period
        self._capacity = max_calls / (1.0 + safety_margin)
        self._tokens = self._capacity
        self._last = time.monotonic()
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def _acquire_token(self, weight: int):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now

            # Refill tokens
            self._tokens += elapsed * self._rate
            self._tokens = min(self._tokens, self._capacity)

            # Calculate required tokens and wait if necessary
            required_tokens = weight
            if self._tokens < required_tokens:
                await asyncio.sleep((required_tokens - self._tokens) / self._rate)
                self._tokens = 0
            else:
                self._tokens -= required_tokens

    def limit(self, weight: int = 1):
        def wrapper(func):
            @wraps(func)
            async def limited(*args, **kwargs):
                async with self._semaphore:
                    await self._acquire_token(weight)
                    return await func(*args, **kwargs)
            return limited
        return wrapper


class Client(ABC):
    def __init__(self, base_url: str, headers: dict):
        self.base_url = base_url
        self._headers = headers
        self._session = aiohttp.ClientSession(headers=self._headers)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._session.close()

    async def _post_signed(
        self, signed_request: str, data: Any = None, **kwargs: Any
    ) -> dict:
        async with self._session.post(signed_request, data=data, **kwargs) as response:
            return await self._handle(response)

    async def _delete_signed(self, signed_request: str, **kwargs: Any) -> dict:
        async with self._session.delete(signed_request, **kwargs) as response:
            return await self._handle(response)

    async def _get(
        self,
        endpoint: str,
        request: Optional[str] = None,
        allow_redirects: bool = True,
        **kwargs: Any,
    ) -> dict:
        url = f"{self.base_url}{endpoint}"
        if request:
            url += f"?{request}"

        async with self._session.get(
            url, allow_redirects=allow_redirects, **kwargs
        ) as response:
            return await self._handle(response)

    async def _post(self, endpoint: str, data: Any = None, **kwargs: Any) -> dict:
        url = f"{self.base_url}{endpoint}"
        async with self._session.post(url, data=data, **kwargs) as response:
            return await self._handle(response)

    async def _put(self, endpoint: str, data: Any = None, **kwargs: Any) -> dict:
        url = f"{self.base_url}{endpoint}"
        async with self._session.put(url, data=data, **kwargs) as response:
            return await self._handle(response)

    async def _delete(self, endpoint: str, **kwargs: Any) -> dict:
        url = f"{self.base_url}{endpoint}"
        async with self._session.delete(url, **kwargs) as response:
            return await self._handle(response)

    async def _handle(self, response: aiohttp.ClientResponse) -> dict:
        try:
            response.raise_for_status()
            return await response.json()
        except ClientResponseError as e:
            # Raised when response status code is 400 or higher
            error_msg = f"HTTP Response Error: {e.status} {e.message}"
            raise ClientError(
                e.status, error_msg, await response.text(), response.headers
            ) from e
        except ClientConnectionError as e:
            # Raised in case of connection errors
            raise RuntimeError("Connection error occurred: ", str(e)) from e
        except ClientPayloadError as e:
            # Raised when there is a payload error (e.g., malformed data)
            raise RuntimeError("Payload error occurred: ", str(e)) from e
        except Exception as e:
            # Catch any other exceptions
            raise RuntimeError("Unexpected error occurred: ", str(e)) from e