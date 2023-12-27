"""
This module provides classes for managing HTTP requests and rate limiting.
"""
from abc import ABC, abstractmethod
import asyncio
from functools import wraps
import time
from typing import Any, Coroutine, Dict, Generic, List, Optional, TypeVar
import httpx
from httpx import Headers, Response
from loguru import logger
from ..db import DatabaseManager


RequestorType = TypeVar("RequestorType", bound="Requestor")
LimiterType = TypeVar("LimiterType", bound="RateLimitContext")
T = TypeVar("T")


from abc import ABC, abstractmethod
import time
import asyncio
from typing import Dict


class RateLimitContext(ABC):
    def __init__(self, max_calls: int, period: int, max_concurrency: int = 24):
        self._rate = max_calls / period  # Tokens added per second
        self._tokens = max_calls  # Maximum tokens
        self._last = time.monotonic()
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def _acquire_token(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self._tokens + elapsed * self._rate, self._tokens)
            if self._tokens < 1:
                await asyncio.sleep(1 - self._tokens / self._rate)
                self._tokens = 1
            self._tokens -= 1

    async def limit_request(self):
        async with self._semaphore:
            await self._acquire_token()

    @abstractmethod
    async def adjust_rate_limit(self, headers: Headers):
        """
        Adjusts the rate limit based on the response headers from an API.
        Subclasses should implement this to handle specific API rate limiting schemes.
        """
        raise NotImplementedError("All subclasses must implement the adjust_rate_limit method")
    

class BinanceRateLimitContext(RateLimitContext):
    def __init__(self, limit: int, interval: int):
        super().__init__(limit, interval)

    async def limit(self, headers: Headers):
        weight = int(headers.get('X-MBX-USED-WEIGHT', 0))
        weight_1m = int(headers.get('X-MBX-USED-WEIGHT-1M', 0))
        order_count_1m = int(headers.get('X-MBX-ORDER-COUNT-1M', 0))
        ip_weight_1m = int(headers.get('X-SAPI-USED-IP-WEIGHT-1M:', 0))
        retry_after = int(headers.get('Retry-After', 0))
        
        required_tokens = weight + weight_1m + order_count_1m + ip_weight_1m
        
        if retry_after > 0:
            async with self._semaphore:
                await self._acquire_token(weight)
                await asyncio.sleep((required_tokens - self._tokens) / self._rate)


class AsyncClient(ABC):
    def __init__(
        self,
        base_url: str,
        headers: dict,
        follow_redirects: bool = True,
        http2: bool = True,
        timeout: int = 30,
        rate_limit_context: LimiterType = None
    ):
        self.base_url = base_url
        self._headers = headers
        self._session = httpx.AsyncClient(headers=self._headers,
                                          follow_redirects=follow_redirects,
                                          http2=http2,
                                          timeout=timeout)
        self._rate_limit_context = rate_limit_context

    async def __aenter__(self):
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._session.__aexit__(exc_type, exc, tb)
        
    async def _post_signed(self, signed_request: str, data: Any = None, **kwargs: Any) -> Response:
        response = await self._session.post(signed_request, data=data, **kwargs)
        return await self._handle(response)

    async def _delete_signed(self, signed_request: str, **kwargs: Any) -> Response:
        response = await self._session.delete(signed_request, **kwargs)
        return await self._handle(response)
    
    @abstractmethod
    async def get(self, endpoint: str, params: Optional[dict] = None):
        """
        """
        raise NotImplementedError("All subclasses must implement the get method")

    async def _get(self, endpoint: str, params: Optional[str] = None) -> Response:
        url = f"{self.base_url}{endpoint}"
        if params:
            url += f"?{params}"

        response = await self._session.get(url)
        await self._rate_limit_context.adjust_rate_limit(response.headers)
        return await self._handle(response)

    async def _post(self, endpoint: str, data: Any = None, **kwargs: Any) -> Response:
        url = f"{self.base_url}{endpoint}"
        response = await self._session.post(url, data=data, **kwargs)
        return await self._handle(response)

    async def _put(self, endpoint: str, data: Any = None, **kwargs: Any) -> Response:
        url = f"{self.base_url}{endpoint}"
        response = await self._session.put(url, data=data, **kwargs)
        return await self._handle(response)

    async def _delete(self, endpoint: str, **kwargs: Any) -> Response:
        url = f"{self.base_url}{endpoint}"
        response = await self._session.delete(url, **kwargs)
        return await self._handle(response)
    
    async def _handle(self, response: httpx.Response) -> Response:
        try:
            response.raise_for_status()
            return response

        except httpx.HTTPStatusError as e:
            # Raised when response status code is 400 or higher
            logger.error(f"HTTP Response Error: {e.response.status_code} {e.response.text}")

        except httpx.RequestError as e:
            # Raised in case of connection errors
            logger.error(f"Connection error occurred while handling the request: {e}")

        except Exception as e:
            # Catch any other exceptions that may occur during response handling
            logger.error(f"Unexpected error occurred while processing the response: {e}")

        return response


class Requestor(Generic[T], ABC):
    """
    This class provides methods for managing HTTP requests.
    """
    def __init__(
        self,
        db_manager: DatabaseManager,
        client: AsyncClient,
        endpoint: str,
        required_params: List[str],
        default_return_value: Any,
    ) -> None:
        self._dbm = db_manager
        self._client = client
        self.endpoint = endpoint
        self.params = {}
        self.__required_params = required_params
        self.__async_tasks: List[Coroutine] = []
        self._return_data: bool = True
        self._save: bool = True
        self._delete_from_db: bool = False
        self._default_return_value: T = default_return_value
    
    @abstractmethod
    async def _request_func(self, params: Dict) -> Coroutine[Any, Any, Optional[Any]]:
        raise NotImplementedError("All subclasses must implement the _request_func method")
    
    def __has_required_params(self) -> bool:
        for p in self.__required_params:
            if p not in self.params:
                logger.warning(f"Missing required param: {p}")
                
        return all([p in self.params for p in self.__required_params])

    def return_data(self: RequestorType, return_data: bool) -> RequestorType:
        self._return_data = return_data
        return self

    def save(self: RequestorType, save: bool) -> RequestorType:
        self._save = save
        return self

    def delete_from_db(self: RequestorType) -> RequestorType:
        if not self.__has_required_params():
            raise RuntimeError("Required params not set")

        self.__async_tasks.append(self._dbm.delete_encoded(self.endpoint, self.params))
        return self
    
    async def request_only(self) -> None:
        if await self._dbm.contains_encoded(self.endpoint, self.params):
            return
        await self.request()

    async def request(self) -> T:
        """Request order book for symbol and save them to cache"""
        if not self.__has_required_params():
            logger.warning("Required params not set")
            return self._default_return_value

        if self.__async_tasks:
            tasks = self.__async_tasks.copy()
            self.__async_tasks = []
            for t in tasks:
                await t

        resp = await self._dbm.fetch_encoded(
            self.endpoint,
            self._request_func,
            self.params,
            self._save,
        )
        if not self._return_data or not resp:
            return self._default_return_value
        else:
            return resp.json()