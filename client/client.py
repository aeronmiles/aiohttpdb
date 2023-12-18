"""
This module provides classes for managing HTTP requests and rate limiting.
"""
from abc import ABC, abstractmethod
import asyncio
from functools import wraps
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional, TypeVar, Union
import httpx
from loguru import logger
from ..db import DatabaseManager

RequestorType = TypeVar("RequestorType", bound="Requestor")


class Requestor(ABC):
    """
    This class provides methods for managing HTTP requests.
    """
    def __init__(
        self,
        db_manager: DatabaseManager,
        endpoint: str,
        required_params: List[str],
        default_return_value: Any,
    ) -> None:
        self._dbm = db_manager
        self.endpoint = endpoint
        self.params = {}
        self.__required_params = required_params
        self.__async_tasks: List[Coroutine] = []
        self._return_data: bool = True
        self._save: bool = True
        self._delete_from_db: bool = False
        self._default_return_value = default_return_value
    
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

    async def request(self) -> Dict:
        """Request order book for symbol and save them to cache"""
        if not self.__has_required_params():
            logger.warning("Required params not set")
            return self._default_return_value

        if self.__async_tasks:
            for t in self.__async_tasks:
                await t
            self.__async_tasks = []

        resp = await self._dbm.fetch_encoded(
            self.endpoint,
            self._request_func,
            self.params,
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


class AsyncClient(ABC):
    def __init__(self, base_url: str, headers: dict, follow_redirects: bool = True, http2: bool = True, timeout: int = 30):
        self.base_url = base_url
        self._headers = headers
        self._session = httpx.AsyncClient(headers=self._headers,
                                          follow_redirects=follow_redirects,
                                          http2=http2,
                                          timeout=timeout)

    async def __aenter__(self):
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._session.__aexit__(exc_type, exc, tb)
        
    async def _post_signed(self, signed_request: str, data: Any = None, **kwargs: Any) -> dict:
        response = await self._session.post(signed_request, data=data, **kwargs)
        return await self._handle(response)

    async def _delete_signed(self, signed_request: str, **kwargs: Any) -> dict:
        response = await self._session.delete(signed_request, **kwargs)
        return await self._handle(response)

    async def _get(self, endpoint: str, request: Optional[str] = None) -> dict:
        url = f"{self.base_url}{endpoint}"
        if request:
            url += f"?{request}"

        response = await self._session.get(url)
        return await self._handle(response)

    async def _post(self, endpoint: str, data: Any = None, **kwargs: Any) -> dict:
        url = f"{self.base_url}{endpoint}"
        response = await self._session.post(url, data=data, **kwargs)
        return await self._handle(response)

    async def _put(self, endpoint: str, data: Any = None, **kwargs: Any) -> dict:
        url = f"{self.base_url}{endpoint}"
        response = await self._session.put(url, data=data, **kwargs)
        return await self._handle(response)

    async def _delete(self, endpoint: str, **kwargs: Any) -> dict:
        url = f"{self.base_url}{endpoint}"
        response = await self._session.delete(url, **kwargs)
        return await self._handle(response)
    
    async def _handle(self, response: httpx.Response) -> dict:
        try:
            response.raise_for_status()
            return response.json()

        except httpx.HTTPStatusError as e:
            # Raised when response status code is 400 or higher
            error_msg = f"HTTP Response Error: {e.response.status_code} {e.response.text}"
            e.args = (error_msg, )  # Update the message
            raise

        except httpx.RequestError as e:
            # Raised in case of connection errors
            raise RuntimeError(f"Connection error occurred while handling the request: {e}") from e

        except Exception as e:
            # Catch any other exceptions that may occur during response handling
            raise RuntimeError(f"Unexpected error occurred while processing the response: {e}") from e

