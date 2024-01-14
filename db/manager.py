"""
This module provides a class for managing a database.
"""
from hashlib import blake2b
import json
from pathlib import Path
from typing import Any, Callable, Coroutine, Dict, Optional, Union
from loguru import logger
from pandas import DataFrame
from .sqlite import AsyncPickleSQLiteDB


class DatabaseManager:   
    """This class provides methods for managing a database."""
    def __init__(self, data_path: Path):
        self._data_path = data_path
        self._db_name = "db"
        self._cache_name = "cache"
        self._DB = AsyncPickleSQLiteDB(self._data_path, self._db_name)
        self._DBCache = AsyncPickleSQLiteDB(self._data_path, self._cache_name)

    def get_db(self, namespace: str) -> AsyncPickleSQLiteDB:
        """
        Get the database for the given namespace.
        """
        return self._DBCache if 'cache' in namespace else self._DB

    async def clear_cache(self):
        await self._DBCache.close()
        cache_path = self._data_path / 'cache.sqlite'
        if cache_path.is_file():
            cache_path.unlink()
        self._DBCache = AsyncPickleSQLiteDB(self._data_path, name='cache')

    def _hash(self, string: str) -> str:
        return blake2b(string.encode('utf-8'), digest_size=32).hexdigest()

    def generate_db_key(
            self,
            namespace: str,
            params: Union[Dict, str]
        ) -> str:
        param_str = json.dumps(params, separators=(',', ':')) if isinstance(params, Dict) else params
        return self._hash(namespace + param_str)

    async def contains_encoded(self, namespace: str, params: Dict) -> bool:
        return await self.get_db(namespace).contains_encoded(self.generate_db_key(namespace, params))

    async def load_encoded(self, namespace: str, params: Dict) -> Optional[Any]:
        db = self.get_db(namespace)
        key = self.generate_db_key(namespace, params)
        if await db.contains_encoded(key):
            return await db.get_encoded(key)
        return None

    async def save_encoded(
            self,
            namespace: str,
            params: Dict,
            json_data: Any
        ) -> None:
        await self.get_db(namespace).save_encoded(self.generate_db_key(namespace, params), json_data)
        logger.info(f"{namespace} :: Saving data : {params}")

    async def save_dataframe(
            self,
            table_name: str,
            df: DataFrame
        ) -> None:
        await self.get_db(table_name).save_dataframe(table_name, df)

    async def fetch_encoded(
            self,
            namespace: str,
            func: Callable[[Dict], Coroutine[Any, Any, Optional[Any]]],
            params: Dict={},
            save: bool = True
        ) -> Optional[Any]:
        data = await self.load_encoded(namespace, params)
        if data:
            logger.info(f"{namespace} :: Loaded data : {params}")
            return data

        try:
            _data = await func(params)
            if save and _data:
                await self.save_encoded(namespace, params, _data)
            return _data
        except Exception as e:
            logger.exception(f"Error fetching data: {e}")
            return None

    async def delete_encoded(self, namespace: str, params: Dict) -> None:
        await self.get_db(namespace).delete_encoded(self.generate_db_key(namespace, params))
        logger.info(f"{namespace} :: Deleting data : {params}")

    async def close_connections(self):
        await self._DB.close()
        await self._DBCache.close()