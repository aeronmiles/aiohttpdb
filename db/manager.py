"""
This module provides a class for managing a database.
"""
from hashlib import blake2b
import json
from pathlib import Path
from typing import Any, Callable, Coroutine, Dict, Optional, Union
from loguru import logger
from pandas import DataFrame
from corex.types import Singleton
from .sqlite import AsyncPickleSQLiteDB


class DatabaseManager(Singleton):
    """
    This class provides methods for managing a database.
    """
    def __new__(cls, data_path: Union[str, Path], db_name: str = 'db', cache_name: str = 'cache'):
        data_path = str(data_path)
        cls._initialize(data_path, db_name, cache_name)

        return super(DatabaseManager, cls).__new__(cls, data_path)
    
    @classmethod
    def I(cls, data_path: Union[str, Path]) -> "DatabaseManager":
        return super().I(str(data_path))

    @classmethod
    def _initialize(cls, data_path: str, db_name: str = 'db', cache_name: str = 'cache'):
        if not hasattr(cls, '_DB'):
            cls._data_path = Path(data_path)
            cls._DB = AsyncPickleSQLiteDB(cls._data_path, db_name)
            cls._DBCache = AsyncPickleSQLiteDB(cls._data_path, cache_name)

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

    def generate_db_key(self, namespace: str, params: Union[Dict, str]) -> str:
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

    async def save_encoded(self, namespace: str, params: Dict, json_data: Any) -> None:
        await self.get_db(namespace).save_encoded(self.generate_db_key(namespace, params), json_data)
        logger.info(f"{namespace} :: Saving cached data : {params}")

    async def save_dataframe(self, table_name: str, df: DataFrame) -> None:
        await self.get_db(table_name).save_dataframe(table_name, df)

    async def fetch_encoded(self, namespace: str, func: Callable[[Dict], Coroutine[Any, Any, Optional[Any]]], params: Dict, save: bool = True) -> Optional[Any]:
        cached = await self.load_encoded(namespace, params)
        if cached:
            logger.info(f"{namespace} :: Loaded cached data : {params}")
            return cached

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
        logger.info(f"{namespace} :: Deleting cached data : {params}")

    async def close_connections(self):
        await self._DB.close()
        await self._DBCache.close()
