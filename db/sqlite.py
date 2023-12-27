"""
This module provides a class for managing an asynchronous SQLite database.
"""
import asyncio
import os
from pathlib import Path
import pickle
import sqlite3
from typing import Any, Iterable, Optional, Union
import zlib

from loguru import logger
import aiosqlite

from pandas import DataFrame


class AsyncPickleSQLiteDB:
    """
    This class provides methods for managing an asynchronous SQLite database.
    """
    def __init__(self, data_path: Path, name: str = 'db', permissions: str = 'crw'):
        """
        Initializes the database.

        :param name: Name of the SQLite database.
        :param permissions: Permissions for the database - c=create, r=read, w=write.
        """
        self.__data_path = data_path
        self.name = name
        self._db_path = os.path.join(data_path, f'{name}.sqlite')
        self._permissions = set(permissions)
        self._lock = asyncio.Lock()

        self._conn = None  # Placeholder for the persistent connection
        if not os.path.isfile(self._db_path):
            if not 'c' in self._permissions:
                raise PermissionError(
                    "No database found, create permission not granted.")


    async def _get_connection(self):
        if self._conn is None:
            self._conn = await aiosqlite.connect(self._db_path)

            # Set PRAGMA statements for optimization
            await self._conn.execute("PRAGMA journal_mode = WAL;")
            await self._conn.execute("PRAGMA cache_size = -10000;")  # Negative value means KB
            await self._conn.execute("PRAGMA synchronous = NORMAL;")
            
        return self._conn

    async def _create_table_if_not_exists(self, table_name: str):
        query = f'CREATE TABLE IF NOT EXISTS "{table_name}" (key TEXT PRIMARY KEY, value BLOB)'
        await self._execute(query)
    
    def _encode(self, obj):
        return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)))

    def _decode(self, obj) -> Any:
        return pickle.loads(zlib.decompress(bytes(obj)))

    async def _execute(self, query: str, args: Optional[tuple] = None):
        async with self._lock:
            try:
                conn = await self._get_connection()
                res = await conn.execute(query, args)
                await conn.commit()
                return res
            except Exception as e:
                print(f"Database Error: {e}")
                return None
    
    async def _execute_many(self, query: str, args: Iterable[Iterable[Any]]):
        async with self._lock:
            try:
                conn = await self._get_connection()
                res = await conn.executemany(query, args)
                await conn.commit()
                return res
            except Exception as e:
                print(f"Database Error: {e}")
                return None

    async def save_dataframe(self, table_name: str, df: DataFrame):
        if 'w' not in self._permissions:
            raise PermissionError("Write permission not granted.")

        await self._create_table_if_not_exists(table_name)

        # Modified to include DataFrame index
        df = df.reset_index()
        data_tuples = [tuple(x) for x in df.to_records(index=False)]

        if data_tuples:
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['?'] * len(df.columns))
            insert_query = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
            await self._execute_many(insert_query, data_tuples)

    async def contains_encoded(self, key: str) -> bool:
        if 'r' not in self._permissions:
            raise PermissionError("Read permission not granted.")

        await self._create_table_if_not_exists('__ENCODED_DATA__')  
        query = f'SELECT 1 FROM "__ENCODED_DATA__" WHERE key = ?'
        return (await self._execute(query, (key,))) is not None

    async def get_encoded(self, key: str) -> Union[Any, None]:
        if 'r' not in self._permissions:
            raise PermissionError("Read permission not granted.")
        
        await self._create_table_if_not_exists('__ENCODED_DATA__')  
        query = f'SELECT value FROM "__ENCODED_DATA__" WHERE key = ?'
        try:
            async with self._lock:
                async with aiosqlite.connect(os.path.join(self.__data_path, self._db_path)) as db:
                    async with db.execute(query, (key,)) as cursor:
                        d = await cursor.fetchone()
                        if d is None:
                            return None
                        else:
                            return self._decode(d[0])
        except Exception as e:
            logger.exception(f"Error fetching data: {e}")
 
    async def save_encoded(self, key: str, value: Any):
        if 'w' not in self._permissions:
            raise PermissionError("Write permission not granted.")
        
        await self._create_table_if_not_exists('__ENCODED_DATA__')  
        query = f'REPLACE INTO "__ENCODED_DATA__" (key, value) VALUES (?,?)'
        await self._execute(query, (key, self._encode(value)))

    async def delete_encoded(self, key: str):
        if 'w' not in self._permissions:
            raise PermissionError("Write permission not granted.")
        
        await self._create_table_if_not_exists('__ENCODED_DATA__')  
        query = f'DELETE FROM "__ENCODED_DATA__" WHERE key = ?'
        await self._execute(query, (key,))

    async def close(self):
        async with self._lock:
            if self._conn:
                await self._conn.close()
                self._conn = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    def __del__(self):
        if self._conn:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self._conn.close())
            else:
                loop.run_until_complete(self._conn.close())