from __future__ import annotations

import asyncio
from pathlib import Path
from threading import Thread, RLock
from typing import ClassVar, Iterator, List, Tuple, Union, Callable

import orjson as json
import ulid


class PantherDBException(Exception):
    pass


class PantherDB:
    _instances: ClassVar[dict] = {}
    _instances_lock: ClassVar[RLock] = RLock()
    db_name: str = 'database.pdb'

    def __new__(cls, *args, **kwargs):
        if cls.__name__ != 'PantherDB':
            return super().__new__(cls)

        if args:
            db_name = args[0] or cls.db_name
        elif 'db_name' in kwargs:
            db_name = kwargs['db_name'] or cls.db_name
        else:
            db_name = cls.db_name

        db_name = str(db_name)  # Can be PosixPath
        with cls._instances_lock:
            if db_name not in cls._instances:
                cls._instances[db_name] = super().__new__(cls)
            return cls._instances[db_name]

    def __init__(
            self,
            db_name: str | None = None,
            return_dict: bool = False,
            return_cursor: bool = False,
            secret_key: bytes | None = None,
    ):
        self.return_dict = return_dict
        self.return_cursor = return_cursor
        self.secret_key = secret_key
        self.content = {}
        self.lock = RLock()

        if self.secret_key:
            from cryptography.fernet import Fernet

            self.fernet = Fernet(self.secret_key)
        else:
            self.fernet = None

        if db_name:
            db_name = str(db_name)  # Can be PosixPath
            if not db_name.endswith(('.pdb', '.json')):
                if self.secret_key:
                    db_name = f'{db_name}.pdb'
                else:
                    db_name = f'{db_name}.json'
            self.db_name = db_name
        Path(self.db_name).touch(exist_ok=True)

    def __str__(self) -> str:
        with self.lock:
            self.refresh()
            # TODO: Refactor this
            db = ',\n'.join(f'    {k}: {len(v)} documents' for k, v in self.content.items())
            return f'{self.__class__.__name__}(\n{db}\n)'

    __repr__ = __str__

    def write(self) -> None:
        content = json.dumps(self.content)

        if self.secret_key:
            content = self.fernet.encrypt(content)

        with open(self.db_name, 'wb') as file:
            file.write(content)

    def refresh(self) -> None:
        with open(self.db_name, 'rb') as file:
            data = file.read()

        if not data:
            self.content = {}

        elif self.fernet:
            try:
                decrypted_data: bytes = self.fernet.decrypt(data)
            except Exception:  # type: type[cryptography.fernet.InvalidToken]
                error = '`secret_key` Is Not Valid'
                raise PantherDBException(error)

            self.content = json.loads(decrypted_data)

        else:
            try:
                self.content = json.loads(data)
            except Exception as e:
                print(f'{e=}\n\n')
                print(f'{data=}')

    def collection(self, collection_name: str) -> PantherCollection:
        return PantherCollection(collection_name=collection_name, db=self)

    def close(self):
        pass


class PantherCollection:
    def __init__(self, collection_name: str, db: PantherDB):
        self.collection_name = collection_name
        self.db = db
        self.documents: list = []

    def __str__(self) -> str:
        with self.db.lock:
            self.db.refresh()

            # TODO: Refactor this
            if self.collection_name not in self.db.content or (documents := self.db.content[self.collection_name]):
                result = ''
            else:
                result = '\n'.join(f'    {k}: {type(v).__name__}' for k, v in documents[0].items())

            return f'{self.__class__.__name__}(\n    collection_name: {self.collection_name}\n\n{result}\n)'

    def _create_result(self, data: dict, /) -> PantherDocument | dict:
        if self.db.return_dict:
            return data

        return PantherDocument(collection=self, **data)

    def _find(self, **kwargs) -> Iterator[tuple[int, PantherDocument | dict]]:
        found = False
        for index, document in enumerate(self.documents):
            for k, v in kwargs.items():
                if document.get(k) != v:
                    break
            else:
                found = True
                yield index, self._create_result(document)

        if not found:
            yield None, None

    def _reload_documents(self):
        self.db.refresh()
        self.documents = self.db.content.get(self.collection_name, [])

    def _write_documents(self) -> None:
        self.db.content[self.collection_name] = self.documents
        self.db.write()

    def find_one(self, **kwargs) -> PantherDocument | dict | None:
        with self.db.lock:
            self._reload_documents()

            # Empty Collection
            if not self.documents:
                return None

            if not kwargs:
                return self._create_result(self.documents[0])

            for _, d in self._find(**kwargs):
                # Return the first document
                return d

    def find(self, **kwargs) -> Cursor | List[PantherDocument | dict]:
        with self.db.lock:
            self._reload_documents()

            result = [d for _, d in self._find(**kwargs) if d is not None]

            if self.db.return_cursor:
                return Cursor(result, kwargs)
            return result

    def first(self, **filters) -> PantherDocument | dict | None:
        with self.db.lock:
            return self.find_one(**filters)

    def last(self, **kwargs) -> PantherDocument | dict | None:
        with self.db.lock:
            self._reload_documents()
            self.documents.reverse()

            # Empty Collection
            if not self.documents:
                return None

            if not kwargs:
                return self._create_result(self.documents[0])

            for _, d in self._find(**kwargs):
                # Return the first one
                return d

    def insert_one(self, **kwargs) -> PantherDocument | dict:
        with self.db.lock:
            self._reload_documents()
            kwargs['_id'] = ulid.new()
            self.documents.append(kwargs)
            self._write_documents()
            return self._create_result(kwargs)

    def delete_one(self, **kwargs) -> bool:
        with self.db.lock:
            self._reload_documents()

            # Empty Collection
            if not self.documents:
                return False

            if not kwargs:
                return False

            for i, _ in self._find(**kwargs):
                if i is None:
                    # Didn't find any match
                    return False

                # Delete matched one and return
                self.documents.pop(i)
                self._write_documents()
                return True

    def delete_many(self, **kwargs) -> int:
        with self.db.lock:
            self._reload_documents()

            # Empty Collection
            if not self.documents:
                return 0

            indexes = [i for i, _ in self._find(**kwargs) if i is not None]

            # Delete Matched Indexes
            for i in indexes[::-1]:
                self.documents.pop(i)
            self._write_documents()
            return len(indexes)

    def update_one(self, condition: dict, **kwargs) -> bool:
        with self.db.lock:
            self._reload_documents()
            result = False

            if not condition:
                return result

            kwargs.pop('_id', None)
            for d in self.documents:
                for k, v in condition.items():
                    if d.get(k) != v:
                        break
                else:
                    result = True
                    for new_k, new_v in kwargs.items():
                        d[new_k] = new_v
                    self._write_documents()
                    break

            return result

    def update_many(self, condition: dict, **kwargs) -> int:
        with self.db.lock:
            self._reload_documents()
            if not condition:
                return 0

            kwargs.pop('_id', None)
            updated_count = 0
            for d in self.documents:
                for k, v in condition.items():
                    if d.get(k) != v:
                        break
                else:
                    updated_count += 1
                    for new_k, new_v in kwargs.items():
                        d[new_k] = new_v

            if updated_count:
                self._write_documents()
            return updated_count

    def count(self, **kwargs) -> int:
        with self.db.lock:
            self._reload_documents()
            if not kwargs:
                return len(self.documents)

            return len([i for i, _ in self._find(**kwargs) if i is not None])

    def drop(self) -> None:
        with self.db.lock:
            self.db.refresh()
            if self.collection_name in self.db.content:
                del self.db.content[self.collection_name]
            self.db.write()


class PantherDocument:
    def __init__(self, collection: PantherCollection, **kwargs):
        self._collection = collection
        self._data = kwargs

    def __str__(self) -> str:
        items = ', '.join(f'{k}={v}' for k, v in self._data.items())
        return f'{self._collection.collection_name}({items})'

    __repr__ = __str__

    def __getattr__(self, item: str):
        try:
            return self._data[item]
        except KeyError:
            error = f'Invalid Collection Field: "{item}"'
            raise PantherDBException(error)

    __getitem__ = __getattr__

    def __setattr__(self, key, value):
        if key not in ['_collection', '_data']:
            try:
                object.__getattribute__(self, key)
            except AttributeError:
                self._data[key] = value
                return

        super().__setattr__(key, value)

    __setitem__ = __setattr__

    @property
    def id(self) -> int:
        return self._data['_id']

    def save(self) -> None:
        """Pop & Insert New Document"""
        with self._collection.db.lock:
            self._collection._reload_documents()
            for i, d in enumerate(self._collection.documents):

                if d['_id'] == self.id:
                    self._collection.documents.pop(i)
                    self._collection.documents.insert(i, self._data)
                    break
            self._collection._write_documents()

    def update(self, **kwargs) -> None:
        with self._collection.db.lock:
            self._collection._reload_documents()
            kwargs.pop('_id', None)

            for d in self._collection.documents:
                if d.get('_id') == self.id:
                    for k, v in kwargs.items():
                        d[k] = v
                        setattr(self, k, v)
                    break
            self._collection._write_documents()

    def delete(self) -> None:
        with self._collection.db.lock:
            self._collection._reload_documents()
            for d in self._collection.documents:
                if d.get('_id') == self.id:
                    self._collection.documents.remove(d)
                    self._collection._write_documents()
                    break

    def json(self) -> str:
        return json.dumps(self._data).decode()


class Cursor:
    def __init__(self, documents: List[dict | PantherDocument], kwargs: dict):
        self.documents = documents
        self.filter = kwargs  # Used in Panther
        self._cursor = -1
        self._limit = None
        self._sorts = None
        self._skip = None
        self.cls = None
        self.response_type = None
        self._conditions_applied = False

    def __aiter__(self):
        return self

    async def next(self, is_async: bool = False):
        error = StopAsyncIteration if is_async else StopIteration

        if not self._conditions_applied:
            self._apply_conditions()

        self._cursor += 1
        if self._limit and self._cursor > self._limit:
            raise error

        try:
            result = self.documents[self._cursor]
        except IndexError:
            raise error

        # Return pure result
        if self.response_type is None:
            return result

        # Convert the result then return
        if self.is_function_async(self.response_type):
            return await self.response_type(result)
        return self.response_type(result)

    def __next__(self):
        return self._run_coroutine(self.next())

    async def __anext__(self):
        return await self.next(is_async=True)

    def __getitem__(self, index: int | slice) -> Union[Cursor, dict, ...]:
        if not self._conditions_applied:
            self._apply_conditions()

        result = self.documents[index]
        if isinstance(index, int) and self.response_type:
            return self._run_coroutine(self.response_type(result))
        return result

    def sort(self, sorts: List[Tuple[str, int]] | str, sort_order: int = None):
        if isinstance(sorts, str):
            self._sorts = [(sorts, sort_order)]
        else:
            self._sorts = sorts
        return self

    def skip(self, skip):
        self._skip = skip
        return self

    def limit(self, limit: int):
        self._limit = limit
        return self

    def _apply_conditions(self):
        self._apply_sort()
        self._apply_skip()
        self._apply_limit()
        self._conditions_applied = True

    def _apply_sort(self):
        if self._sorts:
            for sort in self._sorts[::-1]:
                self.documents.sort(key=lambda x: x[sort[0]], reverse=bool(sort[1] == -1))

    def _apply_skip(self):
        if self._skip:
            self.documents = self.documents[self._skip:]

    def _apply_limit(self):
        if self._limit:
            self.documents = self.documents[:self._limit]

    @classmethod
    def _run_coroutine(cls, coroutine):
        try:
            # Check if there's an event loop already running in this thread
            asyncio.get_running_loop()
        except RuntimeError:
            # No event loop is running in this thread — safe to use asyncio.run
            return asyncio.run(coroutine)

        # Since we cannot block a running event loop with run_until_complete,
        # we execute the coroutine in a separate thread with its own event loop.
        result = []

        def run_in_thread():
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                result.append(new_loop.run_until_complete(coroutine))
            finally:
                new_loop.close()

        thread = Thread(target=run_in_thread)
        thread.start()
        thread.join()
        return result[0]

    @classmethod
    def is_function_async(cls, func: Callable) -> bool:
        """
        Sync result is 0 --> False
        async result is 128 --> True
        """
        return bool(func.__code__.co_flags & (1 << 7))
