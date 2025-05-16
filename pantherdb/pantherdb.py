from __future__ import annotations

import asyncio
from pathlib import Path
from typing import ClassVar, Iterator, Any, List, Tuple, Union

import orjson as json
import ulid


class PantherDBException(Exception):
    pass


class PantherDB:
    _instances: ClassVar[dict] = {}
    db_name: str = 'database.pdb'
    __secret_key: bytes | None
    __fernet: Any  # type: cryptography.fernet.Fernet | None
    __return_dict: bool
    __return_cursor: bool
    __content: dict

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
        # Replace with .removesuffix('.pdb') after python3.8 compatible support
        if db_name.endswith('.pdb'):
            db_name = db_name[:-4]
        elif db_name.endswith('.json'):
            db_name = db_name[:-5]

        if db_name not in cls._instances:
            cls._instances[db_name] = super().__new__(cls)
        return cls._instances[db_name]

    def __init__(
            self,
            db_name: str | None = None,
            *,
            return_dict: bool = False,
            return_cursor: bool = False,
            secret_key: bytes | None = None,
    ):
        self.__return_dict = return_dict
        self.__return_cursor = return_cursor
        self.__secret_key = secret_key
        self.__content = {}
        if self.__secret_key:
            from cryptography.fernet import Fernet

            self.__fernet = Fernet(self.__secret_key)
        else:
            self.__fernet = None

        if db_name:
            if not db_name.endswith(('pdb', 'json')):
                if self.__secret_key:
                    db_name = f'{db_name}.pdb'
                else:
                    db_name = f'{db_name}.json'
            self.db_name = db_name
        Path(self.db_name).touch(exist_ok=True)

    def __str__(self) -> str:
        self._refresh()
        db = ',\n'.join(f'    {k}: {len(v)} documents' for k, v in self.content.items())
        return f'{self.__class__.__name__}(\n{db}\n)'

    __repr__ = __str__

    @property
    def content(self) -> dict:
        return self.__content

    @property
    def return_cursor(self) -> bool:
        return self.__return_cursor

    @property
    def return_dict(self) -> bool:
        return self.__return_dict

    @property
    def secret_key(self) -> bytes | None:
        return self.__secret_key

    def _write(self) -> None:
        content = json.dumps(self.content)

        if self.secret_key:
            content = self.__fernet.encrypt(content)

        with open(self.db_name, 'bw') as file:
            file.write(content)

    def _refresh(self) -> None:
        with open(self.db_name, 'rb') as file:
            data = file.read()

        if not data:
            self.__content = {}

        elif not self.secret_key:
            self.__content = json.loads(data)

        else:
            try:
                decrypted_data: bytes = self.__fernet.decrypt(data)
            except Exception:  # type[cryptography.fernet.InvalidToken]
                error = '"secret_key" Is Not Valid'
                raise PantherDBException(error)

            self.__content = json.loads(decrypted_data)

    def collection(self, collection_name: str) -> PantherCollection:
        return PantherCollection(
            db_name=self.db_name,
            collection_name=collection_name,
            return_dict=self.return_dict,
            return_cursor=self.return_cursor,
            secret_key=self.secret_key,
        )

    def close(self):
        pass


class PantherCollection(PantherDB):
    __collection_name: str

    def __init__(
            self,
            db_name: str,
            *,
            collection_name: str,
            return_dict: bool,
            return_cursor: bool,
            secret_key: bytes,
    ):
        super().__init__(db_name=db_name, return_dict=return_dict, return_cursor=return_cursor, secret_key=secret_key)
        self.__collection_name = collection_name

    def __str__(self) -> str:
        self._refresh()

        if self.collection_name not in self.content or (documents := self.content[self.collection_name]):
            result = ''
        else:
            result = '\n'.join(f'    {k}: {type(v).__name__}' for k, v in documents[0].items())

        return f'{self.__class__.__name__}(\n    collection_name: {self.collection_name}\n\n{result}\n)'

    @property
    def collection_name(self) -> str:
        return self.__collection_name

    def __check_is_panther_document(self) -> None:
        if not hasattr(self, '_id'):
            raise PantherDBException('You should call this method on PantherDocument instance.')

    def __create_result(self, data: dict, /) -> PantherDocument | dict:
        if self.return_dict:
            return data

        return PantherDocument(
            db_name=self.db_name,
            collection_name=self.collection_name,
            return_dict=self.return_dict,
            return_cursor=self.return_cursor,
            secret_key=self.secret_key,
            **data,
        )

    def _write_collection(self, documents: list) -> None:
        self.content[self.collection_name] = documents
        self._write()

    def _drop_collection(self) -> None:
        self._refresh()
        if self.collection_name in self.content:
            del self.content[self.collection_name]
        self._write()

    def _get_collection(self) -> list[dict]:
        """Return documents"""
        self._refresh()
        return self.content.get(self.collection_name, [])

    def _find(self, _documents: list, /, **kwargs) -> Iterator[tuple[int, PantherDocument | dict]]:
        found = False
        for index, document in enumerate(_documents):
            for k, v in kwargs.items():
                if document.get(k) != v:
                    break
            else:
                found = True
                yield index, self.__create_result(document)

        if not found:
            yield None, None

    def find_one(self, **kwargs) -> PantherDocument | dict | None:
        documents = self._get_collection()

        # Empty Collection
        if not documents:
            return None

        if not kwargs:
            return self.__create_result(documents[0])

        for _, d in self._find(documents, **kwargs):
            # Return the first document
            return d

    def find(self, **kwargs) -> Cursor | List[PantherDocument | dict]:
        documents = self._get_collection()

        result = [d for _, d in self._find(documents, **kwargs) if d is not None]

        if self.return_cursor:
            return Cursor(result, kwargs)
        return result

    def first(self, **kwargs) -> PantherDocument | dict | None:
        return self.find_one(**kwargs)

    def last(self, **kwargs) -> PantherDocument | dict | None:
        documents = self._get_collection()
        documents.reverse()

        # Empty Collection
        if not documents:
            return None

        if not kwargs:
            return self.__create_result(documents[0])

        for _, d in self._find(documents, **kwargs):
            # Return the first one
            return d

    def insert_one(self, **kwargs) -> PantherDocument | dict:
        documents = self._get_collection()
        kwargs['_id'] = ulid.new()
        documents.append(kwargs)
        self._write_collection(documents)
        return self.__create_result(kwargs)

    def delete(self) -> None:
        self.__check_is_panther_document()
        documents = self._get_collection()
        for d in documents:
            if d.get('_id') == self._id:  # noqa: Unresolved References
                documents.remove(d)
                self._write_collection(documents)
                break

    def delete_one(self, **kwargs) -> bool:
        documents = self._get_collection()

        # Empty Collection
        if not documents:
            return False

        if not kwargs:
            return False

        for i, _ in self._find(documents, **kwargs):
            if i is None:
                # Didn't find any match
                return False

            # Delete matched one and return
            documents.pop(i)
            self._write_collection(documents)
            return True

    def delete_many(self, **kwargs) -> int:
        documents = self._get_collection()

        # Empty Collection
        if not documents:
            return 0

        if not kwargs:
            return 0

        indexes = [i for i, _ in self._find(documents, **kwargs) if i is not None]

        # Delete Matched Indexes
        for i in indexes[::-1]:
            documents.pop(i)
        self._write_collection(documents)
        return len(indexes)

    def update(self, **kwargs) -> None:
        self.__check_is_panther_document()
        documents = self._get_collection()
        kwargs.pop('_id', None)

        for d in documents:
            if d.get('_id') == self._id:  # noqa: Unresolved References
                for k, v in kwargs.items():
                    d[k] = v
                    setattr(self, k, v)
                self._write_collection(documents)

    def update_one(self, condition: dict, **kwargs) -> bool:
        documents = self._get_collection()
        result = False

        if not condition:
            return result

        kwargs.pop('_id', None)
        for d in documents:
            for k, v in condition.items():
                if d.get(k) != v:
                    break
            else:
                result = True
                for new_k, new_v in kwargs.items():
                    d[new_k] = new_v
                self._write_collection(documents)
                break

        return result

    def update_many(self, condition: dict, **kwargs) -> int:
        documents = self._get_collection()
        if not condition:
            return 0

        kwargs.pop('_id', None)
        updated_count = 0
        for d in documents:
            for k, v in condition.items():
                if d.get(k) != v:
                    break
            else:
                updated_count += 1
                for new_k, new_v in kwargs.items():
                    d[new_k] = new_v

        if updated_count:
            self._write_collection(documents)
        return updated_count

    def count(self, **kwargs) -> int:
        documents = self._get_collection()
        if not kwargs:
            return len(documents)

        return len([i for i, _ in self._find(documents, **kwargs) if i is not None])

    def drop(self) -> None:
        self._drop_collection()


class PantherDocument(PantherCollection):
    __data: dict

    def __init__(
            self,
            db_name: str,
            *,
            collection_name: str,
            return_dict: bool,
            return_cursor: bool,
            secret_key: bytes,
            **kwargs,
    ):
        self.__data = kwargs
        super().__init__(
            db_name=db_name,
            collection_name=collection_name,
            return_dict=return_dict,
            return_cursor=return_cursor,
            secret_key=secret_key,
        )

    def __str__(self) -> str:
        items = ', '.join(f'{k}={v}' for k, v in self.data.items())
        return f'{self.collection_name}({items})'

    __repr__ = __str__

    def __getattr__(self, item: str):
        try:
            return object.__getattribute__(self, '_PantherDocument__data')[item]
        except KeyError:
            error = f'Invalid Collection Field: "{item}"'
            raise PantherDBException(error)

    def __setattr__(self, key, value):
        if key not in [
            '_PantherDB__return_dict',
            '_PantherDB__return_cursor',
            '_PantherDB__secret_key',
            '_PantherDB__content',
            '_PantherDB__fernet',
            '_PantherDB__ulid',
            '_PantherCollection__collection_name',
            '_PantherDocument__data',
        ]:
            try:
                object.__getattribute__(self, key)
            except AttributeError:
                self.data[key] = value
                return

        super().__setattr__(key, value)

    __setitem__ = __setattr__

    __getitem__ = __getattr__

    @property
    def id(self) -> int:
        return self.data['_id']

    @property
    def data(self) -> dict:
        return self.__data

    def save(self) -> None:
        """Pop & Insert New Document"""
        documents = self._get_collection()
        for i, d in enumerate(documents):
            if d['_id'] == self.id:
                documents.pop(i)
                documents.insert(i, self.data)
                break
        self._write_collection(documents)

    def json(self) -> str:
        return json.dumps(self.data).decode()


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
            # Try to get the current event loop
            _ = asyncio.get_running_loop()
            # If we're inside an event loop, create a new task
            return asyncio.create_task(coroutine)
        except RuntimeError:
            # If we're not in an event loop, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(coroutine)
            finally:
                loop.close()

    @classmethod
    def is_function_async(cls, func: Callable) -> bool:
        """
        Sync result is 0 --> False
        async result is 128 --> True
        """
        return bool(func.__code__.co_flags & (1 << 7))
