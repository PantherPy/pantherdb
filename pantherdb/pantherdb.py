from __future__ import annotations
import os.path
import orjson as json
from cryptography.fernet import Fernet, InvalidToken


class PantherDBException(Exception):
    pass


class PantherDB:
    db_name: str = 'database.pdb'
    __secret_key: bytes | None
    __fernet: Fernet | None
    __return_dict: bool
    __content: dict

    def __init__(self, db_name: str | None = None, return_dict: bool = False, secret_key: bytes | None = None):
        self.__return_dict = return_dict
        self.__secret_key = secret_key
        self.__fernet = Fernet(self.__secret_key) if self.__secret_key else None
        if db_name:
            self.db_name = db_name
        if not os.path.exists(self.db_name):
            open(self.db_name, 'w').close()

    def __str__(self) -> str:
        self._refresh()
        db = ',\n'.join(f'    {k}: {len(v)} documents' for k, v in self.content.items())
        return f'{self.__class__.__name__}(\n{db}\n)'

    __repr__ = __str__

    @property
    def content(self) -> dict:
        return self.__content

    @property
    def return_dict(self) -> bool:
        return self.__return_dict

    @property
    def secret_key(self) -> bytes:
        return self.__secret_key

    def _write(self) -> None:
        content = json.dumps(self.content)

        if self.secret_key:
            content = self.__fernet.encrypt(content)

        with open(self.db_name, 'bw') as file:
            file.write(content)

    def _refresh(self) -> None:
        # TODO: Find Solution, so won't refresh on every single query
        with open(self.db_name, 'rb') as file:

            if not (data := file.read()):
                self.__content = {}

            elif not self.secret_key:
                self.__content = json.loads(data)

            else:
                try:
                    decrypted_data: bytes = self.__fernet.decrypt(data)
                    self.__content = json.loads(decrypted_data)
                except InvalidToken:
                    error = '"secret_key" Is Not Valid'
                    raise PantherDBException(error)

    def collection(self, collection_name: str) -> PantherCollection:
        return PantherCollection(
            db_name=self.db_name,
            collection_name=collection_name,
            return_dict=self.return_dict,
            secret_key=self.secret_key
        )

    def close(self):
        pass


class PantherCollection(PantherDB):
    __collection_name: str

    def __init__(self, db_name: str, collection_name: str, return_dict: bool, secret_key: bytes):
        super().__init__(db_name=db_name, return_dict=return_dict, secret_key=secret_key)
        self.__collection_name = collection_name

    def __str__(self) -> str:
        self._refresh()
        documents = self.content[self.collection_name]
        if not documents:
            return f'{self.__class__.__name__}(\n    collection_name: {self.collection_name}\n)'

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
        else:
            return PantherDocument(
                db_name=self.db_name,
                collection_name=self.collection_name,
                return_dict=self.return_dict,
                secret_key=self.secret_key,
                **data
            )

    def _write_collection(self, documents: list) -> None:
        self.content[self.collection_name] = documents
        self._write()

    def _drop_collection(self) -> None:
        self._refresh()
        del self.content[self.collection_name]
        self._write()

    def _get_collection(self) -> list[dict]:
        """return documents"""
        self._refresh()
        return self.content.get(self.collection_name, [])

    def find_one(self, **kwargs) -> PantherDocument | dict | None:
        documents = self._get_collection()
        if not kwargs:
            if self.return_dict:
                return documents[0]
            else:
                return self.__create_result(documents[0])

        for d in documents:
            for k, v in kwargs.items():
                if d.get(k) != v:
                    break
            else:
                return self.__create_result(d)

    def find(self, **kwargs) -> list[PantherDocument | dict]:
        if not kwargs:
            return self.all()

        result = list()
        for r in self._get_collection():
            for k, v in kwargs.items():
                if r.get(k) != v:
                    break
            else:
                result.append(self.__create_result(r))
        return result

    def all(self) -> list[PantherDocument | dict]:
        if self.return_dict:
            return self._get_collection()
        else:
            return [self.__create_result(r) for r in self._get_collection()]

    def insert_one(self, **kwargs) -> PantherDocument | dict:
        documents = self._get_collection()
        kwargs['_id'] = len(documents) + 1
        documents.append(kwargs)
        self._write_collection(documents)
        return self.__create_result(kwargs)

    def delete(self) -> None:
        self.__check_is_panther_document()
        documents = self._get_collection()
        for d in documents:
            if d.get('_id') == self._id:  # NOQA: Unresolved References
                documents.remove(d)
                self._write_collection(documents)
                break

    def delete_one(self, **kwargs) -> bool:
        documents = self._get_collection()
        if not kwargs:
            return False

        index = 0
        found = False
        for d in documents:
            for k, v in kwargs.items():
                if d.get(k) != v:
                    break
            else:
                found = True
                break
            index += 1

        # Didn't find any match
        if not found:
            return False

        # Delete Matched One
        documents.pop(index)
        self._write_collection(documents)
        return True

    def delete_many(self, **kwargs) -> int:
        documents = self._get_collection()
        if not kwargs:
            return 0

        indexes = list()
        index = 0
        found = False
        for d in documents:
            for k, v in kwargs.items():
                if d.get(k) != v:
                    break
            else:
                indexes.append(index)
                found = True
            index += 1

        # Didn't find any match
        if not found:
            return 0

        # Delete Matched Indexes
        for i in indexes[::-1]:
            documents.pop(i)
        self._write_collection(documents)
        return len(indexes)

    def update(self, **kwargs) -> None:
        self.__check_is_panther_document()
        documents = self._get_collection()
        for d in documents:
            if d.get('_id') == self._id:  # NOQA: Unresolved References
                for k, v in kwargs.items():
                    d[k] = v
                self._write_collection(documents)

    def update_one(self, condition: dict, **kwargs) -> bool:
        documents = self._get_collection()
        result = False

        if not condition:
            return result

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

        result = 0
        for d in documents:
            for k, v in kwargs.items():
                if d.get(k) != v:
                    break
            else:
                result += 1
        return result

    def drop(self) -> None:
        self._drop_collection()


class PantherDocument(PantherCollection):
    __data: dict

    def __init__(self, db_name: str, collection_name: str, return_dict: bool, secret_key: bytes, **kwargs):
        self.__data = kwargs
        super().__init__(
            db_name=db_name,
            collection_name=collection_name,
            return_dict=return_dict,
            secret_key=secret_key
        )

    def __str__(self) -> str:
        items = ', '.join(f'id={v}' if k == '_id' else f'{k}={v}' for k, v in self.data.items())
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
            '_PantherDB__secret_key',
            '_PantherDB__fernet',
            '_PantherCollection__collection_name',
            '_PantherDocument__data'
        ]:
            try:
                object.__getattribute__(self, key)
            except AttributeError:
                self.data[key] = value
                return

        super().__setattr__(key, value)

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
        super()._write_collection(documents=documents)

    def json(self) -> str:
        return json.dumps(self.data).decode()
