from __future__ import annotations
import os.path
import orjson as json


class PantherDBException(Exception):
    pass


class PantherDB:
    db_name: str = 'db.json'
    __content: dict

    def __init__(self, db_name: str | None = None, collection_name: str | None = None):
        if db_name:
            self.db_name = db_name
        if not os.path.exists(self.db_name):
            open(self.db_name, 'w').close()
        if collection_name:
            self._collection_name = collection_name

    def __str__(self) -> str:
        self._refresh()
        db = ',\n'.join(f'    {k}: {len(v)} documents' for k, v in self.content.items())
        return f'{self.__class__.__name__}(\n{db}\n)'

    __repr__ = __str__

    @property
    def content(self) -> dict:
        return self.__content

    def __write(self) -> None:
        with open(self.db_name, 'bw') as file:
            file.write(json.dumps(self.content))

    def _write_collection(self, rows: list) -> None:
        self.content[self._collection_name] = rows
        self.__write()

    def _drop_collection(self) -> None:
        del self.content[self._collection_name]
        self.__write()

    def _refresh(self) -> None:
        # TODO: Find Solution, so won't refresh on every single query
        with open(self.db_name, 'rb') as file:
            data = file.read()
            self.__content = json.loads(data) if data else {}

    def collection(self, collection_name: str) -> PantherDBCollection:
        return PantherDBCollection(db_name=self.db_name, collection_name=collection_name)

    def close(self):
        pass


class PantherDBCollection(PantherDB):
    _collection_name: str

    def __init__(self, collection_name: str, db_name: str):
        super().__init__(db_name=db_name, collection_name=collection_name)
        self._collection_name = collection_name

    def __str__(self) -> str:
        self._refresh()
        documents = self.content[self._collection_name]
        if not documents:
            return f'{self.__class__.__name__}(\n    collection_name: {self._collection_name}\n)'

        result = '\n'.join(f'    {k}: {type(v).__name__}' for k, v in documents[0].items())
        return f'{self.__class__.__name__}(\n    collection_name: {self._collection_name}\n\n{result}\n)'

    def _get_collection(self) -> list[dict]:
        """return documents"""
        self._refresh()
        return self.content.get(self._collection_name, [])

    def first(self, **kwargs) -> dict | None:
        rows = self._get_collection()
        if not kwargs:
            return rows[0]

        for r in rows:
            for k, v in kwargs.items():
                if r.get(k) != v:
                    break
            else:
                return r

    def last(self, **kwargs) -> dict | None:
        rows = self._get_collection()
        rows.reverse()

        if not kwargs:
            return rows[0]

        for r in rows:
            for k, v in kwargs.items():
                if r.get(k) != v:
                    break
            else:
                return r

    def find(self, **kwargs) -> list[dict]:
        if not kwargs:
            return self.all()

        result = list()
        for r in self._get_collection():
            for k, v in kwargs.items():
                if r.get(k) != v:
                    break
            else:
                result.append(r)
        return result

    def all(self) -> list[dict]:
        return self._get_collection()
    
    def insert_one(self, **kwargs) -> str:
        rows = self._get_collection()
        kwargs['_id'] = len(rows) + 1
        rows.append(kwargs)
        self._write_collection(rows)
        return kwargs['_id']
    
    def delete_one(self, **kwargs) -> bool:
        rows = self._get_collection()
        if not kwargs:
            return False

        index = 0
        found = False
        for r in rows:
            for k, v in kwargs.items():
                if r.get(k) != v:
                    break
            else:
                found = True
                break
            index += 1

        # Didn't find any match
        if not found:
            return False

        # Delete Matched One
        rows.pop(index)
        self._write_collection(rows)
        return True

    def delete_many(self, **kwargs) -> int:
        rows = self._get_collection()
        if not kwargs:
            return 0

        indexes = list()
        index = 0
        found = False
        for r in rows:
            for k, v in kwargs.items():
                if r.get(k) != v:
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
            rows.pop(i)
        self._write_collection(rows)
        return len(indexes)
    
    def update_one(self, condition: dict, **kwargs) -> bool:
        rows = self._get_collection()
        result = False

        if not condition:
            return result

        for r in rows:
            for k, v in condition.items():
                if r.get(k) != v:
                    break
            else:
                result = True
                for new_k, new_v in kwargs.items():
                    r[new_k] = new_v
                self._write_collection(rows)
                break

        return result

    def update_many(self, condition: dict, **kwargs) -> int:
        rows = self._get_collection()
        if not condition:
            return 0

        updated_count = 0
        for r in rows:
            for k, v in condition.items():
                if r.get(k) != v:
                    break
            else:
                updated_count += 1
                for new_k, new_v in kwargs.items():
                    r[new_k] = new_v

        if updated_count:
            self._write_collection(rows)
        return updated_count

    def count(self, **kwargs) -> int:
        rows = self._get_collection()
        if not kwargs:
            return len(rows)

        result = 0
        for r in rows:
            for k, v in kwargs.items():
                if r.get(k) != v:
                    break
            else:
                result += 1
        return result

    def drop(self) -> None:
        self._get_collection()
        self._drop_collection()
