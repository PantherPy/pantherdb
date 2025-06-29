"""
PantherDB - A lightweight, file-based NoSQL database for Python.

PantherDB is a simple, fast, and secure document database that stores data in JSON format
with optional encryption support. It provides a MongoDB-like interface for document
operations with collections and cursors.

Features:
- File-based storage with JSON format
- Optional encryption using Fernet (cryptography library)
- MongoDB-like API with collections and documents
- Thread-safe operations with RLock
- Cursor-based querying with sorting, limiting, and skipping
- Automatic ID generation using ULID

Example:
    ```python
    from pantherdb import PantherDB
    
    # Create a database instance
    db = PantherDB('my_database')
    
    # Get a collection
    users = db.collection('users')
    
    # Insert a document
    user = users.insert_one(name='John', age=30, email='john@example.com')
    
    # Find documents
    john = users.find_one(name='John')
    all_users = users.find()
    
    # Update documents
    users.update_one({'name': 'John'}, age=31)
    
    # Delete documents
    users.delete_one(name='John')
    ```
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from threading import Thread, RLock
from typing import ClassVar, Iterator, List, Tuple, Union, Callable

import orjson as json
import ulid


class PantherDBException(Exception):
    """Custom exception class for PantherDB operations.
    
    Raised when database operations fail, such as invalid secret keys
    or corrupted data files.
    """
    pass


class PantherDB:
    """
    Main database class for PantherDB operations.
    
    PantherDB implements a singleton pattern per database file, ensuring that
    multiple instances with the same database name share the same data.
    
    Attributes:
        _instances (ClassVar[dict]): Class-level storage for singleton instances
        _instances_lock (ClassVar[RLock]): Thread lock for singleton management
        db_name (str): Name of the database file
        return_dict (bool): Whether to return raw dictionaries instead of PantherDocument objects
        return_cursor (bool): Whether to return Cursor objects for find operations
        secret_key (bytes): Optional encryption key for database security
        content (dict): In-memory storage of database content
        lock (RLock): Thread lock for database operations
        fernet (Fernet): Encryption handler if secret_key is provided
    
    Example:
        ```python
        # Basic usage
        db = PantherDB('my_app')
        
        # With encryption
        from cryptography.fernet import Fernet
        key = Fernet.generate_key()
        secure_db = PantherDB('secure_app', secret_key=key)
        
        # Return raw dictionaries
        db = PantherDB('my_app', return_dict=True)
        ```
    """
    
    _instances: ClassVar[dict] = {}
    _instances_lock: ClassVar[RLock] = RLock()
    db_name: str = 'database.pdb'

    def __new__(cls, *args, **kwargs):
        """Create a singleton instance for each unique database name.
        
        Ensures that multiple PantherDB instances with the same database name
        share the same data and state.
        """
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
        """
        Initialize a PantherDB instance.
        
        Args:
            db_name: Name of the database file. If None, uses default 'database.pdb'.
                     Automatically appends .pdb extension for encrypted databases
                     or .json for unencrypted ones.
            return_dict: If True, methods return raw dictionaries instead of PantherDocument objects.
            return_cursor: If True, find() methods return Cursor objects instead of lists.
            secret_key: Optional encryption key for database security. If provided,
                       the database will be encrypted using Fernet encryption.
        
        Note:
            The database file will be created automatically if it doesn't exist.
        """
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
        """Return a string representation of the database.
        
        Shows the number of documents in each collection.
        
        Returns:
            Formatted string showing database collections and document counts.
        """
        with self.lock:
            self.reload()
            db = ',\n'.join(f'    {k}: {len(v)} documents' for k, v in self.content.items())
            return f'{self.__class__.__name__}(\n{db}\n)'

    __repr__ = __str__

    def write(self) -> None:
        """
        Write the current database content to the file.
        
        If encryption is enabled, the content will be encrypted before writing.
        This method is thread-safe and uses the database lock.
        """
        content = json.dumps(self.content)

        if self.secret_key:
            content = self.fernet.encrypt(content)

        with open(self.db_name, 'wb') as file:
            file.write(content)

    def reload(self) -> None:
        """
        Reload database content from the file.
        
        Reads the database file and updates the in-memory content. If encryption
        is enabled, the content will be decrypted. This method is thread-safe.
        
        Raises:
            PantherDBException: If the secret key is invalid for encrypted databases.
        """
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
            self.content = json.loads(data)

    def collection(self, collection_name: str) -> PantherCollection:
        """
        Get or create a collection within the database.
        
        Args:
            collection_name: Name of the collection to access.
        
        Returns:
            PantherCollection instance for the specified collection.
        
        Example:
            ```python
            db = PantherDB('my_app')
            users = db.collection('users')
            posts = db.collection('posts')
            ```
        """
        return PantherCollection(collection_name=collection_name, db=self)

    def close(self):
        """
        Close the database connection.
        
        Currently, a no-op as PantherDB is file-based and doesn't maintain
        persistent connections. Included for API compatibility.
        """
        pass


class PantherCollection:
    """
    Represents a collection of documents within a PantherDB database.
    
    Collections are similar to tables in SQL databases or collections in MongoDB.
    They contain documents (records) and provide methods for CRUD operations.
    
    Attributes:
        collection_name (str): Name of the collection
        db (PantherDB): Reference to the parent database
        documents (list): List of documents in the collection (in-memory cache)
    
    Example:
        ```python
        db = PantherDB('my_app')
        users = db.collection('users')
        
        # Insert a document
        user = users.insert_one(name='Alice', age=25)
        
        # Find documents
        alice = users.find_one(name='Alice')
        all_users = users.find()
        
        # Update documents
        users.update_one({'name': 'Alice'}, age=26)
        
        # Delete documents
        users.delete_one(name='Alice')
        ```
    """
    
    def __init__(self, collection_name: str, db: PantherDB):
        """
        Initialize a collection.
        
        Args:
            collection_name: Name of the collection
            db: Parent database instance
        """
        self.collection_name = collection_name
        self.db = db
        self.documents: list = []

    def __str__(self) -> str:
        """Return a string representation of the collection.
        
        Shows the collection name and field types of the first document.
        
        Returns:
            Formatted string showing collection information.
        """
        with self.db.lock:
            self.db.reload()
            if self.collection_name not in self.db.content or (documents := self.db.content[self.collection_name]):
                result = ''
            else:
                result = '\n'.join(f'    {k}: {type(v).__name__}' for k, v in documents[0].items())
            return f'{self.__class__.__name__}(\n    collection_name: {self.collection_name}\n\n{result}\n)'

    def _create_result(self, data: dict, /) -> PantherDocument | dict:
        """
        Create the appropriate result object based on database configuration.
        
        Args:
            data: Document data dictionary
        
        Returns:
            Either a PantherDocument object or raw dictionary based on return_dict setting
        """
        if self.db.return_dict:
            return data

        return PantherDocument(collection=self, **data)

    def _find(self, **kwargs) -> Iterator[tuple[int, PantherDocument | dict]]:
        """
        Internal method to find documents matching the given criteria.
        
        Args:
            **kwargs: Field-value pairs to match against documents
        
        Yields:
            Tuples of (index, document) for matching documents
        """
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
        """Reload documents from the database file into memory."""
        self.db.reload()
        self.documents = self.db.content.get(self.collection_name, [])

    def _write_documents(self) -> None:
        """Write the current documents back to the database file."""
        self.db.content[self.collection_name] = self.documents
        self.db.write()

    def find_one(self, **kwargs) -> PantherDocument | dict | None:
        """
        Find a single document matching the given criteria.
        
        Args:
            **kwargs: Field-value pairs to match against documents
        
        Returns:
            The first matching document or None if no match is found.
            If no criteria provided, returns the first document in the collection.
        
        Example:
            ```python
            # Find by specific field
            user = users.find_one(name='Alice')
            
            # Find by multiple fields
            user = users.find_one(name='Alice', age=25)
            
            # Get first document
            first_user = users.find_one()
            ```
        """
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
        """
        Find all documents matching the given criteria.
        
        Args:
            **kwargs: Field-value pairs to match against documents
        
        Returns:
            Either a Cursor object or list of documents based on return_cursor setting.
            If no criteria provided, returns all documents in the collection.
        
        Example:
            ```python
            # Find all users
            all_users = users.find()
            
            # Find users by age
            young_users = users.find(age=25)
            
            # Find users by multiple criteria
            active_users = users.find(status='active', age=25)
            ```
        """
        with self.db.lock:
            self._reload_documents()

            result = [d for _, d in self._find(**kwargs) if d is not None]

            if self.db.return_cursor:
                return Cursor(result, kwargs)
            return result

    def first(self, **filters) -> PantherDocument | dict | None:
        """
        Get the first document in the collection, optionally filtered.
        
        Args:
            **filters: Field-value pairs to match against documents
        
        Returns:
            The first matching document or None if no match is found.
        
        Note:
            This is an alias for find_one() for better readability.
        """
        with self.db.lock:
            return self.find_one(**filters)

    def last(self, **kwargs) -> PantherDocument | dict | None:
        """
        Get the last document in the collection, optionally filtered.
        
        Args:
            **kwargs: Field-value pairs to match against documents
        
        Returns:
            The last matching document or None if no match is found.
            If no criteria provided, returns the last document in the collection.
        
        Example:
            ```python
            # Get last user
            last_user = users.last()
            
            # Get last user with specific criteria
            last_admin = users.last(role='admin')
            ```
        """
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
        """
        Insert a single document into the collection.
        
        Args:
            **kwargs: Field-value pairs for the document
        
        Returns:
            The created document with an automatically generated _id field.
        
        Example:
            ```python
            # Insert a simple document
            user = users.insert_one(name='Bob', age=30, email='bob@example.com')
            
            # Insert with nested data
            post = posts.insert_one(
                title='My Post',
                content='Hello World',
                author='Bob',
                tags=['python', 'database']
            )
            ```
        """
        with self.db.lock:
            self._reload_documents()
            kwargs['_id'] = ulid.new()
            self.documents.append(kwargs)
            self._write_documents()
            return self._create_result(kwargs)

    def delete_one(self, **kwargs) -> bool:
        """
        Delete a single document matching the given criteria.
        
        Args:
            **kwargs: Field-value pairs to match against documents
        
        Returns:
            True if a document was deleted, False otherwise.
        
        Example:
            ```python
            # Delete by specific field
            deleted = users.delete_one(name='Alice')
            
            # Delete by multiple criteria
            deleted = users.delete_one(name='Alice', age=25)
            ```
        """
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
        """
        Delete all documents matching the given criteria.
        
        Args:
            **kwargs: Field-value pairs to match against documents
        
        Returns:
            Number of documents deleted.
        
        Example:
            ```python
            # Delete all inactive users
            deleted_count = users.delete_many(status='inactive')
            
            # Delete all users with specific age
            deleted_count = users.delete_many(age=25)
            ```
        """
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
        """
        Update a single document matching the given condition.
        
        Args:
            condition: Dictionary of field-value pairs to match against documents
            **kwargs: Field-value pairs to update in the matched document
        
        Returns:
            True if a document was updated, False otherwise.
        
        Example:
            ```python
            # Update user age
            updated = users.update_one({'name': 'Alice'}, age=26)
            
            # Update multiple fields
            updated = users.update_one(
                {'email': 'alice@example.com'}, 
                age=26, 
                status='active'
            )
            ```
        """
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
        """
        Update all documents matching the given condition.
        
        Args:
            condition: Dictionary of field-value pairs to match against documents
            **kwargs: Field-value pairs to update in matched documents
        
        Returns:
            Number of documents updated.
        
        Example:
            ```python
            # Update all inactive users to active
            updated_count = users.update_many({'status': 'inactive'}, status='active')
            
            # Update all users with specific age
            updated_count = users.update_many({'age': 25}, age=26)
            ```
        """
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
        """
        Count documents matching the given criteria.
        
        Args:
            **kwargs: Field-value pairs to match against documents
        
        Returns:
            Number of matching documents. If no criteria provided, returns total document count.
        
        Example:
            ```python
            # Count all users
            total_users = users.count()
            
            # Count active users
            active_users = users.count(status='active')
            
            # Count users with specific age
            users_25 = users.count(age=25)
            ```
        """
        with self.db.lock:
            self._reload_documents()
            if not kwargs:
                return len(self.documents)

            return len([i for i, _ in self._find(**kwargs) if i is not None])

    def drop(self) -> None:
        """
        Delete the entire collection and all its documents.
        
        This operation is irreversible and will permanently delete all documents
        in the collection.
        
        Example:
            ```python
            # Drop the entire users collection
            users.drop()
            ```
        """
        with self.db.lock:
            self.db.reload()
            if self.collection_name in self.db.content:
                del self.db.content[self.collection_name]
            self.db.write()


class PantherDocument:
    """
    Represents a single document within a PantherDB collection.
    
    PantherDocument objects provide a convenient interface for accessing and
    modifying document fields, similar to MongoDB documents.
    
    Attributes:
        _collection (PantherCollection): Reference to the parent collection
        _data (dict): Document data dictionary
    
    Example:
        ```python
        # Get a document
        user = users.find_one(name='Alice')
        
        # Access fields
        print(user.name)  # 'Alice'
        print(user.age)   # 25
        
        # Update fields
        user.age = 26
        user.save()
        
        # Update multiple fields
        user.update(status='active', last_login='2023-01-01')
        
        # Delete the document
        user.delete()
        ```
    """
    
    def __init__(self, collection: PantherCollection, **kwargs):
        """
        Initialize a document.
        
        Args:
            collection: Parent collection instance
            **kwargs: Document field-value pairs
        """
        self._collection = collection
        self._data = kwargs

    def __str__(self) -> str:
        """Return a string representation of the document.
        
        Returns:
            Formatted string showing collection name and field values.
        """
        items = ', '.join(f'{k}={v}' for k, v in self._data.items())
        return f'{self._collection.collection_name}({items})'

    __repr__ = __str__

    def __getattr__(self, item: str):
        """
        Get a document field by attribute name.
        
        Args:
            item: Field name to access
        
        Returns:
            Field value
        
        Raises:
            PantherDBException: If the field doesn't exist
        """
        try:
            return self._data[item]
        except KeyError:
            error = f'Invalid Collection Field: "{item}"'
            raise PantherDBException(error)

    __getitem__ = __getattr__

    def __setattr__(self, key, value):
        """
        Set a document field by attribute name.
        
        Args:
            key: Field name to set
            value: Field value to assign
        """
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
        """
        Get the document's unique identifier.
        
        Returns:
            The document's _id field value.
        """
        return self._data['_id']

    def save(self) -> None:
        """
        Save the current document state to the database.
        
        This method replaces the existing document in the collection with
        the current state of this PantherDocument object.
        
        Example:
            ```python
            user = users.find_one(name='Alice')
            user.age = 26
            user.status = 'active'
            user.save()  # Saves changes to database
            ```
        """
        with self._collection.db.lock:
            self._collection._reload_documents()
            for i, d in enumerate(self._collection.documents):

                if d['_id'] == self.id:
                    self._collection.documents.pop(i)
                    self._collection.documents.insert(i, self._data)
                    break
            self._collection._write_documents()

    def update(self, **kwargs) -> None:
        """
        Update specific fields of the document.
        
        Args:
            **kwargs: Field-value pairs to update
        
        Example:
            ```python
            user = users.find_one(name='Alice')
            user.update(age=26, status='active', last_login='2023-01-01')
            ```
        """
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
        """
        Delete this document from the collection.
        
        This operation is irreversible and will permanently remove the
        document from the database.
        
        Example:
            ```python
            user = users.find_one(name='Alice')
            user.delete()  # Removes the document from database
            ```
        """
        with self._collection.db.lock:
            self._collection._reload_documents()
            for d in self._collection.documents:
                if d.get('_id') == self.id:
                    self._collection.documents.remove(d)
                    self._collection._write_documents()
                    break

    def json(self) -> str:
        """
        Get the document as a JSON string.
        
        Returns:
            JSON representation of the document data.
        
        Example:
            ```python
            user = users.find_one(name='Alice')
            json_data = user.json()
            print(json_data)  # '{"_id": "...", "name": "Alice", "age": 25}'
            ```
        """
        return json.dumps(self._data).decode()


class Cursor:
    """
    Cursor for iterating over query results with additional operations.
    
    Cursor objects provide a way to iterate over query results with support
    for sorting, limiting, skipping, and both synchronous and asynchronous iteration.
    
    Attributes:
        documents (List[dict | PantherDocument]): List of documents in the cursor
        filter (dict): Original filter criteria used to create the cursor
        _cursor (int): Current position in the cursor
        _limit (int): Maximum number of documents to return
        _sorts (List[Tuple[str, int]]): Sorting criteria
        _skip (int): Number of documents to skip
        cls: Response class for processing documents
        response_type: Function to process each document
        _conditions_applied (bool): Whether conditions have been applied
    
    Example:
        ```python
        # Basic cursor usage
        cursor = users.find()
        for user in cursor:
            print(user.name)
        
        # With sorting and limiting
        cursor = users.find().sort('age', -1).limit(10).skip(5)
        
        # Async iteration
        async for user in cursor:
            print(user.name)
        ```
    """
    
    def __init__(self, documents: List[dict | PantherDocument], kwargs: dict):
        """
        Initialize a cursor.
        
        Args:
            documents: List of documents to iterate over
            kwargs: Original filter criteria
        """
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
        """Make the cursor iterable in async contexts."""
        return self

    async def next(self, is_async: bool = False):
        """
        Get the next document in the cursor.
        
        Args:
            is_async: Whether this is being called in an async context
        
        Returns:
            Next document in the cursor
        
        Raises:
            StopIteration: When no more documents are available
            StopAsyncIteration: When no more documents are available in async context
        """
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
        """Get the next document in synchronous iteration."""
        return self._run_coroutine(self.next())

    async def __anext__(self):
        """Get the next document in asynchronous iteration."""
        return await self.next(is_async=True)

    def __getitem__(self, index: int | slice) -> Union[Cursor, dict, ...]:
        """
        Get documents by index or slice.
        
        Args:
            index: Integer index or slice object
        
        Returns:
            Document(s) at the specified index(es)
        """
        if not self._conditions_applied:
            self._apply_conditions()

        result = self.documents[index]
        if isinstance(index, int) and self.response_type:
            return self._run_coroutine(self.response_type(result))
        return result

    def sort(self, sorts: List[Tuple[str, int]] | str, sort_order: int = None):
        """
        Sort the cursor results.
        
        Args:
            sorts: Either a string field name or list of (field, order) tuples
            sort_order: Sort order (1 for ascending, -1 for descending) if sorts is a string
        
        Returns:
            Self for method chaining
        
        Example:
            ```python
            # Sort by single field
            cursor = users.find().sort('age', -1)  # Descending by age
            
            # Sort by multiple fields
            cursor = users.find().sort([('age', -1), ('name', 1)])
            ```
        """
        if isinstance(sorts, str):
            self._sorts = [(sorts, sort_order)]
        else:
            self._sorts = sorts
        return self

    def skip(self, skip):
        """
        Skip a number of documents in the cursor.
        
        Args:
            skip: Number of documents to skip
        
        Returns:
            Self for method chaining
        
        Example:
            ```python
            cursor = users.find().skip(10)  # Skip first 10 documents
            ```
        """
        self._skip = skip
        return self

    def limit(self, limit: int):
        """
        Limit the number of documents returned by the cursor.
        
        Args:
            limit: Maximum number of documents to return
        
        Returns:
            Self for method chaining
        
        Example:
            ```python
            cursor = users.find().limit(5)  # Return only 5 documents
            ```
        """
        self._limit = limit
        return self

    def _apply_conditions(self):
        """Apply all pending conditions (sort, skip, limit) to the documents."""
        self._apply_sort()
        self._apply_skip()
        self._apply_limit()
        self._conditions_applied = True

    def _apply_sort(self):
        """Apply sorting to the documents."""
        if self._sorts:
            for sort in self._sorts[::-1]:
                self.documents.sort(key=lambda x: x[sort[0]], reverse=bool(sort[1] == -1))

    def _apply_skip(self):
        """Apply skip operation to the documents."""
        if self._skip:
            self.documents = self.documents[self._skip:]

    def _apply_limit(self):
        """Apply limit operation to the documents."""
        if self._limit:
            self.documents = self.documents[:self._limit]

    @classmethod
    def _run_coroutine(cls, coroutine):
        """
        Run a coroutine in the appropriate context.
        
        This method handles running coroutines in both sync and async contexts,
        creating new event loops when necessary.
        
        Args:
            coroutine: Coroutine to run
        
        Returns:
            Result of the coroutine
        """
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
        Check if a function is asynchronous.
        
        This method inspects the function's code flags to determine if it's
        an async function.
        
        Args:
            func: Function to check
        
        Returns:
            True if the function is async, False otherwise
        
        Note:
            Sync result is 0 --> False
            async result is 128 --> True
        """
        return bool(func.__code__.co_flags & (1 << 7))
