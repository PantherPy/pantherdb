# 🐾 PantherDB

[![PyPI](https://img.shields.io/pypi/v/pantherdb?label=PyPI)](https://pypi.org/project/pantherdb/) [![PyVersion](https://img.shields.io/pypi/pyversions/pantherdb.svg)](https://pypi.org/project/pantherdb/) [![Downloads](https://static.pepy.tech/badge/pantherdb/month)](https://pepy.tech/project/pantherdb) [![license](https://img.shields.io/github/license/alirn76/pantherdb.svg)](https://github.com/alirn76/pantherdb/blob/main/LICENSE)

A lightweight, file-based NoSQL database for Python with MongoDB-like interface and optional encryption support.

## Features

- **Document-Oriented**: Store and query data as flexible JSON documents
- **File-Based**: No server setup required, data stored in JSON files
- **Encryption Support**: Optional Fernet encryption for sensitive data
- **MongoDB-like API**: Familiar interface with collections and documents
- **Thread-Safe**: Built-in thread safety with RLock
- **Cursor Support**: Advanced querying with sorting, limiting, and pagination
- **Singleton Pattern**: Efficient connection management per database
- **Pure Python**: Built with Python 3.8+ and standard type hints

## Installation

```bash
# Basic installation
pip install pantherdb

# With encryption support
pip install pantherdb[full]
```

## Quick Start

```python
from pantherdb import PantherDB

# Create a database
db = PantherDB('my_database')

# Get a collection
users = db.collection('users')

# Insert a document
user = users.insert_one(
    name='John Doe',
    email='john@example.com',
    age=30,
    is_active=True
)

# Find documents
john = users.find_one(name='John Doe')
all_users = users.find()
active_users = users.find(is_active=True)

# Update a document
john.update(age=31)

# Delete a document
john.delete()
```

## Database Operations

### Creating a Database

```python
from pantherdb import PantherDB

# Basic database (creates database.json)
db = PantherDB('my_app')

# Encrypted database (creates database.pdb)
from cryptography.fernet import Fernet
key = Fernet.generate_key()  # Store this key securely!
db = PantherDB('secure_app', secret_key=key)

# Return raw dictionaries instead of PantherDocument objects
db = PantherDB('my_app', return_dict=True)

# Return Cursor objects for find operations
db = PantherDB('my_app', return_cursor=True)
```

### Collection Management

```python
# Access a collection
users = db.collection('users')

# Delete a collection and all its documents
users.drop()
```

## CRUD Operations

### Create

```python
# Insert a single document
user = users.insert_one(
    name='Alice',
    email='alice@example.com',
    age=25,
    tags=['python', 'developer']
)

# Documents automatically get a unique _id field
print(user.id)  # ULID string
```

### Read

```python
# Find one document
user = users.find_one(name='Alice')
user = users.find_one()  # Get first document

# Find first document (alias)
user = users.first(name='Alice')
user = users.first()  # Get first document

# Find last document
user = users.last(name='Alice')
user = users.last()  # Get last document

# Find multiple documents
all_users = users.find()
alice_users = users.find(name='Alice')

# Count documents
total_users = users.count()
alice_count = users.count(name='Alice')
```

### Update

```python
# Update a specific document
user = users.find_one(name='Alice')
user.update(age=26, email='alice.new@example.com')

# Update with filter
updated = users.update_one(
    {'name': 'Alice'}, 
    age=26, 
    email='alice.new@example.com'
)

# Update multiple documents
updated_count = users.update_many(
    {'age': 25}, 
    age=26
)
```

### Delete

```python
# Delete a specific document
user = users.find_one(name='Alice')
user.delete()

# Delete with filter
deleted = users.delete_one(name='Alice')

# Delete multiple documents
deleted_count = users.delete_many(age=25)
```

## Document Operations

### Accessing Document Fields

```python
user = users.find_one(name='Alice')

# Access fields as attributes
print(user.name)      # 'Alice'
print(user.age)       # 25
print(user.email)     # 'alice@example.com'

# Access fields as dictionary items
print(user['name'])   # 'Alice'
print(user['age'])    # 25

# Get document ID
print(user.id)        # ULID string
```

### Modifying Documents

```python
user = users.find_one(name='Alice')

# Set fields directly
user.age = 26
user.status = 'active'
user.save()  # Save changes to database

# Update multiple fields
user.update(
    age=26,
    status='active',
    last_login='2023-01-01'
)

# Get JSON representation
json_data = user.json()
print(json_data)  # '{"_id": "...", "name": "Alice", "age": 26}'
```

## Cursor Operations

When using `return_cursor=True`, find operations return Cursor objects with advanced features:

```python
# Enable cursor mode
db = PantherDB('my_app', return_cursor=True)
users = db.collection('users')

# Basic cursor iteration
cursor = users.find()
for user in cursor:
    print(user.name)

# Sorting
cursor = users.find().sort('age', -1)  # Descending by age
cursor = users.find().sort('name', 1)  # Ascending by name

# Limiting and skipping
cursor = users.find().limit(10).skip(5)  # Pagination

# Chaining operations
cursor = users.find(age=18).sort('age', -1).limit(5)

# Async iteration
async for user in cursor:
    print(user.name)
```

## Encryption

For sensitive data, PantherDB supports database encryption:

```python
from pantherdb import PantherDB
from cryptography.fernet import Fernet

# Generate a key (store this securely!)
key = Fernet.generate_key()

# Create encrypted database
db = PantherDB('secure_database', secret_key=key)

# All operations work the same way
users = db.collection('users')
user = users.insert_one(name='Secret User', password='hashed_password')
```

**Important Notes:**
- Store your encryption key securely
- Don't generate a new key on every run
- Losing the key means losing access to your data
- Encrypted databases use `.pdb` extension, unencrypted use `.json`

## Advanced Features

### Singleton Pattern

PantherDB implements a singleton pattern per database file:

```python
# Multiple instances with same name share data
db1 = PantherDB('my_app')
db2 = PantherDB('my_app')

# Both instances point to the same database
users1 = db1.collection('users')
users2 = db2.collection('users')

# Changes in one instance are visible in the other
users1.insert_one(name='Alice')
user = users2.find_one(name='Alice')  # Found!
```

### Thread Safety

All operations are thread-safe:

```python
import threading

def insert_user(name):
    db = PantherDB('my_app')
    users = db.collection('users')
    users.insert_one(name=name)

# Safe to use from multiple threads
threads = [
    threading.Thread(target=insert_user, args=(f'User{i}',))
    for i in range(10)
]

for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
```

### Database Information

```python
db = PantherDB('my_app')

# String representation shows collections and document counts
print(db)
# PantherDB(
#     users: 5 documents,
#     posts: 10 documents
# )

# Collection information
users = db.collection('users')
print(users)
# PantherCollection(
#     collection_name: users
#     name: str
#     age: int
#     email: str
# )
```

## Error Handling

PantherDB raises `PantherDBException` for database errors:

```python
from pantherdb import PantherDB, PantherDBException

try:
    db = PantherDB('corrupted_db', secret_key=wrong_key)
except PantherDBException as e:
    print(f"Database error: {e}")
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">
  <p>If you find Panther useful, please give it a star! ⭐️</p>
</div>
