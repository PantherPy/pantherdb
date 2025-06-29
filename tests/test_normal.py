
from pathlib import Path
from unittest import TestCase, IsolatedAsyncioTestCase
from uuid import uuid4

import orjson as json
from faker import Faker

from pantherdb import PantherCollection, PantherDB, PantherDocument, Cursor

f = Faker()


class TestNormalPantherDB(TestCase):

    @classmethod
    def setUp(cls):
        cls.db_name = uuid4().hex
        cls.db_name = f'{cls.db_name}.pdb'
        cls.db = PantherDB(db_name=cls.db_name)

    @classmethod
    def tearDown(cls):
        Path(cls.db_name).unlink()

    @classmethod
    def create_junk_document(cls, collection) -> int:
        _count = f.random.randint(2, 10)
        for i in range(_count):
            collection.insert_one(first_name=f'{f.first_name()}{i}', last_name=f'{f.last_name()}{i}')
        return _count

    # Singleton
    def test_pantherdb_singleton(self):
        test_1 = PantherDB(db_name='test1')
        test_2 = PantherDB('test1')
        assert test_1 == test_2

        default_1 = PantherDB()
        default_2 = PantherDB()
        assert default_1 == default_2

        assert test_1 != default_1
        assert test_2 != default_2

        Path(test_1.db_name).unlink()
        Path(default_1.db_name).unlink()

    # Create DB
    def test_creation_of_db(self):
        assert Path(self.db_name).exists()
        assert Path(self.db_name).is_file()
        assert self.db.db_name == self.db_name

    def test_create_db_without_name(self):
        db = PantherDB()
        assert db.db_name == PantherDB.db_name
        assert Path(db.db_name).exists()
        assert Path(db.db_name).is_file()

    def test_creation_of_db_without_extension(self):
        db_name = uuid4().hex
        db = PantherDB(db_name=db_name)
        final_db_name = f'{db_name}.json'

        assert Path(final_db_name).exists()
        assert Path(final_db_name).is_file()
        assert db.db_name == final_db_name

        Path(final_db_name).unlink()

    def test_creation_of_collection(self):
        collection_name = f.word()
        collection = self.db.collection(collection_name)

        assert bool(collection)
        assert isinstance(collection, PantherCollection)
        assert collection.collection_name == collection_name
        assert collection.db.content == {}
        assert collection.db.secret_key is None

    # Drop
    def test_drop_collection(self):
        collection = self.db.collection(f.word())
        collection.drop()
        assert collection.db.content == {}

    # Insert
    def test_insert_one(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()
        obj = collection.insert_one(first_name=first_name, last_name=last_name)

        assert isinstance(obj, PantherDocument)
        assert obj.first_name == first_name
        assert obj.last_name == last_name

    def test_id_assignments(self):
        collection = self.db.collection(f.word())
        ids = set()
        _count = f.random.randint(2, 10)
        for i in range(_count):
            obj = collection.insert_one(first_name=f.first_name(), last_name=f.last_name())
            ids.add(obj.id)
            assert len(obj.id) == 26

        # Each id should be unique
        assert len(ids) == _count

    # Find One
    def test_find_one_first(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Insert with specific names
        collection.insert_one(first_name=first_name, last_name=last_name)

        # Add others
        self.create_junk_document(collection)

        # Find
        obj = collection.find_one(first_name=first_name, last_name=last_name)
        assert isinstance(obj, PantherDocument)
        assert obj.first_name == first_name
        assert obj.last_name == last_name

    def test_find_one_last(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Add others
        self.create_junk_document(collection)

        # Insert with specific names
        collection.insert_one(first_name=first_name, last_name=last_name)

        # Find
        obj = collection.find_one(first_name=first_name, last_name=last_name)
        assert isinstance(obj, PantherDocument)
        assert obj.first_name == first_name
        assert obj.last_name == last_name

    def test_find_one_none(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Add others
        self.create_junk_document(collection)

        # Find
        obj = collection.find_one(first_name=first_name, last_name=last_name)
        assert obj is None

    def test_find_one_with_kwargs_from_empty_collection(self):
        collection = self.db.collection(f.word())

        # Find
        obj = collection.find_one(first_name=f.first_name(), last_name=f.last_name())
        assert obj is None

    def test_find_one_without_kwargs_from_empty_collection(self):
        collection = self.db.collection(f.word())

        # Find
        obj = collection.find_one()
        assert obj is None

    # First
    def test_first_when_its_first(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Insert with specific names
        collection.insert_one(first_name=first_name, last_name=last_name)

        # Add others
        self.create_junk_document(collection)

        # Find
        obj = collection.first(first_name=first_name, last_name=last_name)
        assert isinstance(obj, PantherDocument)
        assert obj.first_name == first_name
        assert obj.last_name == last_name

    def test_first_of_many_finds(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Insert with specific names
        expected = collection.insert_one(first_name=first_name, last_name=last_name)
        collection.insert_one(first_name=first_name, last_name=last_name)
        collection.insert_one(first_name=first_name, last_name=last_name)

        # Add others
        self.create_junk_document(collection)

        # Find
        obj = collection.first(first_name=first_name, last_name=last_name)
        assert isinstance(obj, PantherDocument)
        assert obj.id == expected.id

    def test_first_when_its_last(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Add others
        self.create_junk_document(collection)

        # Insert with specific names
        expected = collection.insert_one(first_name=first_name, last_name=last_name)

        # Find
        obj = collection.first(first_name=first_name, last_name=last_name)
        assert isinstance(obj, PantherDocument)
        assert obj.first_name == first_name
        assert obj.last_name == last_name
        assert obj.id == expected.id

    def test_first_none(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Add others
        self.create_junk_document(collection)

        # Find
        obj = collection.first(first_name=first_name, last_name=last_name)
        assert obj is None

    def test_first_with_kwargs_from_empty_collection(self):
        collection = self.db.collection(f.word())

        # Find
        obj = collection.first(first_name=f.first_name(), last_name=f.last_name())
        assert obj is None

    def test_first_without_kwargs_from_empty_collection(self):
        collection = self.db.collection(f.word())

        # Find
        obj = collection.first()
        assert obj is None

    # Last
    def test_last_when_its_first(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Insert with specific names
        collection.insert_one(first_name=first_name, last_name=last_name)

        # Add others
        self.create_junk_document(collection)

        # Find
        obj = collection.first(first_name=first_name, last_name=last_name)
        assert isinstance(obj, PantherDocument)
        assert obj.first_name == first_name
        assert obj.last_name == last_name

    def test_last_of_many_finds(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Insert with specific names
        collection.insert_one(first_name=first_name, last_name=last_name)
        collection.insert_one(first_name=first_name, last_name=last_name)
        expected = collection.insert_one(first_name=first_name, last_name=last_name)

        # Add others
        self.create_junk_document(collection)

        # Find
        obj = collection.last(first_name=first_name, last_name=last_name)
        assert isinstance(obj, PantherDocument)
        assert obj.id == expected.id

    def test_last_when_its_last(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Add others
        self.create_junk_document(collection)

        # Insert with specific names
        expected = collection.insert_one(first_name=first_name, last_name=last_name)

        # Find
        obj = collection.last(first_name=first_name, last_name=last_name)
        assert isinstance(obj, PantherDocument)
        assert obj.first_name == first_name
        assert obj.last_name == last_name
        assert obj.id == expected.id

    def test_last_none(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Add others
        self.create_junk_document(collection)

        # Find
        obj = collection.last(first_name=first_name, last_name=last_name)
        assert obj is None

    def test_last_with_kwargs_from_empty_collection(self):
        collection = self.db.collection(f.word())

        # Find
        obj = collection.last(first_name=f.first_name(), last_name=f.last_name())
        assert obj is None

    def test_last_without_kwargs_from_empty_collection(self):
        collection = self.db.collection(f.word())

        # Find
        obj = collection.last()
        assert obj is None

    # Find
    def test_find_response_type(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Find
        objs = collection.find(first_name=first_name)

        assert isinstance(objs, list)
        assert len(objs) == 1
        assert isinstance(objs[0], PantherDocument)

    def test_find_with_filter(self):
        collection = self.db.collection(f.word())

        # Add others
        self.create_junk_document(collection)

        # Insert with specific names
        first_name = f.first_name()
        _count = f.random.randint(2, 10)
        for i in range(_count):
            collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Find
        objs = collection.find(first_name=first_name)

        assert isinstance(objs, list)
        assert len(objs) == _count
        for i in range(_count):
            assert objs[i].first_name == first_name

    def test_find_without_filter(self):
        collection = self.db.collection(f.word())

        # Add others
        _count_1 = self.create_junk_document(collection)

        # Insert with specific names
        first_name = f.first_name()
        _count_2 = f.random.randint(2, 10)
        for i in range(_count_2):
            collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Find
        objs = collection.find()
        _count_all = _count_1 + _count_2

        assert isinstance(objs, list)
        assert len(objs) == _count_all
        for i in range(_count_all):
            assert isinstance(objs[i], PantherDocument)

        # Check count of specific name
        specific_count = 0
        for i in range(_count_all):
            if objs[i].first_name == first_name:
                specific_count += 1

        assert specific_count == _count_2

    # Count
    def test_count_with_filter(self):
        collection = self.db.collection(f.word())

        # Add others
        _count_1 = self.create_junk_document(collection)

        # Insert with specific names
        first_name = f.first_name()
        _count_2 = f.random.randint(2, 10)
        for i in range(_count_2):
            collection.insert_one(first_name=first_name, last_name=f.last_name())

        count_specific = collection.count(first_name=first_name)
        assert count_specific == _count_2
        assert count_specific == len(collection.find(first_name=first_name))

    # Delete Self
    def test_delete(self):
        collection = self.db.collection(f.word())

        # Add others
        _count = self.create_junk_document(collection)

        # Insert with specific name
        first_name = f.first_name()
        collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Find
        obj = collection.find_one(first_name=first_name)

        # Delete It
        obj.delete()

        # Find It Again
        new_obj = collection.find_one(first_name=first_name)
        assert new_obj is None

        # Count of all
        objs_count = collection.count()
        assert objs_count == _count

    # Delete One
    def test_delete_one(self):
        collection = self.db.collection(f.word())

        # Add others
        _count = self.create_junk_document(collection)

        first_name = f.first_name()
        collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Delete One
        is_deleted = collection.delete_one(first_name=first_name)
        assert is_deleted is True

        # Find It Again
        new_obj = collection.find_one(first_name=first_name)
        assert new_obj is None

        # Count of all
        objs_count = collection.count()
        assert objs_count == _count

    def test_delete_one_not_found(self):
        collection = self.db.collection(f.word())

        # Add others
        _count = self.create_junk_document(collection)

        first_name = f.first_name()

        # Delete One
        is_deleted = collection.delete_one(first_name=first_name)
        assert is_deleted is False

        # Count of all
        objs_count = collection.count()
        assert objs_count == _count

    def test_delete_one_first(self):
        collection = self.db.collection(f.word())

        # Add others
        _count_1 = self.create_junk_document(collection)

        first_name = f.first_name()
        _count_2 = f.random.randint(2, 10)
        for i in range(_count_2):
            collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Delete One
        is_deleted = collection.delete_one(first_name=first_name)
        assert is_deleted is True

        # Count of all
        objs_count = collection.count()
        assert objs_count == (_count_1 + _count_2 - 1)

        # Count of undeleted
        undeleted_count = collection.count(first_name=first_name)
        assert undeleted_count == (_count_2 - 1)

    # Delete Many
    def test_delete_many(self):
        collection = self.db.collection(f.word())

        # Add others
        _count_1 = self.create_junk_document(collection)

        # Insert with specific name
        first_name = f.first_name()
        _count_2 = f.random.randint(2, 10)
        for i in range(_count_2):
            collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Delete Many
        deleted_count = collection.delete_many(first_name=first_name)
        assert deleted_count == _count_2

        # Count of all
        objs_count = collection.count()
        assert objs_count == _count_1

    def test_delete_many_not_found(self):
        collection = self.db.collection(f.word())

        # Add others
        _count = self.create_junk_document(collection)

        first_name = f.first_name()

        # Delete Many
        deleted_count = collection.delete_many(first_name=first_name)
        assert deleted_count == 0

        # Count of all
        objs_count = collection.count()
        assert objs_count == _count

    # Update Self
    def test_update(self):
        collection = self.db.collection(f.word())

        # Add others
        _count = self.create_junk_document(collection)

        # Insert with specific name
        first_name = f.first_name()
        collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Find One
        obj = collection.find_one(first_name=first_name)
        new_name = f.first_name()
        obj.update(first_name=new_name)
        assert obj.first_name == new_name

        # Find with old name
        old_obj = collection.find_one(first_name=first_name)
        assert old_obj is None

        # Find with new name
        obj = collection.find_one(first_name=new_name)
        assert obj.first_name == new_name

    # Update One
    def test_update_one_single_document(self):
        collection = self.db.collection(f.word())

        # Add others
        _count = self.create_junk_document(collection)

        # Insert with specific name
        first_name = f.first_name()
        collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Update One
        new_name = f.first_name()
        is_updated = collection.update_one({'first_name': first_name}, first_name=new_name)
        assert is_updated is True

        # Find with old name
        old_obj = collection.find_one(first_name=first_name)
        assert old_obj is None

        # Find with new name
        obj = collection.find_one(first_name=new_name)
        assert obj.first_name == new_name

    def test_update_one_single_document_not_found(self):
        collection = self.db.collection(f.word())

        # Add others
        _count = self.create_junk_document(collection)

        # Insert with specific name
        first_name = f.first_name()
        collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Update One
        new_name = f.first_name()
        is_updated = collection.update_one({'first_name': f.first_name()}, first_name=new_name)
        assert is_updated is False

        # Find with old name
        old_obj = collection.find_one(first_name=first_name)
        assert old_obj is not None

        # Find with new name
        obj = collection.find_one(first_name=new_name)
        assert obj is None

    # Update Many
    def test_update_many(self):
        collection = self.db.collection(f.word())

        # Add others
        _count_1 = self.create_junk_document(collection)

        # Insert with specific name
        first_name = f.first_name()
        _count_2 = f.random.randint(2, 10)
        for i in range(_count_2):
            collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Update Many
        new_name = f.first_name()
        updated_count = collection.update_many({'first_name': first_name}, first_name=new_name)
        assert updated_count == _count_2

        # Find Them with old name
        objs = collection.find(first_name=first_name)
        assert objs == []

        # Find Them with new name
        objs = collection.find(first_name=new_name)
        assert len(objs) == _count_2

        # Count of all
        objs_count = collection.count()
        assert objs_count == (_count_1 + _count_2)

    # Fields
    def test_document_fields(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Insert with specific names
        collection.insert_one(first_name=first_name, last_name=last_name)

        # Find
        obj = collection.find_one(first_name=first_name, last_name=last_name)

        assert set(obj._data.keys()) == {'first_name', 'last_name', '_id'}

    # Save
    def test_document_save_method(self):
        collection = self.db.collection(f.word())

        # Insert with specific name
        first_name = f.first_name()
        collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Find One
        obj = collection.find_one(first_name=first_name)
        new_name = f.first_name()

        # Update it
        obj.first_name = new_name
        obj.save()

        assert obj.first_name == new_name

        # Find with old name
        old_obj = collection.find_one(first_name=first_name)
        assert old_obj is None

        # Find with new name
        obj = collection.find_one(first_name=new_name)
        assert obj.first_name == new_name

    # Json
    def test_document_json_method(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Insert with specific names
        collection.insert_one(first_name=first_name, last_name=last_name)

        # Find
        obj = collection.find_one(first_name=first_name, last_name=last_name)

        _json = {
            'first_name': first_name,
            'last_name': last_name,
            '_id': obj.id,
        }
        assert obj.json() == json.dumps(_json).decode()

class TestCursorPantherDB(IsolatedAsyncioTestCase):

    @classmethod
    def setUp(cls):
        cls.db_name = uuid4().hex
        cls.db_name = f'{cls.db_name}.pdb'
        cls.db = PantherDB(db_name=cls.db_name, return_cursor=True)

    @classmethod
    def tearDown(cls):
        Path(cls.db_name).unlink()

    @classmethod
    def create_junk_document(cls, collection) -> int:
        _count = f.random.randint(2, 10)
        for i in range(_count):
            collection.insert_one(first_name=f'{f.first_name()}{i}', last_name=f'{f.last_name()}{i}')
        return _count

    # Find
    def test_find_response_type(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Find
        objs = collection.find(first_name=first_name)

        assert isinstance(objs, Cursor)
        assert len([o for o in objs]) == 1
        assert isinstance(objs[0], PantherDocument)

    def test_find_with_filter(self):
        collection = self.db.collection(f.word())

        # Add others
        self.create_junk_document(collection)

        # Insert with specific names
        first_name = f.first_name()
        _count = f.random.randint(2, 10)
        last_names = []
        for i in range(_count):
            last_name = f.last_name()
            last_names.append(last_name)
            collection.insert_one(first_name=first_name, last_name=last_name)

        # Find
        objs = collection.find(first_name=first_name)

        assert isinstance(objs, Cursor)
        assert len([o for o in objs]) == _count
        for i in range(_count):
            assert objs[i].first_name == first_name
            assert objs[i].last_name == last_names[i]

    def test_find_without_filter(self):
        collection = self.db.collection(f.word())

        # Add others
        _count_1 = self.create_junk_document(collection)

        # Insert with specific names
        first_name = f.first_name()
        _count_2 = f.random.randint(2, 10)
        for i in range(_count_2):
            collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Find
        objs = collection.find()
        _count_all = _count_1 + _count_2

        assert isinstance(objs, Cursor)
        assert len([o for o in objs]) == _count_all
        for i in range(_count_all):
            assert isinstance(objs[i], PantherDocument)

        # Check count of specific name
        specific_count = 0
        for i in range(_count_all):
            if objs[i].first_name == first_name:
                specific_count += 1

        assert specific_count == _count_2

    def test_find_with_sort(self):
        collection = self.db.collection(f.word())

        # Insert with specific values
        collection.insert_one(first_name='A', last_name=0)
        collection.insert_one(first_name='A', last_name=1)
        collection.insert_one(first_name='B', last_name=0)
        collection.insert_one(first_name='B', last_name=1)

        # Find without sort
        objs = collection.find()
        assert (objs[0].first_name, objs[0].last_name) == ('A', 0)
        assert (objs[1].first_name, objs[1].last_name) == ('A', 1)
        assert (objs[2].first_name, objs[2].last_name) == ('B', 0)
        assert (objs[3].first_name, objs[3].last_name) == ('B', 1)

        # Find with single sort
        objs = collection.find().sort('first_name', 1)
        assert (objs[0].first_name, objs[0].last_name) == ('A', 0)
        assert (objs[1].first_name, objs[1].last_name) == ('A', 1)
        assert (objs[2].first_name, objs[2].last_name) == ('B', 0)
        assert (objs[3].first_name, objs[3].last_name) == ('B', 1)

        # Find with single sort as a list
        objs = collection.find().sort([('first_name', 1)])
        assert (objs[0].first_name, objs[0].last_name) == ('A', 0)
        assert (objs[1].first_name, objs[1].last_name) == ('A', 1)
        assert (objs[2].first_name, objs[2].last_name) == ('B', 0)
        assert (objs[3].first_name, objs[3].last_name) == ('B', 1)

        objs = collection.find().sort([('first_name', -1)])
        assert (objs[0].first_name, objs[0].last_name) == ('B', 0)
        assert (objs[1].first_name, objs[1].last_name) == ('B', 1)
        assert (objs[2].first_name, objs[2].last_name) == ('A', 0)
        assert (objs[3].first_name, objs[3].last_name) == ('A', 1)

        objs = collection.find().sort([('last_name', 1)])
        assert (objs[0].first_name, objs[0].last_name) == ('A', 0)
        assert (objs[1].first_name, objs[1].last_name) == ('B', 0)
        assert (objs[2].first_name, objs[2].last_name) == ('A', 1)
        assert (objs[3].first_name, objs[3].last_name) == ('B', 1)

        objs = collection.find().sort([('last_name', -1)])
        assert (objs[0].first_name, objs[0].last_name) == ('A', 1)
        assert (objs[1].first_name, objs[1].last_name) == ('B', 1)
        assert (objs[2].first_name, objs[2].last_name) == ('A', 0)
        assert (objs[3].first_name, objs[3].last_name) == ('B', 0)

        # Find with multiple sort
        objs = collection.find().sort([('first_name', 1), ('last_name', 1)])
        assert (objs[0].first_name, objs[0].last_name) == ('A', 0)
        assert (objs[1].first_name, objs[1].last_name) == ('A', 1)
        assert (objs[2].first_name, objs[2].last_name) == ('B', 0)
        assert (objs[3].first_name, objs[3].last_name) == ('B', 1)

        objs = collection.find().sort([('first_name', 1), ('last_name', -1)])
        assert (objs[0].first_name, objs[0].last_name) == ('A', 1)
        assert (objs[1].first_name, objs[1].last_name) == ('A', 0)
        assert (objs[2].first_name, objs[2].last_name) == ('B', 1)
        assert (objs[3].first_name, objs[3].last_name) == ('B', 0)

        objs = collection.find().sort([('first_name', -1), ('last_name', 1)])
        assert (objs[0].first_name, objs[0].last_name) == ('B', 0)
        assert (objs[1].first_name, objs[1].last_name) == ('B', 1)
        assert (objs[2].first_name, objs[2].last_name) == ('A', 0)
        assert (objs[3].first_name, objs[3].last_name) == ('A', 1)

        objs = collection.find().sort([('first_name', -1), ('last_name', -1)])
        assert (objs[0].first_name, objs[0].last_name) == ('B', 1)
        assert (objs[1].first_name, objs[1].last_name) == ('B', 0)
        assert (objs[2].first_name, objs[2].last_name) == ('A', 1)
        assert (objs[3].first_name, objs[3].last_name) == ('A', 0)

    async def test_find_iterations(self):
        collection = self.db.collection(f.word())

        # Insert with specific values
        collection.insert_one(first_name='A', last_name=0)
        collection.insert_one(first_name='A', last_name=1)
        collection.insert_one(first_name='B', last_name=0)
        collection.insert_one(first_name='B', last_name=1)

        # Find without sort
        expected_without_sort_data = {
            0: ('A', 0),
            1: ('A', 1),
            2: ('B', 0),
            3: ('B', 1),
        }
        objs = collection.find()
        for i, obj in enumerate(objs):
            assert (obj.first_name, obj.last_name) == expected_without_sort_data[i]

        i = 0
        async_objs = collection.find()
        async for obj in async_objs:
            assert (obj.first_name, obj.last_name) == expected_without_sort_data[i]
            i += 1

        # # Find Single sort
        expected_single_sort_data = {
            0: ('B', 0),
            1: ('B', 1),
            2: ('A', 0),
            3: ('A', 1),
        }
        objs = collection.find().sort('first_name', -1)
        for i, obj in enumerate(objs):
            assert (obj.first_name, obj.last_name) == expected_single_sort_data[i]

        i = 0
        async_objs = collection.find().sort('first_name', -1)
        async for obj in async_objs:
            assert (obj.first_name, obj.last_name) == expected_single_sort_data[i]
            i += 1



# TODO: Test whole scenario with -> secret_key, return_dict
# TODO: Test where exceptions happen
