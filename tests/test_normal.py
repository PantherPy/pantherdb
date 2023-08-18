import os
import orjson as json

from uuid import uuid4
from faker import Faker
from unittest import TestCase
from pantherdb import PantherDB, PantherCollection, PantherDocument

f = Faker()


class TestNormalPantherDB(TestCase):

    @classmethod
    def setUp(cls):
        cls.db_name = uuid4().hex
        cls.db_name = f'{cls.db_name}.pdb'
        cls.db = PantherDB(db_name=cls.db_name)

    @classmethod
    def tearDown(cls):
        os.remove(cls.db_name)

    @classmethod
    def create_junk_document(cls, collection) -> int:
        _count = f.random.randint(2, 10)
        for i in range(_count):
            collection.insert_one(first_name=f'{f.first_name()}{i}', last_name=f'{f.last_name()}{i}')
        return _count

    # Create DB
    def test_creation_of_db(self):
        self.assertTrue(os.path.exists(self.db_name))
        self.assertTrue(os.path.isfile(self.db_name))
        self.assertEqual(self.db.db_name, self.db_name)

    def test_creation_of_db_without_extension(self):
        db_name = uuid4().hex
        db = PantherDB(db_name=db_name)
        final_db_name = f'{db_name}.pdb'

        self.assertTrue(os.path.exists(final_db_name))
        self.assertTrue(os.path.isfile(final_db_name))
        self.assertEqual(db.db_name, final_db_name)

        os.remove(final_db_name)

    def test_creation_of_collection(self):
        collection_name = f.word()
        collection = self.db.collection(collection_name)

        self.assertTrue(bool(collection))
        self.assertTrue(isinstance(collection, PantherCollection))
        self.assertEqual(collection.collection_name, collection_name)
        self.assertEqual(collection.content, {})
        self.assertEqual(collection.secret_key, None)

    # Drop
    def test_drop_collection(self):
        collection = self.db.collection(f.word())
        collection.drop()
        self.assertEqual(collection.content, {})

    # Insert
    def test_insert_one(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()
        obj = collection.insert_one(first_name=first_name, last_name=last_name)

        self.assertTrue(isinstance(obj, PantherDocument))
        self.assertEqual(obj.id, 1)
        self.assertEqual(obj.first_name, first_name)
        self.assertEqual(obj.last_name, last_name)

    def test_id_assignments(self):
        collection = self.db.collection(f.word())
        _count = f.random.randint(2, 10)
        for i in range(_count):
            obj = collection.insert_one(first_name=f.first_name(), last_name=f.last_name())

        # obj will overwrite after every insert in loop, and we check the last obj
        self.assertEqual(obj.id, _count)

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
        self.assertTrue(isinstance(obj, PantherDocument))
        self.assertEqual(obj.first_name, first_name)
        self.assertEqual(obj.last_name, last_name)

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
        self.assertTrue(isinstance(obj, PantherDocument))
        self.assertEqual(obj.first_name, first_name)
        self.assertEqual(obj.last_name, last_name)

    def test_find_one_none(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Add others
        self.create_junk_document(collection)

        # Find
        obj = collection.find_one(first_name=first_name, last_name=last_name)
        self.assertIsNone(obj)

    # Find
    def test_find_response_type(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Find
        objs = collection.find(first_name=first_name)

        self.assertTrue(isinstance(objs, list))
        self.assertEqual(len(objs), 1)
        self.assertTrue(isinstance(objs[0], PantherDocument))

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

        self.assertTrue(isinstance(objs, list))
        self.assertEqual(len(objs), _count)
        for i in range(_count):
            self.assertEqual(objs[i].first_name, first_name)

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

        self.assertTrue(isinstance(objs, list))
        self.assertEqual(len(objs), _count_all)
        for i in range(_count_all):
            self.assertTrue(isinstance(objs[i], PantherDocument))

        # Check count of specific name
        specific_count = 0
        for i in range(_count_all):
            if objs[i].first_name == first_name:
                specific_count += 1

        self.assertEqual(specific_count, _count_2)

    def test_find_all(self):
        collection = self.db.collection(f.word())

        # Add others
        _count_1 = self.create_junk_document(collection)

        # Insert with specific names
        first_name = f.first_name()
        _count_2 = f.random.randint(2, 10)
        for i in range(_count_2):
            collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Find
        objs = collection.all()
        _count_all = _count_1 + _count_2

        self.assertTrue(isinstance(objs, list))
        self.assertEqual(len(objs), _count_all)
        for i in range(_count_all):
            self.assertTrue(isinstance(objs[i], PantherDocument))

        # Check count of specific name
        specific_count = 0
        for i in range(_count_all):
            if objs[i].first_name == first_name:
                specific_count += 1

        self.assertEqual(specific_count, _count_2)

    # Count
    def test_count_all(self):
        collection = self.db.collection(f.word())

        # Add others
        _count = self.create_junk_document(collection)

        # Count them
        count_all = collection.count()

        self.assertEqual(count_all, _count)
        self.assertEqual(count_all, len(collection.all()))

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
        self.assertEqual(count_specific, _count_2)
        self.assertEqual(count_specific, len(collection.find(first_name=first_name)))

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
        self.assertEqual(new_obj, None)

        # Count of all
        objs_count = collection.count()
        self.assertEqual(objs_count, _count)

    # Delete One
    def test_delete_one(self):
        collection = self.db.collection(f.word())

        # Add others
        _count = self.create_junk_document(collection)

        first_name = f.first_name()
        collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Delete One
        is_deleted = collection.delete_one(first_name=first_name)
        self.assertTrue(is_deleted)

        # Find It Again
        new_obj = collection.find_one(first_name=first_name)
        self.assertEqual(new_obj, None)

        # Count of all
        objs_count = collection.count()
        self.assertEqual(objs_count, _count)

    def test_delete_one_not_found(self):
        collection = self.db.collection(f.word())

        # Add others
        _count = self.create_junk_document(collection)

        first_name = f.first_name()

        # Delete One
        is_deleted = collection.delete_one(first_name=first_name)
        self.assertFalse(is_deleted)

        # Count of all
        objs_count = collection.count()
        self.assertEqual(objs_count, _count)

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
        self.assertTrue(is_deleted)

        # Count of all
        objs_count = collection.count()
        self.assertEqual(objs_count, _count_1 + _count_2 - 1)

        # Count of undeleted
        undeleted_count = collection.count(first_name=first_name)
        self.assertEqual(undeleted_count, _count_2 - 1)

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
        self.assertEqual(deleted_count, _count_2)

        # Count of all
        objs_count = collection.count()
        self.assertEqual(objs_count, _count_1)

    def test_delete_many_not_found(self):
        collection = self.db.collection(f.word())

        # Add others
        _count = self.create_junk_document(collection)

        first_name = f.first_name()

        # Delete Many
        deleted_count = collection.delete_many(first_name=first_name)
        self.assertEqual(deleted_count, 0)

        # Count of all
        objs_count = collection.count()
        self.assertEqual(objs_count, _count)

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
        self.assertEqual(obj.first_name, new_name)

        # Find with old name
        old_obj = collection.find_one(first_name=first_name)
        self.assertIsNone(old_obj)

        # Find with new name
        obj = collection.find_one(first_name=new_name)
        self.assertEqual(obj.first_name, new_name)

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
        self.assertTrue(is_updated)

        # Find with old name
        old_obj = collection.find_one(first_name=first_name)
        self.assertIsNone(old_obj)

        # Find with new name
        obj = collection.find_one(first_name=new_name)
        self.assertEqual(obj.first_name, new_name)

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
        self.assertFalse(is_updated)

        # Find with old name
        old_obj = collection.find_one(first_name=first_name)
        self.assertIsNotNone(old_obj)

        # Find with new name
        obj = collection.find_one(first_name=new_name)
        self.assertIsNone(obj)

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
        self.assertEqual(updated_count, _count_2)

        # Find Them with old name
        objs = collection.find(first_name=first_name)
        self.assertFalse(objs)

        # Find Them with new name
        objs = collection.find(first_name=new_name)
        self.assertEqual(len(objs), _count_2)

        # Count of all
        objs_count = collection.count()
        self.assertEqual(objs_count, _count_1 + _count_2)

    # Fields
    def test_document_fields(self):
        collection = self.db.collection(f.word())
        first_name = f.first_name()
        last_name = f.last_name()

        # Insert with specific names
        collection.insert_one(first_name=first_name, last_name=last_name)

        # Find
        obj = collection.find_one(first_name=first_name, last_name=last_name)

        self.assertEqual(set(obj.data.keys()), {'first_name', 'last_name', '_id'})

    # Save
    def test_document_save_method(self):
        collection = self.db.collection(f.word())

        # Add others
        # _count = self.create_junk_document(collection)

        # Insert with specific name
        first_name = f.first_name()
        collection.insert_one(first_name=first_name, last_name=f.last_name())

        # Find One
        obj = collection.find_one(first_name=first_name)
        new_name = f.first_name()

        # Update it
        obj.first_name = new_name
        obj.save()

        self.assertEqual(obj.first_name, new_name)

        # Find with old name
        old_obj = collection.find_one(first_name=first_name)
        self.assertIsNone(old_obj)

        # Find with new name
        obj = collection.find_one(first_name=new_name)
        self.assertEqual(obj.first_name, new_name)

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
            '_id': 1,
        }
        self.assertEqual(obj.json(), json.dumps(_json).decode())


# TODO: Test whole scenario with
#   - secret_key
#   - return_dict
# TODO: Test where exceptions happen
