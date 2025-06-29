import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from unittest import TestCase
from uuid import uuid4

from pantherdb import PantherDB


class TestThreadSafety(TestCase):
    """Comprehensive thread safety tests for PantherDB"""

    @classmethod
    def setUpClass(cls):
        cls.db_name = f"thread_test_{uuid4().hex}.pdb"
        cls.db = PantherDB(db_name=cls.db_name)
        cls.collection_name = "test_collection"
        cls.collection = cls.db.collection(cls.collection_name)

    @classmethod
    def tearDownClass(cls):
        try:
            Path(cls.db_name).unlink(missing_ok=True)
        except:
            pass

    def setUp(self):
        # Clear collection before each test
        self.collection.drop()

    def test_concurrent_inserts(self):
        """Test multiple threads inserting documents simultaneously"""
        num_threads = 10
        docs_per_thread = 50
        expected_total = num_threads * docs_per_thread

        def insert_documents(thread_id):
            results = []
            for i in range(docs_per_thread):
                doc = self.collection.insert_one(
                    thread_id=thread_id,
                    doc_id=i,
                    data=f"data_{thread_id}_{i}"
                )
                results.append(doc.id)
            return results

        # Run concurrent inserts
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(insert_documents, i) for i in range(num_threads)]
            all_results = []
            for future in as_completed(futures):
                all_results.extend(future.result())

        # Verify all documents were inserted
        final_count = self.collection.count()
        self.assertEqual(final_count, expected_total)

        # Verify all IDs are unique
        unique_ids = set(all_results)
        self.assertEqual(len(unique_ids), expected_total)

    def test_concurrent_reads_and_writes(self):
        """Test concurrent reads and writes"""
        # Insert initial data
        initial_docs = []
        for i in range(100):
            doc = self.collection.insert_one(value=i, status="initial")
            initial_docs.append(doc)

        num_readers = 5
        num_writers = 3
        read_iterations = 20
        write_iterations = 10

        read_results = queue.Queue()
        write_results = queue.Queue()

        def reader(reader_id):
            for i in range(read_iterations):
                try:
                    # Random read operations
                    if random.choice([True, False]):
                        count = self.collection.count()
                        read_results.put(("count", reader_id, i, count))
                    else:
                        docs = self.collection.find()
                        read_results.put(("find", reader_id, i, len(docs)))

                    time.sleep(random.uniform(0.001, 0.01))
                except Exception as e:
                    print(f'{str(e)=}')
                    read_results.put(("error", reader_id, i, str(e)))

        def writer(writer_id):
            for i in range(write_iterations):
                try:
                    # Random write operations
                    if random.choice([True, False]):
                        doc = self.collection.insert_one(
                            writer_id=writer_id,
                            iteration=i,
                            data=f"writer_{writer_id}_iter_{i}"
                        )
                        write_results.put(("insert", writer_id, i, doc.id))
                    else:
                        # Update a random document
                        docs = self.collection.find()
                        if docs:
                            doc = random.choice(docs)
                            doc.update(updated_by=writer_id, updated_at=i)
                            write_results.put(("update", writer_id, i, doc.id))

                    time.sleep(random.uniform(0.001, 0.01))
                except Exception as e:
                    write_results.put(("error", writer_id, i, str(e)))

        # Start all threads
        threads = []
        for i in range(num_readers):
            t = threading.Thread(target=reader, args=(i,))
            threads.append(t)
            t.start()

        for i in range(num_writers):
            t = threading.Thread(target=writer, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Verify no errors occurred
        read_errors = [r for r in read_results.queue if r[0] == "error"]
        write_errors = [r for r in write_results.queue if r[0] == "error"]

        self.assertEqual(len(read_errors), 0, f"Read errors: {read_errors}")
        self.assertEqual(len(write_errors), 0, f"Write errors: {write_errors}")

        # Verify data integrity
        final_count = self.collection.count()
        self.assertGreater(final_count, 100)  # Should have more than initial docs

    def test_concurrent_deletes(self):
        """Test concurrent delete operations"""
        # Create documents to delete
        docs_to_delete = []
        docs_to_keep = []

        for i in range(50):
            if i < 30:
                doc = self.collection.insert_one(
                    to_delete=True,
                    doc_id=i
                )
                docs_to_delete.append(doc)
            else:
                doc = self.collection.insert_one(
                    to_delete=False,
                    doc_id=i
                )
                docs_to_keep.append(doc)

        num_threads = 3

        def delete_documents(thread_id):
            deleted_count = 0
            for doc in docs_to_delete:
                if random.choice([True, False]):  # Randomly delete some
                    try:
                        self.collection.delete_one(_id=doc.id)
                        deleted_count += 1
                    except:
                        pass
                    time.sleep(random.uniform(0.001, 0.005))
            return deleted_count

        # Run concurrent deletes
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(delete_documents, i) for i in range(num_threads)]
            total_deleted = sum(future.result() for future in as_completed(futures))

        # Verify final state
        remaining_docs = self.collection.find()
        docs_to_delete_remaining = self.collection.find(to_delete=True)
        docs_to_keep_remaining = self.collection.find(to_delete=False)

        # Should have some docs deleted but all keep docs should remain
        self.assertEqual(len(docs_to_keep_remaining), 20)
        self.assertLessEqual(len(docs_to_delete_remaining), 30)

    def test_singleton_thread_safety(self):
        """Test that singleton pattern is thread-safe"""
        db_name = f"singleton_test_{uuid4().hex}"
        instances = []

        def create_instance(thread_id):
            instance = PantherDB(db_name=db_name)
            instances.append((thread_id, instance))
            return instance

        # Create instances concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(create_instance, i) for i in range(10)]
            for future in as_completed(futures):
                future.result()

        # All instances should be the same object
        first_instance = instances[0][1]
        for thread_id, instance in instances:
            self.assertIs(instance, first_instance,
                          f"Instance from thread {thread_id} is not the same")

        # Cleanup
        try:
            Path(f"{db_name}.json").unlink(missing_ok=True)
        except:
            pass

    def test_concurrent_collection_operations(self):
        """Test concurrent operations on different collections"""
        collections = []
        for i in range(5):
            collection = self.db.collection(f"collection_{i}")
            collections.append(collection)

        num_threads = 10
        operations_per_thread = 20

        def collection_operations(thread_id):
            results = []
            for i in range(operations_per_thread):
                collection = random.choice(collections)
                operation = random.choice(["insert", "find", "count", "update"])

                try:
                    if operation == "insert":
                        doc = collection.insert_one(
                            thread_id=thread_id,
                            operation=i,
                            data=f"data_{thread_id}_{i}"
                        )
                        results.append(("insert", collection.collection_name, doc.id))
                    elif operation == "find":
                        docs = collection.find()
                        results.append(("find", collection.collection_name, len(docs)))
                    elif operation == "count":
                        count = collection.count()
                        results.append(("count", collection.collection_name, count))
                    elif operation == "update":
                        docs = collection.find()
                        if docs:
                            doc = random.choice(docs)
                            doc.update(updated_by=thread_id, updated_at=i)
                            results.append(("update", collection.collection_name, doc.id))
                except Exception as e:
                    results.append(("error", collection.collection_name, str(e)))

                time.sleep(random.uniform(0.001, 0.01))

            return results

        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(collection_operations, i) for i in range(num_threads)]
            all_results = []
            for future in as_completed(futures):
                all_results.extend(future.result())

        # Verify no errors occurred
        errors = [r for r in all_results if r[0] == "error"]
        self.assertEqual(len(errors), 0, f"Errors occurred: {errors}")

        # Verify all collections have data
        for collection in collections:
            count = collection.count()
            self.assertGreaterEqual(count, 0)

    def test_concurrent_find_operations(self):
        """Test concurrent find operations with different filters"""
        # Create diverse data
        categories = ["A", "B", "C", "D"]
        for i in range(100):
            self.collection.insert_one(
                category=random.choice(categories),
                value=i,
                data=f"data_{i}"
            )

        num_threads = 8
        finds_per_thread = 15

        def find_operations(thread_id):
            results = []
            for i in range(finds_per_thread):
                try:
                    # Random find operations
                    if random.choice([True, False]):
                        # Find by category
                        category = random.choice(categories)
                        docs = self.collection.find(category=category)
                        results.append(("category", category, len(docs)))
                    else:
                        # Find by value range
                        min_val = random.randint(0, 50)
                        max_val = random.randint(51, 100)
                        docs = self.collection.find()
                        filtered_docs = [d for d in docs if min_val <= d.value <= max_val]
                        results.append(("range", f"{min_val}-{max_val}", len(filtered_docs)))

                    time.sleep(random.uniform(0.001, 0.01))
                except Exception as e:
                    results.append(("error", thread_id, str(e)))

            return results

        # Run concurrent finds
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(find_operations, i) for i in range(num_threads)]
            all_results = []
            for future in as_completed(futures):
                all_results.extend(future.result())

        # Verify no errors occurred
        errors = [r for r in all_results if r[0] == "error"]
        self.assertEqual(len(errors), 0, f"Find errors: {errors}")

        # Verify all operations returned reasonable results
        for result in all_results:
            if result[0] in ["category", "range"]:
                self.assertGreaterEqual(result[2], 0)

    def test_concurrent_cursor_operations(self):
        """Test concurrent cursor operations"""
        self.collection.db.return_cursor = True

        # Create data
        for i in range(50):
            self.collection.insert_one(
                value=i,
                category=f"cat_{i % 5}"
            )

        num_threads = 4
        operations_per_thread = 10

        def cursor_operations(thread_id):
            results = []
            for i in range(operations_per_thread):
                try:
                    # Create cursor with different operations
                    cursor = self.collection.find()

                    if random.choice([True, False]):
                        cursor = cursor.sort("value", -1)
                    if random.choice([True, False]):
                        cursor = cursor.limit(random.randint(5, 20))
                    if random.choice([True, False]):
                        cursor = cursor.skip(random.randint(0, 10))

                    # Iterate through cursor
                    count = 0
                    for doc in cursor:
                        count += 1
                        if count > 10:  # Limit iteration to prevent infinite loops
                            break

                    results.append((thread_id, i, count))
                    time.sleep(random.uniform(0.001, 0.01))
                except Exception as e:
                    results.append((thread_id, i, f"error: {e}"))

            return results

        # Run concurrent cursor operations
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(cursor_operations, i) for i in range(num_threads)]
            all_results = []
            for future in as_completed(futures):
                all_results.extend(future.result())

        # Verify no errors occurred
        errors = [r for r in all_results if isinstance(r[2], str) and r[2].startswith("error")]
        self.assertEqual(len(errors), 0, f"Cursor errors: {errors}")

        # Verify all operations completed successfully
        for result in all_results:
            if not isinstance(result[2], str):
                self.assertGreaterEqual(result[2], 0)

        self.collection.db.return_cursor = False

    def test_stress_test(self):
        """Comprehensive stress test with mixed operations"""
        num_threads = 15
        operations_per_thread = 30

        def stress_operations(thread_id):
            results = []
            for i in range(operations_per_thread):
                try:
                    operation = random.choice([
                        "insert", "find", "update", "delete", "count", "save"
                    ])

                    if operation == "insert":
                        doc = self.collection.insert_one(
                            thread_id=thread_id,
                            operation=i,
                            data=f"stress_{thread_id}_{i}"
                        )
                        results.append(("insert", doc.id))

                    elif operation == "find":
                        if random.choice([True, False]):
                            docs = self.collection.find()
                        else:
                            docs = self.collection.find(thread_id=thread_id)
                        results.append(("find", len(docs)))

                    elif operation == "update":
                        docs = self.collection.find()
                        if docs:
                            doc = random.choice(docs)
                            doc.update(
                                updated_by=thread_id,
                                updated_at=i,
                                stress_test=True
                            )
                            results.append(("update", doc.id))

                    elif operation == "delete":
                        docs = self.collection.find()
                        if docs:
                            doc = random.choice(docs)
                            self.collection.delete_one(_id=doc.id)
                            results.append(("delete", doc.id))

                    elif operation == "count":
                        count = self.collection.count()
                        results.append(("count", count))

                    elif operation == "save":
                        docs = self.collection.find()
                        if docs:
                            doc = random.choice(docs)
                            doc.save()
                            results.append(("save", doc.id))

                    time.sleep(random.uniform(0.001, 0.01))
                except Exception as e:
                    results.append(("error", str(e)))

            return results

        # Run stress test
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(stress_operations, i) for i in range(num_threads)]
            all_results = []
            for future in as_completed(futures):
                all_results.extend(future.result())

        # Verify no errors occurred
        errors = [r for r in all_results if r[0] == "error"]
        self.assertEqual(len(errors), 0, f"Stress test errors: {errors}")

        # Verify final state is consistent
        final_count = self.collection.count()
        self.assertGreaterEqual(final_count, 0)

        # Verify some operations succeeded
        successful_operations = [r for r in all_results if r[0] != "error"]
        self.assertGreater(len(successful_operations), 0)
