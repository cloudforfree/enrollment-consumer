"""Handler tests.

`EnrollmentScenarios` is backend-agnostic. It runs against SQLite always (stdlib only) and
against real Postgres when TEST_DATABASE_URL is set.
"""
import json
import logging
import os
import shutil
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

from enrollment import handler
from enrollment.models import EnrollmentRequest, MalformedMessage, parse_body
from enrollment.sqlite_store import SqliteStore
from enrollment.store import Outcome

from .helpers import body, event, failed_ids, imei, record

TENANT = "test-int"


class FlakyStore:
    """Wraps a real store and raises for chosen IMEIs (simulates transient DB errors)."""

    def __init__(self, inner, bad_imeis):
        self._inner, self._bad = inner, set(bad_imeis)

    def reserve(self, request, limit):
        if request.imei in self._bad:
            raise ConnectionError("simulated transient DB failure")
        return self._inner.reserve(request, limit)


class EnrollmentScenarios:
    """Mixin (not a TestCase, so it is not collected on its own)."""

    LIMIT = 5

    def database_url(self) -> str:
        raise NotImplementedError

    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)
        self.url = self.database_url()
        patcher = mock.patch.dict(
            os.environ, {"DATABASE_URL": self.url, "ENROLLMENT_LIMIT": str(self.LIMIT)}
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        self.verifier = handler.create_store(self.url)
        self.addCleanup(self.verifier.close)
        self.verifier.init_schema()
        self.verifier.truncate_all()
        self.addCleanup(handler.close_store)

    def invoke(self, *records):
        return handler.lambda_handler(event(*records), context=None)

    # 1 Clean test with zero batchItemFailures
    def test_clean_enrollment(self):
        response = self.invoke(record("m1", body(imei(1))))

        self.assertEqual(response, {"batchItemFailures": []})
        self.assertEqual(self.verifier.active_count(TENANT), 1)

    # 2 Test duplicate sqs message delivey resulting no operation not failure
    def test_duplicate_delivery_is_no_op(self):
        msg = record("m1", body(imei(1)))
        self.assertEqual(failed_ids(self.invoke(msg)), [])
        # SQS redelivery, same messageId
        self.assertEqual(failed_ids(self.invoke(msg)), [])  
        # producer-side retry: same business event under a different messageId
        self.assertEqual(failed_ids(self.invoke(record("m2", body(imei(1))))), [])

        self.assertEqual(self.verifier.active_count(TENANT), 1)

    def test_duplicate_at_capacity_does_not_fail_or_consume_slot(self):
        self.invoke(*[record(f"m{i}", body(imei(i))) for i in range(self.LIMIT)])
        self.assertEqual(self.verifier.active_count(TENANT), self.LIMIT)

        response = self.invoke(record("redelivered", body(imei(0))))

        self.assertEqual(failed_ids(response), [])
        self.assertEqual(self.verifier.active_count(TENANT), self.LIMIT)

    # 3 Testing concurrency test by creating 16 threads
    def test_concurrent_enrollments_never_exceed_limit(self):
        print('concurrency test')
        attempts = 16
        barrier = threading.Barrier(attempts)
        results, errors = {}, []

        def worker(i):
            try:
                barrier.wait(timeout=10)
                results[i] = failed_ids(self.invoke(record(f"m{i}", body(imei(i)))))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                handler.close_store()  # closes this worker thread's connection

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(attempts)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        self.assertEqual(errors, [])
        self.assertEqual(self.verifier.active_count(TENANT), self.LIMIT)
        rejected = [i for i, failed in results.items() if failed]
        self.assertEqual(len(rejected), attempts - self.LIMIT)
        for i in rejected:
            self.assertEqual(results[i], [f"m{i}"])

    def test_concurrent_duplicates_of_one_device_consume_one_slot(self):
        attempts = 8
        barrier = threading.Barrier(attempts)
        results, errors = [], []

        def worker(i):
            try:
                barrier.wait(timeout=10)
                results.append(failed_ids(self.invoke(record(f"m{i}", body(imei(42))))))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                handler.close_store()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(attempts)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        self.assertEqual(errors, [])
        self.assertEqual(results, [[]] * attempts)
        self.assertEqual(self.verifier.active_count(TENANT), 1)

    # 4 Partial batch failure for bad messages
    def test_partial_batch_failure(self):
        with mock.patch.dict(os.environ, {"ENROLLMENT_LIMIT": "2"}):
            response = self.invoke(
                record("ok-1", body(imei(1))),
                record("bad-json", "{not json"),
                record("bad-imei", body("12345")),
                record("missing-field", {"imei": imei(9), "tenant_id": TENANT}),
                record("ok-2", body(imei(2))),
                record("dup-of-ok-1", body(imei(1))),       # no-op success
                record("over-limit", {"imei": imei(4), "tenant_id": "other"}),         # tenant already holds 2
                record("other-tenant", body(imei(5), tenant_id="other")),  # own quota
            )

        self.assertEqual(
            sorted(failed_ids(response)),
            sorted(["bad-json", "bad-imei", "missing-field", "over-limit"]),
        )
        self.assertEqual(self.verifier.active_count(TENANT), 2)
        self.assertEqual(self.verifier.active_count("other"), 1)

    def test_transient_db_error_fails_only_that_record(self):
        flaky = FlakyStore(handler.get_store(), bad_imeis={imei(2)})
        with mock.patch.object(handler, "get_store", return_value=flaky):
            response = self.invoke(
                record("a", body(imei(1))),
                record("b", body(imei(2))),
                record("c", body(imei(3))),
            )

        self.assertEqual(failed_ids(response), ["b"])
        self.assertEqual(self.verifier.active_count(TENANT), 2)


    def test_empty_batch(self):
        self.assertEqual(handler.lambda_handler({"Records": []}, None), {"batchItemFailures": []})


class SqliteTests(EnrollmentScenarios, unittest.TestCase):
    def database_url(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        return f"sqlite:///{os.path.join(tmp, 'enrollment.db')}"


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "TEST_DATABASE_URL not set")
class PostgresTests(EnrollmentScenarios, unittest.TestCase):
    def database_url(self):
        return os.environ["TEST_DATABASE_URL"]


class ConfigAndParsingTests(unittest.TestCase):
    def test_default_limit_is_five(self):
        with mock.patch.dict(os.environ, clear=True):
            self.assertEqual(handler.get_limit(), 5)

    def test_invalid_limit_is_rejected(self):
        for bad in ("abc", "0", "-3"):
            with mock.patch.dict(os.environ, {"ENROLLMENT_LIMIT": bad}):
                with self.assertRaises(ValueError):
                    handler.get_limit()

    def test_parse_valid_body(self):
        req = parse_body(json.dumps(body(imei(7))))
        self.assertEqual((req.tenant_id, req.imei), (TENANT, imei(7)))
        self.assertIsNotNone(req.requested_at.tzinfo)

    def test_parse_rejects_bad_bodies(self):
        bad_bodies = [
            None,
            "",
            "[]",
            "{",
            json.dumps(body(imei(1), requested_at="yesterday")),
            json.dumps(body(imei(1), tenant_id="")),
            json.dumps(body(imei(1), tenant_id="a\u0000b")),
            json.dumps(body(imei(1), device_type=5)),
            json.dumps(body("35693803564380X")),
        ]
        for bad in bad_bodies:
            with self.subTest(body=bad):
                with self.assertRaises(MalformedMessage):
                    parse_body(bad)

    def test_sqlite_store_rolls_back_and_stays_usable_after_db_error(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        store = SqliteStore(os.path.join(tmp, "x.db"))
        self.addCleanup(store.close)
        store.init_schema()
        good = parse_body(json.dumps(body(imei(1))))
        violating = EnrollmentRequest(TENANT, imei(2), None, good.requested_at)  # NOT NULL violation

        with self.assertRaises(sqlite3.IntegrityError):
            store.reserve(violating, 5)

        self.assertEqual(store.active_count(TENANT), 0)
        self.assertEqual(store.reserve(good, 5), Outcome.ENROLLED)


if __name__ == "__main__":
    unittest.main()
