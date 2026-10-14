"""Unit tests for null getTransaction retry and fail-safe semantics."""

import os
import unittest
from unittest.mock import patch

from ingestion import incremental_offerbook as module


class GetTransactionTests(unittest.TestCase):
    def setUp(self):
        self.signature = "dummy-signature"

    @patch.dict(os.environ, {"SOLANA_NULL_TX_RETRIES": "3", "SOLANA_ARCHIVE_RPC_URL": ""})
    @patch.object(module.time, "sleep")
    @patch.object(module, "rpc_call")
    def test_retries_null_then_succeeds(self, rpc, sleep):
        rpc.side_effect = [None, None, {"slot": 123}]
        self.assertEqual(module.get_transaction(self.signature)["slot"], 123)
        self.assertEqual(rpc.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    @patch.dict(os.environ, {"SOLANA_NULL_TX_RETRIES": "2", "SOLANA_ARCHIVE_RPC_URL": "https://archive.example"})
    @patch.object(module.time, "sleep")
    @patch.object(module, "rpc_call")
    def test_falls_back_to_archive(self, rpc, sleep):
        rpc.side_effect = [None, None, {"slot": 999}]
        self.assertEqual(module.get_transaction(self.signature)["slot"], 999)
        self.assertEqual(rpc.call_args.kwargs["rpc_url"], "https://archive.example")
        self.assertEqual(rpc.call_count, 3)

    @patch.dict(os.environ, {"SOLANA_NULL_TX_RETRIES": "2", "SOLANA_ARCHIVE_RPC_URL": ""})
    @patch.object(module.time, "sleep")
    @patch.object(module, "rpc_call", return_value=None)
    def test_stops_when_transaction_is_unavailable(self, rpc, sleep):
        with self.assertRaisesRegex(RuntimeError, "forward boundary"):
            module.get_transaction(self.signature)
        self.assertEqual(rpc.call_count, 2)


if __name__ == "__main__":
    unittest.main()
