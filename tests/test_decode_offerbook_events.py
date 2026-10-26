"""Regression coverage for the event decoder's nullable fields and statuses."""

import unittest

import base58

from ingestion.decode_offerbook_events import ANCHOR_EVENT_TAG, decode_event


class DecodeEventTests(unittest.TestCase):
    discriminator = bytes(range(8))
    event_map = {discriminator: "TestEvent"}
    type_map = {
        "TestEvent": {
            "kind": "struct",
            "fields": [{"name": "amount", "type": "u64"}],
        }
    }

    def decode(self, payload, event_map=None, type_map=None):
        instruction_data = base58.b58encode(
            ANCHOR_EVENT_TAG + self.discriminator + payload
        ).decode()
        return decode_event(
            instruction_data,
            self.event_map if event_map is None else event_map,
            self.type_map if type_map is None else type_map,
        )

    def test_unknown_event_preserves_discriminator_and_payload_without_name(self):
        result = self.decode(b"unrecognized payload", event_map={})
        self.assertEqual(result["decode_status"], "unknown_event")
        self.assertIsNone(result["event_name"])
        self.assertIsNone(result["decoded_event"])
        self.assertEqual(result["event_discriminator"], self.discriminator.hex())
        self.assertEqual(result["remaining_bytes_hex"], b"unrecognized payload".hex())

    def test_known_event_has_name_and_decoded_payload(self):
        result = self.decode((42).to_bytes(8, "little"))
        self.assertEqual(result["decode_status"], "ok")
        self.assertEqual(result["event_name"], "TestEvent")
        self.assertEqual(result["decoded_event"], {"amount": 42})

    def test_known_event_with_extra_bytes_keeps_name(self):
        result = self.decode((42).to_bytes(8, "little") + b"extra")
        self.assertEqual(result["decode_status"], "ok_with_remaining_bytes")
        self.assertEqual(result["event_name"], "TestEvent")
        self.assertEqual(result["remaining_bytes_hex"], b"extra".hex())

    def test_missing_type_keeps_known_event_name(self):
        result = self.decode(b"", type_map={})
        self.assertEqual(result["decode_status"], "missing_event_type")
        self.assertEqual(result["event_name"], "TestEvent")
        self.assertIsNone(result["decoded_event"])

    def test_malformed_body_keeps_known_event_name(self):
        result = self.decode(b"\x01")
        self.assertEqual(result["decode_status"], "decode_error")
        self.assertEqual(result["event_name"], "TestEvent")
        self.assertIsNone(result["decoded_event"])

    def test_normal_inner_instruction_is_not_an_event(self):
        instruction_data = base58.b58encode(bytes(32)).decode()
        self.assertIsNone(decode_event(instruction_data, self.event_map, self.type_map))


if __name__ == "__main__":
    unittest.main()
