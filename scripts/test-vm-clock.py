#!/usr/bin/env python3
"""Fast prerequisite checks; synthetic headers never qualify a physical clock."""
from pathlib import Path
import runpy
import unittest

probe = runpy.run_path(str(Path(__file__).with_name('check-vm-clock.py')))
capabilities = probe['capabilities']


def header():
    data = bytearray(112)
    for offset, value, size in [(0, 0x4b4c4356, 4), (4, 4096, 4), (8, 1, 2),
                                (10, 1, 1), (11, 1, 1), (24, 0x150, 8),
                                (34, 2, 1), (48, 1, 8)]:
        data[offset:offset + size] = value.to_bytes(size, 'little')
    return data


class ClockCapabilities(unittest.TestCase):
    def test_valid_zero_bounds_are_distinct_from_missing_bounds(self):
        data = header()
        result = capabilities(data)
        self.assertTrue(result['requirements_present'])
        self.assertFalse(result['qualified_for_serving'])
        self.assertEqual(result['time_maxerror_nanosec'], 0)
        self.assertEqual(result['counter_period_maxerror_rate_frac_sec'], 0)
        # Estimated errors do not substitute for maximum error guarantees.
        data[24:32] = (0x128).to_bytes(8, 'little')
        result = capabilities(data)
        self.assertFalse(result['requirements_present'])
        self.assertIsNone(result['time_maxerror_nanosec'])
        self.assertIsNone(result['counter_period_maxerror_rate_frac_sec'])

    def test_each_requirement_is_necessary_and_never_qualifies_serving(self):
        for offset, value in [(10, 255), (11, 0), (34, 0), (34, 1), (34, 4),
                              (24, 0x40), (24, 0x10), (48, 0), (25, 0)]:
            with self.subTest(offset=offset, value=value):
                data = header()
                data[offset] = value
                result = capabilities(data)
                self.assertFalse(result['requirements_present'])
                self.assertFalse(result['qualified_for_serving'])
        data = header()
        data[34] = 3  # Free-running calibration remains valid per VMClock v1.
        self.assertTrue(capabilities(data)['requirements_present'])

    def test_invalid_or_incomplete_headers_cannot_supply_bounds(self):
        for offset, value in [(0, 0), (5, 0), (8, 2), (12, 1)]:
            data = header()
            data[offset] = value
            with self.assertRaises(ValueError):
                capabilities(data)
        for length in [0, 31, 103, 111]:
            with self.assertRaises(ValueError):
                capabilities(header()[:length])
        data = header()[:32]
        data[4:8] = (32).to_bytes(4, 'little')
        result = capabilities(data)  # Legal disruption-only device.
        self.assertFalse(result['requirements_present'])
        self.assertIsNone(result['time_maxerror_nanosec'])


if __name__ == '__main__':
    unittest.main()
