"""Pengujian kualitas hasil dan kasus yang berisiko salah diproses.

Jalankan dari folder project: python -m unittest discover -s tests -v
"""

import contextlib
import io
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import pipeline


class PipelineTests(unittest.TestCase):
    def test_dates_keep_day_and_month_in_the_right_order(self):
        values = pd.Series([
            "2025-01-01", "02/01/2025", "01-03-2025", "04-Jan-2025",
            "03/02/2025", "02-04-2025", "01-15-2025", None,
        ])
        parsed = pipeline.parse_transaction_dates(values)
        self.assertEqual(parsed.iloc[:7].dt.strftime("%Y-%m-%d").tolist(), [
            "2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04",
            "2025-02-03", "2025-02-04", "2025-01-15",
        ])
        self.assertTrue(pd.isna(parsed.iloc[7]))

    def test_invalid_dates_are_reported_instead_of_silently_disappearing(self):
        with self.assertRaisesRegex(ValueError, "Tanggal terisi"):
            pipeline.parse_transaction_dates(pd.Series(["30/02/2025"]))

    def test_full_dataset_reconciles_and_keeps_raw_unchanged(self):
        original_hash = pipeline.file_sha256(pipeline.INPUT_PATH)
        raw = pipeline.load_data()
        snapshot = raw.copy(deep=True)
        with contextlib.redirect_stdout(io.StringIO()):
            clean, cleaning = pipeline.clean_data(raw)
            processed, transformation = pipeline.transform_data(clean)
            pipeline.validate_data(processed, len(raw), cleaning, transformation)

        pd.testing.assert_frame_equal(raw, snapshot)
        self.assertEqual(original_hash, pipeline.file_sha256(pipeline.INPUT_PATH))
        self.assertEqual(raw.shape, (205, 30))
        self.assertEqual(cleaning["duplicates_removed"], 4)
        self.assertEqual(cleaning["rows_dropped_missing_price"], 3)
        self.assertEqual(processed.shape, (198, 38))
        self.assertEqual(processed.isna().sum()[lambda s: s > 0].to_dict(), {
            "transaction_date": 2, "stroke": 4,
        })
        self.assertEqual(int(processed["horsepower_imputed"].sum()), 3)
        self.assertTrue(processed.loc[
            processed["horsepower_imputed"] == 1, "horsepower-binned"
        ].eq("unknown").all())
        self.assertEqual(processed.loc[1, "transaction_date"], pd.Timestamp("2025-01-02"))
        self.assertEqual(processed.loc[2, "transaction_date"], pd.Timestamp("2025-01-03"))
        self.assertAlmostEqual(processed.loc[0, "engine_size_minmax"], 69 / 265)
        self.assertEqual(processed["engine_size_minmax"].min(), 0)
        self.assertEqual(processed["engine_size_minmax"].max(), 1)

    def test_constant_numeric_column_does_not_divide_by_zero(self):
        sample = pd.DataFrame({
            "engine-size": [100, 100], "body-style": ["sedan", "wagon"],
        })
        with contextlib.redirect_stdout(io.StringIO()):
            transformed, _ = pipeline.transform_data(sample)
        self.assertEqual(transformed["engine_size_minmax"].tolist(), [0, 0])

    def test_saving_cannot_overwrite_the_raw_folder(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tests", prefix=".tmp-") as directory:
            raw_path = Path(directory) / "raw" / "input.csv"
            raw_path.parent.mkdir()
            raw_path.write_bytes(b"original\n")
            with self.assertRaisesRegex(ValueError, "data/raw"):
                pipeline.save_data(pd.DataFrame({"x": [1]}), raw_path, raw_path)
            self.assertEqual(raw_path.read_bytes(), b"original\n")
            self.assertEqual(list(raw_path.parent.iterdir()), [raw_path])

    def test_unknown_count_category_is_not_silently_replaced(self):
        with self.assertRaisesRegex(ValueError, "tidak dapat dipetakan"):
            pipeline.map_count_column(pd.Series(["three"]), pipeline.DOOR_MAPPING, "num-of-doors")


if __name__ == "__main__":
    unittest.main()
