"""Pipeline ETL dataset otomotif untuk assignment Data Engineering.

Jalankan dari folder project: python src/pipeline.py
Input selalu berasal dari data/raw, bukan dataset referensi mentor.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_PATH = BASE_DIR / "data" / "raw" / "automobileEDA_dirty_training.csv"
OUTPUT_PATH = BASE_DIR / "data" / "processed" / "automobileEDA_processed.csv"
REPORT_PATH = BASE_DIR / "documentation" / "pipeline-report.json"

CATEGORICAL_COLUMNS = [
    "make", "aspiration", "num-of-doors", "body-style", "drive-wheels",
    "engine-location", "engine-type", "num-of-cylinders", "fuel-system",
    "horsepower-binned",
]
NUMERIC_COLUMNS = [
    "symboling", "normalized-losses", "wheel-base", "length", "width",
    "height", "curb-weight", "engine-size", "bore", "stroke",
    "compression-ratio", "horsepower", "peak-rpm", "city-mpg",
    "highway-mpg", "price", "city-L/100km", "diesel", "gas",
]
POSITIVE_COLUMNS = [
    column for column in NUMERIC_COLUMNS
    if column not in {"symboling", "diesel", "gas"}
]
DOOR_MAPPING = {"two": 2, "four": 4}
CYLINDER_MAPPING = {
    "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "eight": 8, "twelve": 12,
}
DATE_FORMATS = [
    (r"\d{4}-\d{2}-\d{2}", "%Y-%m-%d"),
    (r"\d{2}/\d{2}/\d{4}", "%d/%m/%Y"),
    (r"\d{2}-\d{2}-\d{4}", "%m-%d-%Y"),
    (r"\d{2}-[A-Za-z]{3}-\d{4}", "%d-%b-%Y"),
]
ALLOWED_MISSING_COLUMNS = {"transaction_date", "stroke"}


def file_sha256(path: Path) -> str:
    """Sidik jari file untuk membuktikan dataset mentah tidak berubah."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def missing_counts(df: pd.DataFrame) -> dict[str, int]:
    """Jumlah nilai kosong pada setiap kolom, termasuk kolom tanpa missing."""
    return {column: int(count) for column, count in df.isna().sum().items()}


def display_value(value: object) -> str:
    """Bentuk ringkas untuk contoh perbandingan di terminal dan laporan."""
    if pd.isna(value):
        return "<kosong>"
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    return str(value)


def load_data(input_path: Path = INPUT_PATH) -> pd.DataFrame:
    """EXTRACT: baca CSV, lalu periksa ketersediaan kolom yang diperlukan."""
    if not input_path.is_file():
        raise FileNotFoundError(f"Dataset input tidak ditemukan: {input_path}")
    df = pd.read_csv(input_path)
    required = {"transaction_date", *CATEGORICAL_COLUMNS, *NUMERIC_COLUMNS}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Kolom wajib tidak ditemukan: {missing}")
    if df.empty:
        raise ValueError("Dataset input tidak memiliki baris data.")
    return df


def inspect_data(df: pd.DataFrame) -> dict:
    """Tampilkan semua pemeriksaan awal yang diwajibkan assignment."""
    print("\n1. DATA INSPECTION")
    print("Lima baris pertama:")
    print(df.head().to_string(index=False))
    print(f"\nUkuran awal: {len(df)} baris x {len(df.columns)} kolom")
    print("Nama kolom:", df.columns.tolist())
    print("\nTipe data setiap kolom:")
    print(df.dtypes.to_string())
    print("\nMissing values setiap kolom:")
    print(df.isna().sum().to_string())
    duplicates = int(df.duplicated().sum())
    print(f"Total sel kosong: {int(df.isna().sum().sum())}")
    print(f"Duplicate records (di luar kemunculan pertama): {duplicates}")
    print("\nNilai unik kategori:")
    categories = {}
    for column in CATEGORICAL_COLUMNS:
        values = [display_value(value) for value in df[column].unique()]
        categories[column] = values
        print(f"- {column}: {values}")
    print("\nContoh tanggal mentah:", df["transaction_date"].head(4).tolist())
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "column_names": df.columns.tolist(),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "missing_by_column": missing_counts(df),
        "total_missing_cells": int(df.isna().sum().sum()),
        "duplicate_records": duplicates,
        "categorical_unique_values": categories,
    }


def parse_transaction_dates(values: pd.Series) -> pd.Series:
    """Baca empat pola tanggal tanpa menebak urutan hari dan bulan.

    Pola NN-NN-YYYY ditetapkan sebagai MM-DD-YYYY, berdasarkan nilai
    tidak ambigu seperti 01-15-2025 dan urutan tanggal dataset.
    Tanggal kosong tetap NaT. Nilai terisi yang gagal dibaca menghentikan
    pipeline agar kesalahan parsing tidak tersembunyi sebagai missing.
    """
    text = values.astype("string").str.strip().replace("", pd.NA)
    parsed = pd.Series(pd.NaT, index=text.index, dtype="datetime64[ns]")
    for pattern, date_format in DATE_FORMATS:
        mask = text.str.fullmatch(pattern, na=False)
        parsed.loc[mask] = pd.to_datetime(
            text.loc[mask], format=date_format, errors="coerce"
        )
    invalid = text.notna() & parsed.isna()
    if invalid.any():
        examples = text.loc[invalid].head(5).tolist()
        raise ValueError(f"Tanggal terisi tidak valid/tidak dikenal: {examples}")
    return parsed


def map_count_column(values: pd.Series, mapping: dict, name: str) -> pd.Series:
    """Ubah jumlah dalam kata menjadi integer dan tolak kategori asing."""
    mapped = values.map(mapping)
    invalid = values.notna() & mapped.isna()
    if invalid.any() or mapped.isna().any():
        raise ValueError(f"Kategori {name} tidak dapat dipetakan: {values[mapped.isna()].tolist()}")
    return mapped.astype("int64")


def clean_data(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """TRANSFORM: bersihkan salinan DataFrame tanpa mengubah raw."""
    print("\n2. DATA CLEANING")
    df = raw.copy(deep=True)

    # Rapikan representasi teks sebelum memeriksa duplikat.
    for column in CATEGORICAL_COLUMNS:
        df[column] = (
            df[column].astype("string").str.strip().str.lower()
            .replace({"": pd.NA, "?": pd.NA})
        )
    df["transaction_date"] = (
        df["transaction_date"].astype("string").str.strip()
        .replace({"": pd.NA, "?": pd.NA})
    )

    # Jangan mengubah teks numerik yang salah menjadi missing secara diam-diam.
    for column in NUMERIC_COLUMNS:
        original = df[column].replace({"?": pd.NA, "": pd.NA})
        numeric = pd.to_numeric(original, errors="coerce")
        invalid = original.notna() & numeric.isna()
        if invalid.any():
            raise ValueError(f"Nilai nonnumerik pada {column}: {original[invalid].tolist()}")
        if not np.isfinite(numeric.dropna().to_numpy(dtype=float)).all():
            raise ValueError(f"Nilai tidak finite pada {column}.")
        df[column] = numeric

    # Nilai tinggi yang masih positif tidak otomatis dianggap salah/outlier.
    for column in POSITIVE_COLUMNS:
        if (df[column].dropna() <= 0).any():
            raise ValueError(f"Nilai nol/negatif yang perlu ditinjau pada {column}.")
    for column in ["diesel", "gas"]:
        if not df[column].dropna().isin([0, 1]).all():
            raise ValueError(f"Indikator {column} harus bernilai 0 atau 1.")

    duplicate_mask = df.duplicated(keep="first")
    duplicate_rows = (df.index[duplicate_mask] + 1).tolist()
    df = df.loc[~duplicate_mask].copy()
    after_dedup_rows = len(df)
    after_dedup_missing = missing_counts(df)

    # Harga diperlukan untuk analisis harga. Hindari membuat harga pengganti.
    missing_price = df["price"].isna()
    dropped_price_rows = (df.index[missing_price] + 1).tolist()
    df = df.loc[~missing_price].copy()
    if df.empty:
        raise ValueError("Tidak ada baris tersisa setelah penanganan price kosong.")

    df["transaction_date"] = parse_transaction_dates(df["transaction_date"])
    df["transaction_date_missing"] = df["transaction_date"].isna().astype("int64")

    # Empat stroke kosong berada pada baris rotor. Jangan menebak nilainya.
    df["stroke_missing"] = df["stroke"].isna().astype("int64")
    stroke_missing_by_engine = (
        df.loc[df["stroke"].isna(), "engine-type"].value_counts().to_dict()
    )

    horsepower_missing = df["horsepower"].isna()
    horsepower_median = float(df["horsepower"].median())
    if not np.isfinite(horsepower_median):
        raise ValueError("Median horsepower tidak tersedia untuk imputasi.")
    df["horsepower_imputed"] = horsepower_missing.astype("int64")
    df["horsepower"] = df["horsepower"].fillna(horsepower_median)

    door_modes = df["num-of-doors"].mode()
    if door_modes.empty:
        raise ValueError("Modus num-of-doors tidak tersedia untuk imputasi.")
    door_mode = str(door_modes.iloc[0])
    df["num-of-doors"] = map_count_column(
        df["num-of-doors"].fillna(door_mode), DOOR_MAPPING, "num-of-doors"
    )
    df["num-of-cylinders"] = map_count_column(
        df["num-of-cylinders"], CYLINDER_MAPPING, "num-of-cylinders"
    )
    df["make"] = df["make"].fillna("unknown")

    # Batas bin dari mentor tidak disediakan. Jangan menetapkan ulang batasnya.
    # Label juga ditandai unknown ketika horsepower merupakan hasil imputasi.
    df["horsepower-binned"] = df["horsepower-binned"].fillna("unknown")
    df.loc[horsepower_missing, "horsepower-binned"] = "unknown"

    changes = {}
    for column in raw.columns:
        before = raw.loc[df.index, column]
        before_text = before.map(display_value)
        after_text = df[column].map(display_value)
        changed_cells = int((before_text != after_text).sum())
        semantic_dtype_change = (
            pd.api.types.is_numeric_dtype(before)
            != pd.api.types.is_numeric_dtype(df[column])
            or pd.api.types.is_datetime64_any_dtype(df[column])
        )
        if changed_cells or semantic_dtype_change:
            changes[column] = {
                "changed_cells_on_retained_rows": changed_cells,
                "dtype_before": str(before.dtype),
                "dtype_after": str(df[column].dtype),
            }

    # Simpan contoh dengan nomor baris data asli (header CSV tidak dihitung).
    examples = []
    for column in ["transaction_date", "make", "body-style", "drive-wheels",
                   "fuel-system", "num-of-doors", "num-of-cylinders",
                   "horsepower", "horsepower-binned"]:
        for row_index in df.index:
            before = display_value(raw.at[row_index, column])
            after = display_value(df.at[row_index, column])
            if before != after:
                examples.append({
                    "raw_data_row": int(row_index + 1), "column": column,
                    "before": before, "after": after,
                })
                break

    report = {
        "rows_before": len(raw),
        "duplicates_removed": len(duplicate_rows),
        "duplicate_raw_data_rows_removed": duplicate_rows,
        "rows_after_deduplication": after_dedup_rows,
        "missing_after_deduplication": after_dedup_missing,
        "rows_dropped_missing_price": len(dropped_price_rows),
        "missing_price_raw_data_rows_removed": dropped_price_rows,
        "rows_after_cleaning": len(df),
        "columns_after_cleaning": len(df.columns),
        "missing_before": missing_counts(raw),
        "missing_after": missing_counts(df),
        "total_missing_before": int(raw.isna().sum().sum()),
        "total_missing_after": int(df.isna().sum().sum()),
        "remaining_duplicate_records": int(df.duplicated().sum()),
        "horsepower_median": horsepower_median,
        "horsepower_imputed_count": int(horsepower_missing.sum()),
        "num_of_doors_mode": door_mode,
        "stroke_missing_by_engine_type": {
            key: int(value) for key, value in stroke_missing_by_engine.items()
        },
        "columns_changed": changes,
        "columns_added": [column for column in df if column not in raw],
        "examples": examples,
    }
    print(f"Baris: {len(raw)} -> {after_dedup_rows} -> {len(df)}")
    print(f"Duplikat dihapus: {len(duplicate_rows)}")
    print(f"Baris tanpa price dihapus: {len(dropped_price_rows)}")
    print(f"Missing cells: {report['total_missing_before']} -> {report['total_missing_after']}")
    print(f"Median horsepower: {horsepower_median}; modus pintu: {door_mode}")
    print("Kolom berubah:", list(changes))
    print("Kolom penanda baru:", report["columns_added"])
    print("\nMissing values sesudah cleaning:")
    print(df.isna().sum().to_string())
    print("\nContoh cleaning:")
    print(pd.DataFrame(examples).to_string(index=False))
    return df, report


def transform_data(cleaned: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """TRANSFORM: Min-Max engine-size dan one-hot encoding body-style."""
    print("\n3. DATA TRANSFORMATION")
    df = cleaned.copy(deep=True)
    minimum = float(df["engine-size"].min())
    maximum = float(df["engine-size"].max())
    if not np.isfinite([minimum, maximum]).all():
        raise ValueError("Rentang engine-size tidak valid.")
    if minimum == maximum:
        # Kebijakan untuk kolom konstan: semua hasil scaling ditetapkan 0.
        df["engine_size_minmax"] = 0.0
    else:
        df["engine_size_minmax"] = (
            (df["engine-size"] - minimum) / (maximum - minimum)
        )

    if df["body-style"].isna().any():
        raise ValueError("body-style tidak boleh kosong sebelum encoding.")
    categories = sorted(df["body-style"].unique().tolist())
    df = pd.get_dummies(df, columns=["body-style"], dtype=int)
    dummy_columns = [f"body-style_{category}" for category in categories]

    sample_indexes = list(dict.fromkeys([
        cleaned.index[0], cleaned["engine-size"].idxmin(),
        cleaned["engine-size"].idxmax(),
    ]))
    scaling_examples = [{
        "raw_data_row": int(index + 1),
        "engine_size_before": int(cleaned.at[index, "engine-size"]),
        "engine_size_minmax_after": float(df.at[index, "engine_size_minmax"]),
    } for index in sample_indexes]
    encoding_examples = []
    for category in categories:
        index = cleaned.index[cleaned["body-style"] == category][0]
        encoding_examples.append({
            "raw_data_row": int(index + 1), "body_style_before": category,
            **{column: int(df.at[index, column]) for column in dummy_columns},
        })
    report = {
        "scaling_column": "engine-size",
        "scaling_output_column": "engine_size_minmax",
        "scaling_method": "Min-Max Scaling",
        "minimum": minimum,
        "maximum": maximum,
        "constant_column_policy": "all zero if maximum equals minimum",
        "scaling_examples": scaling_examples,
        "encoding_column": "body-style",
        "encoding_method": "one-hot encoding",
        "encoding_categories": categories,
        "encoding_output_columns": dummy_columns,
        "encoding_examples": encoding_examples,
        "columns_added": [column for column in df if column not in cleaned],
        "columns_removed": [column for column in cleaned if column not in df],
    }
    print(f"Min-Max engine-size: min={minimum:g}, max={maximum:g}")
    print(pd.DataFrame(scaling_examples).to_string(index=False))
    print("\nContoh one-hot encoding:")
    print(pd.DataFrame(encoding_examples).to_string(index=False))
    print("Kolom transformasi baru:", report["columns_added"])
    return df, report


def validate_data(processed: pd.DataFrame, raw_rows: int,
                  cleaning: dict, transformation: dict) -> None:
    """Periksa aturan kualitas dan rekonsiliasi jumlah baris sebelum disimpan."""
    expected_rows = raw_rows - cleaning["duplicates_removed"] - cleaning["rows_dropped_missing_price"]
    if len(processed) != expected_rows:
        raise ValueError("Jumlah baris akhir tidak sesuai rekonsiliasi cleaning.")
    if processed.duplicated().any():
        raise ValueError("Masih ada duplikat penuh pada dataset hasil.")
    missing = {column for column in processed if processed[column].isna().any()}
    if missing - ALLOWED_MISSING_COLUMNS:
        raise ValueError(f"Missing tidak terencana: {sorted(missing - ALLOWED_MISSING_COLUMNS)}")
    for column in ["transaction_date", "stroke"]:
        expected_flag = processed[column].isna().astype("int64")
        if not processed[f"{column}_missing"].equals(expected_flag):
            raise ValueError(f"Penanda missing {column} tidak sesuai data.")
    if not processed["horsepower_imputed"].isin([0, 1]).all():
        raise ValueError("Penanda imputasi horsepower tidak valid.")
    scaled = processed["engine_size_minmax"].to_numpy(dtype=float)
    if not np.isfinite(scaled).all() or not ((scaled >= -1e-12) & (scaled <= 1 + 1e-12)).all():
        raise ValueError("Hasil Min-Max berada di luar rentang 0-1.")
    dummy = processed[transformation["encoding_output_columns"]]
    if not dummy.isin([0, 1]).all().all() or not dummy.sum(axis=1).eq(1).all():
        raise ValueError("One-hot encoding tidak valid: perlu tepat satu nilai 1 per baris.")
    print("\n4. VALIDATION")
    print("Rekonsiliasi baris, missing yang diizinkan, penanda, scaling, dan encoding: lulus.")


def save_data(df: pd.DataFrame, output_path: Path = OUTPUT_PATH,
              input_path: Path = INPUT_PATH) -> None:
    """LOAD: simpan sebagai file baru; larang menulis ke folder raw."""
    output_path = output_path.resolve()
    if output_path.is_relative_to(input_path.resolve().parent):
        raise ValueError("Output tidak boleh ditulis ke folder data/raw.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, date_format="%Y-%m-%d", encoding="utf-8")
    print("\n5. LOAD / SAVE")
    print(f"Dataset hasil: {output_path}")
    print(f"Ukuran akhir: {len(df)} baris x {len(df.columns)} kolom")


def main() -> None:
    """Jalankan pipeline dari awal sampai akhir dalam satu eksekusi."""
    raw_hash_before = file_sha256(INPUT_PATH)
    raw = load_data()
    inspection = inspect_data(raw)
    cleaned, cleaning = clean_data(raw)
    processed, transformation = transform_data(cleaned)
    validate_data(processed, len(raw), cleaning, transformation)
    save_data(processed)
    raw_hash_after = file_sha256(INPUT_PATH)
    if raw_hash_before != raw_hash_after:
        raise RuntimeError("Isi dataset mentah berubah selama eksekusi.")
    report = {
        "input_file": INPUT_PATH.relative_to(BASE_DIR).as_posix(),
        "output_file": OUTPUT_PATH.relative_to(BASE_DIR).as_posix(),
        "input_sha256_before": raw_hash_before,
        "input_sha256_after": raw_hash_after,
        "raw_unchanged": True,
        "runtime": {
            "python": sys.version.split()[0],
            "pandas": pd.__version__, "numpy": np.__version__,
        },
        "inspection": inspection,
        "cleaning": cleaning,
        "transformation": transformation,
        "final": {
            "rows": len(processed), "columns": len(processed.columns),
            "column_names": processed.columns.tolist(),
            "dtypes_in_memory": processed.dtypes.astype(str).to_dict(),
            "missing_by_column": missing_counts(processed),
            "total_missing_cells": int(processed.isna().sum().sum()),
            "duplicate_records": int(processed.duplicated().sum()),
        },
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(f"Laporan otomatis: {REPORT_PATH}")
    print("Dataset mentah tidak berubah (SHA-256 sebelum = sesudah).")
    print("Pipeline selesai.")


if __name__ == "__main__":
    main()
