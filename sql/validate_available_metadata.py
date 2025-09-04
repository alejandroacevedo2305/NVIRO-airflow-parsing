"""Utilities to validate metadata and set index from ``file_identifier``.

This module reads a Parquet metadata table and replaces the DataFrame index
with the string from the ``file_identifier`` column.
"""

# %%
import pandas as pd


def set_file_identifier_index(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``df`` indexed by the ``file_identifier`` column.

    The resulting index is named ``file_identifier``. Values are cast to pandas'
    nullable string dtype and missing values are treated as empty strings.
    """
    if "file_identifier" not in df.columns:
        raise KeyError("Missing required column to build index: file_identifier")

    df_with_index = df.copy()
    index_series = df_with_index["file_identifier"].astype("string").fillna("")
    df_with_index.index = index_series
    df_with_index.index.name = "file_identifier"
    return df_with_index


def set_composite_index(df: pd.DataFrame) -> pd.DataFrame:
    """Deprecated: use ``set_file_identifier_index`` instead."""
    return set_file_identifier_index(df)


def save_indexed_metadata(df: pd.DataFrame) -> None:
    """Save the indexed metadata to a Parquet file."""
    df.to_parquet(
        "sql/metadata_table/flora_fauna_metadata_indexed.parquet",
        index=True,
        compression="gzip",
    )


def load_indexed_metadata() -> pd.DataFrame:
    """Load the indexed metadata from a Parquet file."""
    return pd.read_parquet("sql/metadata_table/flora_fauna_metadata_indexed.parquet")


if __name__ == "__main__":
    # Simple demonstration / validation
    PARQUET_PATH = "sql/metadata_table/flora_fauna_metadata.parquet"
    df_original = pd.read_parquet(PARQUET_PATH)
    print("original shape:", df_original.shape)

    df_indexed = set_file_identifier_index(df_original)
    print("indexed shape:", df_indexed.shape)
    print("index name:", df_indexed.index.name)

    # Quick sanity checks
    assert len(df_indexed) == len(df_original)
    print(
        "sample index keys:\n",
        df_indexed.index.to_series().head().to_string(index=False),
    )

    save_indexed_metadata(df_indexed)
    df_indexed_loaded = load_indexed_metadata()
    print("indexed shape:", df_indexed_loaded.shape)
    print("index name:", df_indexed_loaded.index.name)

    # Quick sanity checks
    assert len(df_indexed_loaded) == len(df_indexed)
    print(
        "sample index keys:\n",
        df_indexed_loaded.index.to_series().head().to_string(index=False),
    )

# %%
