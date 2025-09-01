"""Utilities to validate metadata and set a composite index.

This module reads a Parquet metadata table and replaces the DataFrame index with a
string built by concatenating the columns `id`, `type`, and `subtype`.
"""

# %%
import pandas as pd


def set_composite_index(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``df`` with index built from ``id``, ``type`` and ``subtype``.

    The resulting index is named ``id_type_anexes`` and joins the three columns with
    underscores. Missing values are treated as empty strings. If the ``subtype``
    column is not present in the DataFrame, an empty string is used instead.
    """
    required_columns = ["id", "type"]
    missing_required = [c for c in required_columns if c not in df.columns]
    if missing_required:
        missing_str = ", ".join(missing_required)
        raise KeyError(f"Missing required columns to build index: {missing_str}")

    df_with_index = df.copy()
    id_series = df_with_index["id"].astype("string").fillna("")
    type_series = df_with_index["type"].astype("string").fillna("")
    if "subtype" in df_with_index.columns:
        third_series = df_with_index["subtype"].astype("string").fillna("")
    else:
        third_series = pd.Series(
            [""] * len(df_with_index),
            index=df_with_index.index,
            dtype="string",
        )

    index_series = id_series + "_" + type_series + "_" + third_series
    df_with_index.index = index_series
    df_with_index.index.name = "id_type_anexes"
    return df_with_index


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

    df_indexed = set_composite_index(df_original)
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
