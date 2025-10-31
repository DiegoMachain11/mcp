# transform_variables.py
from pathlib import Path
from typing import Optional, Union, Any
import pandas as pd
import json
import sys
import argparse

"""
transform_variables.py

Read a CSV and output:
{
  "Description": [...],  # from Code_Description
  "Code": [...]          # from Code
}
"""


def load_csv(
    path: Union[str, Path],
    usecols: Optional[Union[str, list]] = None,
    dtype: Optional[dict] = None,
    parse_dates: Optional[Union[str, list]] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"CSV file not found: {p}")

    try:
        df = pd.read_csv(
            p,
            usecols=usecols,
            dtype=dtype,
            parse_dates=parse_dates,
            # sensible defaults for these kinds of files; can be overridden via **kwargs
            encoding=kwargs.pop("encoding", "utf-8-sig"),
            sep=kwargs.pop("sep", ","),
            engine=kwargs.pop("engine", "python"),
            **kwargs,
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to read CSV file {p}: {exc}") from exc

    return df


def transform(
    df: pd.DataFrame, desc_col: str = "Code_Description", code_col: str = "Code"
) -> dict:
    # basic column validation
    missing = [c for c in (desc_col, code_col) if c not in df.columns]
    if missing:
        raise KeyError(
            f"Missing expected column(s): {', '.join(missing)}. "
            f"Available: {list(df.columns)}"
        )

    out = {
        "Description": df[desc_col].astype(str).str.strip().tolist(),
        "Code": df[code_col].astype(str).str.strip().tolist(),
    }
    return out


def main():
    ap = argparse.ArgumentParser(description="Transform CSV → {Description, Code} JSON")
    ap.add_argument("csv", help="Path to input CSV")
    ap.add_argument("-o", "--out", help="Path to write JSON (defaults to stdout)")
    ap.add_argument(
        "--desc-col",
        default="Code_Description",
        help="Column name to use for Description (default: Code_Description)",
    )
    ap.add_argument(
        "--code-col", default="Code", help="Column name to use for Code (default: Code)"
    )
    ap.add_argument(
        "--filter",
        nargs="*",
        default=[],
        help="Optional filters like Section=RG (space-separated)",
    )
    args = ap.parse_args()

    df = load_csv(args.csv)

    # optional simple equality filters, e.g. --filter Section=RG Section_Description="RESUMEN GENERAL DEL ESTABLO"
    for f in args.filter:
        if "=" not in f:
            raise ValueError(f"Bad --filter '{f}'. Use Column=Value.")
        col, val = f.split("=", 1)
        val = val.strip().strip('"').strip("'")
        if col not in df.columns:
            raise KeyError(
                f"Filter column '{col}' not in CSV. Columns: {list(df.columns)}"
            )
        df = df[df[col].astype(str).str.strip() == val]

    result = transform(df, desc_col=args.desc_col, code_col=args.code_col)

    js = json.dumps(result, ensure_ascii=False, indent=4)
    if args.out:
        Path(args.out).write_text(js, encoding="utf-8")
    else:
        print(js)


if __name__ == "__main__":
    main()
