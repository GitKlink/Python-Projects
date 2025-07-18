import pandas as pd
import os
from collections import Counter

# ==== SETTINGS ====
TXT_SCAN_FOLDER = r"/your/path/to/txts"    # <--- UPDATE THIS!
ENCODING = "cp1252"                        # For SAP's Windows European exports
HEADER_MARKERS = [
    "Pers.no.",         # For employee-centric SAP
    "ProductID",        # For product-centric SAP
    "PayrollNumber",    # For payroll-centric SAP
    "Start Date",       # Add more as needed
    # "AnotherHeader",
]
strict_mode = False

def make_unique(header):
    counts = Counter()
    new_header = []
    for i, col in enumerate(header):
        base = col.strip() if col.strip() else "Unnamed"
        if counts[base]:
            new_col = f"{base}_{counts[base]+1}"
        else:
            new_col = base
        counts[base] += 1
        new_header.append(new_col)
    return new_header

def is_header_row(cols):
    # Check up to the first three columns for a known header marker
    return any(
        (len(cols) > i and cols[i] in HEADER_MARKERS)
        for i in range(3)
    )

def process_block(block_lines, header):
    header_len = len(header)
    data_rows = []
    for cols in block_lines:
        if cols == header:
            continue
        if len(cols) < header_len:
            cols = cols + [''] * (header_len - len(cols))
        elif len(cols) > header_len:
            cols = cols[:header_len]
        data_rows.append(cols)
    # Ensure header uniqueness and no blanks
    if len(set(header)) != len(header) or any(not col.strip() for col in header):
        if strict_mode:
            print(f"ERROR: Blank or duplicate columns in header {header}. Skipping block.")
            return None
        header = make_unique(header)
    if not data_rows:
        return None
    return pd.DataFrame(data_rows, columns=header)

def process_file(filepath):
    try:
        with open(filepath, "r", encoding=ENCODING) as f:
            lines = f.readlines()

        split_lines = [[c.strip() for c in line.rstrip('\n').split('\t')] for line in lines]

        dataframes = []
        block_lines = []
        header = None
        for cols in split_lines:
            if is_header_row(cols):
                if block_lines and header:
                    df = process_block(block_lines, header)
                    if df is not None:
                        dataframes.append(df)
                header = cols
                block_lines = []
            else:
                if header:
                    block_lines.append(cols)
        # Final block
        if block_lines and header:
            df = process_block(block_lines, header)
            if df is not None:
                dataframes.append(df)
        if not dataframes:
            print(f"No data found in {filepath}")
            return False
        bigdf = pd.concat(dataframes, ignore_index=True)
        # Remove any row that's a repeated header
        mask = (bigdf.apply(lambda row: list(row.values) == list(bigdf.columns), axis=1))
        bigdf = bigdf[~mask]
        output_path = os.path.splitext(filepath)[0] + ".parquet"
        bigdf.to_parquet(output_path, index=False, compression='snappy')
        # Validity check
        try:
            pd.read_parquet(output_path)
            print(f"✓ {os.path.basename(filepath)} → {os.path.basename(output_path)} [rows: {bigdf.shape[0]}, columns: {bigdf.shape[1]}]")
            return True
        except Exception as e:
            print(f"✗ Parquet validation failed for {output_path}: {e}")
            return False
    except Exception as e:
        print(f"ERROR processing {filepath}: {e}")
        return False

def main():
    filepaths = [
        os.path.join(TXT_SCAN_FOLDER, f)
        for f in os.listdir(TXT_SCAN_FOLDER)
        if f.lower().endswith(".txt") and os.path.isfile(os.path.join(TXT_SCAN_FOLDER, f))
    ]
    print(f"Auto-discovered {len(filepaths)} .txt files in {TXT_SCAN_FOLDER}")

    total = len(filepaths)
    success, failed = 0, 0

    for filepath in filepaths:
        ok = process_file(filepath)
        if ok:
            success += 1
        else:
            failed += 1

    print(f"\nDone! {success}/{total} files converted, {failed} failed.\n")

if __name__ == "__main__":
    main()
