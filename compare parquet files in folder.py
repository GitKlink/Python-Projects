import os
import pandas as pd

# ==== USER CONFIG ====
FOLDER1 = r"/path/to/folder1"   # <-- update
FOLDER2 = r"/path/to/folder2"   # <-- update

def get_parquet_files(folder):
    return {f for f in os.listdir(folder) if f.lower().endswith(".parquet") and os.path.isfile(os.path.join(folder, f))}

def compare_parquet_files(file1, file2):
    try:
        df1 = pd.read_parquet(file1)
        df2 = pd.read_parquet(file2)
        # Sort columns and rows for robust comparison
        df1_sorted = df1.sort_index(axis=1).sort_values(list(df1.columns)).reset_index(drop=True)
        df2_sorted = df2.sort_index(axis=1).sort_values(list(df2.columns)).reset_index(drop=True)
        # Compare DataFrames
        return df1_sorted.equals(df2_sorted)
    except Exception as e:
        print(f"Error comparing {file1} and {file2}: {e}")
        return False

def main():
    files1 = get_parquet_files(FOLDER1)
    files2 = get_parquet_files(FOLDER2)
    common = files1 & files2

    if not common:
        print("No matching parquet files found in both folders.")
        return

    same = []
    different = []
    for fname in sorted(common):
        f1 = os.path.join(FOLDER1, fname)
        f2 = os.path.join(FOLDER2, fname)
        print(f"Comparing: {fname} ...", end="")
        if compare_parquet_files(f1, f2):
            print(" SAME")
            same.append(fname)
        else:
            print(" DIFFERENT")
            different.append(fname)

    print("\nSummary:")
    print(f"  Identical files ({len(same)}): {same}")
    print(f"  Different files ({len(different)}): {different}")

if __name__ == "__main__":
    main()
