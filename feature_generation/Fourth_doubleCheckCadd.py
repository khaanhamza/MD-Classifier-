# Fast Parallel + Vectorised Script with Progress Bar + Logging + Timers

import pandas as pd
import os
import numbers
import time
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# --- STEP 1: Load both CSV rule files with actual column names ---

null_path = "/mnt/shared/Sayedali/Kolling/temp_file/null caddphredd.csv"
nonnull_path = "/mnt/shared/Sayedali/Kolling/temp_file/cadd_phredd.csv"

# Load and clean null CADD_PHRED rules
null_df = pd.read_csv(null_path)
null_df = null_df.rename(columns={
    'Unique Null Consequences': 'Consequence',
    'CADD_PHRED imputed': 'CADD_PHRED'
})
null_df = null_df.applymap(lambda x: x.strip().lower() if isinstance(x, str) else x)
null_df['CADD_PHRED'] = null_df['CADD_PHRED'].replace({'low': 'MIN', 'max': 'MAX', '0': 0})
null_df['CADD_PHRED'] = pd.to_numeric(null_df['CADD_PHRED'], errors='ignore')

# Load and clean non-null CADD_PHRED rules
nonnull_df = pd.read_csv(nonnull_path)
nonnull_df = nonnull_df.rename(columns={
    'CADD_PHEDD': 'CADD_PHRED'
})
nonnull_df = nonnull_df.applymap(lambda x: x.strip().lower() if isinstance(x, str) else x)
nonnull_df['CADD_PHRED'] = nonnull_df['CADD_PHRED'].replace({'low': 'MIN', 'max': 'MAX', '0': 0})
nonnull_df['CADD_PHRED'] = pd.to_numeric(nonnull_df['CADD_PHRED'], errors='ignore')

# --- STEP 2: Convert to rule dictionaries ---

def build_rule_dict(df):
    rule_dict = {}
    numeric_vals = pd.to_numeric(df['CADD_PHRED'], errors='coerce')
    min_val = numeric_vals.min()
    max_val = numeric_vals.max()
    for _, row in df.iterrows():
        cons = row['Consequence']
        val = row['CADD_PHRED']
        if isinstance(val, str):
            rule_dict[cons] = val.upper()  # 'MIN' or 'MAX'
        else:
            rule_dict[cons] = val
    return rule_dict

null_rules = build_rule_dict(null_df)
nonnull_rules = build_rule_dict(nonnull_df)

# --- STEP 3: Fast Parallel + Vectorised Processing ---

input_folder = "/mnt/shared/Sayedali/Kolling/Individual_data_Uniti_CPU_parallel/Individual_data_Uniti_generated_features/Individual_data_Uniti_generated_features_withDCs/Individual_data_Uniti_generated_features_withDC_with_Adjusted AF for CRI"
output_folder = os.path.join(input_folder, "cadd_phredd_corrections_final_Uniti")
os.makedirs(output_folder, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def process_file(file_path):
    start = time.time()
    file = os.path.basename(file_path)
    try:
        df = pd.read_csv(file_path, sep="\t", low_memory=False)
    except Exception as e:
        return f"❌ Error reading {file}: {e}"

    if 'CADD_PHRED' not in df.columns or 'Consequence' not in df.columns:
        return f"⚠️ Skipping {file}: Missing required columns."

    df['CADD_PHRED'] = pd.to_numeric(df['CADD_PHRED'], errors='coerce')
    file_min = df['CADD_PHRED'].min(skipna=True)
    file_max = df['CADD_PHRED'].max(skipna=True)

    if pd.isna(file_min) or pd.isna(file_max):
        return f"⚠️ Skipping {file}: No numeric values for MIN/MAX."

    updated = False

    # Apply null rules
    null_mask = df['CADD_PHRED'].isna()
    if null_mask.any():
        for cons, rule in null_rules.items():
            cons_mask = df['Consequence'].str.strip().str.lower() == cons
            mask = null_mask & cons_mask
            if mask.any():
                if rule == 'MIN':
                    df.loc[mask, 'CADD_PHRED'] = file_min
                elif rule == 'MAX':
                    df.loc[mask, 'CADD_PHRED'] = file_max
                elif rule == 0:
                    df.loc[mask, 'CADD_PHRED'] = 0
                elif isinstance(rule, numbers.Number):
                    df.loc[mask, 'CADD_PHRED'] = rule
                updated = True

    # Apply non-null rules
    nonnull_mask = df['CADD_PHRED'].notna()
    if nonnull_mask.any():
        for cons, rule in nonnull_rules.items():
            cons_mask = df['Consequence'].str.strip().str.lower() == cons
            mask = nonnull_mask & cons_mask
            if mask.any():
                if rule == 'MIN':
                    df.loc[mask, 'CADD_PHRED'] = file_min
                elif rule == 'MAX':
                    df.loc[mask, 'CADD_PHRED'] = file_max
                elif rule == 0:
                    df.loc[mask, 'CADD_PHRED'] = 0
                elif isinstance(rule, numbers.Number):
                    df.loc[mask, 'CADD_PHRED'] = rule
                updated = True

    elapsed = time.time() - start

    if updated:
        corrected_path = os.path.join(output_folder, file)
        df.to_csv(corrected_path, sep="\t", index=False)
        return f"🔧 Corrected and saved: {file} in {elapsed:.2f} sec"
    else:
        return f"✅ No changes needed: {file} ({elapsed:.2f} sec)"

# --- Run all files in parallel ---
if __name__ == "__main__":
    files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith(".tsv")]

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_file, f) for f in files]

        # tqdm over futures directly (updates as tasks complete)
        for result in tqdm(
            map(lambda f: f.result(), futures),
            total=len(futures),
            desc="Processing files",
            dynamic_ncols=True,
            unit="file",
            smoothing=0.3
        ):
            logger.info(result)

    logger.info("🎉 All files processed successfully!")
