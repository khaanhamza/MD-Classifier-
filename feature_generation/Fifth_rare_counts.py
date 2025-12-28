# Fast Parallel + Vectorised Version with Real-Time Progress Bar

import pandas as pd
import itertools
import os
import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# ---------------------------#
#       CONFIGURATION        #
# ---------------------------#

input_folder = '/mnt/shared/Sayedali/Kolling/Individual_data_Uniti_CPU_parallel/Individual_data_Uniti_generated_features/Individual_data_Uniti_generated_features_withDCs/Individual_data_Uniti_generated_features_withDC_with_Adjusted AF for CRI/cadd_phredd_corrections_final_Uniti'
output_folder = os.path.join(input_folder, 'inds_rareAFCounts_Uniti')
os.makedirs(output_folder, exist_ok=True)

# ---------------------------#
#       LOGGING SETUP        #
# ---------------------------#

log_file = os.path.join(output_folder, 'processing_log.txt')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(levelname)s: %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

logging.info("===== Starting TSV modification with Rare_AF_Pair_Count =====")
logging.info(f"Input folder: {input_folder}")
logging.info(f"Output folder: {output_folder}")

# ---------------------------#
#    PROCESSING FUNCTION     #
# ---------------------------#

def process_file(file_path):
    start = time.time()
    filename = os.path.basename(file_path)
    base_name = os.path.splitext(filename)[0]
    logging.info(f"Started processing: {filename}")

    try:
        df = pd.read_csv(file_path, sep="\t", low_memory=False)
    except Exception as e:
        return f"❌ Failed to read {filename}: {e}"

    # Locate genotype column (just after 'samples_hom')
    try:
        samples_hom_index = df.columns.get_loc("samples_hom")
        genotype_col = df.columns[samples_hom_index + 1]
    except Exception as e:
        return f"⚠️ Could not locate genotype column for {filename}: {e}"

    if "AF" not in df.columns or "SYMBOL" not in df.columns:
        return f"⚠️ 'AF' or 'SYMBOL' column missing in {filename}"

    # Step 1+2: Filter in one pass (het & rare AF)
    het_mask = df[genotype_col].astype(str).str.contains("0/1", na=False)
    rare_mask = df["AF"] < 0.01
    df_filtered = df[het_mask & rare_mask]

    # Step 3: Candidate genes with ≥2 rare het variants
    gene_variant_counts = df_filtered["SYMBOL"].value_counts()
    candidate_genes = gene_variant_counts[gene_variant_counts >= 2].index.tolist()

    if not candidate_genes:
        return f"ℹ️ No candidate genes found in {filename}"

    # Step 4: Compute Rare_AF_Pair_Count
    af_product_counts = {}
    af_pair_details = {}

    for gene in candidate_genes:
        af_values = df.loc[df["SYMBOL"] == gene, "AF"].dropna().tolist()
        if len(af_values) < 2:
            continue
        qualifying_pairs = [
            (gene, af1, af2, af1 * af2)
            for af1, af2 in itertools.combinations(af_values, 2)
            if af1 * af2 <= 0.001
        ]
        af_product_counts[gene] = len(qualifying_pairs)
        af_pair_details[gene] = qualifying_pairs

    # Step 5: Add Rare_AF_Pair_Count
    df["Rare_AF_Pair_Count"] = df["SYMBOL"].map(af_product_counts).fillna(0).astype(int)

    # Step 6: Save updated TSV
    output_path = os.path.join(output_folder, f"{base_name}.tsv")
    try:
        df.to_csv(output_path, sep="\t", index=False)
    except Exception as e:
        return f"❌ Failed to save {filename}: {e}"

    elapsed = time.time() - start
    return f"✅ Finished {filename} in {elapsed:.2f} sec -> Saved to {output_path}"

# ---------------------------#
#    PARALLEL FILE LOOP      #
# ---------------------------#

if __name__ == "__main__":
    files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith(".tsv")]

    with ProcessPoolExecutor() as executor:
        futures = []
        with tqdm(total=len(files), desc="Processing files", dynamic_ncols=True, unit="file") as pbar:
            for f in files:
                future = executor.submit(process_file, f)
                futures.append(future)
                pbar.update(1)   # tick when job is submitted

            for future in as_completed(futures):
                try:
                    result = future.result()
                    logging.info(result)
                except Exception as e:
                    
                    logging.error(f"Unhandled error: {e}")

    logging.info("🎉 All files processed and saved with Rare_AF_Pair_Count.")
