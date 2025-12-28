import pandas as pd
import glob
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm   # ✅ for realtime progress bar

# Define the paths
digenic_file = "/mnt/shared/Sayedali/Kolling/feature_data/specific_digenic_combination.csv"
input_folder = "/mnt/shared/Sayedali/Kolling/Individual_data_Uniti_CPU_parallel/Individual_data_Uniti_generated_features"
output_folder = "/mnt/shared/Sayedali/Kolling/Individual_data_Uniti_CPU_parallel/Individual_data_Uniti_generated_features/Individual_data_Uniti_generated_features_withDCs"

# Ensure output folder exists
os.makedirs(output_folder, exist_ok=True)

# ---------------------------#
#    LOAD DIGENIC DATA       #
# ---------------------------#

df_digenic = pd.read_csv(digenic_file)

# Build dictionary of protein → max combined_score
protein_scores = {}
for _, row in df_digenic.iterrows():
    p1, p2, score = row["protein1_original_name"], row["protein2_original_name"], row["combined_score"]
    protein_scores[p1] = max(protein_scores.get(p1, 0), score)
    protein_scores[p2] = max(protein_scores.get(p2, 0), score)

# ---------------------------#
#   PROCESSING FUNCTION      #
# ---------------------------#

def process_exome_file(exome_file):
    file_name = os.path.basename(exome_file)
    try:
        df_exome = pd.read_csv(exome_file, sep="\t", low_memory=False)
    except Exception as e:
        return f"❌ Error reading {file_name}: {e}"

    if "SYMBOL" not in df_exome.columns:
        return f"⚠️ Skipping {file_name}: Missing SYMBOL column."

    # Vectorised: map SYMBOL → protein_scores
    df_exome["DCs_score"] = df_exome["SYMBOL"].map(protein_scores).fillna(0).astype(int)

    # Save updated file
    output_file = os.path.join(output_folder, file_name)
    try:
        df_exome.to_csv(output_file, sep="\t", index=False)
        return f"✅ Processed {file_name}"
    except Exception as e:
        return f"❌ Failed to save {file_name}: {e}"

# ---------------------------#
#   PARALLEL FILE PROCESS    #
# ---------------------------#

if __name__ == "__main__":
    exome_files = glob.glob(os.path.join(input_folder, "*.tsv"))

    results = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_exome_file, f): f for f in exome_files}

        # tqdm progress bar for realtime feedback
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files", unit="file"):
            results.append(future.result())

    # Print final per-file results
    for r in results:
        print(r)

    print(f"\n🎉 Processing completed. Updated files are saved in '{output_folder}'.")
