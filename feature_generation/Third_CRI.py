# Fast Parallel + Vectorised Version with Logging + Progress Bar + Timers

import pandas as pd
import os
import time
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# Define input and output directories
input_folder = "/mnt/shared/Sayedali/Kolling/Individual_data_Uniti_CPU_parallel/Individual_data_Uniti_generated_features/Individual_data_Uniti_generated_features_withDCs"
output_folder = "/mnt/shared/Sayedali/Kolling/Individual_data_Uniti_CPU_parallel/Individual_data_Uniti_generated_features/Individual_data_Uniti_generated_features_withDCs/Individual_data_Uniti_generated_features_withDC_with_Adjusted AF for CRI"

# Ensure the output directory exists
os.makedirs(output_folder, exist_ok=True)

# Configure logging to show messages in terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def process_exome_file(file_path, output_path):
    """Process a single exome TSV file: add 'Adjusted AF for CRI' column."""
    start = time.time()
    file = os.path.basename(file_path)

    try:
        df = pd.read_csv(file_path, sep="\t", low_memory=False)
    except Exception as e:
        return f"❌ Error reading {file}: {e}"

    if "SYMBOL" not in df.columns or "AF" not in df.columns:
        return f"⚠️ Skipping {file}: Missing required columns."

    # Add the new column 'Adjusted AF for CRI' with default NaN
    df["Adjusted AF for CRI"] = None  

    # Vectorised: assign AF^2 where SYMBOL appears >= 2
    duplicated_mask = df["SYMBOL"].duplicated(keep=False)
    df.loc[duplicated_mask, "Adjusted AF for CRI"] = df.loc[duplicated_mask, "AF"] ** 2

    # Save the modified file in the output directory
    df.to_csv(output_path, sep="\t", index=False)

    elapsed = time.time() - start
    return f"✅ Processed {file} in {elapsed:.2f} sec -> Saved as {output_path}"


# --- Run all files in parallel ---
if __name__ == "__main__":
    files = [f for f in os.listdir(input_folder) if f.endswith(".tsv")]
    input_paths = [os.path.join(input_folder, f) for f in files]
    output_paths = [os.path.join(output_folder, f) for f in files]

    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(process_exome_file, in_f, out_f): in_f
            for in_f, out_f in zip(input_paths, output_paths)
        }

        # tqdm progress bar with ETA
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Processing files",
            dynamic_ncols=True,   # auto-fit terminal width
            unit="file",
            smoothing=0.3         # smoother ETA
        ):
            result = future.result()
            logger.info(result)

    logger.info("🎉 All files processed successfully!")
