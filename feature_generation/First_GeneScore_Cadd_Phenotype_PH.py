import pandas as pd
import os
import glob
from joblib import Parallel, delayed
from tqdm import tqdm

# ---------------------------#
#       CONFIGURATION        #
# ---------------------------#

input_folder = '/mnt/shared/Sayedali/Kolling/Individual_data_Uniti_CPU_parallel'
output_folder = '/mnt/shared/Sayedali/Kolling/Individual_data_Uniti_CPU_parallel/Individual_data_Uniti_generated_features'
os.makedirs(output_folder, exist_ok=True)

# Preload static resources (to avoid reloading in each worker)
gene_mapping = pd.read_csv('/mnt/shared/Sayedali/Kolling/feature_data/gene_mapping.csv')
mapping_df = pd.read_csv('/mnt/shared/Sayedali/Kolling/feature_data/unique_consequences_corresponding_with_imputed_CADDPHRED_Values_FIXED.csv')
mapping_df.columns = mapping_df.columns.str.strip().str.replace(" ", "_")
gene_lengths = pd.read_csv('/mnt/shared/Sayedali/Kolling/feature_data/gene_length_GRCh38_113.csv')

ensembl_id_set = set(gene_mapping['Ensembl_ID'])

# ---------------------------#
#     PROCESS FUNCTION       #
# ---------------------------#

def process_file(file_path):
    try:
        main_df = pd.read_csv(file_path, sep='\t')

        # Drop rows without gene
        df_gene_cleaned = main_df.dropna(subset=['Gene'])

        # Define min/max CADD for this file
        min_cadd = df_gene_cleaned['CADD_PHRED'].min()
        max_cadd = df_gene_cleaned['CADD_PHRED'].max()

        # Build mapping dict (low/max replaced with file-specific min/max)
        mapping = {
            row['Unique_Null_Consequences']: (
                min_cadd if row['CADD_PHRED_imputed'] == 'low' else
                max_cadd if row['CADD_PHRED_imputed'] == 'max' else
                float(row['CADD_PHRED_imputed'])
            )
            for _, row in mapping_df.iterrows()
        }

        # Vectorised imputation of missing CADD_PHRED
        mask_missing = df_gene_cleaned['CADD_PHRED'].isna()
        df_gene_cleaned.loc[mask_missing, 'CADD_PHRED'] = (
            df_gene_cleaned.loc[mask_missing, 'Consequence'].map(mapping)
        )

        # Merge with gene lengths
        updated_dataset = df_gene_cleaned.merge(
            gene_lengths, how='left', left_on='Gene', right_on='gene_id'
        )

        # Gene score calculation
        frequency_disease = 0.001
        length_median = updated_dataset['gene_length'].median()

        grouped = updated_dataset.groupby('gene_id')
        gene_scores = (
            grouped.apply(
                lambda group: len(group) *
                              (length_median / group['gene_length'].iloc[0]) *
                              min((group['AF'].prod() / frequency_disease), 1)
            )
            .reset_index(name='gene_score')
        )

        updated_dataset = updated_dataset.merge(gene_scores, on='gene_id', how='left')

        # Phenotype feature
        updated_dataset['Phenotype'] = updated_dataset['Gene'].isin(ensembl_id_set).astype(int)

        # Add PH column
        ph_counts = (
            updated_dataset[updated_dataset['Phenotype'] == 1]
            .groupby('SYMBOL').size()
        )
        updated_dataset['PH'] = updated_dataset['SYMBOL'].map(ph_counts).fillna(0).astype(int)

        # Save file
        output_file = os.path.join(output_folder, os.path.basename(file_path))
        updated_dataset.to_csv(output_file, sep='\t', index=False)

        return f"✅ Processed {os.path.basename(file_path)}"
    except Exception as e:
        return f"❌ Failed {os.path.basename(file_path)}: {e}"

# ---------------------------#
#   PARALLEL FILE PROCESS    #
# ---------------------------#

tsv_files = glob.glob(os.path.join(input_folder, "Uniti_case_*.tsv"))

results = Parallel(n_jobs=-1)(
    delayed(process_file)(f) for f in tqdm(tsv_files, desc="Processing files", unit="file")
)

# Print summary
print("\n".join(results))
print(f"\n🎉 All files processed and saved in '{output_folder}'")
