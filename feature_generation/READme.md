# Variant TSV Feature Engineering Pipeline (TSV → Patient-level CSV)

This pipeline generates **patient-level engineered features** from **per-patient variant TSV files**.  
It is **dataset-agnostic** and can be applied to any cohort with similarly structured variant annotation files.

The pipeline runs in sequential stages that enrich each TSV with additional variant- and gene-level features and finally aggregates each patient into **one row per patient**, suitable for downstream statistical analysis or machine learning.

---

## What this pipeline produces

1. Enhanced **per-patient TSV files** with engineered features  
2. **Patient-level CSV files** (one record per patient)  
3. A derived proxy feature **`Compound_Het_Score`** capturing compound heterozygosity signal from rare variants  

---

## Repository contents

- **First_GeneScore_Cadd_Phenotype_PH.py**  
  Stage 1 – Core feature generation (gene score, phenotype flags, PH, initial CADD handling)

- **Second_DCs.py**  
  Stage 2 – Adds disease-causing score (`DCs_score`)

- **Third_CRI.py**  
  Stage 3 – Adds CRI-adjusted allele frequency feature

- **Fourth_doubleCheckCadd.py**  
  Stage 4 – Final correction and normalisation of `CADD_PHRED`

- **Fifth_rare_counts.py**  
  Stage 5 – Computes rare allele-frequency pair counts (`Rare_AF_Pair_Count`)

- **Sixth_data_aggregration.ipynb**  
  Stage 6 – Aggregates per-patient TSVs into patient-level CSVs and computes `Compound_Het_Score`

---

## Prerequisites

### Python environment
- Python **3.8+** recommended

Required packages:
- pandas  
- numpy  
- tqdm  
- joblib  

(Standard libraries such as `os`, `glob`, `itertools`, `logging`, and `concurrent.futures` are also used.)

---

## Input data assumptions

- One **TSV file per patient/sample**
- TSVs contain variant-level annotations including:
  - `SYMBOL` (gene symbol)
  - `Consequence`
  - `AF` (allele frequency)
  - `CADD_PHRED` (may be missing initially)
- Later stages expect:
  - `samples_hom`
  - a genotype column immediately following `samples_hom` (used for rare-heterozygous detection)

---

## Pipeline stages

### Stage 1 — Core engineered features  
**Script:** `First_GeneScore_Cadd_Phenotype_PH.py`

**Purpose**
- Generate baseline gene- and phenotype-based features
- Perform initial imputation or assignment of `CADD_PHRED` using consequence mappings and per-file statistics

**Adds**
- `gene_score`
- `Phenotype` (binary indicator)
- `PH` (phenotype-related frequency feature)
- Initial / imputed `CADD_PHRED`

---

### Stage 2 — Disease-causing score  
**Script:** `Second_DCs.py`

**Purpose**
- Add a gene-level disease-causing score derived from an external curated resource

**Adds**
- `DCs_score`

---

### Stage 3 — CRI-adjusted allele frequency  
**Script:** `Third_CRI.py`

**Purpose**
- Capture allele-frequency effects for genes with multiple variants

**Logic**
- For genes (`SYMBOL`) appearing multiple times:
  - `Adjusted AF for CRI = AF²`
- Otherwise left null or unchanged

**Adds**
- `Adjusted AF for CRI`

---

### Stage 4 — Final `CADD_PHRED` correction  
**Script:** `Fourth_doubleCheckCadd.py`

**Purpose**
- Apply rule-based corrections to `CADD_PHRED`
  - Fill missing values
  - Normalise existing values using rule tables

**Output**
- Corrected and standardised `CADD_PHRED`

---

### Stage 5 — Rare allele-frequency pair counts  
**Script:** `Fifth_rare_counts.py`

**Purpose**
- Quantify compound heterozygosity signal at the gene level

**Logic**
1. Identify heterozygous variants (`0/1`)
2. Filter to rare variants (`AF < 0.01`)
3. For genes with ≥2 rare heterozygous variants:
   - Count all AF pairs where `af₁ × af₂ ≤ 0.001`
4. Assign the count to all rows for that gene

**Adds**
- `Rare_AF_Pair_Count`

---

### Stage 6 — Patient-level aggregation  
**Notebook:** `Sixth_data_aggregration.ipynb`

**Purpose**
- Aggregate variant-level TSVs into **one row per patient**
- Compute a composite compound heterozygosity score

**Aggregated features**
- `CADD_PHRED`: maximum  
- `gene_score`: maximum (±inf replaced with 0)  
- `Phenotype`: proportion of rows where `Phenotype == 1`  
- `PH`: mean  
- `DCs_score`: proportion of rows equal to patient-level maximum  
- `Adjusted AF for CRI`: mean  

**Compound_Het_Score**
- Filter rows with `Rare_AF_Pair_Count > 0`
- `Compound_Het_Score = (number of candidate genes) × (sum of Rare_AF_Pair_Count)`

---

## Recommended directory structure
PIPELINE_RUN/
├── input_tsv/
├── stage1_core_features/
├── stage2_dcs/
├── stage3_adjusted_af/
├── stage4_cadd_corrected/
├── stage5_rare_pair_counts/
├── stage6_patient_level_outputs/
└── resources/


---

## Troubleshooting

- **Missing columns during aggregation**  
  Ensure TSVs have passed through all required prior stages.

- **Genotype column mismatch**  
  Stage 5 assumes a fixed TSV schema. Update genotype-column detection logic if the schema differs.

- **Parallel execution on shared storage**  
  Reduce worker counts or copy data to local scratch space if I/O issues occur.


---

**This pipeline is intentionally general and reusable across datasets, cohorts, and projects that use per-sample variant TSV files.**
