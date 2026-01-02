# MD-Classifier

This repository contains a single Jupyter notebook, **`MD_Classifier.ipynb`**, implementing an **academic-grade, leakage-safe** binary classification pipeline for **patient-level tabular data**.

The notebook trains and compares multiple classical ML models with:
- **Stratified train/validation/test split** (70% / 15% / 15%)
- **SMOTE applied *only inside cross-validation folds*** via `imblearn.Pipeline` (to avoid leakage)
- **Hyperparameter tuning** with `GridSearchCV` (scoring: ROC AUC)
- **Threshold selection on the validation set** via **Youden’s J** (maximize TPR − FPR)
- **Final, single-pass test evaluation** (no refitting on test)
- **Bootstrapped confidence intervals** for key metrics
- **Model interpretability** (feature importance + permutation importance fallback)
- **Ablation study** (drop specific feature blocks and measure performance deltas)

---

## Files

- **`MD_Classifier.ipynb`** — end-to-end pipeline and figures.
- **`transformed_patient_data.csv`** — expected input CSV name (not included in this repo by default, This is going to be the features you generate after 6 steps of feature generation provided in the repository).

The notebook can also write a few output artifacts (filenames are created dynamically from the best model name):
- `roc_cm_balanced_<N>_<N>_<model>_blue.png`
- `ablation_summary_<model>.csv`
- `ablation_delta_auc_<model>.png`
- `ablation_test_auc_ci_<model>.png`
- `ablation_delta_heatmap_<model>.png`

---

### Data format expected is CSV.


### Acknowledgement 
The core feature generation pipeline and the majority of the model development were implemented by Sayedali A. Mohseni.
Figure generation, selected model validation experiments, and supporting analyses were conducted by Muhammad Hamza B. Khan.

