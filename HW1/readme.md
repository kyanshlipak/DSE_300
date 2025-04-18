# Aircraft Inventory Data Cleaning & Transformation

This repository contains a Jupyter notebook that performs data cleaning, transformation, and feature engineering on a U.S. aircraft inventory dataset (`T_F41SCHEDULE_B43.zip`). This is part of coursework for **DSE 300 - Homework 1**.

## Contents

- `DSE_300_Hw1.ipynb`: Main notebook with data analysis and processing.
- Imputation of missing data
- Standardization of aircraft metadata
- Variable transformation and visualization
- Feature engineering based on seat capacity

---

## How to Run

### Requirements

To run the notebook, install the following Python libraries:
pip install pandas numpy matplotlib seaborn scipy

The `random` module is also used and is part of Python’s standard library.

You can run the notebook locally using Jupyter or use an online environment like [Google Colab](https://colab.research.google.com/).

### Steps

1. Place the dataset `T_F41SCHEDULE_B43 (1).zip` in the `sample_data/` directory.
2. Open `DSE_300_Hw1.ipynb`.
3. Execute all cells in order.

---

## Expected Outputs

### Missing Data Handling
- Imputation of missing values for:
  - `CARRIER`, `CARRIER_NAME`, `MANUFACTURE_YEAR`
  - `NUMBER_OF_SEATS`, `CAPACITY_IN_POUNDS`, `AIRLINE_ID`
- Logic-based and model-based sampling using the `random` module

### Data Transformation
- Standardized manufacturer and model names
- Created general model families (e.g., `B737`, `A320`)
- Cleaned categorical columns like `AIRCRAFT_STATUS` and `OPERATING_STATUS`

### Final Clean Dataset
- Rows with unrecoverable missing data (`ACQUISITION_DATE`, `AIRCRAFT_TYPE`, etc.) are removed
- Final dataset retains approximately **76.6%** of the original 132,313 rows

### Variable Transformation
- Box-Cox transformation applied to:
  - `NUMBER_OF_SEATS`
  - `CAPACITY_IN_POUNDS`
- Histograms before and after transformation show reduced skew

### Feature Engineering
- New `SIZE` column based on seat count quartiles:
  - `SMALL`, `MEDIUM`, `LARGE`, `XLARGE`
- Visualizations:
  - Proportion of operating vs non-operating aircraft by size
  - Aircraft status (`A`, `B`, `O`, `L`) distribution by size

---

## Summary

This notebook provides a full data processing pipeline, including:
- Imputation strategies
- Cleaning and standardization of categorical data
- Variable transformations and visualizations

---

## Author
Kyan Shlipak

**DSE 300 – Homework 1**

