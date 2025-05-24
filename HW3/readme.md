# Homework 3 – Data Analysis on Drug Usage, Procedures, and ICU Stays

**Course**: Data Engineering Studio (DATA_ENG 300)
**Assignment**: Homework 2
**Student**: Kyan Shlipak

## Overview

This notebook builds analysis functions that utilize Spark map and reduce principles to permit scaling to distributed computing architectures.

1. Term fraction and TF-IDF measurement function, applied to a dataset of news articles
2. Soft-margin support vector machine classification model (SVM) objective and prediction functions programmed with Spark dataframes and map-reduce functionality.

## How to build the Docker container

## 1. TF-IDF Word Scoring

**Goal**: Compute the TF-IDF (term frequency–inverse document frequency) score for a given word across documents in the AG News dataset.

**Assumptions**:

- The input dataset contains a column `filtered` with tokenized and pre-cleaned words per article.
- The word count uses substring matching (i.e., if the search term is contained in a word).

**Methods**

 For a target word $t$, compute:

\mathrm{IDF}_t = \log\left(\frac{N}{\text{# documents with } t}\right)

- **Term Frequency (TF)** per document:

  $$
  \mathrm{TF}_{d,t} = \frac{\text{Count of } t \text{ in document } d}{\text{Total words in } d}
  $$
- **Inverse Document Frequency (IDF)** across the corpus:

  $$
  \mathrm{IDF}_t = \log\left(\frac{N}{\text{# documents with } t}\right)
  $$
- **TF-IDF** score for each document:

  $$
  \mathrm{TFIDF}_{d,t} = \mathrm{TF}_{d,t} \cdot \mathrm{IDF}_t
  $$

**Findings**:

- Function `tf_idf("trump")` demonstrates the method by returning a DataFrame with TF, IDF, and TF-IDF scores.
- Results can be used to identify the most "informative" documents for a given term.

---

## 2. Soft-Margin SVM Classification

**Goal**: Implement soft-margin SVM loss function and prediction rule using PySpark to evaluate model performance on a dataset.

**Assumptions**:

- Data contains 64 feature columns and 1 label column with values in \(\{-1, 1\}\).
- Weight vector and bias are stored in separate CSV files with no header.

**Method**:

- **Loss Function**:

  $$
  L(\mathbf{w}, b) = \lambda \|\mathbf{w}\|^2 + \frac{1}{n} \sum_{i=1}^n \max\left(0, 1 - y_i(\mathbf{w}^\top \mathbf{x}_i + b)\right)
  $$
- **Prediction Rule**:

  $$
  \hat{y}_i = \operatorname{sign}(\mathbf{w}^\top \mathbf{x}_i + b)
  $$

**Findings**:

- Top-level function `compute_loss(w_df, b_df, data_df)` evaluates SVM loss.
- `predict_SVM()` applies the model to compute predicted labels.
- Evaluation revealed **low classification accuracy**, likely due to model initialization or feature/label inconsistencies.

**Supporting Functions**:

- `compute_dot_product_expr()`: Builds Spark expression for \(\mathbf{w}^\top \mathbf{x}_i\)
- `add_margin_column()`: Computes margin \(y_i(\mathbf{w}^\top \mathbf{x}_i + b)\)
- `compute_hinge_loss()`: Applies hinge loss logic
- `predict_SVM()`: Applies decision rule and generates predictions

## GenAI Disclosure Statement

- **Purpose**: Used to interpret SVM math, repair broken markdown/LaTeX, and modularize PySpark code.
- **Tool**: ChatGPT by OpenAI
- **Prompts Used**:
  - “Explain SVM”
  - “SVM loss vs. prediction”
  - “Fix and render this markdown LaTeX”

1. Having cloned the repo, open a terminal and navigate to the `HW2/` folder.
2. Build the Docker container by running:

   ```bash
   docker build -t hw3-container .
   ```

## How to run the notebook in Jupyter

After building the Docker container, run it with:

```bash
docker run -p 8888:8888 hw3-container
```
