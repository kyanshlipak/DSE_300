# Homework 2 – Data Analysis on Drug Usage, Procedures, and ICU Stays

**Course**: Data Engineering Studio (DATA_ENG 300)
**Assignment**: Homework 2
**Student**: Kyan Shlipak

## Overview

This notebook analyzes trends in hospital data using SQL, Cassandra, and Python. It addresses three primary questions:

1. What types of drugs are most used by ethnicity?
2. What are the top procedures performed in different age groups?
3. How long do patients stay in the ICU, and does this vary by gender or ethnicity?

Both relational (PostgreSQL) and non-relational (Cassandra) databases were used, with results verified for consistency between them. Aggregation and analysis were performed primarily in `pandas`.

---

## 1. Drug Usage by Ethnicity

**Goal**: Identify the most commonly used drugs overall and by ethnic group.**Assumptions**:

- Drug name (`drug`), not `drug_type`, was used.
- "Total amount used" was interpreted as number of applications due to dosage inconsistencies.

**Findings**:

- Top overall drugs by unique users: **Sodium Chloride 0.9% Flush**, **Acetaminophen**, **Heparin**.
- Top drugs by ethnicity revealed:
  - **Potassium Chloride** for White patients (508 prescriptions)
  - **0.9% Sodium Chloride** for Puerto Rican Hispanic/Latino patients (1,290 prescriptions)
  - **Insulin** for Black/African American patients (60 prescriptions)
  - IV fluids like **5% Dextrose** and **D5W** were common across multiple groups

---

## 2. Top Procedures by Age Group

**Goal**: Find the three most common procedures in each age group (0–19, 20–49, 50–79, 80+).

**Method**:

- Calculated patient age at admission
- Mapped procedures using ICD codes
- Ranked top 3 procedures per group

**Findings**:

- **Venous catheterization** was the most frequent procedure across all groups.
- **Blood transfusions** were top-ranked for all but the 20–49 group.
- **Enteral infusions** were frequent among adults and elderly.

---

## 3. ICU Length of Stay by Gender and Ethnicity

**Goal**: Calculate ICU length of stay (LOS) and compare across gender and ethnicity.

**Results**:

- Overall average ICU LOS: **4.45 days**
- Shortest average LOS: **Other (0.93 days)**
- Longest average LOS:
  - **American Indian/Alaska Native**: 11.34 days
  - **Unable to Obtain**: 13.36 days
- **Females** had a slightly longer ICU stay than **males**

*Note: Interpret with caution due to small dataset size.*

---

## Cassandra Usage

Cassandra tables were designed to support each query's primary key needs.

- **Batched inserts (batch size: 30)** were used for performance.
- Aggregations (e.g., group by) were performed in `pandas` post-query due to Cassandra limitations.
- Results matched SQL outputs to ensure consistency.

---

## Generative AI Disclosure

**Purpose**: Used for SQL syntax troubleshooting and Python code commenting.**Tool**: ChatGPT by OpenAI**Prompts Used**:

- “What is wrong with this SQL query?”
- “Add comments to this Python script”

---

## How to build the Docker container

1. Having cloned the repo, open a terminal and navigate to the `HW2/` folder.
2. Build the Docker container by running:

   ```bash
   docker build -t hw2-container .
   ```

## How to run the notebook in Jupyter

After building the Docker container, run it with:

```bash
docker run -p 8888:8888 hw2-container
```
