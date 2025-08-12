# War and Democratization in Portugal

This repository contains the data processing pipeline and replication files for the ongoing project **War and Democratization in Portugal**.  
The research aims to examine how Portugal’s late-colonial wars (1961–1974) shaped local political, demographic, and electoral dynamics during and after the country’s transition to democracy.

## Overview

The project integrates historical military casualty records, administrative geographic data, electoral results, and demographic indicators to build a reproducible dataset for empirical analysis.

### Main components

1. **OCR Processing**  
   - Azure OCR outputs of the *Comissão para o Estudo das Campanhas de África* (CECA) digitalized books.
   - Custom parsing functions to fix OCR errors, standardize formats, and create structured datasets.

2. **Geocoding & Matching**  
   - Linking casualty birthplace information to 2001 administrative divisions (concelho/freguesia) using exact and fuzzy matching.  
   - Special handling for overseas territories (Angola, Moçambique, Guiné-Bissau, etc.).

3. **Municipal-Level Data Compilation**  
   - Legislative election results (votes, shares by party)  
   - Turnout and abstention rates  
   - Population data by sex and age groups (INE census)

4. **Preliminary Analysis**  
   - Descriptive statistics and visualizations of war casualties, demographics, and electoral patterns.

---

## File Structure

| File | Purpose |
|------|---------|
| `0_aux_dic_concelhos.R` | Dictionaries for overseas concelhos and place names |
| `data/ceca/data_ceca_functions.R` | Utility functions for OCR parsing and cleaning |
| `data/ceca/data_ceca.R` | Pre-processing of CECA OCR JSON outputs |
| `1_data_preprocessing.R` | Pre-process electoral & demographic data |
| `2_merge.R` | Merge CECA casualty data with administrative geo IDs |
| `3_prelim_analysis.Rmd` | Descriptive analysis of merged dataset |

Intermediate and final data files are stored in `data/` subfolders.

---

## Replication Instructions

### Requirements

- **R** (>= 4.0)
- Packages: `tidyverse`, `data.table`, `sf`, `fixest`, `knitr`, `kableExtra`, `ggpubr`, `stringi`, `stringdist`, `fuzzyjoin`, `readxl`, `jsonlite`

### Steps

1. Clone this repository

3. Download and place raw data in the expected folders:

- data/ceca/azure_ocr/
- data/eleicoes/raw/
- data/ine/
- data/dgterritorio/Version 1.0/

2. Run scripts in order:

- source("1_data_preprocessing.R")
- source("2_merge.R")

3. Open and knit the preliminary analysis:

- rmarkdown::render("3_prelim_analysis.Rmd")

---

## Data Sources:

- CECA: Comissão para o Estudo das Campanhas de África (Portuguese Armed Forces)
- INE: Instituto Nacional de Estatística (Census & Demographics)
- CNE: Comissão Nacional de Eleições (Election Results)
- DGTerritório: Official cartography and administrative divisions (2001)

---

## Author:

Gabriel C. Caseiro

PhD Student, Politics — New York University

- Email: gabrielcaseiro99@gmail.com
- GitHub: gabrielcaseiro