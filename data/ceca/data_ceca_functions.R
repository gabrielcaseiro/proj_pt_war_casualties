# ============================================================
# Project: War and Democratization in Portugal
# Script: data_ceca_functions.R — OCR Parsing Utilities
# Purpose:
#   Utility functions used by data_ceca.R to clean and
#   standardize Azure OCR outputs from the CECA casualty books.
#   Provides string normalization, fuzzy header matching, regex
#   safety checks, and canonical column names.
#
# Exports:
#   - remove_accents(x)          # ASCII transliteration + strip non-alnum
#   - true_colnames              # Canonical ordered field names (chr vec)
#   - adjust_colnames(word, colnames, max_dist=3)
#                                # Fuzzy match label → canonical name
#   - check_regex_valid(patterns)# Validate a vector of regex patterns
#   - adjust_regex(regex)        # Escape special chars; Unicode-safe
#
# Inputs:    none (functions only)
# Outputs:   none (objects/functions in memory)
#
# Dependencies:
#   - stringdist
#   - stringi
#
# Notes:
#   - true_colnames is pre-ordered by descending nchar to reduce
#     greedy matches when scanning OCR’d labels.
#   - adjust_colnames uses Levenshtein distance; tune max_dist as needed.
#
# Author: Gabriel Caseiro (gabrielcaseiro99@gmail)
# Date: 2025-08-12
# ============================================================

library(stringdist)
library(stringi)

# Function to remove accents and special characters:

remove_accents <- function(x) {
  
  xx<-x |>  iconv(from = "UTF-8", to = "ASCII//TRANSLIT")
  
  xx<-gsub("[^[:alnum:] ]", "", xx)  
  
  return(xx)
  
}


# List of true columns names:

true_colnames <- c(
  "Nome", 
  "Posto", 
  "Número", 
  "Unidade", 
  "Unidade Mob",
  "Estado Civil", 
  "Data do Falecimento", 
  "Pai", 
  "Mãe", 
  "Lugar", 
  "Causas da morte", 
  "Freguesia", 
  "Concelho", 
  "Local de Operações", 
  "Local da sepultura", 
  "Observações"
)

true_colnames<-true_colnames[order(-str_length(true_colnames))]

# Function to adjust columns names:

adjust_colnames <- function(word, colnames, max_dist = 3) {
  
  # 1. Pre-processing text:
  word_clean <- tolower(remove_accents(trimws(word)))
  
  # 2. Calculate string distances:
  colnames_clean <- tolower(remove_accents(colnames))
  dist <- stringdist(word_clean, colnames_clean, method = "lv")
  
  # 3. Find minimum distance match:
  i_min <- which.min(dist)
  min_dist <- dist[i_min]
  
  # 4. Keep match if not too distant:
  if (min_dist <= max_dist) {
    return(colnames[i_min])
  } else {
    return(word)  
  }
  
}

check_regex_valid <- function(patterns) {
  # patterns: a character vector of regex patterns
  # returns: integer vector of same length: 1 if valid, 0 if invalid
  
  sapply(patterns, function(pt) {
    test <- try(grepl(pt, ""), silent = TRUE)
    if (inherits(test, "try-error")) {
      0  # invalid
    } else {
      1  # valid
    }
  })
}

adjust_regex<-function(regex){
  
  x<-gsub('[(]','[(]',regex)
  x<-gsub('[{]','[{]',x)
  x<-gsub('[)]','[)]',x)
  x<-gsub('[}]','[}]',x)
  x<-stri_escape_unicode(x)
  
  return(x)
}
