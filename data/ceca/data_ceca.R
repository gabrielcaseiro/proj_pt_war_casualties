# ============================================================
# Project: War and Democratization in Portugal
# Script: data_ceca.R  — Casualties OCR → Tidy Table
# Purpose:
#   Ingest Azure OCR JSON outputs for the CECA casualty books,
#   extract and normalize page/line content, resolve common OCR
#   issues (whitespace, spurious tokens, column detection),
#   reshape to individual-level wide records, perform targeted
#   fixes (date-field recovery, misplaced fields), create stable
#   IDs, and save consolidated datasets for downstream merging.
#
# Inputs (read-only):
#   - data/ceca/azure_ocr/*.json
#       • Azure Read/Analyze JSON with structure:
#         $analyzeResult$pages[[...]]$lines[[...]]$content, $polygon
#   - data/ceca/data_ceca_functions.R
#       • Must define: adjust_regex(), adjust_colnames(), true_colnames
#
# Intermediate I/O (manual review step):
#   - writes: data/ceca/aux_data/data_to_adjust.csv
#       • Rows flagged for potential parsing issues (duplicates, residual “NA” col)
#   - expects: data/ceca/aux_data/data_adjusted.csv
#       • Analyst-edited corrections, merged back into the pipeline
#
# Outputs:
#   - data/ceca/tab_mortos_em_campanha.csv   (CSV2, UTF-8)
#   - data/ceca/tab_mortos_em_campanha.rds   (RDS)
#
# Key operations & assumptions:
#   - Filters pages mentioning "freguesia" (case-insensitive).
#   - Uses line polygon coords to order content; infers two-column layout.
#   - Detects field labels via regex built from OCR’d labels (then standardized
#     to true_colnames). Collapses multi-line fields before widening.
#   - Repairs common date OCR errors (e.g., “Il/IT/LI/II” → “11”, “Ol” → “01”).
#   - Recovers missing death dates from the “Número” string when available.
#   - Generates stable record IDs per book (zero-padded).
#
# Dependencies:
#   - tidyverse, jsonlite, data.table
#
# Repro tips:
#   - Ensure Azure JSON structure matches the expected schema.
#   - Run this script before 2_merge.R.
#   - This script ends with `rm(list = ls())` (clears workspace). Remove if undesired.
#
# Author: Gabriel Caseiro (gabrielcaseiro99@gmail.com)
# Date: 2025-08-12
# ============================================================



library(tidyverse)
library(jsonlite)
library(data.table)

source('data/ceca/data_ceca_functions.R') # Load auxiliary functions

# 1. Pre-Processing ----

files<-list.files('data/ceca/azure_ocr','json',full.names = T) # List JSON files

data <- lapply(files, function(f){
  
  print(f)  # Print the current file name for progress tracking
  
  json <- fromJSON(f)  # Read JSON content from file
  json <- json$analyzeResult$pages  # Navigate to the "pages" section
  
  json <- json |> 
    select(pageNumber, lines) |>  # Select relevant fields
    unnest(lines)  # Expand the list-column of lines into rows
  
  json <- json |> 
    select(-spans) |>  # Drop the 'spans' column (not needed)
    mutate(
      content = str_squish(content),  # Remove extra whitespace
      content = str_squish(str_remove(content, "^[[:punct:]]{1,} "))  # Clean leading punctuation
    ) |> 
    group_by(pageNumber) |> 
    mutate(ind = sum(grepl('^freguesia', content, ignore.case = TRUE))) |>  # Flag pages mentioning "freguesia" (i.e., relevant content)
    ungroup()
  
  json <- json |> 
    filter(ind > 0) |>  # Keep only pages with relevant content
    select(-ind) |> 
    rowwise() |> 
    mutate(
      y = polygon[2],  # Extract Y coordinate
      x = polygon[1],  # Extract X coordinate
      col = ifelse(x > 3, 2, 1)  # Infer column based on X position
    )
  
  json <- json |> 
    filter(!grepl('^[0-9]{1,3}$|^ob ofgsmalohaupA', content)) |>  # Filter out junk/noise lines
    arrange(pageNumber, y) |>  # Order by visual Y coordinate
    ungroup() |> 
    mutate(
      id = cumsum(grepl('^nome', content, ignore.case = TRUE)),  # Create block/individual IDs based on "nome"
      temp = 1:n()  # Temporary sequential index
    )
  
  json <- json |> 
    mutate(
      content = str_split(content, 
                          "(?= Data do Falecimento:| Número:| Causas da morte:)", 
                          simplify = FALSE),  # Split, keeping keywords
      content = map(content, ~str_trim(.x))  # Trim whitespace in each split part
    ) |> 
    unnest(content) |>  # Expand splits into separate rows
    arrange(pageNumber, y) |> 
    group_by(temp) |> 
    mutate(tempn = 1:n()) |> 
    ungroup() |> 
    mutate(col = ifelse(tempn == 2, 2, col)) |>  # If 2nd part of split, assign column = 2
    select(-temp, -tempn)  # Drop temp helper columns
  
  # Extract candidate column labels from the content
  cols <- na.omit(str_extract(json$content, '^.*:($| )')) |> 
    str_trim() |> 
    unique()
  cols <- cols[str_count(cols, ':') == 1]  # Keep only those with exactly one colon
  cols <- gsub(':$', "", cols)  # Remove trailing colon
  cols <- cols[order(-str_length(cols))]  # Order by length (longer first)
  cols <- adjust_regex(cols)  # Apply your custom regex adjustment function
  
  json <- json |> 
    group_by(id, col) |> 
    mutate(temp = cumsum(str_detect(content, 
                                    paste(paste0('^', cols), 
                                          collapse = "|")))) |>  # Detect new variable blocks
    ungroup()
  
  json <- json |> 
    group_by(id, col, temp) |> 
    mutate(
      content = paste(content, collapse = ' '),  # Collapse multi-line content into one string
      n = 1:n()  # Add index
    ) |> 
    ungroup()
  
  json <- json |> 
    filter(n == 1) |>  # Keep only one row per collapsed block
    select(-n, -temp)  # Drop temp columns
  
  json <- json |> 
    mutate(
      col = str_extract(content, paste(paste0('^', cols), collapse = "|")),  # Extract column label
      content = str_remove(content, paste(paste0('^', cols), collapse = "|")),  # Remove label from content
      content = str_remove(content, '^[[:punct:]]'),  # Clean leading punctuation
      content = str_squish(content)  # Final cleanup
    )
  
  json <- json |> 
    rowwise() |> 
    mutate(
      col = ifelse(is.na(col), NA, adjust_colnames(col, true_colnames)),  # Match to standardized col names
      col = ifelse(!col %in% true_colnames,
                   str_extract(content, paste(paste0('^', true_colnames), collapse = "|")),
                   col)  # Fallback: try to directly extract col names if not matched
    )
  
  json <- json |> 
    group_by(id, pageNumber, col) |> 
    mutate(n = 1:n()) |> 
    ungroup()
  
  json <- json |> 
    select(pageNumber, id, n, col, content) |> 
    pivot_wider(names_from = col, values_from = content) |>  # Reshape to wide format
    mutate(
      book = basename(f),  # Add file name
      colonia = str_extract(book, '(?<=\\] ).*(?= - )')  # Extract region/colony from file name
    )
  
  return(json)  # Return structured result for this file
  
})


# 2. Consolidate and Save ----

data<-do.call(bind_rows,data)

  ## Filter potential parsing errors:
  
  data<-data |> 
    group_by(id,pageNumber,book) |> 
    mutate(check=n()) |> # Non-unique individuals
    ungroup() |> 
    group_by(pageNumber,book) |> 
    mutate(check=sum(check>1|!is.na(`NA`))) |> # Non-unique individuals and non-empty residual column
    ungroup()
  
  check<-data |> 
    filter(check>0)
  
    ### Manual adjust:
    
    write.csv(check,'data/ceca/aux_data/data_to_adjust.csv',row.names = F,fileEncoding = 'UTF-8')
    
    check<-fread('data/ceca/aux_data/data_adjusted.csv')
    
    data<-data|> 
      filter(check==0) |> 
      bind_rows(check) |> 
      arrange(book,id) |> 
      select(-check,-`NA`)
    
    rm(check)
  
  ## Adjust date of death variable:
    
    ### Information in wrong column (from the original data):
    
    check<-data |> 
      filter(grepl('^[0-9]',Pai))
    
      data<-data |> 
        anti_join(check)
      
      check <- check |> 
        mutate(`Data do Falecimento`=ifelse(`Data do Falecimento`=='',Pai,`Data do Falecimento`),
               Pai=NA)
      
      data<-data |> 
        bind_rows(check) |> 
        arrange(book,id)
      
        rm(check)
        
    ### Other potential problems:
  
    check<-data |> 
      filter(is.na(`Data do Falecimento`)|`Data do Falecimento`=='')
    
      data<-data |> 
        anti_join(check)
      
      check<-check |> 
        mutate(`Data do Falecimento`=str_extract(Número, "\\d{1,2} de [A-Za-zçãéíú]+ de \\d{4}( \\(.*?\\))?"),
               Número=str_remove(Número, "\\d{1,2} de [A-Za-zçãéíú]+ de \\d{4}( \\(.*?\\))?"))
    
      data<-data |> 
        bind_rows(check) |> 
        arrange(book,id)
      
        rm(check)
        
    ### Minor OCR adjustments:
        
    data<-data |> 
      mutate(`Data do Falecimento`=gsub("Il|IT|LI|II",'11',`Data do Falecimento`),
             `Data do Falecimento`=gsub("Ol",'01',`Data do Falecimento`))
      
  ## Create unique id for individuals:
    
  ids<-cross_join(data.frame(id1=paste0(0,3:1)),data.frame(id2=paste0(0,1:2)))
    ids<-ids |> 
      mutate(temp=paste0(id1,id2),
             book=unique(data$book)) |> 
      select(-id1,-id2)
    
    data<-data |> 
      left_join(ids) |> 
      mutate(id=paste0(temp,str_pad(id,4,'left',0))) |> 
      select(-temp,-n) |> 
      arrange(id)
    
    rm(ids)
    
    
  ## Save dataset:  
    
    write.csv2(data,'data/ceca/tab_mortos_em_campanha.csv',row.names = F,fileEncoding = 'UTF-8')
    saveRDS(data,'data/ceca/tab_mortos_em_campanha.rds')
    
    rm(list = ls())
    
    