# ============================================================
# Project: War and Democratization in Portugal
# Script: 1_data_preprocessing.R — Casialties, Elections, Turnout, Population
# Purpose:
#   Build core municipal-level datasets used downstream:
#   (1) Legislative election results by party (votes, shares)
#   (2) Turnout, total electors, and abstentions
#   (3) Population (sex totals + age groups, Census waves)
#   Also sources CEMA casualties pre-processing (data/ceca/data_ceca.R).
#
# Inputs (read-only):
#   - data/ceca/data_ceca.R                   # war casualties preprocessing (sourced)
#   - data/eleicoes/raw/*Municipios_Votos*.xlsx
#       • Sheet 1 (skip = 10): votes by party; first row has mixed headers
#       • Sheet 3 (skip = 5): geo reference (name/type/code)
#   - data/eleicoes/raw/*Municipios_Eleitores*.xlsx
#       • Sheet 1 (skip = 11): electors / turnout / abstention (wide, repeated year cols)
#       • Sheet 3 (skip = 5): geo reference (name/type/code)
#   - data/ine/Municipios_Populacao_residente_segundo_os_Censos__total_e_por_sexo.xlsx
#       • Sheet 1 (skip = 10): sex totals by Census year (wide header layout)
#   - data/ine/Municipios_Populacao_residente_segundo_os_Censos__total_e_por_grupo_etario.xlsx
#       • Sheet 1 (skip = 10): age-group totals by Census year (wide header layout)
#
# Outputs:
#   - data/eleicoes/df_eleicoes_legislativas.rds   # long: geo_type, geo_name, party, votes, votes_share, year, geo_code
#   - data/eleicoes/df_eleicoes_turnout.rds        # wide (renamed): tot_YYYY, turnout_YYYY, absten_YYYY + geo fields
#   - data/ine/pop.rds                             # merged sex + age tables, aligned to municipal IDs
#
# Key operations & assumptions:
#   - Extract year from filenames; standardize first two columns to geo_type/geo_name.
#   - Drop rows with any NA in header-defined columns and the 'Anos' summary row.
#   - Convert numeric blocks, pivot votes wide→long, compute party shares using 'Total'.
#   - For turnout file, deduplicate repeated year columns and rename as:
#       'tot_<year>', 'turnout_<year>', 'absten_<year>'.
#   - Build municipal ID via zero-padded geo_code (4 digits) for joining with INE.
#   - Reconstruct wide, messy Census headers by propagating year labels.
#
# Dependencies:
#   - tidyverse (readr/dplyr/tidyr/stringr), readxl
#
# Repro tips:
#   - Run this before `2_merge.R`.
#   - This script calls `rm(list = ls())` twice to clear the workspace after each major block.
#     Remove those lines if sourcing within a larger pipeline.
#
# Author: Gabriel Caseiro (gabrielcaseiro99@gmail.com)
# Date: 2025-08-12
# ============================================================

library(tidyverse)
library(readxl)

## War Casualties ----

  source('data/ceca/data_ceca.R')

## Elections ----
  
  files<-list.files('data/eleicoes/raw','Municipios_Votos',full.names = T)
  
  
  geo<-read_xlsx(files[1],
                 sheet = 3,skip = 5) 
  
  geo<-geo |> 
    select(1,4,6)
  
  colnames(geo)<-c('geo_name','geo_type','geo_code')
  
  
  
  data<-lapply(files, function(f){
    
    ano<-as.numeric(str_extract(f,'[0-9]{4,4}'))
    
    df<-read_xlsx(f,sheet = 1,skip = 10)
    
    colnames(df)[1:2]<-c('geo_type','geo_name')
    
    ind<-which(!is.na(df[1,]))
    
    df<-df |> 
      select(any_of(ind))
    
    ind<-apply(df, 1, function(x) sum(is.na(x)))
    
    df<-df |> 
      filter(ind==0,!geo_name=='Anos') |>  
      mutate(across(3:ncol(df),as.numeric))
    
    
    df<-df |> 
      pivot_longer(3:ncol(df)) |> 
      rename(
        votes=value,
        party=name
      ) |> 
      mutate(
        year=ano
      )
    
    df<-df |> 
      group_by(geo_type,geo_name) |> 
      mutate(
        votes_tot=votes[party=='Total']
      ) |> 
      ungroup() |> 
      mutate(
        votes_share=100*votes/votes_tot
      )
    
    return(df)
  })
  
  data<-do.call(bind_rows,data)
  
  data<-data |> 
    left_join(geo)
  
  saveRDS(data,'data/eleicoes/df_eleicoes_legislativas.rds')
  
  rm(list = ls())
  
### Turnout ----
  
  
  files<-list.files('data/eleicoes/raw','Municipios_Eleitores',full.names = T)
  
  
  geo<-read_xlsx(files[1],
                 sheet = 3,skip = 5) 
  
  geo<-geo |> 
    select(1,4,6)
  
  colnames(geo)<-c('geo_name','geo_type','geo_code')
  
    
    df<-read_xlsx(files,sheet = 1,skip = 11)
    
    colnames(df)[1:2]<-c('geo_type','geo_name')
    
    ind<-which(!is.na(df[1,]))
    
    df<-df |> 
      select(any_of(ind))
    
    ind<-apply(df, 1, function(x) sum(is.na(x)))
    
    
    df<-df |> 
      filter(ind==0,!geo_name=='Anos') |>  
      mutate(across(3:ncol(df),as.numeric))
    
    cols<-colnames(df)[3:ncol(df)]
      cols<-str_sub(cols,1,4)
      cols<-data.frame(cols) |> 
        group_by(cols) |> 
        mutate(ind=cumsum(duplicated(cols))) |> 
        ungroup() |> 
        mutate(
          ind=ifelse(ind==0,
                     'tot',
                     ifelse(ind==1,
                            'turnout',
                            'absten'))
        )
      
      colnames(df)[3:ncol(df)]<-paste0(cols$ind,'_',cols$cols)
    
   
      df<-df|> 
        left_join(geo)
      
  
  saveRDS(df,'data/eleicoes/df_eleicoes_turnout.rds')
  
  rm(list = ls())
  

## Population ----
  
  dt_elei<-readRDS('data/eleicoes/df_eleicoes_legislativas.rds')
  
  mun_id<-dt_elei |> 
    select(geo_type,geo_name,geo_code) |> 
    mutate(id_c=str_pad(geo_code,4,'left',0)) |> 
    unique()
  
  pop_sex<-read_xlsx('data/ine/Municipios_Populacao_residente_segundo_os_Censos__total_e_por_sexo.xlsx',
                     sheet = 1,skip = 10)
  
  pop_sex<-pop_sex |> 
    select(1:20)
  
  cols<-data.frame(cols=colnames(pop_sex),anos=as.character(pop_sex[1,]))
  cols<-cols |> 
    mutate(ind=cumsum(!grepl('[.]',cols)),
           anos=gsub('┴ ','',anos)) |> 
    group_by(ind) |>
    mutate(cols=paste0(cols[1],'_',anos)) |> 
    ungroup()
  
  pop_sex<-pop_sex[-1,]  
  colnames(pop_sex)<-cols$cols
  colnames(pop_sex)[1:2]<-c('geo_type','geo_name')
  pop_sex<-pop_sex |> 
    filter(!is.na(`Masculino_1960`))
  
  
  pop_age<-read_xlsx('data/ine/Municipios_Populacao_residente_segundo_os_Censos__total_e_por_grupo_etario.xlsx',
                     sheet = 1,skip = 10)
  
  pop_age<-pop_age |> 
    select(1:104)
  
  cols<-data.frame(cols=colnames(pop_age),anos=as.character(pop_age[1,]))
  cols<-cols |> 
    mutate(ind=cumsum(!grepl('[.]',cols)),
           anos=gsub('┴ ','',anos)) |> 
    group_by(ind) |>
    mutate(cols=paste0(cols[1],'_',anos)) |> 
    ungroup()
  
  pop_age<-pop_age[-1,]  
  colnames(pop_age)<-cols$cols
  colnames(pop_age)[1:2]<-c('geo_type','geo_name')
  pop_age<-pop_age |> 
    filter(!is.na(Total_2021),!is.na(geo_name))

  
  pop<-left_join(pop_sex,pop_age)
  
  pop<-mun_id |> 
    left_join(pop)
  

  saveRDS(pop,'data/ine/pop.rds')
