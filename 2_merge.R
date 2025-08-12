# ============================================================
# Project: War and Democratization in Portugal
# Script: 2_merge.R — CECA Casualties ↔ 2001 Admin IDs (Concelho/Freguesia)
# Purpose:
#   Link individual casualty records from the CECA books to official
#   2001 Portuguese administrative identifiers:
#     (i) Concelho (id_c) and (ii) Freguesia (id_f).
#   Implements a staged matching pipeline (exact → fuzzy → manual),
#   including special handling for Açores and Madeira.
#
# Inputs (read-only):
#   - data/ceca/tab_mortos_em_campanha.rds                 # cleaned casualties
#   - data/dgterritorio/Version 1.0/Cont_Freg_V1.shp       # mainland (2001)
#   - data/dgterritorio/Version 1.0/*(Acores/Madeira).shp  # islands (2001)
#   - data/aux_data/Lista de Lugares, Freguesias e Oragos(Lugares).csv
#   - 0_aux_dic_concelhos.R  # provides `ultramar_dic_names`, `non_cont_concelhos`, etc.
#
# Intermediate files (manual review step):
#   - data/aux_data/freg_tomatch.csv   # prompts for manual/ChatGPT assistance (mainland)
#   - data/aux_data/freg_match.csv     # analyst-filled matches (mainland)
#   - data/aux_data/freg_tomatch2.csv  # prompts for islands
#   - data/aux_data/freg_match2.csv    # analyst-filled matches (islands)
#
# Outputs:
#   - data/tab_mortos_em_campanha_merged.rds
#       Columns include original CECA fields + id_c, id_c_new (if reassigned),
#       and id_f (possibly list when ambiguous), plus 'ultramar' tagging.
#
# Matching strategy (high level):
#   1) Standardize names (ASCII, upper, trim; SAO/SANTO → S.). Remove punctuation.
#   2) Tag records born outside mainland (ultramar) using dictionaries.
#   3) Mainland — Concelho:
#      a. Exact match; b. Exact Freguesia + fuzzy Concelho (OSA, substring guards);
#      c. Fuzzy Concelho (stringdist_left_join, OSA, max_dist=1);
#      d. Manual overrides (vector of known variants).
#   4) Mainland — Freguesia (conditional on id_c):
#      a. Exact; b. Fuzzy with distance + whole-word subset tests; c. Partial-word
#         intersection heuristics; d. Lugar→Freguesia bridge using INE “Lugares” list;
#      e. Fallback: same-named unique Freguesia in a different Concelho (flag via id_c_new);
#      f. Manual/ChatGPT prompts written to CSV and merged back from analyst file.
#   5) Islands (Açores/Madeira): repeat Concelho/Freguesia logic within NUT1 regions,
#      including small manual relabels and distance/intersection thresholds adapted for islands.
#
# Custom helpers (defined here):
#   - str_inter_cover(a,b): token overlap ratio (coverage)
#   - str_inter_length(a,b): total length of intersecting tokens
#
# Key assumptions & thresholds:
#   - Fuzzy distances use OSA normalized by min string length (typical cutoffs: 0.2–0.4).
#   - Word-boundary tests to prevent spurious substring matches (e.g., "^AMADORA").
#   - Ambiguous matches may yield id_f as a list (retain for later disambiguation).
#
# Dependencies:
#   - tidyverse, sf, fuzzyjoin, stringdist, stringi, data.table, janitor
#
# Repro tips:
#   - Run after `1_data_preprocessing.R` (needs tab_mortos_em_campanha.rds).
#   - Ensure DGTerritório 2001 layers are present under Version 1.0.
#   - Fill the CSVs produced in the manual steps before re-running to finalize matches.
#   - Script ends with `rm(list = ls())`.
#
# Author: Gabriel Caseiro (gabrielcaseiro99@gmail.com)
# Date: 2025-08-12
# ============================================================

library(tidyverse)
library(sf)
library(fuzzyjoin)
library(stringdist)
library(stringi)
library(data.table)
library(janitor)


str_inter_cover <- function(a, b) {
  a<-gsub('[[:punct:]]',' ',a)
  b<-gsub('[[:punct:]]',' ',b)
  words_a <- strsplit(a, "\\s+")[[1]]
  words_b <- strsplit(b, "\\s+")[[1]]
  length(intersect(words_a, words_b))/min(length(words_a),length(words_b))
}

str_inter_length <- function(a, b) {
  a<-gsub('[[:punct:]]',' ',a)
  b<-gsub('[[:punct:]]',' ',b)
  words_a <- strsplit(a, "\\s+")[[1]]
  words_b <- strsplit(b, "\\s+")[[1]]
  sum(str_length(intersect(words_a, words_b)))
}

source('0_aux_dic_concelhos.R')

# 1. Load datasets ----

## Continental Portugal Map (2001):

map<-read_sf('data/dgterritorio/Version 1.0','Cont_Freg_V1')

  ### Prepare list of Freguesias:

  freguesias<-map |> 
    select(FREGUESIA,CONCELHO,DICOFRE) |> 
    st_drop_geometry() |> 
    distinct() |> 
    rename(id_f=DICOFRE) |> 
    mutate(id_c=str_sub(id_f,1,4),
           FREGUESIA=stri_trans_general(FREGUESIA,'latin-ascii'),
           CONCELHO=stri_trans_general(CONCELHO,'latin-ascii'),
           FREGUESIA=str_remove(FREGUESIA,'[:]{1,}'),
           FREGUESIA=gsub('( |)-( |)','-',FREGUESIA),
           FREGUESIA=str_to_upper(FREGUESIA),
           FREGUESIA=str_squish(gsub('\\b(SAO|SANTO)\\b','S.',FREGUESIA)),
           CONCELHO=str_remove(CONCELHO,'[:]{1,}'),
           CONCELHO=str_to_upper(CONCELHO),
           CONCELHO=gsub('( |)-( |)','-',CONCELHO),
           CONCELHO=str_squish(gsub('\\b(SAO|SANTO)\\b','S.',CONCELHO))
           )
  
  ### Prepare list of Concelhos:
  
  concelhos<-freguesias |> 
    select(CONCELHO,id_c) |> 
    unique()
    
    
## Decolonization Wars Casualties:

  data<-readRDS('data/ceca/tab_mortos_em_campanha.rds')

  data<-data|> 
    mutate(FREGUESIA=stri_trans_general(Freguesia,'latin-ascii'),
           CONCELHO=stri_trans_general(Concelho,'latin-ascii'),
           FREGUESIA=str_remove(FREGUESIA,'[:]{1,}'),
           FREGUESIA=gsub('( |)-( |)','-',FREGUESIA),
           FREGUESIA=str_to_upper(FREGUESIA),
           FREGUESIA=str_squish(gsub('\\b(SAO|SANTO)\\b','S.',FREGUESIA)),
           CONCELHO=str_remove(CONCELHO,'[:]{1,}'),
           CONCELHO=str_to_upper(CONCELHO),
           CONCELHO=gsub('( |)-( |)','-',CONCELHO),
           CONCELHO=str_squish(gsub('\\b(SAO|SANTO)\\b','S.',CONCELHO)),
           CONCELHO=ifelse(CONCELHO=='NAO CONSTA NO PROCESSO INDIVIDUAL','',CONCELHO))

  ### Identify if not born in continental Portugal:
  
  ultramar_dic<-paste0("(?i)(",paste(names(ultramar_dic_names),collapse = "|"),")")

  data<-data |> 
    mutate(ultramar=ifelse(!CONCELHO%in%freguesias$CONCELHO,str_extract(CONCELHO,ultramar_dic),NA),
           ultramar=ifelse(str_detect(CONCELHO,'JOAO DA MADEIRA'),NA,ultramar),
           ultramar=ultramar_dic_names[str_to_lower(ultramar)],
           ultramar=ifelse(is.na(ultramar),non_cont_concelhos[CONCELHO],ultramar))
  
  
# 2. Concelho Join -----
  
  tomatch<-data |> 
    filter(!CONCELHO=='',is.na(ultramar)) |> 
    select(CONCELHO) |> 
    distinct()
  
  ## First Join - Exact Concelho Name Match:
  
  ok<-tomatch |> 
    left_join(concelhos) |> 
    filter(!is.na(id_c))
  
  tomatch<-tomatch |> 
    anti_join(ok)
  
  ## Second Join - Exact Freguesia Name Match + Fuzzy Concelho Name Match:
  
  temp<-data |> 
    filter(CONCELHO%in%tomatch$CONCELHO) |> 
    select(CONCELHO,FREGUESIA) |> 
    distinct() |> 
    mutate(id_temp=1:n())
    
    temp<-temp |> 
      left_join(freguesias,by='FREGUESIA') |> 
      mutate(dist=stringdist(CONCELHO.x,CONCELHO.y,'osa')/pmin(str_length(CONCELHO.x),str_length(CONCELHO.y))) |> 
      rowwise() |> 
      mutate(ind_sub=1*(grepl(CONCELHO.x,CONCELHO.y)|grepl(CONCELHO.y,CONCELHO.x))) |> 
      ungroup()
    
    temp<-temp |> 
      group_by(id_temp) |> 
      mutate(n=sum(ind_sub)) |> 
      ungroup() |> 
      filter((ind_sub==1&n==1)|(n==0&dist<0.4)) |> 
      select(CONCELHO.x,id_c) |> 
      distinct() |> 
      rename(CONCELHO=CONCELHO.x)
    
  ok<-ok |> 
    bind_rows(temp)
    
  tomatch<-tomatch |> 
    anti_join(ok)
  
  ## Third Join - Fuzzy Concelho Name Match
  
  temp<-tomatch |> 
    stringdist_left_join(concelhos,max_dist=1,distance_col='dist',method='osa') |> 
    filter(!is.na(CONCELHO.y))
    
    temp<-temp |> 
      select(CONCELHO.x,id_c) |> 
      rename(CONCELHO=CONCELHO.x) 
  
  ok<-ok |> 
    bind_rows(temp)
    
  tomatch<-tomatch |> 
    anti_join(ok)
  
  ## Fourth Join - Manual Match
    
    temp<-data |> 
      filter(CONCELHO%in%tomatch$CONCELHO) |> 
      group_by(CONCELHO) |> 
      summarise(FREGUESIA=paste(unique(FREGUESIA),collapse = ';')) |> 
      ungroup()
    
      concelho_pareado <- c(
        "ALCACER-DO-SAL" = "ALCACER DO SAL",
        "FREIXO-DE-ESPADA-A-CINTA" = "FREIXO DE ESPADA A CINTA",
        "LAFOES" = "OLIVEIRA DE FRADES",        
        "MOITA DO RIBATEJO" = "MOITA",
        "NOVA GAIA" = "VILA NOVA DE GAIA",
        "REGUENGOS MONSARAZ" = "REGUENGOS DE MONSARAZ",
        "VILA DA FEIRA" = "SANTA MARIA DA FEIRA",
        "VILA FEIRA" = "SANTA MARIA DA FEIRA", # Variante do mesmo nome
        "VOUGA" = "AVEIRO"
      )
      
    temp<-temp |> 
      mutate(temp=concelho_pareado[CONCELHO]) |> 
      left_join(concelhos,by=c('temp'='CONCELHO')) |> 
      filter(!is.na(id_c)) |> 
      select(-temp,-FREGUESIA)
  
    ok<-ok |> 
      bind_rows(temp)
    
    tomatch<-tomatch |> # Two ambiguous cases without match
      anti_join(ok)
  
   data<-data |> 
     left_join(ok)
   
   rm(temp)
   rm(ok)
   rm(tomatch)
    
    

# 3. Freguesia Join ----
   
  ## First Join - Exact Name

  tomatch<-data |> 
    filter(!is.na(FREGUESIA),!FREGUESIA=='',!is.na(id_c)) |> 
    select(FREGUESIA,id_c) |> 
    distinct()
  
  ok<-tomatch |> 
    left_join(freguesias) |> 
    filter(!is.na(id_f))
  
  tomatch<-tomatch |> 
    anti_join(ok)
  
  ## Second Join - Fuzzy Freguesia Name Match (stringdist and complete intersection):
  
  temp<-tomatch |> 
    mutate(id_temp=1:n())|> 
    left_join(freguesias,by='id_c')
  
  temp<-temp |> 
    mutate(dist=stringdist(FREGUESIA.x,FREGUESIA.y,'osa')/pmin(str_length(FREGUESIA.x),str_length(FREGUESIA.y))) |> 
    rowwise() |> 
    mutate(ind_sub=1*(grepl(paste0("\\b", FREGUESIA.x, "\\b"), FREGUESIA.y)|grepl(paste0("\\b", FREGUESIA.y, "\\b"), FREGUESIA.x))) |> 
    ungroup()
  
  temp<-temp |> 
    group_by(id_temp) |> 
    mutate(n=sum(ind_sub),
           min_dist=min(dist)) |> 
    ungroup() |> 
    filter((ind_sub==1&n==1)|(!n==1&dist<=0.28&dist==min_dist)) |> 
    group_by(FREGUESIA.x,CONCELHO,id_c) |> 
    select(FREGUESIA.x,CONCELHO,id_c,id_f) |> 
    distinct() |> 
    rename(FREGUESIA=FREGUESIA.x)
  
  ok<-ok |> 
    bind_rows(temp)
  
  tomatch<-tomatch |> 
    anti_join(temp)
  
  ## Third Join - Fuzzy Freguesia Name Match (partial intersection)
  
  temp<-tomatch |> 
    mutate(id_temp=1:n())|> 
    left_join(freguesias,by='id_c')
  
  temp<-temp |> 
    rowwise() |> 
    mutate(inter_cover=str_inter_cover(FREGUESIA.x,FREGUESIA.y),
           inter_length=str_inter_length(FREGUESIA.x,FREGUESIA.y)) |> 
    ungroup() |> 
    filter(inter_cover>.8)
  
  temp<-temp |> 
    group_by(id_temp) |> 
    mutate(inter_max=max(inter_length)) |> 
    ungroup() |> 
    filter(inter_length==inter_max)
  
  temp<-temp |> 
    group_by(FREGUESIA.x,id_c,CONCELHO) |> 
    summarise(id_f=list(id_f)) |> 
    ungroup() |> 
    rename(FREGUESIA=FREGUESIA.x)
  
  ok<-ok |>
    mutate(id_f=as.list(id_f)) |> 
    bind_rows(temp)
  
  tomatch<-tomatch |> 
    anti_join(temp)
  
  ## Fourth Join - Exact + Fuzzy Lugar name match:
  
  lugares<-fread('data/aux_data/Lista de Lugares, Freguesias e Oragos(Lugares).csv',
                 skip = 4,select = 1:6)
    lugares<-row_to_names(lugares,1)
    
    lugares<-lugares|> 
      mutate(id_l=1:n(),
             FREGUESIA=stri_trans_general(Freguesia,'latin-ascii'),
             CONCELHO=stri_trans_general(Concelho,'latin-ascii'),
             LUGAR=stri_trans_general(`Lugares (Census 2011)`,'latin-ascii'),
             FREGUESIA=str_remove(FREGUESIA,'[:]{1,}'),
             FREGUESIA=gsub('( |)-( |)','-',FREGUESIA),
             FREGUESIA=str_to_upper(FREGUESIA),
             FREGUESIA=str_squish(gsub('\\b(SAO|SANTO)\\b','S.',FREGUESIA)),
             CONCELHO=str_remove(CONCELHO,'[:]{1,}'),
             CONCELHO=str_to_upper(CONCELHO),
             CONCELHO=gsub('( |)-( |)','-',CONCELHO),
             CONCELHO=str_squish(gsub('\\b(SAO|SANTO)\\b','S.',CONCELHO)),
             LUGAR=str_remove(LUGAR,'[:]{1,}'),
             LUGAR=str_to_upper(LUGAR),
             LUGAR=gsub('( |)-( |)','-',LUGAR),
             LUGAR=str_squish(gsub('\\b(SAO|SANTO)\\b','S.',LUGAR))
             )
  
    lugares<-lugares |> 
      left_join(concelhos) |> 
      left_join(freguesias)
    
    lugares_tomatch<-lugares |> 
      filter(!is.na(id_f)) |> 
      transmute(id_c,
                id_f,
                FREGUESIA=LUGAR) |> 
      group_by(id_c,FREGUESIA) |> 
      summarise(id_f=list(id_f)) |> 
      ungroup()
  
    temp<-tomatch |>
      left_join(lugares_tomatch,by='id_c')|> 
      mutate(dist=stringdist(FREGUESIA.x,FREGUESIA.y,'osa')/pmin(str_length(FREGUESIA.x),str_length(FREGUESIA.y))) |> 
      rowwise()
    
      temp1<-temp |> 
        filter(dist==0)
    
      temp<-temp |> 
        anti_join(temp1 |> select(1:2))
      
    temp<-temp |> 
      filter(dist<0.2) |> 
      bind_rows(temp1) |> 
      rename(FREGUESIA=FREGUESIA.x) |> 
      group_by(FREGUESIA,id_c) |> 
      summarise(id_f=list(unlist(id_f))) |> 
      ungroup()
    
      rm(temp1)
        
    ok<-ok |>
      bind_rows(temp)
    
    tomatch<-tomatch |> 
      anti_join(temp)
    
    
  ## Fifth Join - Exact Freguesia name match (different Concelho):
    
    temp<-freguesias |> 
      filter(!FREGUESIA%in%FREGUESIA[duplicated(FREGUESIA)])
    
    temp<-tomatch  |> 
      left_join(
        temp,
        by='FREGUESIA') |> 
      filter(!is.na(id_f)) |> 
      select(-CONCELHO) |> 
      rename(id_c=id_c.x,
             id_c_new=id_c.y)
      
    
    ok<-ok |>
      bind_rows(temp |> 
                  mutate(id_f=as.list(id_f)))
    
    tomatch<-tomatch |> 
      anti_join(temp |> select(FREGUESIA))
    
  
  ## Manual Join - with ChatGPT:

  temp<-tomatch |> 
    left_join(freguesias,by='id_c') |> 
    group_by(FREGUESIA.x,CONCELHO,id_c) |> 
    summarise(
      query=paste0(
      "You are matching parish names (freguesias) within the municipality (concelho) of '", CONCELHO[1], "' in Portugal, using the official 2001 administrative divisions.\n",
      "Here is the list of official freguesias in this municipality:\n- ",
      paste(FREGUESIA.y, collapse = "\n- "), "\n\n",
      "Does the name '", FREGUESIA.x[1], "' correspond to, or is it a possible variant, abbreviation, or old spelling of, any freguesia in this list?\n",
      "If yes, return the matching name(s) exactly as written in the list, comma-separated. ",
      "If not, return 'NONE'. If there is ambiguity, list all plausible options, comma-separated. Do not invent new names."
    )) |> 
    ungroup()
  
  write.csv(temp,'data/aux_data/freg_tomatch.csv',row.names = F)
  
  temp<-fread('data/aux_data/freg_match.csv') |> 
    mutate(id_c=str_pad(id_c,4,'left',0),
           new_id_c=str_pad(new_id_c,4,'left',0),
           chatgpt=ifelse(chatgpt=='NA',NA,chatgpt))
    
    add<-freguesias |> 
      filter(CONCELHO=='AMADORA'&grepl('^AMADORA',FREGUESIA)) |> 
      group_by(CONCELHO,id_c) |> 
      summarise(id_f=list(id_f)) |> 
      ungroup() |> 
      mutate(FREGUESIA='AMADORA')
      
    freguesias<-freguesias |> 
      mutate(id_f=as.list(id_f)) |> 
      bind_rows(add)
  
  temp<-temp |> 
    mutate(chatgpt=ifelse(!is.na(new_id_c)&is.na(chatgpt),FREGUESIA.x,chatgpt),
           chatgpt=str_split(str_squish(chatgpt),';'),
           new_id_c=ifelse(is.na(new_id_c),id_c,new_id_c)) |> 
    unnest(chatgpt) |> 
    rename(FREGUESIA=chatgpt) |> 
    left_join(freguesias |> select(-CONCELHO),by=c('new_id_c'='id_c','FREGUESIA')) |> 
    filter(!is.na(FREGUESIA))
  
  temp<-temp |> 
    group_by(FREGUESIA.x,CONCELHO,id_c,new_id_c) |> 
    summarise(id_f=list(id_f)) |> 
    ungroup() |> 
    rename(FREGUESIA=FREGUESIA.x) |> 
    mutate(new_id_c=ifelse(new_id_c==id_c,NA,new_id_c)) |> 
    rename(id_c_new=new_id_c)
  
  ok<-ok |>
    bind_rows(temp)
  
  tomatch<-tomatch |> 
    anti_join(temp) # 23 cases without match

  ok<-ok |> 
    select(-CONCELHO)
  
  data<-data |> 
    left_join(ok)
  
  rm(list = ls()[!ls()=='data'])
  
# 4. Açores e Madeira ----
  
  
  files<-list.files('data/dgterritorio/Version 1.0','shp')
    files<-files[!grepl("^Cont",files)]
    files<-gsub('[.]shp','',files)
  
  map<-lapply(files, function(f){
    
    read_sf('data/dgterritorio/Version 1.0',f) |> 
      st_drop_geometry()
    
  })
    
  map<-do.call(bind_rows,map)
  
  freguesias<-map |> 
    select(NUT1,FREGUESIA,CONCELHO,DICOFRE) |> 
    distinct() |> 
    rename(id_f=DICOFRE) |> 
    mutate(id_c=str_sub(id_f,1,4),
           FREGUESIA=stri_trans_general(FREGUESIA,'latin-ascii'),
           CONCELHO=stri_trans_general(CONCELHO,'latin-ascii'),
           FREGUESIA=str_remove(FREGUESIA,'[:]{1,}'),
           FREGUESIA=gsub('( |)-( |)','-',FREGUESIA),
           FREGUESIA=str_to_upper(FREGUESIA),
           FREGUESIA=str_squish(gsub('\\b(SAO|SANTO)\\b','S.',FREGUESIA)),
           CONCELHO=str_remove(CONCELHO,'[:]{1,}'),
           CONCELHO=str_to_upper(CONCELHO),
           CONCELHO=gsub('( |)-( |)','-',CONCELHO),
           CONCELHO=str_squish(gsub('\\b(SAO|SANTO)\\b','S.',CONCELHO)),
           ultramar=ifelse(NUT1=='MADEIRA','Portugal (Madeira)','Portugal (Açores)')
    ) |> 
    select(-NUT1)
  
  concelhos<-freguesias |> 
    select(ultramar,CONCELHO,id_c) |> 
    unique()
  
    
  
  data<-data |> 
    mutate(temp=1:n())
  
  data_toadj<-data |> 
    filter(ultramar%in%c('Portugal (Açores)',
                         'Portugal (Madeira)')) |> 
    select(-id_c,-id_c_new,-id_f) |> 
    mutate(CONCELHO=ifelse(FREGUESIA=='FENAIS DA AJUDA','RIBEIRA GRANDE',CONCELHO))
  
  
## Concelhos:
  
  tomatch<-data_toadj |> 
    select(ultramar,CONCELHO) |> 
    distinct()
  
  ok<-tomatch |> 
    left_join(concelhos,by='ultramar') |> 
    rowwise() |> 
    mutate(temp=1*grepl(paste0('^',CONCELHO.y),CONCELHO.x)) |>
    ungroup() |> 
    filter(temp==1)
  
    ok<-ok |> 
      group_by(CONCELHO.x) |> 
      mutate(temp=n()) |> 
      ungroup()
    
    ok<-ok |> 
      filter(temp==1|(temp==2&CONCELHO.y=='SANTA CRUZ DAS FLORES'))
    
    ok<-ok |> 
      select(-CONCELHO.y,-temp) |> 
      rename(CONCELHO=CONCELHO.x)
    
    tomatch<-tomatch |> anti_join(ok)
    
    temp<-tomatch |> 
      left_join(concelhos,by='ultramar') |> 
      rowwise() |> 
      mutate(temp=str_inter_cover(CONCELHO.y,CONCELHO.x)) |>
      ungroup() |> 
      filter(temp>.5)
    
    temp<-temp|> 
      select(-CONCELHO.y,-temp) |> 
      rename(CONCELHO=CONCELHO.x)
    
    ok<-ok |> bind_rows(temp)
    
    tomatch<-tomatch |> anti_join(temp)
    
    
    concelho_pareado <- c(
      "S. ROQUE DO FAIAL"="SANTANA",
      "S. MIGUEL-ACORES"="VILA FRANCA DO CAMPO",
      "PICO-ACORES"="S. ROQUE DO PICO",
      "REIBEIRA GRANDE-ACORES"="RIBEIRA GRANDE"
    )
    
    temp<-tomatch |> 
      mutate(temp=concelho_pareado[CONCELHO]) |> 
      left_join(concelhos,by=c('temp'='CONCELHO','ultramar')) |> 
      filter(!is.na(id_c)) |> 
      select(-temp)
    
    ok<-ok |> bind_rows(temp)
    
    data_toadj<-data_toadj |> 
      left_join(ok)
    
    
## Freguesias:
    
  tomatch<-data_toadj |> 
    select(ultramar,FREGUESIA,id_c) |> 
    distinct() 
    
  ok<-tomatch |> 
    left_join(freguesias |> select(-CONCELHO)) |> 
    filter(!is.na(id_f))
  
  tomatch<-tomatch |> anti_join(ok)
  
  
  temp<-tomatch |> 
    left_join(freguesias |> select(-CONCELHO),
              by=c('ultramar','id_c')) |> 
    rowwise() |> 
    mutate(temp=str_inter_cover(FREGUESIA.y,FREGUESIA.x)) |>
    ungroup() |> 
    filter(temp>.8,!FREGUESIA.y=='CAPELAS')
  
  temp <- temp |> 
    group_by(FREGUESIA.x,id_c,ultramar) |> 
    summarise(id_f=list(id_f)) |> 
    ungroup() |> 
    rename(FREGUESIA=FREGUESIA.x)
  
  
  ok<-ok |> 
    mutate(id_f=as.list(id_f)) |> 
    bind_rows(temp)
  
  tomatch<-tomatch |> anti_join(temp)
  
  
  
  temp<-tomatch |> 
    left_join(freguesias |> select(-CONCELHO),
              by=c('ultramar','id_c')) |> 
    rowwise() |> 
    mutate(dist=stringdist(FREGUESIA.x,FREGUESIA.y,'osa')/pmin(str_length(FREGUESIA.x),str_length(FREGUESIA.y))) |>
    ungroup() |> 
    filter(dist<0.3)
  
  temp<-temp |> 
    select(-dist,-FREGUESIA.y) |> 
    rename(FREGUESIA=FREGUESIA.x) |> 
    mutate(id_f=as.list(id_f))
  
  ok<-ok |> 
    bind_rows(temp)
  
  tomatch<-tomatch |> anti_join(temp)
  
  temp<-tomatch |> 
    left_join(freguesias ,
              by=c('ultramar','id_c')) 
  
  temp<-temp |> 
    group_by(FREGUESIA.x,CONCELHO,id_c) |> 
    summarise(
      query=paste0(
        "You are matching parish names (freguesias) within the municipality (concelho) of '", CONCELHO[1], "' in Portugal, using the official 2001 administrative divisions.\n",
        "Here is the list of official freguesias in this municipality:\n- ",
        paste(FREGUESIA.y, collapse = "\n- "), "\n\n",
        "Does the name '", FREGUESIA.x[1], "' correspond to, or is it a possible variant, abbreviation, or old spelling of, any freguesia in this list?\n",
        "If yes, return the matching name(s) exactly as written in the list, comma-separated. ",
        "If not, return 'NONE'. If there is ambiguity, list all plausible options, comma-separated. Do not invent new names."
      )) |> 
    ungroup()
  
  write.csv(temp,'data/aux_data/freg_tomatch2.csv',row.names = F)
  
  temp<-fread('data/aux_data/freg_match2.csv')
    temp[temp=='']<-NA
    
    temp<-temp |> 
      left_join(concelhos,by=c('CONCELHO_at'='CONCELHO')) |> 
      select(-ultramar,-query) |> 
      rename(id_c=id_c.x,
             id_c_new=id_c.y)
    
    temp<-temp |> 
      mutate(id_c_new=ifelse(is.na(id_c_new),id_c,id_c_new)) |> 
      left_join(
        freguesias |> 
          select(-CONCELHO),
        by=c('FREGUESIA','id_c_new'='id_c')
      )
  
    temp<-temp |> 
      select(-FREGUESIA,-CONCELHO_at,-CONCELHO) |> 
      mutate(id_c_new=ifelse(id_c_new==id_c,NA,id_c_new)) |> 
      mutate(id_f=as.list(id_f),
             id_c=as.character(id_c)) |> 
      rename(FREGUESIA=FREGUESIA.x) |> 
      filter(!is.na(id_f))
    
    ok<-ok |> 
      bind_rows(temp)
    
    tomatch<-tomatch |> anti_join(temp)
    
    data_toadj<-data_toadj |> 
      left_join(ok)
    
    
    
    data<-data |> 
      filter(!temp%in%data_toadj$temp) |> 
      bind_rows(data_toadj) |> 
      arrange(temp) |> 
      select(-temp)
  

  saveRDS(data,'data/tab_mortos_em_campanha_merged.rds')
    

rm(list = ls())






 