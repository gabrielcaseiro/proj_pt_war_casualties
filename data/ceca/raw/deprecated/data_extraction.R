library(tidyverse)
library(pdftools)
library(stringdist)
library(stringi)

files<-list.files('data/cema/raw',pattern = 'Volume 8',full.names = T)

data<-lapply(files, function(f){

source('data/cema/functions.R')
  
## Load text from .pdf:

text<-pdf_text(f)

## Break paragraphs:

text<-str_split(text,'\n')
  text<-lapply(text, str_trim)
  
  ### Add page number:
  
  text <- lapply(seq_along(text), function(i) {
    
    x<-text[[i]]
    
    x<-data.frame(text=x,page=i)
    
    return(x)
    
  } )
  
## Consolidate single data.frame:

text<-do.call(bind_rows,text) 

  ### Keep only pages with relevant information (i.e., contains bio info):

  text<-text |> 
    group_by(page) |> 
    mutate(dinfo=sum(str_detect(text,'Nome(:| )(?!.*Post)'))) |> 
    ungroup()
  
## Create individual id:  
  
dt<-text |> 
  filter(dinfo>0) |> 
  select(-dinfo) |> 
  mutate(id=cumsum(str_detect(text,'^Nome')),
         line=1:n())

## Break lines that contain more than one column:

temp<-str_split(dt$text,' {20,}',simplify = T) |> 
  data.frame() |> 
  select(1,2) |> 
  rename(text=X1)

  dt<-temp |> 
    bind_cols(dt |> select(-text))
  
  temp<-dt |> 
    select(-text)|> 
    mutate(col=2)
  
  dt<-dt |> 
    select(-X2) |> 
    mutate(col=1)
  
## Adjust extra columns from the split data.frame:

  temp<-temp |> 
    mutate(X2=str_squish(X2)) |> 
    filter(!X2=='') 
  
    ### Get columns names:
  
    anchors<-na.omit(str_extract(temp$X2,'^.*:($| )')) |> unique() |> str_trim()

      # Consider only ones that are matched to the true columnames:

      anchors<-sapply(anchors, adjust_colnames, colnames = true_colnames, max_dist = 5)
      anchors<-anchors[grepl('data|falecimento|número|morte',anchors,ignore.case = T)]
      anchors<-names(anchors)
      anchors<-adjust_regex(anchors)

    anchors<-paste0('^',gsub(':$',"(:| )",anchors))
      
    ### Identify information per column:

    temp<-temp|> 
      group_by(id) |> 
      mutate(id2=cumsum(str_detect(X2,paste(anchors,collapse = '|')))) |> 
      ungroup()
    
    ### Identify line distance between information (keeping in the same column only if one line apart):
    
    temp<-temp|> 
      group_by(id,id2) |> 
      mutate(id3=line-lag(line),
             id3=ifelse(is.na(id3),1,id3),
             id3=cumsum(!id3==1)) |> 
      ungroup() 
    
    ### Concatenate text by individual-column and keep only the main line:
    
    temp<-temp |> 
      group_by(id,id2,id3) |> 
      mutate(text=paste(X2,collapse  = ' '),
             id4=1:n()) |> 
      ungroup()
    
    temp<-temp |> 
      filter(id4==1) |> 
      select(any_of(colnames(dt))) 
        
    ### Bind to main data.frame:
  
    dt<-dt |> 
      bind_rows(temp) |> 
      arrange(page,id,line,col) 

## Trim text and filter relevant (non-blank) lines:  

dt<-dt |> 
  mutate(text=str_squish(text)) |> 
  filter(!text=='',!grepl('^[0-9]{1,3}$',text))

## Check and correct different columns in the same line/text:
  
  ### Get columns names:

  anchors<-na.omit(str_extract(dt$text,'^.+?:($| )')) |> unique() |> str_trim()
    anchors<-anchors[str_length(anchors)<50]
    anchors<-anchors[str_length(anchors)>3]
    
    # Consider only ones that are matched to the true columnames:
    
    anchors<-sapply(anchors, adjust_colnames, colnames = true_colnames, max_dist = 5)
    anchors<-anchors[!names(anchors)==anchors|names(anchors)%in%true_colnames]
      anchors_sub<-anchors
    anchors<-names(anchors)
    anchors<-adjust_regex(anchors)
  
  combined_pattern <- paste0("(?=", paste(paste0(' ',anchors), collapse = "|"), ")|(?<=", paste(paste0(' ',anchors), collapse = "|"), ")")

  ### Split string if it contains more than one column:
  
  adj<-str_split(dt$text,combined_pattern,simplify = T,n=2) |> data.frame()
  
  ### Consolidate main data.frame:
  
  dt<-adj |> 
    transmute(text=X1) |> 
    bind_cols(dt |> select(-text)) |> 
    bind_rows(
      adj |> 
        transmute(text=X2) |> 
        bind_cols(dt |> select(-text))
    ) |> 
    filter(!text=='')|> 
    arrange(page,id,line,col) |> 
    mutate(text=str_squish(text))
  
## Adjust "Data do Falecimento" and "Causas da morte" columns:  

  anchors_sub<-anchors_sub[grepl('falecimento|morte',anchors,ignore.case = T)]  
    anchors_sub<-names(anchors_sub)
    anchors_sub<-adjust_regex(anchors_sub)
  
  ### Filter potential lines for adjustment:

  adj<-dt |> 
    filter(text%in%anchors_sub|!str_detect(text,paste(paste0('^',gsub(':',"",anchors)),collapse = '|'))) |> 
    group_by(page,id) |> 
    mutate(n=n()) |> 
    ungroup() |> 
    filter(n>1)
  
  ### Filter lines that could be part of the relevant columns:

  adj<-adj|> 
    group_by(id) |> 
    mutate(id2=cumsum(str_detect(text,paste(paste0('^',gsub(':',"",anchors_sub)),collapse = '|')))) |> 
    ungroup() |> 
    group_by(id,id2) |> 
    mutate(dif=line-lag(line)) |> 
    ungroup() |> 
    filter(is.na(dif)|dif<=2)|> 
    group_by(id,id2) |> 
    mutate(n=n()) |> 
    ungroup() |> 
    filter(n>1)
  
  ### Remove lines to be adjusted from main data.frame:
    
  dt<-dt |> 
    anti_join(adj)

  ### Concatenate text by individual-column:

  adj<-adj |> 
    group_by(id,id2) |> 
    mutate(text=paste(text,collapse  = ' '),
           id4=1:n()) |> 
    ungroup() |> 
    filter(id4==1) |> 
    select(any_of(colnames(dt)))
  
## Adjust main data.frame:
  
  ### Find the lines part of each individual-column:

  anchors<-paste0('^',gsub(':',"",anchors))

  dt<-dt|> 
    group_by(id) |> 
    mutate(id2=cumsum(str_detect(text,paste(anchors,collapse = '|')))) |> 
    ungroup()
  
  ### Concatenate text by individual columns and keep only main line:

  dt<-dt |> 
    group_by(id,id2)|> 
    mutate(text=paste(text,collapse  = ' '),
           id4=1:n()) |> 
    ungroup() |> 
    filter(id4==1) |> 
    select(-id4,-id2)
  
  ### Bind lines from "Data do Falecimento" and "Causas da morte" columns: 

  dt<-dt |> 
    bind_rows(adj)|> 
    arrange(page,id,line) 
  
  ### Adjust common OCR mistake:

  dt<-dt |> 
    mutate(text=gsub('^Mob','Unidade Mob',text)) |> 
    filter(!text=='unidade',!text=='Unidade')
  

## Consolidate main data.frame:
  
  ### Get columns names:

  anchors<-na.omit(str_extract(dt$text,'^.+?:( |$)')) |> unique() |> str_trim()
    anchors<-anchors[str_length(anchors)<50]
    anchors<-anchors[str_length(anchors)>3]
    
    anchors<-gsub(':',"",anchors)
      anchors<-anchors[order(-str_length(anchors))]
      
  ### Standardize column names:
      
  adj<-data.frame(
    anchor=anchors,
    colname=sapply(anchors, adjust_colnames, colnames = true_colnames, max_dist = 5),
    row.names = NULL
  ) |> unique()
  
  adj<-adj |> 
    filter(!anchor==colname|anchor%in%true_colnames)
  
  anchors<-adjust_regex(adj$anchor)

  dt<-dt |> 
    mutate(anchor=str_extract(text,paste(paste0('^',anchors),collapse="|")),
           text=str_remove(text,paste(paste0('^',anchors),collapse="|")),
           text=str_remove(text,'^[[:punct:]]'),
           text=str_squish(text)) |> 
    left_join(adj)
  
  ### Filter data.frame for pages with relevant infomation (i.e., contains bios):

  dt<-dt |> 
    group_by(page) |> 
    mutate(d=sum(colname=='Nome',na.rm = T)) |> 
    ungroup() |> 
    filter(d>0) |> 
    select(-d) 
  
  
## Consolidate final data.frame:
  
  ### Create additional id for non unique individual-column observations:
  
  dt<-dt |> 
    group_by(id,page,colname) |> 
    mutate(n=1:n()) |> 
    ungroup() 
  
  ### Pivot wider and add book and colony information:
  
  dt<-dt |> 
    select(page,id,n,colname,text) |>
    pivot_wider(names_from = colname,values_from = text) |> 
    mutate(book=basename(f),
           colonia=str_extract(book,'(?<=\\] ).*(?= - )'))
  
return(dt)
  
})

data<-do.call(bind_rows,data)

  # Create auxiliary variable to check potentially problematic observations:

  data<-data |> 
    group_by(id,page,book) |> 
    mutate(check=n()) |> 
    ungroup() 
  
# Save dataset:  

write.csv2(data,'data/cema/raw/raw_tab_mortos.csv',row.names = F,fileEncoding = 'UTF-8')
saveRDS(data,'data/cema/raw/raw_tab_mortos.rds')

rm(list = ls())
