## ---------------------------
## PROGRAMME : Ena_cohorte_80S.R
## ENCODAGE : utf-8
##
## AUTEUR·RICE·S :
## - Eve SAMANI - eve.samani@insee.fr
## Stage DERA-ERASE (avec Benjamin GILBERT et Fanny GODET)
##
## DATE DE CREATION : 05/07/2024
## ---------------------------

## indication d'un répertoire de travail
setwd("X:/HAB-EFP-STAGES/")

## ---------------------------
## OPTIONS

## option de parallélisation du calcul
options(arrow.use_threads = TRUE)
arrow::set_cpu_count(parallel::detectCores() %/% 2)

install.packages("data.table")
install.packages("tidyverse")
install.packages("tidylog")

## ---------------------------
## LIBRAIRIES

## chargement des librairies de manière directe
library(arrow)
library(data.table)
library(tidyverse)
library(tidylog)
library(duckdb)



## CODE
# Suivi de la cohorte des élèves énarques des années 1980
# Attention : il manque les années 1979, 1981 et 1987 dans les fichiers FGE ainsi que les années 1993, 1994 et 1995


## -----Etape 0 : JOINTURE des fichiers fge et des libellés  ------------

vect_annee_fge = c("1978", "1980", "1982", "1983", "1984", "1985", "1986", "1988", "1989", "1990", "1991", "1992", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009")
vect_anne_ane = c("1994", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009")

ane_1994 <- read_delim("X:/HAB-EFP-STAGES/Données/FGE/Documentation/Nomenclatures/Grades/grades_ane_1994.csv", col_types = cols(grade_ANE = col_character()))
ane_1994 <- ane_1994 %>%
  select("grade_ANE", "LICORPS", "LIGRADE") %>%
  collect()
list_annee <- list()

#Boucle de jointure pour les fichiers fge allant de 1978 à 1992 avec le fichier ane_1994
for (annee in vect_annee_fge[1:12]) {
  fge_annee <- open_dataset(
    paste0("X:/HAB-EFP-STAGES/Données/FGE/fge_",
           annee,
           ".parquet"),
    format="parquet") %>%
    select(GRADE)
  list_annee[[annee]] <- fge_annee %>%
    # jointure avec la table de grades ANE de 1994
    left_join(ane_1994,
              by = join_by("GRADE" == "grade_ANE"),
    ) %>%
    mutate(CORPS = substr(GRADE, 1, 6)) %>%
    group_by(CORPS, LICORPS, GRADE, LIGRADE) %>%
    summarise(n=n()) %>%
    arrange(-n) %>%
    mutate(ANNEE = annee) %>%
    collect() 
}
join_fge_ane <- do.call(rbind, lapply(list_annee, as.data.frame)) %>% collect()

#Jointure des fichiers fge d'après 1994
for (annee in vect_anne_ane[-1]) {
  ane_annee <- fread(
    paste0("X:/HAB-EFP-STAGES/Données/FGE/Documentation/Nomenclatures/Grades/grades_ane_",
           annee,
           ".csv"),
    colClasses="character", keepLeadingZeros = TRUE) %>%
    select(LICORPS, LIGRADE, grade_ANE)
  fge_annee <- open_dataset(
    paste0("X:/HAB-EFP-STAGES/Données/FGE/fge_",
           annee,
           ".parquet"),
    format="parquet") %>%
    select(GRADE)
  list_annee[[annee]]<- fge_annee %>%
    left_join(ane_annee,
              by = join_by("GRADE" == "grade_ANE"),
    ) %>%
    mutate(CORPS = substr(GRADE, 1, 6)) %>%
    group_by(CORPS, LICORPS, GRADE, LIGRADE) %>%
    summarise(n=n()) %>%
    arrange(-n) %>%
    mutate(ANNEE = annee) %>%
    collect()
}
join_fge_ane <- do.call(rbind, lapply(list_annee, as.data.frame)) %>% collect()





## -------- Etape 1 : SUIVI POPULATION | GRANDS CORPS ADMINISTRATIFS 1980-1989 -------------------------

## ------- INITIALISATION : 1980 -------------------------------------------
ena_80s <- open_dataset("X:/HAB-EFP-STAGES/Données/FGE/fge_1980.parquet", format="parquet") %>%
  select(GRADE, SN, NI, DPC, ENREG) %>%
  filter(GRADE == "12907511") %>%
  #Le grade 12907511 correspond aux énarques
  filter(ENREG %in% c(0,2)) %>%
  mutate(ANNEE = "1980") %>%
  collect()
NIR_1980 <- ena_80s$NI
NIR_1980 <- unique(NIR_1980)


conversion <- read.csv2("Z:/WINDOWS/conversion_1978_2022.csv", colClasses="character")
conversion <- conversion %>%
  mutate(COEFFICIENT = str_replace(COEFFICIENT, ",", ".")) %>%
  mutate(COEFFICIENT = as.numeric(COEFFICIENT)) %>%
  collect()

smic_post_2000 <- read.csv2("Z:/WINDOWS/smic35.csv") %>%
  mutate(ANNEE12 = as.character(ANNEE12)) %>%
  left_join(conversion, by = join_by("ANNEE12" == "PERIODE")) %>%
  mutate(SMIC_MENSUEL_euros_constants = (as.numeric(SMICNM)*100)/COEFFICIENT) %>%
  select(ANNEE12, SMICNM, SMIC_MENSUEL_euros_constants) %>%
  rename("ANNEE" = ANNEE12) %>%
  collect()

smic_ante_2000 <- read.csv2("Z:/WINDOWS/smic39.csv") %>%
  mutate(ANNEE13 = as.character(ANNEE13)) %>%
  filter(ANNEE13 %in% seq(1978, 1999)) %>%
  left_join(conversion, by = join_by("ANNEE13" == "PERIODE")) %>%
  mutate(SMIC_MENSUEL_euros_constants = (as.numeric(SMICNM)*100)/COEFFICIENT) %>%
  select(ANNEE13, SMICNM, SMIC_MENSUEL_euros_constants) %>%
  rename("ANNEE" = ANNEE13) %>%
  collect()

smic <- rbind(smic_ante_2000, smic_post_2000) %>% distinct(.keep_all = TRUE) %>% collect()

salaire <- open_dataset("X:/HAB-EFP-STAGES/Données/FGE/fge_1980.parquet", format="parquet") %>%
  print("suivi salaires") %>%
  select(GRADE, SN, NI, DPC, CAN, ENREG, MIN) %>%
  filter(NI %in% NIR_1980) %>%
  mutate(DPC = pmin(as.numeric(DPC), 24),
         SN = as.numeric(SN)) %>%
  filter(DPC > 3) %>%
  filter(ENREG %in% c(0,2)) %>%
  filter(CAN =="01") %>%
  mutate(SALAIRE_ANNUEL = SN * (24/DPC)*0.15245) %>%
  mutate(SALAIRE_MENSUEL = (SALAIRE_ANNUEL / 12)) %>%
  mutate(ANNEE = "1980") %>%
  mutate(ANNEE = as.character(ANNEE)) %>%
  left_join(conversion,
            by = join_by("ANNEE" == "PERIODE")) %>%
  mutate(SALAIRE_MENSUEL = (SALAIRE_MENSUEL*100)/COEFFICIENT) %>%
  ungroup() %>%
  left_join(smic, by = "ANNEE") %>%
  filter(SN*0.15245 > 3*SMIC_MENSUEL_euros_constants) %>% 
  select(GRADE, NI, ANNEE, SN, SALAIRE_MENSUEL, SALAIRE_ANNUEL, MIN, DPC) %>%
  collect()

for (an in setdiff(seq(1982, 2009, by=1), c("1981", "1987","1993", "1994", "1995"))) {
  print(an)
  salaire_ena_ad <- open_dataset(
    paste0("X:/HAB-EFP-STAGES/Données/FGE/fge_",
           an,
           ".parquet"),
    format="parquet") %>%
    select(GRADE, SN, NI, DPC, CAN, ENREG, MIN) %>%
    filter(NI %in% NIR_1980) %>%
    filter(ENREG %in% c(0, 2)) %>%
    filter(CAN == "01") %>%
    mutate(DPC = pmin(as.numeric(DPC), 24),
           SN = as.numeric(SN)) %>%
    filter(DPC > 3) %>%
    mutate(ANNEE = an) %>%
    mutate(SALAIRE_ANNUEL_1 = if_else(as.integer(an) < 2001, SN*0.15245, SN)) %>%
    mutate(ANNEE = as.character(ANNEE)) %>%
    left_join(conversion,
              by = join_by("ANNEE" == "PERIODE")) %>%
    mutate(SALAIRE_ANNUEL_1 = (SALAIRE_ANNUEL_1*100)/COEFFICIENT) %>%
    ungroup() %>%
    left_join(smic, by = "ANNEE") %>%
    filter(SALAIRE_ANNUEL_1 > 3*SMIC_MENSUEL_euros_constants) %>%
    mutate(SALAIRE_ANNUEL = SALAIRE_ANNUEL_1*(24/DPC)) %>%
    mutate(SALAIRE_MENSUEL = SALAIRE_ANNUEL/12) %>%
    mutate(ANNEE = as.character(ANNEE)) %>%
    select(GRADE, NI, ANNEE, SN, SALAIRE_MENSUEL, SALAIRE_ANNUEL, DPC, MIN) %>%
    collect()
  
  salaire <- rbind(salaire, salaire_ena_ad) %>% distinct(.keep_all = TRUE) %>%
    collect()
}

salaire_ena <- salaire %>%
  left_join(join_fge_ane,
            by = c("GRADE", "ANNEE")) %>%
  select(GRADE, NI, LIGRADE, SN, SALAIRE_MENSUEL, ANNEE, SALAIRE_ANNUEL, MIN, DPC) %>%
  collect()

vect_annee_siasp = c("2010", "2011", "2012", "2013", "2014","2015", "2016","2017", "2018", "2019", "2020", "2021")

wage <- open_dataset("X:/HAB-EFP-STAGES/Données/SIASP/siasp_2010.parquet", format="parquet") %>%
  print("wage 2010") %>%
  filter(EMP_CHAMP == "FPE") %>%
  select(GRADE, S_NET, nir, LIB_EMPLOI_1, NB_HEURES_REMUN, POSTE_PRINC_AN, N, EMP_MIN) %>%
  filter(nir %in% NIR_1980) %>%
  filter(NB_HEURES_REMUN > 280) %>%
  mutate(ANNEE = "2010") %>%
  left_join(conversion,
            by = join_by("ANNEE" == "PERIODE")) %>%
  left_join(smic, by = "ANNEE") %>%
  to_duckdb()%>%
  group_by(nir) %>%
  mutate(S_NET = (sum(S_NET)*100)/COEFFICIENT) %>%
  ungroup() %>%
  filter(POSTE_PRINC_AN == "P") %>%
  filter(N == "1") %>%
  filter(S_NET > 3*SMIC_MENSUEL_euros_constants) %>%
  mutate(SALAIRE_ANNUEL = S_NET*(1820/pmin(NB_HEURES_REMUN, 1820))) %>%
  mutate(SALAIRE_MENSUEL = SALAIRE_ANNUEL/12) %>%
  collect()


for (ano in vect_annee_siasp[2:3]) {
  print(ano)
  wage_techn_80_ad <- open_dataset(
    paste0("X:/HAB-EFP-STAGES/Données/SIASP/siasp_",
           ano,
           ".parquet"),
    format="parquet") %>%
    
    filter(EMP_CHAMP == "FPE") %>%
    select(GRADE, S_NET, nir, LIB_EMPLOI_1, NB_HEURES_REMUN, POSTE_PRINC_AN, N, EMP_MIN) %>%
    filter(nir %in% NIR_1980) %>%
    filter(NB_HEURES_REMUN > 280) %>%
    mutate(ANNEE = "2010") %>%
    left_join(conversion,
              by = join_by("ANNEE" == "PERIODE")) %>%
    left_join(smic, by = "ANNEE") %>%
    to_duckdb() %>%
    group_by(nir) %>%
    mutate(S_NET = (sum(S_NET)*100)/COEFFICIENT) %>%
    ungroup() %>%
    filter(POSTE_PRINC_AN == "P") %>%
    filter(N == "1") %>%
    filter(S_NET > 3*SMIC_MENSUEL_euros_constants) %>%
    mutate(SALAIRE_ANNUEL = S_NET*(1820/pmin(NB_HEURES_REMUN, 1820))) %>%
    mutate(SALAIRE_MENSUEL = SALAIRE_ANNUEL/12) %>%
    mutate(ANNEE = ano) %>%
    collect()
  
  wage <- rbind(wage, wage_techn_80_ad) %>% distinct(.keep_all = TRUE) %>%
    collect()
}

for (ano in vect_annee_siasp[4:12]) {
  print(ano)
  wage_techn_80_ad <- open_dataset(
    paste0("X:/HAB-EFP-STAGES/Données/SIASP/siasp_",
           ano,
           ".parquet"),
    format="parquet") %>%
    filter(EMP_CHAMP == "FPE") %>%
    select(GRADE, S_NET, nir, LIB_EMPLOI_1, NB_HEURES_REMUN, poste_princ_an, N, EMP_MIN) %>%
    filter(nir %in% NIR_1980) %>%
    filter(NB_HEURES_REMUN > 280) %>%
    mutate(ANNEE = "2010") %>%
    left_join(conversion,
              by = join_by("ANNEE" == "PERIODE")) %>%
    left_join(smic, by = "ANNEE") %>%
    to_duckdb() %>%
    group_by(nir) %>%
    mutate(S_NET = (sum(S_NET)*100)/COEFFICIENT) %>%
    ungroup() %>%
    filter(poste_princ_an == "P") %>%
    filter(N == "1") %>%
    filter(S_NET > 3*SMIC_MENSUEL_euros_constants) %>%
    mutate(SALAIRE_ANNUEL = S_NET*(1820/pmin(NB_HEURES_REMUN, 1820))) %>%
    mutate(SALAIRE_MENSUEL = SALAIRE_ANNUEL/12) %>%
    rename("POSTE_PRINC_AN" = poste_princ_an) %>%
    mutate(ANNEE = ano) %>%
    collect()
  
  wage <- rbind(wage, wage_techn_80_ad) %>% distinct(.keep_all = TRUE) %>%
    collect()
}

salaire_new <- salaire_ena %>% 
  select(NI, ANNEE, GRADE, LIGRADE, SALAIRE_MENSUEL, SALAIRE_ANNUEL, DPC, SN, MIN) %>%
  rename("NIR"=NI, "Libelle_corps"=LIGRADE,, "DPC_NBH"=DPC, "Salaire net"=SN)
wage_new <- wage %>% 
  select(nir, ANNEE, GRADE, LIB_EMPLOI_1, SALAIRE_MENSUEL, SALAIRE_ANNUEL, NB_HEURES_REMUN, S_NET, EMP_MIN) %>%
  rename("NIR"=nir, "Libelle_corps"=LIB_EMPLOI_1, "DPC_NBH"=NB_HEURES_REMUN, "Salaire net"=S_NET, "MIN" = EMP_MIN) 

ena_annee_ter <- rbind(salaire_new, wage_new) %>% 
  distinct(.keep_all = TRUE) %>%
  mutate(ANNEE = as.numeric(ANNEE)) %>%
  collect()

ena_interpolation_80 <- ena_annee_ter %>%
  mutate(SEXE = str_sub(NIR, 1, 1)) %>%
  filter(SEXE %in% c(1,2)) %>%
  group_by(ANNEE, SEXE) %>%
  summarise(Effectif = n()) %>%
  collect()
result_0 <- interpolation_lineaire_0(ena_interpolation_80)
result_1 <- interpolation_lineaire_1(ena_interpolation_80)
result_2 <- interpolation_lineaire_2(ena_interpolation_80) 
effectif_ena_80_interpolation <- rbind(result_0, result_1, result_2)

annees <- tibble(ANNEE = seq(1980, 2019, by = 1))
ena_annee <- annees %>%
  left_join(ena_annee_ter, by = "ANNEE") %>%
  mutate(ANNEE = dense_rank(ANNEE)) %>%
  collect()

effectif_ena_80_interpolation <- annees %>%
  left_join(effectif_ena_80_interpolation, by = "ANNEE")  %>%
  mutate(ANNEE = dense_rank(ANNEE)) %>%
  collect()




ena_80_interpolation <- ena_annee %>%
  mutate(SEXE = str_sub(NIR, 1, 1)) %>%
  filter(SEXE %in% c(1,2)) %>%
  group_by(ANNEE, SEXE) %>%
  summarise(Effectif = n()) %>%
  collect()
ena_80_interpolation <- rbind(ena_80_interpolation, effectif_ena_80_interpolation)



## ------ HEREDITE : années 1982, 1983, 1984, 1985, 1986, 1988, 1989 -----------------------

NIR_annee <- list()
decennie80 <- c("1982", "1983", "1984", "1985", "1986", "1988", "1989")
ena_annee_bis <- list()
i <- 0

for (annee in decennie80) {
  
  print(annee)
  ena <- open_dataset(
    paste0("X:/HAB-EFP-STAGES/Données/FGE/fge_",
           annee,
           ".parquet"),
    format="parquet") %>%
    select(GRADE, SN, NI, DPC, ENREG) %>%
    filter(GRADE == "12907511") %>%
    filter(ENREG %in% c(0, 2)) %>%
    filter(DPC > 2)
  
  if (annee == "1982") {
    print("here insee")
    ena <- ena %>%
      filter(!(NI %in% NIR_1980)) %>%
      mutate(ANNEE = annee) %>%
      collect()
  } else {
    print("enlever insee")
    print(decennie80[i])
    ena <- ena %>%
      filter(!(NI %in% NIR_annee[[decennie80[i]]])) %>%
      mutate(ANNEE = annee) %>%
      collect()
  }
  
  
  NIR_annee[[annee]] <- ena$NI
  NIR_annee[[annee]] <- unique(NIR_annee[[annee]])
  
  i <- i + 1
  ena_80s <- rbind(ena_80s, ena) %>% distinct(.keep_all = TRUE) %>% collect()
  
  
  #Suivi des carrières et salaires
  
  salaire <- open_dataset(paste0("X:/HAB-EFP-STAGES/Données/FGE/fge_", annee,".parquet"), format="parquet") %>%
    print("suivi salaires") %>%
    select(GRADE, SN, NI, DPC, ENREG, CAN, MIN) %>%
    filter(NI %in% NIR_annee[[annee]]) %>%
    mutate(DPC = pmin(as.numeric(DPC), 24),
           SN = as.numeric(SN)) %>%
    filter(DPC > 3) %>%
    filter(ENREG %in% c(0,2)) %>%
    filter(CAN =="01") %>%
    mutate(SALAIRE_ANNUEL = SN * (24/DPC)*0.15245) %>%
    mutate(SALAIRE_MENSUEL = (SALAIRE_ANNUEL / 12)) %>%
    mutate(ANNEE = as.character(annee)) %>%
    left_join(conversion,
              by = join_by("ANNEE" == "PERIODE")) %>%
    mutate(SALAIRE_MENSUEL = (SALAIRE_MENSUEL*100)/COEFFICIENT) %>%
    ungroup() %>%
    left_join(smic, by = "ANNEE") %>%
    filter(SN*0.15245 > 3*SMIC_MENSUEL_euros_constants) %>% 
    select(GRADE, NI, ANNEE, SN, SALAIRE_MENSUEL, SALAIRE_ANNUEL, MIN, DPC) %>%
    collect()
  
  for (an in setdiff(seq(as.numeric(annee)+1, 2009, by=1), c("1987","1993", "1994", "1995"))) {
    print(an)
    salaire_ena_ad <- open_dataset(
      paste0("X:/HAB-EFP-STAGES/Données/FGE/fge_",
             an,
             ".parquet"),
      format="parquet") %>%
      select(GRADE, SN, NI, DPC, ENREG, CAN, MIN) %>%
      filter(NI %in% NIR_annee[[annee]]) %>%
      filter(ENREG %in% c(0, 2)) %>%
      mutate(DPC = pmin(as.numeric(DPC), 24),
             SN = as.numeric(SN)) %>%
      filter(DPC > 3) %>%
      filter(CAN =="01") %>%
      mutate(ANNEE = an) %>%
      mutate(SALAIRE_ANNUEL_1 = if_else(as.integer(an) < 2001, SN*0.15245, SN)) %>%
      mutate(ANNEE = as.character(ANNEE)) %>%
      left_join(conversion,
                by = join_by("ANNEE" == "PERIODE")) %>%
      mutate(SALAIRE_ANNUEL_1 = (SALAIRE_ANNUEL_1*100)/COEFFICIENT) %>%
      ungroup() %>%
      left_join(smic, by = "ANNEE") %>%
      filter(SALAIRE_ANNUEL_1 > 3*SMIC_MENSUEL_euros_constants) %>%
      mutate(SALAIRE_ANNUEL = SALAIRE_ANNUEL_1*(24/DPC)) %>%
      mutate(SALAIRE_MENSUEL = SALAIRE_ANNUEL/12) %>%
      mutate(ANNEE = as.character(ANNEE)) %>%
      select(GRADE, NI, ANNEE, SN, SALAIRE_MENSUEL, SALAIRE_ANNUEL, MIN, DPC) %>%
      collect()
    
    salaire <- rbind(salaire, salaire_ena_ad) %>% distinct(.keep_all = TRUE) %>%
      collect()
  }
  
  conversion <- read.csv2("Z:/WINDOWS/conversion_1978_2022.csv", colClasses="character")
  conversion <- conversion %>%
    mutate(COEFFICIENT = str_replace(COEFFICIENT, ",", ".")) %>%
    mutate(COEFFICIENT = as.numeric(COEFFICIENT)) %>%
    collect()
  smic_post_2000 <- read.csv2("Z:/WINDOWS/smic35.csv") %>%
    mutate(ANNEE12 = as.character(ANNEE12)) %>%
    left_join(conversion, by = join_by("ANNEE12" == "PERIODE")) %>%
    mutate(SMIC_MENSUEL_euros_constants = (as.numeric(SMICNM)*100)/COEFFICIENT) %>%
    select(ANNEE12, SMICNM, SMIC_MENSUEL_euros_constants) %>%
    rename("ANNEE" = ANNEE12) %>%
    collect()
  smic_ante_2000 <- read.csv2("Z:/WINDOWS/smic39.csv") %>%
    mutate(ANNEE13 = as.character(ANNEE13)) %>%
    filter(ANNEE13 %in% seq(1978, 1999)) %>%
    left_join(conversion, by = join_by("ANNEE13" == "PERIODE")) %>%
    mutate(SMIC_MENSUEL_euros_constants = (as.numeric(SMICNM)*100)/COEFFICIENT) %>%
    select(ANNEE13, SMICNM, SMIC_MENSUEL_euros_constants) %>%
    rename("ANNEE" = ANNEE13) %>%
    collect()
  smic <- rbind(smic_ante_2000, smic_post_2000) %>% distinct(.keep_all = TRUE) %>% collect()
  
  salaire_ena <- salaire %>%
    left_join(join_fge_ane,
              by = c("GRADE", "ANNEE")) %>%
    select(GRADE, NI, LIGRADE, SN, SALAIRE_MENSUEL, ANNEE, SALAIRE_ANNUEL, MIN, DPC) %>%
    collect()
  
  
  vect_annee_siasp = c("2010", "2011", "2012", "2013", "2014","2015", "2016","2017", "2018", "2019", "2020", "2021")
  
  wage <- open_dataset("X:/HAB-EFP-STAGES/Données/SIASP/siasp_2010.parquet", format="parquet") %>%
    print("wage 2010") %>%
    filter(EMP_CHAMP == "FPE") %>%
    select(GRADE, S_NET, nir, LIB_EMPLOI_1, NB_HEURES_REMUN, POSTE_PRINC_AN, N, EMP_MIN) %>%
    filter(nir %in% NIR_annee[[annee]]) %>%
    filter(NB_HEURES_REMUN > 280) %>%
    mutate(ANNEE = "2010") %>%
    left_join(conversion,
              by = join_by("ANNEE" == "PERIODE")) %>%
    left_join(smic, by = "ANNEE") %>%
    to_duckdb()%>%
    group_by(nir) %>%
    mutate(S_NET = (sum(S_NET)*100)/COEFFICIENT) %>%
    ungroup() %>%
    filter(POSTE_PRINC_AN == "P") %>%
    filter(N == "1") %>%
    filter(S_NET > 3*SMIC_MENSUEL_euros_constants) %>%
    mutate(SALAIRE_ANNUEL = S_NET*(1820/pmin(NB_HEURES_REMUN, 1820))) %>%
    mutate(SALAIRE_MENSUEL = SALAIRE_ANNUEL/12) %>%
    mutate(ANNEE = "2010") %>%
    collect()
  
  for (ano in vect_annee_siasp[2:3]) {
    print(ano)
    wage_ena_80_ad <- open_dataset(
      paste0("X:/HAB-EFP-STAGES/Données/SIASP/siasp_",
             ano,
             ".parquet"),
      format="parquet") %>%
      
      filter(EMP_CHAMP == "FPE") %>%
      select(GRADE, S_NET, nir, LIB_EMPLOI_1, NB_HEURES_REMUN, POSTE_PRINC_AN, N, EMP_MIN) %>%
      filter(nir %in% NIR_annee[[annee]]) %>%   
      filter(NB_HEURES_REMUN > 280) %>%
      mutate(ANNEE = "2010") %>%
      left_join(conversion,
                by = join_by("ANNEE" == "PERIODE")) %>%
      left_join(smic, by = "ANNEE") %>%
      to_duckdb() %>%
      group_by(nir) %>%
      mutate(S_NET = (sum(S_NET)*100)/COEFFICIENT) %>%
      ungroup() %>%
      filter(POSTE_PRINC_AN == "P") %>%
      filter(N == "1") %>%
      filter(S_NET > 3*SMIC_MENSUEL_euros_constants) %>%
      mutate(SALAIRE_ANNUEL = S_NET*(1820/pmin(NB_HEURES_REMUN, 1820))) %>%
      mutate(SALAIRE_MENSUEL = SALAIRE_ANNUEL/12) %>%
      mutate(ANNEE = ano) %>%
      collect()
    wage <- rbind(wage, wage_ena_80_ad) %>% distinct(.keep_all = TRUE) %>%
      collect()
  }
  
  for (ano in vect_annee_siasp[4:12]) {
    print(ano)
    wage_ena_80_ad <- open_dataset(
      paste0("X:/HAB-EFP-STAGES/Données/SIASP/siasp_",
             ano,
             ".parquet"),
      format="parquet") %>%
      filter(EMP_CHAMP == "FPE") %>%
      select(GRADE, S_NET, nir, LIB_EMPLOI_1, NB_HEURES_REMUN, poste_princ_an, N, EMP_MIN) %>%
      filter(nir %in% NIR_annee[[annee]]) %>%
      filter(NB_HEURES_REMUN > 280) %>%
      mutate(ANNEE = "2010") %>%
      left_join(conversion,
                by = join_by("ANNEE" == "PERIODE")) %>%
      left_join(smic, by = "ANNEE") %>%
      to_duckdb() %>%
      group_by(nir) %>%
      mutate(S_NET = (sum(S_NET)*100)/COEFFICIENT) %>%
      ungroup() %>%
      filter(poste_princ_an == "P") %>%
      filter(N == "1") %>%
      filter(S_NET > 3*SMIC_MENSUEL_euros_constants) %>%
      mutate(SALAIRE_ANNUEL = S_NET*(1820/pmin(NB_HEURES_REMUN, 1820))) %>%
      mutate(SALAIRE_MENSUEL = SALAIRE_ANNUEL/12) %>%
      rename("POSTE_PRINC_AN" = poste_princ_an) %>%
      mutate(ANNEE = ano) %>%
      collect()
    wage <- rbind(wage, wage_ena_80_ad) %>% distinct(.keep_all = TRUE) %>%
      collect()
  }
  
  
  salaire_new <- salaire_ena %>% 
    select(NI, ANNEE, GRADE, LIGRADE, SALAIRE_MENSUEL, SALAIRE_ANNUEL, DPC, SN, MIN) %>%
    rename("NIR"=NI, "Libelle_corps"=LIGRADE,, "DPC_NBH"=DPC, "Salaire net"=SN)
  wage_new <- wage %>% 
    select(nir, ANNEE, GRADE, LIB_EMPLOI_1, SALAIRE_MENSUEL, SALAIRE_ANNUEL, NB_HEURES_REMUN, S_NET, EMP_MIN) %>%
    rename("NIR"=nir, "Libelle_corps"=LIB_EMPLOI_1, "DPC_NBH"=NB_HEURES_REMUN, "Salaire net"=S_NET, "MIN"=EMP_MIN) 
  
  print("hey")
  ena_annee_quat <- rbind(salaire_new, wage_new) %>% 
    distinct(.keep_all = TRUE) %>%
    mutate(ANNEE = as.integer(ANNEE)) %>%
    collect()
  
  ena_interpolation <- ena_annee_quat %>%
    mutate(SEXE = str_sub(NIR, 1, 1)) %>%
    filter(SEXE %in% c(1,2)) %>%
    group_by(ANNEE, SEXE) %>%
    summarise(Effectif = n_distinct(NIR)) %>%
    collect()
  
  if (annee < 1988) {
    result_1 <- interpolation_lineaire_1(ena_interpolation)
    result_2 <- interpolation_lineaire_2(ena_interpolation) 
    
    effectif_interpolation <- rbind(result_1, result_2)
  } else {
    effectif_interpolation <- interpolation_lineaire_2(ena_interpolation)
  }
    
  annees <- tibble(ANNEE = seq(annee, 2019, by = 1))
  ena_annee_bis <- annees %>%
    left_join(ena_annee_quat, by = "ANNEE") %>%
    mutate(ANNEE = dense_rank(ANNEE)) %>%
    collect()
  
  effectif_interpolation <- annees %>%
    left_join(effectif_interpolation, by = "ANNEE")  %>%
    mutate(ANNEE = dense_rank(ANNEE)) %>%
    collect()
  
  effectif_ena_80_interpolation <- rbind(effectif_ena_80_interpolation, effectif_interpolation)
  ena_annee <- rbind(ena_annee, ena_annee_bis) %>% collect()
  ena_annee_ter <- rbind(ena_annee_ter, ena_annee_quat) %>% collect()
  
}


## --------- Etape 2 : STATISTIQUES DESCRIPTIVES ------------------------------------

effectif_ena_80s <- effectif_ena_80_interpolation %>%
  filter(!(is.na(SEXE))) %>%
  group_by(ANNEE, SEXE) %>%
  summarize(Effectif = sum(Effectif, na.rm = TRUE))
effectif_ena_80 <- ena_annee %>%
  mutate(SEXE = str_sub(NIR, 1, 1)) %>%
  filter(SEXE %in% c(1,2)) %>%
  group_by(ANNEE, SEXE) %>%
  summarise(Effectif=n()) %>%
  mutate(ANNEE = as.integer(ANNEE)) %>%
  collect()

#Effectifs finaux
effectif_ena_80s <- rbind(effectif_ena_80s, effectif_ena_80) %>%
  group_by(ANNEE, SEXE) %>%
  summarise(Effectif = sum(Effectif, na.rm=TRUE)) %>%
  mutate(Effectif = round2(Effectif, 0)) %>%
  ungroup() %>%
  group_by(SEXE) %>%
  mutate(Base100 = Effectif / Effectif[ANNEE == 1] * 100) %>%
  filter(ANNEE %in% c(1:30)) %>%
  collect()
effectif_ena_80s$SEXE <- factor(effectif_ena_80s$SEXE, levels = c(1, 2), labels = c("Homme", "Femme")) 

write.csv(effectif_ena_80s, file = "X:/HAB-EFP-STAGES/Résultats/Effectifs/effectifs_ena_80s.csv", row.names = FALSE)


proportion_ena_80s <- effectif_ena_80s %>%
  group_by(ANNEE) %>%
  summarize(Total = sum(Effectif),
            Femmes = sum(Effectif[SEXE == 'Femme']),
            Proportion_Femmes = Femmes / Total) %>%
  ungroup() %>%
  collect()



## ------------------------------------
#Correction des effectifs par interpolation
interpolation_lineaire_0 <- function(data) {
  annees_interpolation <- 1981
  interpolation <- function(annee_debut, annee_fin, valeur_debut, valeur_fin, years) {
    slope <- (valeur_fin - valeur_debut) / (annee_fin - annee_debut)
    intercept <- valeur_debut - slope * annee_debut
    return(slope * years + intercept)
  }
  interpolated_data <- data %>%
    group_by(SEXE) %>%
    do({
      group_data <- .
      valeur_debut <- group_data$Effectif[group_data$ANNEE == 1980]
      valeur_fin <- group_data$Effectif[group_data$ANNEE == 1982]
      interpolated_values <- interpolation(1980, 1982, valeur_debut, valeur_fin, annees_interpolation)
      data.frame(ANNEE = annees_interpolation, SEXE = group_data$SEXE[1], Effectif = interpolated_values)
    })
  return(interpolated_data)
}

interpolation_lineaire_1 <- function(data) {
  annees_interpolation <- 1987
  interpolation <- function(annee_debut, annee_fin, valeur_debut, valeur_fin, years) {
    slope <- (valeur_fin - valeur_debut) / (annee_fin - annee_debut)
    intercept <- valeur_debut - slope * annee_debut
    return(slope * years + intercept)
  }
  interpolated_data <- data %>%
    group_by(SEXE) %>%
    do({
      group_data <- .
      valeur_debut <- group_data$Effectif[group_data$ANNEE == 1986]
      valeur_fin <- group_data$Effectif[group_data$ANNEE == 1988]
      interpolated_values <- interpolation(1986, 1988, valeur_debut, valeur_fin, annees_interpolation)
      data.frame(ANNEE = annees_interpolation, SEXE = group_data$SEXE[1], Effectif = interpolated_values)
    })
  return(interpolated_data)
}

interpolation_lineaire_2 <- function(data) {
  annees_interpolation <- 1993:1995
  interpolation <- function(annee_debut, annee_fin, valeur_debut, valeur_fin, years) {
    slope <- (valeur_fin - valeur_debut) / (annee_fin - annee_debut)
    intercept <- valeur_debut - slope * annee_debut
    return(slope * years + intercept)
  }
  interpolated_data <- data %>%
    group_by(SEXE) %>%
    do({
      group_data <- .
      valeur_debut <- group_data$Effectif[group_data$ANNEE == 1992]
      valeur_fin <- group_data$Effectif[group_data$ANNEE == 1996]
      interpolated_values <- interpolation(1992, 1996, valeur_debut, valeur_fin, annees_interpolation)
      data.frame(ANNEE = annees_interpolation, SEXE = group_data$SEXE[1], Effectif = interpolated_values)
    })
  return(interpolated_data)
}

#Fonction permettant d'arrondir correctement les effectifs
round2 = function(x, digits) {
  posneg = sign(x)
  z = abs(x)*10^digits
  z = z + 0.5 + sqrt(.Machine$double.eps)
  z = trunc(z)
  z = z/10^digits
  z*posneg
}


##--------------------------------------------------------------------------------

stat_80s <- ena_annee %>%
  mutate(SEXE = str_sub(NIR, 1, 1)) %>%
  filter(SEXE %in% c(1, 2)) %>%
  group_by(SEXE, ANNEE) %>%
  summarise(Effectif = n(), 
            SALAIRE_MENSUEL_MOYEN = round(mean(SALAIRE_MENSUEL, na.rm=TRUE)),
            SALAIRE_MENSUEL_MEDIAN= round(median(SALAIRE_MENSUEL, na.rm=TRUE)),
            d1 = round2(quantile(SALAIRE_MENSUEL, 0.1, na.rm=TRUE), 0),
            q1 = quantile(SALAIRE_MENSUEL, 0.25, na.rm = TRUE),
            q3 = quantile(SALAIRE_MENSUEL, 0.75, na.rm=TRUE),
            d9 = round2(quantile(SALAIRE_MENSUEL, 0.9, na.rm=TRUE), 0)) %>%
  collect()

stat_80s$SEXE <- factor(stat_80s$SEXE, levels = c(1, 2), labels = c("Hommes", "Femmes")) 
write.csv(stat_80s, file = "X:/HAB-EFP-STAGES/Résultats/Salaires/salaires_80s.csv", row.names = FALSE)


## ------- EXTRACTION DES FICHIERS FINAUX ------------------------------------
install.packages("openxlsx")
library(openxlsx)

ena_80s_excel <- stat_80s %>%
  select(SEXE, ANNEE, SALAIRE_MENSUEL_MOYEN, SALAIRE_MENSUEL_MEDIAN) %>%
  filter(ANNEE %in% c(1:30)) %>%
  collect()
write.xlsx(ena_80s_excel, file = "X:/HAB-EFP-STAGES/Résultats/Fichiers excel/salaires_ena_80s.xlsx")

              
write.xlsx(effectif_ena_80s, file = "X:/HAB-EFP-STAGES/Résultats/Fichiers excel/effectifs_ena_80s.xlsx")

decile_ena_80s <- stat_80s %>%
  mutate(Ecart_interdecile = d9 / d1) %>%
  select(ANNEE, SEXE, Ecart_interdecile, d1, d9) %>%
  collect()
write.xlsx(decile_ena_80s, file = "X:/HAB-EFP-STAGES/Résultats/Fichiers excel/decile_ena_80s.xlsx")



## ------- Etape 3 : VISUALISATION -------------------------------

#Effectifs des énarques 1980s
ggplot(effectif_ena_80s, aes(x = ANNEE, y = Effectif, fill = SEXE)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(title = "Effectifs des hauts fonctionnaires selon le genre au cours de la carrière",
       subtitle = "Cohorte des élèves énarques des années 1980", 
       x = "Année de carrière",
       y = "Effectif",
       fill = "Sexe") +
  scale_x_continuous(
    breaks = seq(1, 30, by = 1), # Définit les intervalles des abscisses
    limits = c(0, 31),           # Définit les limites des abscisses
    labels = seq(1, 30, by = 1) # Définit les étiquettes personnalisées
  ) +
  geom_text(aes(label = Effectif), position = position_stack(vjust = 0.8), size = 2.5) +
  theme_minimal()



ggplot(effectif_ena_80s, aes(x = ANNEE, y = Base100, fill = SEXE)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(title = "Evolution des effectifs selon le genre au cours de la carrière, base 100",
       subtitle = "Cohorte des commissaires élèves des années 1980", 
       x = "Année de carrière",
       y = "Effectif",
       fill = "Sexe") +
  scale_x_continuous(
    breaks = seq(1, 30, by = 1), # Définit les intervalles des abscisses
    limits = c(0, 31),           # Définit les limites des abscisses
    labels = seq(1, 30, by = 1) # Définit les étiquettes personnalisées
  ) +
  theme_minimal()

ggplot(effectif_ena_80s, aes(x = ANNEE, y = Effectif, fill=SEXE)) +
  geom_bar(stat = "identity", position = "fill") +
  labs(title = "Evolution de la proportion de femmes parmi les hauts fonctionnaires",
       subtitle = "Cohorte des commissaires élèves des années 1980", 
       x = "Année de carrière",
       y = "Effectif",
       fill = "Sexe") +
  scale_x_continuous(
    breaks = seq(1, 30, by = 1), # Définit les intervalles des abscisses
    limits = c(0, 31),           # Définit les limites des abscisses
    labels = seq(1, 30, by = 1) # Définit les étiquettes personnalisées
  ) +
  theme_minimal()


# Salaires moyens et quantiles des énarques des années 1980s
ggplot(stat_80s) +
  aes(x = ANNEE, y = SALAIRE_MENSUEL_MOYEN, colour = SEXE) +
  geom_line(na.rm = FALSE) +
  geom_point(aes(size = Effectif)) +
  geom_errorbar(aes(ymin=q1, ymax=q3), width=.1) +
  scale_x_continuous(
    breaks = seq(1, 30, by = 1), # Définit les intervalles des abscisses
    limits = c(0, 31),           # Définit les limites des abscisses
    labels = seq(1, 30, by = 1) # Définit les étiquettes personnalisées
  ) +
  scale_color_hue(direction = 1) +
  labs(title = "Evolution du salaire mensuel moyen, du 1er et du 3e quantile selon le genre au cours de la carrière", 
       subtitle = "Cas des élèves énarques des années 1980",
       x = "Année de carrière", 
       y = "Salaire mensuel moyen",
       colour = "Sexe") +
  geom_text(aes(label = SALAIRE_MENSUEL_MOYEN), position = position_stack(vjust = 0.5), size = 3) +
  theme_minimal()


# Salaires médians et déciles des énarques des années 1980s
ggplot(stat_80s) +
  aes(x = ANNEE, y = SALAIRE_MENSUEL_MEDIAN, colour = SEXE) +
  geom_line(na.rm = FALSE) +
  geom_point(aes(size = Effectif)) +
  geom_errorbar(aes(ymin=d1, ymax=d9), width=.1) +
  scale_x_continuous(
    breaks = seq(1, 30, by = 1), # Définit les intervalles des abscisses
    limits = c(0, 31),           # Définit les limites des abscisses
    labels = seq(1, 30, by = 1) # Définit les étiquettes personnalisées
  ) +
  scale_color_hue(direction = 1) +
  labs(title = "Evolution du salaire mensuel médian du 1er décile et du 9e décile selon le genre au cours de la carrière", 
       subtitle = "Cas des élèves énarques des années 1980",
       x = "Année de carrière", 
       y = "Salaire mensuel médian",
       colour = "Sexe") +
  geom_text(aes(label = SALAIRE_MENSUEL_MEDIAN), position = position_stack(vjust = 0.5), size = 3) +
  theme_minimal()





## ------- Etape 4 : DIFFERENCES SALARIALES -----------------------------------

ena_diff_sal <- ena_annee %>%
  filter(ANNEE %in% c(1:20)) %>%
  group_by(NIR) %>%
  summarise(nb_annees_FPE = n_distinct(ANNEE)) %>%
  filter(nb_annees_FPE > 14) %>%
  collect()
NIR_25ans <- c(ena_diff_sal$NIR)

stat_diff <- ena_annee %>%
  filter(NIR %in% NIR_25ans) %>%
  mutate(SEXE = str_sub(NIR, 1, 1)) %>%
  filter(SEXE %in% c(1,2)) %>%
  group_by(ANNEE, SEXE) %>%
  summarise(Effectif = n(), 
            SALAIRE_MENSUEL_MOYEN = mean(SALAIRE_MENSUEL, na.rm=TRUE),
            SALAIRE_MENSUEL_MEDIAN=median(SALAIRE_MENSUEL, na.rm=TRUE),
            d1 = quantile(SALAIRE_MENSUEL, 0.1, na.rm=TRUE),
            q1 = quantile(SALAIRE_MENSUEL, 0.25, na.rm = TRUE),
            q3 = quantile(SALAIRE_MENSUEL, 0.75, na.rm=TRUE),
            d9 = quantile(SALAIRE_MENSUEL, 0.9, na.rm=TRUE)) %>%
  filter(ANNEE %in% c(1:20)) %>%
  collect()


diff_sal <- stat_diff %>%
  group_by(ANNEE, SEXE) %>%
  select(SALAIRE_MENSUEL_MOYEN) %>%
  pivot_wider(names_from = SEXE, values_from = SALAIRE_MENSUEL_MOYEN, names_prefix = "SEXE_") %>%
  collect()

diff_sal <- diff_sal %>%
  mutate(DIFFERENCE_SALAIRE = SEXE_1 - SEXE_2) %>%
  group_by(ANNEE, DIFFERENCE_SALAIRE) %>%
  summarise(diff_salariale_annuelle = round(sum(DIFFERENCE_SALAIRE*12))) %>%
  collect()


ggplot(diff_sal) +
  aes(x=ANNEE, y=diff_salariale_annuelle) + 
  geom_bar(stat = "identity", position="dodge")+
  scale_x_continuous(
    breaks = seq(1, 20, by = 1), # Définit les intervalles des abscisses
    limits = c(0, 21),           # Définit les limites des abscisses
    labels = seq(1, 20, by = 1) # Définit les étiquettes personnalisées
  ) +
  scale_color_hue(direction = 1) +
  labs(title = "Evolution de la différence salariale entre les sexes", 
       subtitle = "Cas des élèves énarques des années 1980 ayant au moins 15 ans de carrière dans la FPE",
       x = "Année de carrière",
       y = "Différence annuelle moyenne de salaire") +
  geom_text(aes(label = diff_salariale_annuelle), position = position_stack(vjust = 1.2), size = 2.8) +
  theme_minimal()

diff_salariale_carriere <- sum(diff_sal$diff_salariale_annuelle) 
print(paste("La différence salariale entre les femmes et les hommes sur 30 ans de carrière est en moyenne de :", diff_salariale_carriere))






