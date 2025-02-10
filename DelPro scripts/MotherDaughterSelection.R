#This script select information about Dairy cows and their mothers.
#It first create a list of candidate mothers and daughters.
#It then collects data on the birth date and culling decision data.
#It then also adds the recent milking event for each individual.

library(odbc)
library(DBI)
library(dplyr)
library(dbplyr)


#Connecting to the database using the R user credentions.
con <- dbConnect(odbc(),
                 Driver = "SQL Server",
                 Server = "gigacow.db.slu.se",
                 Database = "Gigacow"
)
odbcListObjects(con)

#Shows the available tables in the schema.
odbcListObjects(con, catalog="Gigacow", schema="SciDel")

#Select mother candidates
Mother_List <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_Cow")) %>%
  filter(FarmName_Pseudo == "a624fb9a" && Father == "9-8821 Vesty") %>%
  pull(Mother)

#Select daughter candidates
Daughter_List <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_Cow")) %>%
  filter(FarmName_Pseudo == "a624fb9a" & Father == "9-8821 Vesty") %>%
  pull(SE_Number)

#Create Table with daughter data
DF.Cows <- con %>%
  tbl(in_catalog("Gigacow", "SciDel", "Del_Cow")) %>%
  filter(SE_Number %in% Daughter_List) %>%
  select(SE_Number, Mother, BirthDate, CullDecisionDate, CullReason1) %>%
  collect()

# Create DF.Mothers table from Mother_List
DF.Mothers <- con %>%
  tbl(in_catalog("Gigacow", "SciDel", "Del_Cow")) %>%
  filter(SE_Number %in% Mother_List) %>%
  select(SE_Number, BirthDate, CullDecisionDate, CullReason1) %>%
  collect()

# Step 3: Join DF.Mothers to DF.Cows
DF.Cows <- DF.Cows %>%
  left_join(DF.Mothers, by = c("Mother" = "SE_Number"), suffix = c("", "_Mother")) %>%
  rename(
    MotherBirthDate = BirthDate_Mother,
    MotherCullDecisionDate = CullDecisionDate_Mother,
    MotherCullReason = CullReason1_Mother
  )

#Calculate the most recent milking event for daughters.
DF.DaughterMilking <- con %>%
  tbl(in_catalog("Gigacow", "SciDel", "Del_Milk_Robot")) %>%
  filter(SE_Number %in% Daughter_List) %>%
  group_by(SE_Number) %>%
  summarise(LastMilking = max(MilkingStartDateTime, na.rm = TRUE)) %>%
  collect()

#Calculate 
DF.MotherMilking <- con %>%
  tbl(in_catalog("Gigacow", "SciDel", "Del_Milk_Robot")) %>%
  filter(SE_Number %in% Mother_List) %>%
  group_by(SE_Number) %>%
  summarise(MotherLastMilking = max(MilkingStartDateTime, na.rm = TRUE)) %>%
  collect()

DF.Cows <- DF.Cows %>%
  left_join(DF.DaughterMilking, by = "SE_Number")

DF.Cows <- DF.Cows %>%
  left_join(DF.MotherMilking, by = c("Mother" = "SE_Number"))