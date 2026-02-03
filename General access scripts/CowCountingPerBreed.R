#install.packages("odbc")
#install.packages("DBI")
#install.packages("dplyr")
#install.packages("dbplyr")
#install.packages("stringr")
#install.packages("openxlsx")


library(odbc)
library(DBI)
library(dplyr)
library(dbplyr)
library(stringr)
library(openxlsx)

#Connecting to the database using the R user credentions.
con <- dbConnect(odbc(),
                 Driver = "SQL Server",
                 Server = "gigacow.db.slu.se",
                 Database = "Gigacow"
)


#Shows the available tables in the schema.
odbcListObjects(con, catalog="Gigacow", schema="SciDel")

#Step 1 Calculate total breed distribution based on DeLaval data

#Create a list of farms with milking robots
Del_Milk_Robot_List <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_Milk_Robot")) %>%
  distinct(FarmName_Pseudo) %>%  # Keep only unique FarmName_Pseudo values
  pull(FarmName_Pseudo)          # Extract the column as a list

#Count the breed distribution on farms with milking robots
Del_Cow_Counts <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_Cow")) %>% 
  filter(FarmName_Pseudo %in% Del_Milk_Robot_List) %>% # Filter for FarmName_Pseudo in Del_Milk_Robot_List
  group_by(FarmName_Pseudo, BreedName) %>%           # Group by FarmName_Pseudo and BreedName
  summarise(Count_all = n(), .groups = "drop") %>%       # Count instances and drop grouping
  group_by(FarmName_Pseudo) %>%                      # Group by FarmName_Pseudo for percentage calculation
  mutate(Percentage_all = (Count_all / sum(as.numeric(Count_all)) * 100)) %>%# Calculate percentage
  arrange(FarmName_Pseudo, desc(Percentage_all)) %>%     # Sort by FarmName_Pseudo and Percentage in descending order
  collect()                                          # Fetch the result as a local dataframe

#Step 2 Calculate breed distribution for cows that are milked in milking robots

#Create a list of SE_Number values from Del_Milk_Robot
SE_Number_List_milked <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_Milk_Robot")) %>% 
  distinct(SE_Number) %>%        # Select unique SE_Number values
  pull(SE_Number)                # Convert to a list

#Count the number of animals s
Filtered_Del_Cow_Counts <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_Cow")) %>% 
  filter(FarmName_Pseudo %in% Del_Milk_Robot_List) %>% # Filter for FarmName_Pseudo in Del_Milk_Robot_Con
  filter(SE_Number %in% SE_Number_List_milked) %>%          # Filter for rows where SE_Number matches the list
  group_by(FarmName_Pseudo, BreedName) %>%           # Group by FarmName_Pseudo and BreedName
  summarise(Count_milked = n(), .groups = "drop") %>%       # Count instances and drop grouping
  group_by(FarmName_Pseudo) %>%                      # Group by FarmName_Pseudo for percentage calculation
  mutate(Percentage_milked = (Count_milked / sum(as.numeric(Count_milked))) * 100) %>% # Calculate percentage
  arrange(FarmName_Pseudo, desc(Percentage_milked)) %>%     # Sort by FarmName_Pseudo and Percentage in descending order
  collect()                                          # Fetch the result as a local dataframe

#Step 3 Calculate the same data but from Kokontrollen for all animals
DF.Kok_Lineage<- con %>% 
  tbl(in_catalog("Gigacow", "SciKok", "Kok_Lineage")) %>%
  filter(ActiveHerdNumber %in% Del_Milk_Robot_List) %>%
  group_by(ActiveHerdNumber, Mother_Breed, Father_Breed) %>%           # Group by FarmName_Pseudo (ActiveHerdNumber in Kok)
  summarise(Count_all = n(), .groups = "drop") %>%       # Count instances and drop grouping
  group_by(ActiveHerdNumber) %>%                      # Group by FarmName_Pseudo for percentage calculation
  mutate(Percentage_all = (Count_all / sum(as.numeric(Count_all)) * 100)) %>%# Calculate percentage
  arrange(ActiveHerdNumber, desc(Percentage_all)) %>%     # Sort by FarmName_Pseudo and Percentage in descending order
  collect()                                          # Fetch the result as a local dataframe

#Translate data to look like in DelPro but without breed numbers
DF.Kok_Lineage_DelPro <- DF.Kok_Lineage %>%
  mutate(
    # Translate ActiveHerdNumber to FarmName_Pseudo
    FarmName_Pseudo = ActiveHerdNumber,
    
    # Apply logic for BreedName
    BreedName = if_else(
      Mother_Breed == Father_Breed,                  # If Mother_Breed equals Father_Breed
      Mother_Breed,                                  # Use the breed value
      if_else(
        (Mother_Breed == "RB" & Father_Breed == "SRB") | (Mother_Breed == "SRB" & Father_Breed == "RB"),  # If one breed is RB and the other is SRB
        "SRB/RB",                                     # Assign "SRB/RB"
        "Korsning/övriga raser"                       # Otherwise, assign "Korsning/övriga raser"
      )
    )
  ) %>%
  ungroup() %>%
  select(FarmName_Pseudo, BreedName, Count_all, Percentage_all) # Keep only relevant columns

#Remove integers from DelPro
Del_Cow_Counts_cleaned <- Del_Cow_Counts %>%
  mutate(
    BreedName = str_trim(str_remove(BreedName, "^\\d{2} "))
  )

# Join DF.Kok_LineageDelPro with Del_Cow_Counts
BreedDistribution_all <- DF.Kok_Lineage_DelPro %>%
  full_join(
    Del_Cow_Counts_cleaned,
    by = c("FarmName_Pseudo", "BreedName")
  ) %>%
  select(
    FarmName_Pseudo,
    BreedName,
    Count_all_Kok = Count_all.x,
    Count_all_DelPro = Count_all.y,
    Percentage_all_Kok = Percentage_all.x,
    Percentage_all_DelPro = Percentage_all.y
  ) %>%
  arrange(FarmName_Pseudo, BreedName)

#Save data to workbook

# Create a new workbook
BreedDistributionData <- createWorkbook()

# Add first sheet
addWorksheet(BreedDistributionData, "AllAnimals")
writeData(BreedDistributionData, "AllAnimals", BreedDistribution_all)

#Save the workbook
saveWorkbook(BreedDistributionData, "BreedDistributionData.xlsx", overwrite = TRUE)


#Step 4 Calculate breed distribution in Kokontrollen for animals being milked
DF.Kok_Lineage_milked<- con %>% 
  tbl(in_catalog("Gigacow", "SciKok", "Kok_Lineage")) %>%
  filter(ActiveHerdNumber %in% Del_Milk_Robot_List) %>%
  filter(BirthID %in% SE_Number_List_milked) %>%
  group_by(ActiveHerdNumber, Mother_Breed, Father_Breed) %>%           # Group by FarmName_Pseudo (ActiveHerdNumber in Kok)
  summarise(Count_milked = n(), .groups = "drop") %>%       # Count instances and drop grouping
  group_by(ActiveHerdNumber) %>%                      # Group by FarmName_Pseudo for percentage calculation
  mutate(Percentage_milked = (Count_milked / sum(as.numeric(Count_milked)) * 100)) %>%# Calculate percentage
  arrange(ActiveHerdNumber, desc(Percentage_milked)) %>%     # Sort by FarmName_Pseudo and Percentage in descending order
  collect()                                          # Fetch the result as a local dataframe

#Translate data to look like in DelPro but without breed numbers
DF.Kok_Lineage_DelPro_milked <- DF.Kok_Lineage_milked %>%
  mutate(
    # Translate ActiveHerdNumber to FarmName_Pseudo
    FarmName_Pseudo = ActiveHerdNumber,
    
#Assign breed names
    BreedName = if_else(
      Mother_Breed == Father_Breed,                  # If Mother_Breed equals Father_Breed
      Mother_Breed,                                  # Use the breed value
      if_else(
        (Mother_Breed == "RB" & Father_Breed == "SRB") | (Mother_Breed == "SRB" & Father_Breed == "RB"),  # If one breed is RB and the other is SRB
        "SRB/RB",                                     # Assign "SRB/RB"
        "Korsning/övriga raser"                       # Otherwise, assign "Korsning/övriga raser"
      )
    )
  ) %>%
  ungroup() %>%
  select(FarmName_Pseudo, BreedName, Count_milked, Percentage_milked) # Keep only relevant columns

#Remove integers from DelPro
Filtered_Del_Cow_Counts_cleaned <- Filtered_Del_Cow_Counts %>%
  mutate(
    BreedName = str_trim(str_remove(BreedName, "^\\d{2} "))
  )

# Join DF.Kok_LineageDelPro with Del_Cow_Counts
BreedDistribution_milked <- DF.Kok_Lineage_DelPro_milked %>%
  full_join(
    Filtered_Del_Cow_Counts_cleaned,
    by = c("FarmName_Pseudo", "BreedName")
  ) %>%
  select(
    FarmName_Pseudo,
    BreedName,
    Count_milked_Kok = Count_milked.x,
    Count_milked_DelPro = Count_milked.y,
    Percentage_milked_Kok = Percentage_milked.x,
    Percentage_milked_DelPro = Percentage_milked.y
  ) %>%
  arrange(FarmName_Pseudo, BreedName)

#Save data to workbook


# Add first sheet
addWorksheet(BreedDistributionData, "MilkedAnimals")
writeData(BreedDistributionData, "MilkedAnimals", BreedDistribution_milked)

#Save the workbook
saveWorkbook(BreedDistributionData, "BreedDistributionData.xlsx", overwrite = TRUE)



#Count the breed distribution on farms with milking robots
Kok_Cow_raw <- con %>% 
  tbl(in_catalog("Gigacow", "SciKok", "Kok_Lineage")) %>% 
  filter(ActiveHerdNumber == "f454e660") %>%
  filter(BirthID %in% SE_Number_List_milked) %>%
  collect()# Filter for FarmName_Pseudo in Del_Milk_Robot_List

unique(Kok_Cow_raw$Mother_Breed)
