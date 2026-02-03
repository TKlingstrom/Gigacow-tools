# Load necessary libraries
library(odbc)
library(DBI)
library(dplyr)
library(dbplyr)
library(purrr)
library(stringr) 

# Before running the script, enter a list of FarmName_Pseudos (run this row)
farm_names_input <- readline(prompt = "Enter FarmName_Pseudo values separated by commas: ")

# Then load the names
farm_names <- strsplit(farm_names_input, ",\\s*")[[1]]


#Run the rest of the script from here:
# Connect to the database
con <- dbConnect(odbc(),
                 Driver = "SQL Server",
                 Server = "gigacow.db.slu.se",
                 Database = "Gigacow"
)

# Step 1: Retrieve all views in the 'sciDel' schema with an SQL query
views_query <- dbGetQuery(con, "
    SELECT table_name
    FROM information_schema.views
    WHERE table_schema = 'sciDel'
")

# Extract view names as a vector
views_list <- views_query$table_name

# Step 2: Retrieve SE_Numbers from 'Del_CowMilkYield_Common' for specified FarmName_Pseudos
CowMilkYieldCommon_Con <- con %>% 
  tbl(in_catalog("Gigacow", "sciDel", "Del_CowMilkYield_Common")) %>%
  filter(
    StartDate >= "2023-04-20" & 
      !is.na(SE_Number) & 
      !is.na(TotalYield) & 
      FarmName_Pseudo %in% farm_names
  ) %>%
  group_by(FarmName_Pseudo) %>%
  slice_sample(n = 2) %>%
  ungroup()

DF.CowMilkYieldCommon <- collect(CowMilkYieldCommon_Con)

# Equalize row numbers for each FarmName_Pseudo
min_count <- DF.CowMilkYieldCommon %>%
  count(FarmName_Pseudo) %>%
  summarise(min_count = min(n)) %>%
  pull(min_count)

DF.CowMilkYieldCommon_balanced <- DF.CowMilkYieldCommon %>%
  group_by(FarmName_Pseudo) %>%
  slice_sample(n = min_count) %>%
  ungroup()

# Extract unique SE_Number values
unique_SE_Numbers <- DF.CowMilkYieldCommon_balanced %>%
  distinct(SE_Number) %>%
  pull(SE_Number)

# Step 3: Retrieve data from each view in 'views_list'
data_list <- map(views_list, ~ {
  view_name <- .x
  tryCatch({
    tbl_data <- tbl(con, in_catalog("Gigacow", "sciDel", view_name))
    if ("SE_Number" %in% colnames(tbl_data)) {
      filtered_data <- tbl_data %>%
        filter(SE_Number %in% unique_SE_Numbers) %>%
        collect()
      if (nrow(filtered_data) > 0) {
        filtered_data
      } else {
        NULL
      }
    } else {
      NULL
    }
  }, error = function(e) {
    message(paste("Skipping view:", view_name, "- SE_Number column missing or error encountered"))
    NULL
  })
}) %>% set_names(views_list)

# Remove NULL entries
data_list <- compact(data_list)

# Define the folder to save CSV files
output_folder <- "exampledata/"  # Replace with desired folder path
if (!dir.exists(output_folder)) {
  dir.create(output_folder, recursive = TRUE)
}

# Save dataframes to CSV files
walk2(data_list, names(data_list), ~ {
  file_path <- file.path(output_folder, paste0(.y, ".csv"))
  write.csv(.x, file = file_path, row.names = FALSE)
  message(paste("Saved:", file_path))
})

# Step 4: Anonymizer function to replace FarmName_Pseudo values
anonymize_data <- function(df, farm_names) {
  
  # Generate anonymized names
  anon_map <- setNames(
    paste0("Farm", sprintf("%04d", seq_along(farm_names))),
    farm_names
  )
  
  # Apply anonymization to FarmName_Pseudo
  df <- df %>%
    mutate(FarmName_Pseudo = if_else(FarmName_Pseudo %in% farm_names,
                                     anon_map[FarmName_Pseudo],
                                     "OtherFar"))
  
  # Apply anonymization to SE_Number by replacing the prefix (FarmName_Pseudo part)
  df <- df %>%
    mutate(SE_Number = if_else(
      str_extract(SE_Number, "^[^\\-]+") %in% farm_names,
      str_replace(SE_Number, "^[^\\-]+", anon_map[str_extract(SE_Number, "^[^\\-]+")]),
      str_replace(SE_Number, "^[^\\-]+", "OtherFar")
    ))
  
  # Check if OriginalFileSource exists, and if so, apply anonymization
  if ("OriginalFileSource" %in% colnames(df)) {
    df <- df %>%
      mutate(OriginalFileSource = if_else(
        str_extract(OriginalFileSource, "^[^\\\\]+") %in% farm_names,
        str_replace(OriginalFileSource, "^[^\\\\]+", anon_map[str_extract(OriginalFileSource, "^[^\\\\]+")]),
        str_replace(OriginalFileSource, "^[^\\\\]+", "OtherFar")
      ))
  }
  
  return(df)
}

# Step 5: Apply anonymizer to each dataframe and save to CSV

# Define the folder to save the anonymized CSV files
output_folder_anonymized <- "anonymized/"  # Replace with your desired folder path

# Create the folder if it doesn't exist
if (!dir.exists(output_folder_anonymized)) {
  dir.create(output_folder_anonymized, recursive = TRUE)
}

data_list_anonymized <- map(data_list, ~ anonymize_data(.x, farm_names))
walk2(data_list_anonymized, names(data_list_anonymized), ~ {
  file_path <- file.path(output_folder_anonymized, paste0(.y, "_anonymized.csv"))
  write.csv(.x, file = file_path, row.names = FALSE)
  message(paste("Anonymized and saved:", file_path))
})
