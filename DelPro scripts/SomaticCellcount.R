library(odbc)
library(DBI)
library(dplyr)
library(dbplyr)
suppressMessages(library(janitor))
library(ggplot2)
library(lubridate)


con <- dbConnect(odbc(), 
                 Driver = "SQL Server", 
                 Server = "gigacow.db.slu.se", 
                 Database = "gigacow", 
                 TrustServerCertificate = "yes")

Del_Milk_Robot_Con = con %>% tbl(in_catalog("gigacow", "sciDel", "Del_Milk_Robot"))
DF.Milk_Robot = collect(Del_Milk_Robot_Con)


# Step 1: Filter and count non-NA values in the 'Occ' column, grouped by day
DF.Milk_Robot_clean <- DF.Milk_Robot %>%
  filter(!is.na(Occ)) %>%                              # Remove NA values in 'Occ'
  mutate(Date = as.Date(MilkingStartDateTime)) %>%     # Extract the date from 'MilkingStartDateTime'
  group_by(Date) %>%                                   # Group by date
  summarize(Count = n())                               # Count the number of rows per day

# Step 2: Plot the timeline of values per day
ggplot(DF.Milk_Robot_clean, aes(x = Date, y = Count)) +
  geom_line() +                                        # Create a line plot
  labs(title = "Number of Occurrences per Day", 
       x = "Date", 
       y = "Number of Occurrences") +
  theme_minimal()



# Step 1: Filter DF.MilkRobotDL for non-NA Occ values and get unique BirthIDs
filtered_BirthIDs <- DF.Milk_Robot %>%
  filter(!is.na(Occ)) %>%
  pull(SE_Number) %>%                       # Extract BirthID column
  unique()                                # Get unique BirthIDs

# Step 2: Download the CowMilkSampling rows where BirthID is in filtered_BirthIDs
DF.Samples <- con %>%
  tbl(in_catalog("Gigacow", "kokon", "CowMilkSampling")) %>%
  filter(BirthID %in% filtered_BirthIDs) %>%
  group_by(ActiveHerdNumber) %>%
  collect()

# Step 3: Count the number of rows in DF.Samples
total_rows <- nrow(DF.Samples)
print(paste("Total number of rows in DF.Samples:", total_rows))

# Step 4: Add the number of samples per day based on SamplingDate
DF.Samples_PerDay <- DF.Samples %>%
  mutate(SamplingDate = as.Date(SamplingDate)) %>%   # Ensure SamplingDate is in Date format
  group_by(ActiveHerdNumber, SamplingDate) %>%                         # Group by SamplingDate
  summarize(SampleCount = n())                       # Count the number of samples per day

# View the result
print(DF.Samples_PerDay)

# Group by ActiveHerdNumber and summarise SampleCount, first and last SamplingDate
summary_samples <- DF.Samples_PerDay %>%
  group_by(ActiveHerdNumber) %>%
  summarise(
    Total_SampleCount = sum(SampleCount, na.rm = TRUE),            # Sum of SampleCount
    First_SamplingDate = min(SamplingDate, na.rm = TRUE),           # First date in SamplingDate
    Last_SamplingDate = max(SamplingDate, na.rm = TRUE)             # Last date in SamplingDate
  )



# Ensure MilkingStartDateTime is in POSIXct format for date comparison
DF.Milk_Robot <- DF.Milk_Robot %>%
  mutate(MilkingStartDateTime = ymd_hms(MilkingStartDateTime))  # Convert to POSIXct

# Filter rows based on the criteria
filtered_data <- DF.Milk_Robot %>%
  # Step 1: SE_Number matching BirthID in DF.Samples
  filter(SE_Number %in% DF.Samples$BirthID) %>%
  # Step 2: Date comparison with summary_samples (match FarmName_Pseudo with ActiveHerdNumber)
  inner_join(summary_samples, by = c("FarmName_Pseudo" = "ActiveHerdNumber")) %>%  # Match on FarmName_Pseudo and ActiveHerdNumber
  filter(MilkingStartDateTime >= First_SamplingDate &      # Ensure MilkingStartDateTime is between
           MilkingStartDateTime <= Last_SamplingDate) %>%
  # Step 3: Exclude rows where OCC is NA
  filter(!is.na(Occ))

# Step 4: Summarise the data by FarmName_Pseudo
summary_data <- filtered_data %>%
  group_by(FarmName_Pseudo) %>%
  summarise(
    Total_Rows = n(),                                     # Total number of rows per FarmName_Pseudo
    Unique_SE_Numbers = n_distinct(SE_Number),            # Count of unique SE_Numbers
    First_MilkingStartDate = min(MilkingStartDateTime),   # First date in MilkingStartDateTime
    Last_MilkingStartDate = max(MilkingStartDateTime)     # Last date in MilkingStartDateTime
  )