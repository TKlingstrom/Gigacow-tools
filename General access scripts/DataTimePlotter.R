# This script is made to plot the number of values per table and/or value on a 
# per farm level. It supports two values per plot and farm to be displayed.

#The script uses the Windows credentials of the user to log into the Gigacow test server to retrieve data.
#In order to use this script you need to be running R as a registered Gigacow user on a computer capable of
#accessing the Active Directory of SLU.

#The script uses dbplyr to take dplyr commands, translate them to SQL and send them to the server.
#This way the user can make selective downloads of data by compiling, filtering and joining tables
#prior to downloading the datasets using the collect(function). Pretty much all dplyr commands can be used
#to select, filter and manipulate data which is done in several of the Gigacow-tools scripts.
#Running the script down to the odbcListObjects() function will open a connection to the Gigacow SQL database
#and the "Connections" tab on the right (if you use Rstudio) should show you the databases accessible. You can
#here see all tables available and also load the first 1000 rows of each one to quickly see the content.

#install.packages("odbc")
#install.packages("DBI")
#install.packages("dplyr")
#install.packages("dbplyr")
#install.packages("ggplot2")


library(odbc)
library(DBI)
library(dplyr)
library(dbplyr)
library(ggplot2)


#Connecting to the database using the R user credentions.
con <- dbConnect(odbc(),
                 Driver = "SQL Server",
                 Server = "gigacow.db.slu.se",
                 Database = "Gigacow"
)


#Shows the available tables in the schema.
odbcListObjects(con, catalog="Gigacow", schema="SciDel")

#Create a list of farms
Del_Cow_List <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_Cow")) %>%
  distinct(FarmName_Pseudo) %>%  # Keep only unique FarmName_Pseudo values
  pull(FarmName_Pseudo)          # Extract the column as a list

#Example to create list from farms in milk robot table with at least one OCC measure
Del_Milk_Robot_List <- con %>%
  tbl(in_catalog("Gigacow", "SciDel", "Del_Milk_Robot")) %>%
  filter(!is.na(Occ)) %>%
  distinct(FarmName_Pseudo) %>%  # Keep only unique FarmName_Pseudo values
  pull(FarmName_Pseudo)          # Extract the column as a list

#counts the number of milking events listed
Count_Del_CowMilkYield_Common  <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_CowMilkYield_Common")) %>% 
  filter(FarmName_Pseudo %in% Del_Milk_Robot_List) %>%
  group_by(FarmName_Pseudo, StartDate) %>%
  summarise(MilkRecords = n(), .groups = "drop") %>%
  collect()

#Counts NA values for lactation number.
Count_Na_lac_Del_CowMilkYield_Common  <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_CowMilkYield_Common")) %>% 
  filter(FarmName_Pseudo %in% Del_Milk_Robot_List) %>%
  filter(is.na(LactationNumber)) %>%
  group_by(FarmName_Pseudo, StartDate) %>%
  summarise(NA_Lactation = n(), .groups = "drop") %>%
  collect()

# Merge the two datasets
combined_counts <- Count_Del_CowMilkYield_Common %>%
  full_join(Count_Na_lac_Del_CowMilkYield_Common, by = c("FarmName_Pseudo", "StartDate")) %>%
  mutate(
    StartDate = as.Date(StartDate),
    NA_Lactation = ifelse(is.na(NA_Lactation), 0, NA_Lactation)) # Replace NA with 0 for plotting

#Print individual plots to files
print(paste("Saving files to:", getwd()))

unique_farms <- unique(combined_counts$FarmName_Pseudo)
for (farm in unique_farms) {
  farm_data <- combined_counts %>% filter(FarmName_Pseudo == farm)
  p <- ggplot(farm_data, aes(x = StartDate)) +
    geom_bar(aes(y = MilkRecords), stat = "identity", fill = "skyblue", color = "black", alpha = 0.7, position = "identity") +
    geom_bar(aes(y = NA_Lactation), stat = "identity", fill = "red", color = "skyblue", alpha = 0.7, position = "identity") +
    scale_x_date(
      date_breaks = "1 month",
      date_labels = "%Y-%m-%d",
      expand = c(0, 0)
    ) +
    labs(
      title = paste("Number of Rows Per Day -", farm),
      x = "Date (First of Each Month)",
      y = "Number of Rows",
      fill = "Type"
    ) +
    theme_minimal() +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      panel.background = element_rect(fill = "white", color = NA),  # White panel background
      plot.background = element_rect(fill = "white", color = NA)    # White plot background
    )
  
  # Save each plot with a farm-specific filename
  file_name <- paste0("Plot_", farm, ".png")
  print(paste("Saving:", file_name))  # Debug output
  ggsave(file_name, plot = p, width = 10, height = 6)
}

#Plot with facet instead.
ggplot(combined_counts, aes(x = StartDate)) +
  geom_bar(aes(y = MilkRecords), stat = "identity", fill = "skyblue", color = "black", alpha = 0.7, position = "identity") +
  geom_bar(aes(y = NA_Lactation), stat = "identity", fill = "red", color = "skyblue", alpha = 0.7, position = "identity") +
  scale_x_date(
    date_breaks = "1 month",
    date_labels = "%Y-%m-%d",
    expand = c(0, 0)
  ) +
  labs(
    title = "Number of Rows Per Day by Farm",
    x = "Date (First of Each Month)",
    y = "Number of Rows",
    fill = "Type"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  facet_wrap(~ FarmName_Pseudo, ncol = 1)  # Create separate plots for each FarmName_Pseudo
