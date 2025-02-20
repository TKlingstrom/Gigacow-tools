#This script connects to the Gigacow database and compile data on how many
#records there are of cows being milked Robot farms,Parlour/carousel farms
#and how many calving records there are from the farms in Kokontrollen data.
#Please note that if a farm switch from parlour to robot or vice versa it will
# have records of both.

#Connecting to the database using the R user credentions.
con <- dbConnect(odbc(),
                 Driver = "SQL Server",
                 Server = "gigacow.db.slu.se",
                 Database = "Gigacow"
)


#Shows the available tables in the schema.
odbcListObjects(con, catalog="Gigacow", schema="SciDel")


# Create a list of farms with milking robots and count unique SE_Numbers
Robot_Farms <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_Milk_Robot")) %>%
  group_by(FarmName_Pseudo) %>%
  summarise(
    AnimalsMilked = n_distinct(SE_Number)  # Count unique SE_Numbers
  ) %>%
  mutate(FarmType = "AMS") %>%  # Add FarmType column
  collect()  # Extract data into R

#  Create a list of farms with milking parlours or carousels and count unique SE_Numbers
Parlour_Farms <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_Milk_Other")) %>%
  group_by(FarmName_Pseudo) %>%
  summarise(
    AnimalsMilked = n_distinct(SE_Number)  # Count unique SE_Numbers
  ) %>%
  mutate(FarmType = "Parlour") %>%  # Add FarmType column
  collect()  # Extract data into R

# Combine both lists into one dataframe
Del_Farm_List <- bind_rows(Robot_Farms, Parlour_Farms)

# Create a list of farms with data from Kokontrollen and count number of cows which has given birth
Kokontrollen_Farms <- con %>% 
  tbl(in_catalog("Gigacow", "SciKok", "Kok_Calving")) %>%
  group_by(ActiveHerdNumber) %>%
  summarise(
    CalvingCows = n_distinct(BirthID)  # Count unique SE_Numbers
  ) %>%
  filter(CalvingCows > 100) %>%
  mutate(InKokontrollen = "Yes") %>%  # Add FarmType column
  collect()  # Extract data into R

# Perform a full join between Del_Farm_List and Kokontrollen_Farms
Full_Farm_List <- Del_Farm_List %>%
  full_join(Kokontrollen_Farms, by = c("FarmName_Pseudo" = "ActiveHerdNumber"))
