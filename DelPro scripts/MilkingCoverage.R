
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
odbcListObjects(con)

#Shows the available tables in the schema.
odbcListObjects(con, catalog="Gigacow", schema="SciDel")

Count_Del_CowMilkYield_Common <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_CowMilkYield_Common")) %>%
  group_by(FarmName_Pseudo, SE_Number) %>%
  summarise(
    FirstDate     = min(StartDate),
    LastDate      = max(StartDate),
    DaysOfMilk    = n_distinct(StartDate),
    TotalRecords  = n(),
    Coverage      = sql("CAST(COUNT(DISTINCT StartDate) AS FLOAT) / (DATEDIFF(day, MIN(StartDate), MAX(StartDate)) + 1)"),
    RecordsPerDay = sql("CAST(COUNT(*) AS FLOAT) / NULLIF(COUNT(DISTINCT StartDate), 0)"),
    .groups = "drop"
  ) %>%
  collect

CountCoreAnimals <- Count_Del_CowMilkYield_Common %>%
  filter(DaysOfMilk > 365 & Coverage > 0.8)


# Prepare data
days_curve <- CountCoreAnimals %>%
  summarise(MaxDays = max(DaysOfMilk)) %>%
  pull(MaxDays) %>%
  { seq(365, ., by = 1) } %>%
  tibble(DayThreshold = .) %>%
  rowwise() %>%
  mutate(
    PctAnimals = mean(CountCoreAnimals$DaysOfMilk >= DayThreshold) * 100
  ) %>%
  ungroup()

# Get N
N_animals_core <- nrow(CountCoreAnimals)

# Plot
ggplot(days_curve, aes(x = DayThreshold, y = PctAnimals)) +
  geom_line(color = "steelblue", size = 1.2) +
  geom_point(color = "steelblue", size = 1) +
  geom_vline(xintercept = 365, linetype = "dashed", color = "red") +
  scale_y_continuous(labels = scales::percent_format(scale = 1)) +
  labs(
    x = "Minimum number of days (coverage > 0.8)",
    y = "Percentage of animals",
    title = "Lactation days, Gigacow core dataset",
    subtitle = paste0("N = ", N_animals_core, " animals")
  ) +
  theme_minimal(base_size = 14)


# Prepare data
days_curve_total <- Count_Del_CowMilkYield_Common %>%
  summarise(MaxDays = max(DaysOfMilk)) %>%
  pull(MaxDays) %>%
  { seq(1, ., by = 1) } %>%
  tibble(DayThreshold = .) %>%
  rowwise() %>%
  mutate(
    PctAnimals = mean(Count_Del_CowMilkYield_Common$DaysOfMilk >= DayThreshold) * 100
  ) %>%
  ungroup()

# Get N
N_animals_total <- nrow(Count_Del_CowMilkYield_Common)

# Plot
ggplot(days_curve_total, aes(x = DayThreshold, y = PctAnimals)) +
  geom_line(color = "steelblue", size = 1.2) +
  geom_point(color = "steelblue", size = 1) +
  geom_vline(xintercept = 365, linetype = "dashed", color = "red") +
  scale_y_continuous(labels = scales::percent_format(scale = 1)) +
  labs(
    x = "Minimum number of days",
    y = "Percentage of animals",
    title = "Lactation days, Gigacow total dataset",
    subtitle = paste0("N = ", N_animals_total, " animals")
  ) +
  theme_minimal(base_size = 14)