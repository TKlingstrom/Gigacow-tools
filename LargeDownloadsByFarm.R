#This R-script is made to connect to the SLU Gigacow database, create a list
#of farms to include in a study and then download data from a number of frames.

#Necessary libraries
library(odbc)
library(DBI)
library(dplyr)
library(dbplyr)
library(data.table)
library(readr)

#Connecting to the database using the user credentials from SLU AD (requires
#SLU issued laptop with Windows).
con <- dbConnect(odbc(),
                 Driver = "SQL Server",
                 Server = "gigacow.db.slu.se",
                 Database = "Gigacow"
)
odbcListObjects(con)

#Shows the available tables in the schema sciDel (alternatives sciKok and sciNav)
odbcListObjects(con, catalog="Gigacow", schema="sciDel")


#Step 1, create a table with inclusion criteria and create a list of selected
#values (in this case FarmName_Pseudo).
farm_days_tbl <- con %>% 
  tbl(in_catalog("Gigacow", "SciDel", "Del_CowMilkYield_Common")) %>%
  mutate(StartDay = as.Date(StartDate)) %>% 
  group_by(FarmName_Pseudo) %>%
  summarise(
    n_days = n_distinct(StartDay),
    n_cows = n_distinct(SE_Number),
    .groups = "drop"
  ) %>%
  filter(n_days >= 365) %>%
  arrange(desc(n_days)) %>%
  collect()

farmlist_dynamic <- farm_days_tbl %>% pull(FarmName_Pseudo)

#Step 2, create export folder and an export function.
#-------------------------
export_dir <- "exports"
if (!dir.exists(export_dir)) dir.create(export_dir)

today_stamp <- format(Sys.Date(), "%Y%m%d")

export_view <- function(viewname, df, export_dir = "exports", date_stamp = today_stamp) {
  nrows <- nrow(df)
  fn <- file.path(export_dir, sprintf("%s_%s_%s.csv", viewname, date_stamp, nrows))
  readr::write_csv(df, fn)
  message("Saved: ", fn)
  invisible(df)
}

#Step 3 Define each view as a function that returns a LAZY tbl filtered by one
#farm, this is to reduce the size per download from each view to prevent
#time outs from the server.
#-------------------------
views <- list(
  Del_CowMilkYield_Common = function(con, farm) {
    con %>%
      tbl(in_catalog("Gigacow", "SciDel", "Del_CowMilkYield_Common")) %>%
      filter(FarmName_Pseudo == farm)
  },
  
  Del_Milk_Robot_subset = function(con, farm) {
    con %>%
      tbl(in_catalog("Gigacow", "SciDel", "Del_Milk_Robot")) %>%
      filter(FarmName_Pseudo == farm) %>%
      select(
        FarmName_Pseudo, SE_Number, GroupName, MilkingUnitName, SessionNumber,
        TotalYield, TotalYieldLF, TotalYieldRF, TotalYieldLR, TotalYieldRR,
        AMD_Udder, KickOffLF, KickOffLR, KickOffRF, KickOffRR,
        IncompleteLF, IncompleteLR, IncompleteRF, IncompleteRR,
        NotMilkedTeatLF, NotMilkedTeatLR, NotMilkedTeatRF, NotMilkedTeatRR
      )
  },
  
  Del_Cow = function(con, farm) {
    con %>%
      tbl(in_catalog("Gigacow", "SciDel", "Del_Cow")) %>%
      filter(FarmName_Pseudo == farm) %>%
      select(FarmName_Pseudo, SE_Number, BirthDate, BreedName,
             CullDecisionDate, CullReason1, CullReason2)
  },
  
  Del_BCS = function(con, farm) {
    con %>%
      tbl(in_catalog("Gigacow", "SciDel", "Del_BCS")) %>%
      filter(FarmName_Pseudo == farm) %>%
      select(FarmName_Pseudo, SE_Number, BCSEventDate, BCSValue)
  },
  
  Del_Insemination = function(con, farm) {
    con %>%
      tbl(in_catalog("Gigacow", "SciDel", "Del_Insemination")) %>%
      filter(FarmName_Pseudo == farm) %>%
      select(FarmName_Pseudo, SE_Number, InseminationDate, Breeder)
  },
  
  Del_PregnancyCheck = function(con, farm) {
    con %>%
      tbl(in_catalog("Gigacow", "SciDel", "Del_PregnancyCheck")) %>%
      filter(FarmName_Pseudo == farm) %>%
      select(FarmName_Pseudo, SE_Number, PregnancyCheckDate, PregnancyCheckResult)
  },
  
  Del_ReproductionStatus = function(con, farm) {
    con %>%
      tbl(in_catalog("Gigacow", "SciDel", "Del_ReproductionStatus")) %>%
      filter(FarmName_Pseudo == farm) %>%
      select(FarmName_Pseudo, SE_Number, RepoductionStatusDate, ReproductionStatusCode)
  },
  
  Del_Feed = function(con, farm) {
    con %>%
      tbl(in_catalog("Gigacow", "SciDel", "Del_Feed")) %>%
      filter(FarmName_Pseudo == farm)
  },
  
  Del_FeedRations = function(con, farm) {
    con %>%
      tbl(in_catalog("Gigacow", "SciDel", "Del_FeedRations")) %>%
      filter(FarmName_Pseudo == farm)
  }
)

#Step 5, a loop for each view that collect data for one farm at a time.
#It then bind the rows and saves a CSV named after the view, date and nr rows.
dfs_to_export <- list()

for (viewname in names(views)) {
  message("\n=== Collecting view: ", viewname, " ===")
  
  per_farm_dfs <- vector("list", length(farmlist_dynamic))
  names(per_farm_dfs) <- farmlist_dynamic
  
  for (farm in farmlist_dynamic) {
    message("  - Farm: ", farm)
    
    # ONE DB call per farm per view:
    # (views[[viewname]] returns lazy tbl; collect triggers the call)
    per_farm_dfs[[farm]] <- views[[viewname]](con, farm) %>%
      collect()
  }
  
  # Concatenate all farms for this view into ONE dataframe
  combined_df <- bind_rows(per_farm_dfs)
  
  # Store in list (one entry per view)
  dfs_to_export[[viewname]] <- combined_df
  
  # Make it show up as a normal dataframe in the Environment tab
  assign(viewname, combined_df, envir = .GlobalEnv)
  
  # Export CSV: viewname_date_nrofrows.csv
  export_view(viewname, combined_df, export_dir = export_dir, date_stamp = today_stamp)
}

cow_farm_summary <- Del_Cow %>%
  group_by(FarmName_Pseudo) %>%
  summarise(
    n_rows = n(),
    n_unique_SE = n_distinct(SE_Number),
    n_duplicates = n_rows - n_unique_SE,
    has_duplicates = n_duplicates > 0,
    .groups = "drop"
  ) %>%
  arrange(desc(n_rows))

cow_farm_summary