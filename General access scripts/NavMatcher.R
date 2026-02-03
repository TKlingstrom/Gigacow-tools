# This script detects and repairs broken SE_Number IDs in DelPro (Del_Cow),
# then matches repaired IDs against NAV Genotype BirthIDs.
#
# Valid SE format:
#   SE-YYYYYYYY-XXXX
# Where:
#   - YYYYYYYY = Exactly 8 alphanumeric characters
#   - XXXX     = Exactly 4 digits

library(dplyr)
library(readr)
library(stringr)
library(tidyr)
library(purrr)

# ------------------------------------------------------------
# Step 0) Load SE_Number.csv (Selection list)
# ------------------------------------------------------------

se_list <- read_csv("SE_Number.csv", show_col_types = FALSE) %>%
  transmute(SE_Number = str_trim(as.character(SE_Number))) %>%
  filter(!is.na(SE_Number), SE_Number != "") %>%
  distinct()

# ------------------------------------------------------------
# Step 1) Load Del_Cow (All rows) and select animals from SE_Number.csv
# ------------------------------------------------------------

DF.Del_Cow_Selected <- con %>%
  tbl(in_catalog("gigacow", "sciDel", "Del_Cow")) %>%
  distinct(SE_Number, BirthDate, FarmName_Pseudo, .keep_all = TRUE) %>%
  filter(SE_Number != "Unknown") %>%
  collect() %>%
  mutate(SE_Number = str_trim(as.character(SE_Number))) %>%
  semi_join(se_list, by = "SE_Number")

# This is the dataset we will "fix" (it contains BirthDate as YYYY-MM-DD).
df <- DF.Del_Cow_Selected %>%
  transmute(
    SE_Number = SE_Number,
    BirthDate = BirthDate,
    FarmName_Pseudo = FarmName_Pseudo
  )

# ------------------------------------------------------------
# Step 2) Load NAV GenotypeData distinct BirthID/BirthYear
# ------------------------------------------------------------

DF.Nav_Cow_Genotyped <- con %>%
  tbl(in_catalog("gigacow", "sciNav", "Nav_GenotypeData")) %>%
  distinct(BirthID, BirthYear) %>%
  filter(BirthID != "Unknown") %>%
  collect()

# ------------------------------------------------------------
# Step 3) Build NAV reference table (BirthID -> BirthYear_nav)
# ------------------------------------------------------------

nav_ref <- DF.Nav_Cow_Genotyped %>%
  distinct(BirthID, BirthYear) %>%
  rename(BirthYear_nav = BirthYear)

# ------------------------------------------------------------
# Define Patterns
# ------------------------------------------------------------

pattern_valid  <- "^SE-[A-Za-z0-9]{8}-[0-9]{4}$"
pattern_prefix <- "^SE-"

# ------------------------------------------------------------
# Classify Invalid Formats
# ------------------------------------------------------------

classified <- df %>%
  mutate(
    
    # Store original raw value.
    SE_raw = SE_Number,
    
    # Trim whitespace.
    SE_trim = str_trim(SE_raw),
    
    # Extract birth year from BirthDate (Expected: YYYY-MM-DD).
    BirthYear_del = suppressWarnings(as.integer(str_sub(BirthDate, 1, 4))),
    
    # Check strict validity.
    is_valid = str_detect(SE_trim, pattern_valid),
    
    # Split into dash-separated parts.
    n_parts = str_count(SE_trim, "-") + 1L,
    
    part1 = if_else(n_parts >= 1, str_split_fixed(SE_trim, "-", 4)[,1], NA_character_),
    part2 = if_else(n_parts >= 2, str_split_fixed(SE_trim, "-", 4)[,2], NA_character_),
    part3 = if_else(n_parts >= 3, str_split_fixed(SE_trim, "-", 4)[,3], NA_character_),
    part4 = if_else(n_parts >= 4, str_split_fixed(SE_trim, "-", 4)[,4], NA_character_),
    
    mid_len  = if_else(!is.na(part2), str_length(part2), NA_integer_),
    tail_len = if_else(!is.na(part3), str_length(part3), NA_integer_),
    
    mid_is_alnum = if_else(!is.na(part2), str_detect(part2, "^[A-Za-z0-9]+$"), FALSE),
    tail_is_digit = if_else(!is.na(part3), str_detect(part3, "^[0-9]+$"), FALSE),
    
    fail_reason = case_when(
      is.na(SE_raw) | SE_trim == "" ~ "Missing value",
      SE_raw != SE_trim ~ "Leading/trailing whitespace",
      !str_detect(SE_trim, pattern_prefix) ~ "Missing/incorrect 'SE-' prefix",
      n_parts < 3 ~ "Too few sections (missing '-')",
      n_parts > 3 & tail_is_digit & !is.na(tail_len) & tail_len < 4 ~ "Short code with extra suffix",
      n_parts > 3 & str_detect(part4, "^[0-9]+$") ~ "Extra '-digits' suffix after code",
      n_parts > 3 ~ "Extra suffix after code (non-digit)",
      mid_len != 8 ~ "Middle part not length 8",
      !mid_is_alnum ~ "Middle part has non-alphanumeric characters",
      !tail_is_digit ~ "Last part contains non-digits",
      !is.na(tail_len) & tail_len < 4 ~ "Last part has 1–3 digits (needs zero-padding?)",
      !is.na(tail_len) & tail_len > 4 ~ "Last part has 5+ digits (needs trimming/removal?)",
      TRUE ~ "Other formatting issue"
    ),
    
    tail_digits  = if_else(tail_is_digit, part3, NA_character_),
    extra_suffix = if_else(n_parts > 3, part4, NA_character_)
  )

# ------------------------------------------------------------
# Candidate Generation (Valid IDs + Repair Strategies)
# ------------------------------------------------------------

make_id <- function(mid, tail4) paste0("SE-", mid, "-", tail4)

# Include already-valid IDs as candidates (Use as-is).
cand_valid <- classified %>%
  filter(is_valid) %>%
  transmute(
    SE_raw, SE_trim, BirthYear_del,
    candidate_id = SE_trim,
    method = "0) Valid (As-is)"
  )

# Only invalid ones go into repair strategies.
to_fix <- classified %>%
  filter(!is_valid) %>%
  select(SE_raw, SE_trim, BirthYear_del, n_parts, part2, part3, part4, tail_is_digit)

# 1) No fix (Try as-is anyway).
cand_1 <- to_fix %>%
  transmute(
    SE_raw, SE_trim, BirthYear_del,
    candidate_id = SE_trim,
    method = "1) No fix"
  )

# 2) Pad short codes (1–3 digits) to 4 digits.
cand_2_left <- to_fix %>%
  mutate(tail = as.character(part3)) %>%
  filter(!is.na(part2), !is.na(tail), tail != "", tail_is_digit, nchar(tail) >= 1, nchar(tail) <= 3) %>%
  transmute(
    SE_raw, SE_trim, BirthYear_del,
    candidate_id = make_id(part2, str_pad(tail, 4, side = "left", pad = "0")),
    method = "2) Pad short code (Left)"
  )

cand_2_right <- to_fix %>%
  mutate(tail = as.character(part3)) %>%
  filter(!is.na(part2), !is.na(tail), tail != "", tail_is_digit, nchar(tail) >= 1, nchar(tail) <= 3) %>%
  transmute(
    SE_raw, SE_trim, BirthYear_del,
    candidate_id = make_id(part2, str_pad(tail, 4, side = "right", pad = "0")),
    method = "2) Pad short code (Right)"
  )

# 3) Merge suffix digits into code (SE-XXXXXXXX-12-34 -> 1234).
cand_3 <- to_fix %>%
  mutate(tail = as.character(part3), suf = as.character(part4)) %>%
  filter(!is.na(part2), !is.na(tail), tail != "", tail_is_digit,
         nchar(tail) >= 1, nchar(tail) <= 3,
         n_parts > 3,
         !is.na(suf), suf != "", str_detect(suf, "^[0-9]+$")) %>%
  mutate(
    combined = paste0(tail, suf),
    tail4 = str_sub(combined, 1, 4)
  ) %>%
  filter(str_length(tail4) == 4) %>%
  transmute(
    SE_raw, SE_trim, BirthYear_del,
    candidate_id = make_id(part2, tail4),
    method = "3) Merge suffix digits into 4-digit code"
  )

# 4a) Sliding windows (L→R and R→L; also after dash removal by appending numeric suffix).
cand_4a <- to_fix %>%
  mutate(
    tail = as.character(part3),
    suf  = as.character(part4),
    tail_dash_removed = case_when(
      !is.na(suf) & suf != "" & str_detect(suf, "^[0-9]+$") ~ paste0(tail, suf),
      TRUE ~ tail
    )
  ) %>%
  filter(!is.na(part2), !is.na(tail), tail != "", tail_is_digit) %>%
  rowwise() %>%
  mutate(
    windows_lr = list({
      nwin <- nchar(tail) - 3L
      if (is.na(nwin) || nwin < 1L) character(0)
      else map_chr(seq_len(nwin), ~ str_sub(tail, .x, .x + 3))
    }),
    windows_rl = list({
      nwin <- nchar(tail) - 3L
      if (is.na(nwin) || nwin < 1L) character(0)
      else map_chr(seq_len(nwin), ~ {
        start <- nchar(tail) - (.x + 3) + 1L
        end   <- nchar(tail) - .x + 1L
        str_sub(tail, start, end)
      })
    }),
    windows_lr_dash_removed = list({
      nwin <- nchar(tail_dash_removed) - 3L
      if (is.na(nwin) || nwin < 1L) character(0)
      else map_chr(seq_len(nwin), ~ str_sub(tail_dash_removed, .x, .x + 3))
    }),
    windows_rl_dash_removed = list({
      nwin <- nchar(tail_dash_removed) - 3L
      if (is.na(nwin) || nwin < 1L) character(0)
      else map_chr(seq_len(nwin), ~ {
        start <- nchar(tail_dash_removed) - (.x + 3) + 1L
        end   <- nchar(tail_dash_removed) - .x + 1L
        str_sub(tail_dash_removed, start, end)
      })
    })
  ) %>%
  ungroup() %>%
  select(SE_raw, SE_trim, BirthYear_del, part2,
         windows_lr, windows_rl, windows_lr_dash_removed, windows_rl_dash_removed) %>%
  pivot_longer(
    cols = starts_with("windows_"),
    names_to = "window_variant",
    values_to = "windows"
  ) %>%
  unnest(windows) %>%
  distinct(SE_raw, SE_trim, BirthYear_del, part2, windows, window_variant) %>%
  transmute(
    SE_raw, SE_trim, BirthYear_del,
    candidate_id = make_id(part2, windows),
    method = case_when(
      window_variant == "windows_lr" ~ "4) Window (L→R) from part3",
      window_variant == "windows_rl" ~ "4) Window (R→L) from part3",
      window_variant == "windows_lr_dash_removed" ~ "4) Window (L→R) after dash removal",
      window_variant == "windows_rl_dash_removed" ~ "4) Window (R→L) after dash removal",
      TRUE ~ "4) Window (Other)"
    )
  ) %>%
  distinct(BirthYear_del, candidate_id, method, .keep_all = TRUE)

# 4b) If exactly 5 digits, drop one digit (5 candidates).
cand_4b <- to_fix %>%
  mutate(tail = as.character(part3)) %>%
  filter(!is.na(part2), !is.na(tail), tail != "", tail_is_digit) %>%
  rowwise() %>%
  mutate(
    n = nchar(tail),
    drops = list(
      if (is.na(n) || n != 5L) character(0)
      else map_chr(seq_len(5), ~ paste0(str_sub(tail, 1, .x - 1), str_sub(tail, .x + 1, 5)))
    )
  ) %>%
  ungroup() %>%
  unnest(drops) %>%
  transmute(
    SE_raw, SE_trim, BirthYear_del,
    candidate_id = make_id(part2, drops),
    method = "4) Drop 1 digit (5→4)"
  )

# Extra) Strip suffix after valid 4-digit code (SE-XXXXXXXX-1234-99 -> SE-XXXXXXXX-1234).
cand_extra_b <- to_fix %>%
  filter(!is.na(part2), str_detect(part3, "^[0-9]{4}$"), n_parts > 3) %>%
  transmute(
    SE_raw, SE_trim, BirthYear_del,
    candidate_id = make_id(part2, part3),
    method = "Extra) Strip suffix after valid 4-digit code"
  )

# Combine all candidates.
candidates <- bind_rows(
  cand_valid,
  cand_1,
  cand_2_left,
  cand_2_right,
  cand_3,
  cand_4a,
  cand_4b,
  cand_extra_b
) %>%
  mutate(candidate_id = str_trim(candidate_id)) %>%
  distinct(SE_raw, BirthYear_del, candidate_id, method)

# ------------------------------------------------------------
# Match Candidates Against NAV Genotype BirthIDs
# ------------------------------------------------------------

matched_raw <- candidates %>%
  left_join(nav_ref, by = c("candidate_id" = "BirthID")) %>%
  mutate(
    MatchNAV = !is.na(BirthYear_nav),
    MatchType = case_when(
      MatchNAV ~ "NAV only",
      TRUE ~ "None"
    ),
    
    # Birth year agreement relative to DEL vs NAV.
    BirthYearMatch = case_when(
      is.na(BirthYear_del) ~ "Del Missing",
      MatchNAV & BirthYear_del == BirthYear_nav ~ "NAV only",
      MatchNAV & BirthYear_del != BirthYear_nav ~ "No",
      TRUE ~ "No"
    )
  )

# ------------------------------------------------------------
# Simple outputs
# ------------------------------------------------------------

# How many matched NAV at all?
matched_raw %>%
  summarise(
    total_rows = n(),
    matched_nav_rows = sum(MatchNAV),
    matched_nav_SE_raw = n_distinct(SE_raw[MatchNAV])
  )

# A match list (only NAV matches).
match_list <- matched_raw %>%
  filter(MatchNAV) %>%
  select(any_of(c(
    "SE_raw", "SE_trim", "candidate_id", "method",
    "BirthYear_del", "BirthYear_nav", "BirthYearMatch",
    "FarmName_Pseudo", "BirthDate"
  )))

match_list
