# This script is used to detect and classify broken SE_Number IDs in DelPro data.
# The only valid format is:
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
# Load Inputs
# ------------------------------------------------------------

# Load a file containing questionable IDs from CSV.
# This can be replaced with a direct database download.
df <- read_csv("invalidMilkings.csv", show_col_types = FALSE)

#df <- con %>% tbl(in_catalog("gigacow", "sciDel", "Del_Cow")) %>%
#  distinct(SE_Number, BirthDate, FarmName_Pseudo, .keep_all = TRUE) %>%
#  filter(SE_Number != "Unknown") %>%
#  collect()


# Note: This script assumes DF.Kok_Cow and DF.Nav_Cow already exist in your session.
# Run your database code to create them before running this script.

# ------------------------------------------------------------
# Define Patterns
# ------------------------------------------------------------

# Define the strict valid SE_Number pattern.
pattern_valid <- "^SE-[A-Za-z0-9]{8}-[0-9]{4}$"

# Define a helper pattern for the required prefix.
pattern_prefix <- "^SE-"

# ------------------------------------------------------------
# Classify Invalid Formats (Why They Fail)
# ------------------------------------------------------------

classified <- df %>%
  mutate(
    
    # Store the original raw value.
    SE_raw = SE_Number,
    
    # Trim whitespace to avoid false errors caused by spaces.
    SE_trim = str_trim(SE_raw),
    
    # Extract birth year from BirthDate (Expected format: YYYY-MM-DD).
    # If BirthDate is missing or malformed, BirthYear_del becomes NA.
    BirthYear_del = suppressWarnings(as.integer(str_sub(BirthDate, 1, 4))),
    
    # Check whether the value fully matches the valid SE_Number format.
    is_valid = str_detect(SE_trim, pattern_valid),
    
    # Split the ID into parts separated by "-".
    # Valid IDs must have exactly 3 parts: SE / middle / 4-digit code.
    n_parts = str_count(SE_trim, "-") + 1L,
    
    # Extract each section of the ID (Up to 4 parts; part4 stores any suffix beyond the 3rd dash).
    part1 = if_else(n_parts >= 1, str_split_fixed(SE_trim, "-", 4)[,1], NA_character_),
    part2 = if_else(n_parts >= 2, str_split_fixed(SE_trim, "-", 4)[,2], NA_character_),
    part3 = if_else(n_parts >= 3, str_split_fixed(SE_trim, "-", 4)[,3], NA_character_),
    part4 = if_else(n_parts >= 4, str_split_fixed(SE_trim, "-", 4)[,4], NA_character_),
    
    # Compute the length of the middle and last parts.
    mid_len  = if_else(!is.na(part2), str_length(part2), NA_integer_),
    tail_len = if_else(!is.na(part3), str_length(part3), NA_integer_),
    
    # Check whether the middle part is strictly alphanumeric.
    mid_is_alnum = if_else(!is.na(part2),
                           str_detect(part2, "^[A-Za-z0-9]+$"),
                           FALSE),
    
    # Check whether the last part contains only digits.
    tail_is_digit = if_else(!is.na(part3),
                            str_detect(part3, "^[0-9]+$"),
                            FALSE),
    
    # Assign a detailed failure reason for each invalid SE_Number.
    fail_reason = case_when(
      
      # Case: Missing or empty values.
      is.na(SE_raw) | SE_trim == "" ~ "Missing value",
      
      # Case: Extra whitespace around the ID.
      SE_raw != SE_trim ~ "Leading/trailing whitespace",
      
      # Case: Prefix is not correctly written as "SE-".
      !str_detect(SE_trim, pattern_prefix) ~ "Missing/incorrect 'SE-' prefix",
      
      # Case: Too few sections (Missing one or more dashes).
      n_parts < 3 ~ "Too few sections (missing '-')",
      
      # Case: Short code (1–3 digits) with an extra suffix part.
      # Example: SE-XXXXXXXX-12-99
      n_parts > 3 & tail_is_digit & !is.na(tail_len) & tail_len < 4 ~ "Short code with extra suffix",
      
      # Case: Extra numeric suffix after the code.
      # Example: SE-XXXXXXXX-1234-99
      n_parts > 3 & str_detect(part4, "^[0-9]+$") ~ "Extra '-digits' suffix after code",
      
      # Case: Extra non-numeric suffix after the code.
      # Example: SE-XXXXXXXX-1234-ABC
      n_parts > 3 ~ "Extra suffix after code (non-digit)",
      
      # Case: Middle part is not exactly 8 characters long.
      mid_len != 8 ~ "Middle part not length 8",
      
      # Case: Middle part contains invalid characters.
      !mid_is_alnum ~ "Middle part has non-alphanumeric characters",
      
      # Case: Last part contains letters or symbols instead of digits.
      !tail_is_digit ~ "Last part contains non-digits",
      
      # Case: Last part has only 1–3 digits (Possible zero-padding needed).
      !is.na(tail_len) & tail_len < 4 ~ "Last part has 1–3 digits (needs zero-padding?)",
      
      # Case: Last part has more than 4 digits (Possible trimming needed).
      !is.na(tail_len) & tail_len > 4 ~ "Last part has 5+ digits (needs trimming/removal?)",
      
      # Catch-all fallback.
      TRUE ~ "Other formatting issue"
    ),
    
    # Store useful extracted pieces for later salvage attempts.
    tail_digits  = if_else(tail_is_digit, part3, NA_character_),
    extra_suffix = if_else(n_parts > 3, part4, NA_character_)
  )

# ------------------------------------------------------------
# Output Summaries For Invalid Patterns
# ------------------------------------------------------------

# Sanity check: Count how many IDs are actually valid.
classified %>%
  count(is_valid)

# Summary table: Count invalid IDs by failure reason.
summary_counts <- classified %>%
  filter(!is_valid) %>%
  count(fail_reason, sort = TRUE)

summary_counts

# Example table: Show up to 10 sample IDs for each failure category.
examples <- classified %>%
  filter(!is_valid) %>%
  group_by(fail_reason) %>%
  slice_head(n = 10) %>%
  ungroup() %>%
  select(SE_raw, SE_trim, BirthDate, BirthYear_del, fail_reason, part2, part3, part4, mid_len, tail_len)

examples

# ------------------------------------------------------------
# Quick Diagnostic For Strategy 4 (Window Extraction)
# ------------------------------------------------------------

# This diagnostic shows rows where part3 is missing/empty or too short for 4-digit windows.
diagnostic_tail <- classified %>%
  filter(!is_valid) %>%
  mutate(
    tail = as.character(part3),
    n_tail = nchar(tail)
  ) %>%
  filter(is.na(tail) | tail == "" | is.na(n_tail) | n_tail < 5) %>%
  select(SE_trim, part2, part3, tail, n_tail, tail_is_digit, fail_reason) %>%
  distinct() %>%
  head(50)

diagnostic_tail

# ------------------------------------------------------------
# Build Candidate Fixes For Matching Against KOK And NAV
# ------------------------------------------------------------

# Define a helper to build an SE-like ID from middle and 4-digit code.
make_id <- function(mid, tail4) paste0("SE-", mid, "-", tail4)

# Keep only rows that are not already valid (We are trying to salvage errors).
to_fix <- classified %>%
  filter(!is_valid) %>%
  select(SE_raw, SE_trim, BirthYear_del, n_parts, part2, part3, part4, tail_is_digit)

# Strategy 1) No fixing (Match SE_trim as-is).
cand_1 <- to_fix %>%
  transmute(
    SE_raw, SE_trim, BirthYear_del,
    candidate_id = SE_trim,
    method = "1) No fix"
  )

# Strategy 2) Zero-padding short codes in part3 to 4 digits.
# Create two candidates: Pad on the left and pad on the right.
cand_2_left <- to_fix %>%
  mutate(tail = as.character(part3)) %>%
  filter(!is.na(part2),
         !is.na(tail),
         tail != "",
         tail_is_digit,
         nchar(tail) >= 1,
         nchar(tail) <= 3) %>%
  transmute(
    SE_raw, SE_trim, BirthYear_del,
    candidate_id = make_id(part2, str_pad(tail, 4, side = "left", pad = "0")),
    method = "2) Pad short code (Left)"
  )

cand_2_right <- to_fix %>%
  mutate(tail = as.character(part3)) %>%
  filter(!is.na(part2),
         !is.na(tail),
         tail != "",
         tail_is_digit,
         nchar(tail) >= 1,
         nchar(tail) <= 3) %>%
  transmute(
    SE_raw, SE_trim, BirthYear_del,
    candidate_id = make_id(part2, str_pad(tail, 4, side = "right", pad = "0")),
    method = "2) Pad short code (Right)"
  )

# Strategy 3) Remove trailing dash by using part4 digits to complete a 4-digit code.
# Example: SE-XXXXXXXX-12-34 -> Use "1234" as the 4-digit code.
cand_3 <- to_fix %>%
  mutate(
    tail = as.character(part3),
    suf  = as.character(part4)
  ) %>%
  filter(!is.na(part2),
         !is.na(tail),
         tail != "",
         tail_is_digit,
         nchar(tail) >= 1,
         nchar(tail) <= 3,
         n_parts > 3,
         !is.na(suf),
         suf != "",
         str_detect(suf, "^[0-9]+$")) %>%
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

## Strategy 4a) Sliding 4-digit windows across long digit strings.
# This version generates windows left-to-right AND right-to-left.
# It also tries the same after "dash removal" by appending numeric part4 to part3.

cand_4a <- to_fix %>%
  mutate(
    tail = as.character(part3),
    suf  = as.character(part4),
    
    # Build an optional "dash-removed" tail by appending numeric suffix digits if present.
    tail_dash_removed = case_when(
      !is.na(suf) & suf != "" & str_detect(suf, "^[0-9]+$") ~ paste0(tail, suf),
      TRUE ~ tail
    )
  ) %>%
  filter(
    !is.na(part2),
    !is.na(tail),
    tail != "",
    tail_is_digit
  ) %>%
  rowwise() %>%
  mutate(
    # Build windows from part3 (Left to right).
    windows_lr = list({
      nwin <- nchar(tail) - 3L
      if (is.na(nwin) || nwin < 1L) character(0)
      else map_chr(seq_len(nwin), ~ str_sub(tail, .x, .x + 3))
    }),
    
    # Build windows from part3 (Right to left).
    windows_rl = list({
      nwin <- nchar(tail) - 3L
      if (is.na(nwin) || nwin < 1L) character(0)
      else map_chr(seq_len(nwin), ~ {
        start <- nchar(tail) - (.x + 3) + 1L
        end   <- nchar(tail) - .x + 1L
        str_sub(tail, start, end)
      })
    }),
    
    # Build windows from dash-removed tail (Left to right).
    windows_lr_dash_removed = list({
      nwin <- nchar(tail_dash_removed) - 3L
      if (is.na(nwin) || nwin < 1L) character(0)
      else map_chr(seq_len(nwin), ~ str_sub(tail_dash_removed, .x, .x + 3))
    }),
    
    # Build windows from dash-removed tail (Right to left).
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
  select(
    SE_raw, SE_trim, BirthYear_del, part2,
    windows_lr, windows_rl, windows_lr_dash_removed, windows_rl_dash_removed
  ) %>%
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
  distinct(BirthYear_del, candidate_id, .keep_all = TRUE)
# Strategy 4b) For exactly 5 digits, try dropping one digit (All 5 possible drops).
# This version is fully guarded and cannot crash.
cand_4b <- to_fix %>%
  mutate(tail = as.character(part3)) %>%
  filter(!is.na(part2),
         !is.na(tail),
         tail != "",
         tail_is_digit) %>%
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

# Extra Idea) If part3 is already 4 digits but there is an extra suffix, strip the suffix.
cand_extra_b <- to_fix %>%
  filter(!is.na(part2), str_detect(part3, "^[0-9]{4}$"), n_parts > 3) %>%
  transmute(
    SE_raw, SE_trim, BirthYear_del,
    candidate_id = make_id(part2, part3),
    method = "Extra) Strip suffix after valid 4-digit code"
  )

# Combine all candidates and deduplicate.
candidates <- bind_rows(
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
# Match Candidates Against KOK And NAV
# ------------------------------------------------------------

# Prepare reference tables with unique BirthID and BirthYear.
kok_ref <- DF.Kok_Cow %>%
  distinct(BirthID, BirthYear) %>%
  rename(BirthYear_kok = BirthYear)

nav_ref <- DF.Nav_Cow %>%
  distinct(BirthID, BirthYear) %>%
  rename(BirthYear_nav = BirthYear)

# Join candidates to KOK and NAV using BirthID.
# Note that this is a one to many join so
matched_raw <- candidates %>%
  left_join(kok_ref, by = c("candidate_id" = "BirthID")) %>%
  left_join(nav_ref, by = c("candidate_id" = "BirthID")) %>%
  mutate(
    
    # Mark whether we have a match in each dataset.
    MatchKOK = !is.na(BirthYear_kok),
    MatchNAV = !is.na(BirthYear_nav),
    
    # Assign a combined match type.
    MatchType = case_when(
      MatchKOK & MatchNAV ~ "Both",
      MatchKOK ~ "KOK only",
      MatchNAV ~ "NAV only",
      TRUE ~ "None"
    ),
    
    # Evaluate birth year agreement.
    # Both: All three birth years are present and identical.
    # No: DEL is present but neither BirthYear_kok or BirthYear_nav is matched.
    # KOK only: BirthYear_del matches KOK but BirthYear_nav is different or NA.
    # NAV only: BirthYear_del matches NAV but BirthYear_kok is different or NA.
    BirthYearMatch = case_when(
      is.na(BirthYear_del) ~ "Del Missing",
      
      !is.na(BirthYear_kok) & !is.na(BirthYear_nav) &
        BirthYear_del == BirthYear_kok & BirthYear_del == BirthYear_nav ~ "Both",
      
      !is.na(BirthYear_kok) &
        BirthYear_del == BirthYear_kok &
        (is.na(BirthYear_nav) | BirthYear_del != BirthYear_nav) ~ "KOK only",
      
      !is.na(BirthYear_nav) &
        BirthYear_del == BirthYear_nav &
        (is.na(BirthYear_kok) | BirthYear_del != BirthYear_kok) ~ "NAV only",
      
      TRUE ~ "No"
    )
  )

matched -> matched_raw %>%

# ------------------------------------------------------------
# Summary Statistics For Your Four Requested Strategies
# ------------------------------------------------------------

# Summarize matches for strategy 1 (No fix).
summary_1 <- matched %>%
  filter(method == "1) No fix") %>%
  summarise(
    Total = n(),
    MatchAny = sum(MatchType != "None"),
    MatchKOK = sum(MatchKOK),
    MatchNAV = sum(MatchNAV),
    MatchBoth = sum(MatchType == "Both")
  )

summary_1

# Summarize matches for strategy 2 (Zero padding short codes).
summary_2 <- matched %>%
  filter(str_detect(method, "^2\\) Pad short code")) %>%
  group_by(method) %>%
  summarise(
    Total = n(),
    MatchAny = sum(MatchType != "None"),
    MatchKOK = sum(MatchKOK),
    MatchNAV = sum(MatchNAV),
    MatchBoth = sum(MatchType == "Both"),
    .groups = "drop"
  ) %>%
  arrange(desc(MatchAny))

summary_2

# Summarize matches for strategy 3 (Merge suffix digits into 4-digit code).
summary_3 <- matched %>%
  filter(method == "3) Merge suffix digits into 4-digit code") %>%
  summarise(
    Total = n(),
    MatchAny = sum(MatchType != "None"),
    MatchKOK = sum(MatchKOK),
    MatchNAV = sum(MatchNAV),
    MatchBoth = sum(MatchType == "Both")
  )

summary_3

# Summarize matches for strategy 4 (Extract 4 digits from 5+ digits).
summary_4 <- matched %>%
  filter(str_detect(method, "^4\\)")) %>%
  group_by(method) %>%
  summarise(
    Total = n(),
    MatchAny = sum(MatchType != "None"),
    MatchKOK = sum(MatchKOK),
    MatchNAV = sum(MatchNAV),
    MatchBoth = sum(MatchType == "Both"),
    .groups = "drop"
  ) %>%
  arrange(desc(MatchAny))

summary_4

# Summarize matches for extra strategies (Optional).
summary_extra <- matched %>%
  filter(str_detect(method, "^Extra\\)")) %>%
  group_by(method) %>%
  summarise(
    Total = n(),
    MatchAny = sum(MatchType != "None"),
    MatchKOK = sum(MatchKOK),
    MatchNAV = sum(MatchNAV),
    MatchBoth = sum(MatchType == "Both"),
    .groups = "drop"
  ) %>%
  arrange(desc(MatchAny))

summary_extra

# ------------------------------------------------------------
# Build The Final Match List
# ------------------------------------------------------------

# Create a list of all candidate matches (Excluding non-matches).
match_list <- matched %>%
  filter(MatchType != "None") %>%
  select(any_of(c(
    "SE_raw", "SE_trim",
    "candidate_id", "method",
    "MatchType", "MatchKOK", "MatchNAV",
    "BirthYear_del", "BirthYear_kok", "BirthYear_nav",
    "BirthYearMatch"
  ))) %>%
  arrange(SE_raw, desc(MatchType == "Both"), method)

match_list

# Summarize birth year agreement across match types.
birthyear_summary <- match_list %>%
  count(MatchType, BirthYearMatch, sort = TRUE)

birthyear_summary

method_summary <- matched %>%
  group_by(method) %>%
  summarise(
    SE_raw_handled = n_distinct(SE_raw),   # Unique cows/IDs processed
    total_rows     = n(),                  # Total candidate rows produced
    
    # MatchType outcomes (row-level)
    Match_Both     = sum(MatchType == "Both"),
    Match_KOKonly  = sum(MatchType == "KOK only"),
    Match_NAVonly  = sum(MatchType == "NAV only"),
    Match_None     = sum(MatchType == "None"),
    
    # BirthYearMatch outcomes (row-level)
    BY_Both        = sum(BirthYearMatch == "Both"),
    BY_KOKonly     = sum(BirthYearMatch == "KOK only"),
    BY_NAVonly     = sum(BirthYearMatch == "NAV only"),
    BY_No          = sum(BirthYearMatch == "No"),
    BY_DelMissing  = sum(BirthYearMatch == "Del Missing"),
    
    .groups = "drop"
  ) %>%
  arrange(desc(SE_raw_handled))

method_summary

pad_left_multi <- matched %>%
  filter(method == "2) Pad short code (Left)") %>%
  group_by(SE_raw) %>%
  filter(n() > 1) %>%          # Keep SE_raw with multiple rows
  ungroup() %>%
  arrange(SE_raw, candidate_id, MatchType, BirthYearMatch)

pad_left_multi

dash_slide <- matched %>%
  filter(method == 	"Extra) Strip suffix after valid 4-digit code") %>%
  group_by(SE_raw) %>%
  ungroup() %>%
  arrange(SE_raw, candidate_id, MatchType, BirthYearMatch)
