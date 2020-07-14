#########################
# AUTHOR: UGUR YILDIRIM #
# DATE:   2020-07-14    #
#########################

# Define remove_titles
#' @export
remove_titles <- function(row) {
  fname_clean <- row
  for (title in titles$title) {
    pattern <- paste0(" ", title, " ")
    fname_clean <- stringi::stri_replace_all_regex(fname_clean, pattern, "")
  }
  return(fname_clean)
}
# NOTES
# 1. Title order matters.
# 2. More titles can be added to the titles list, such as "judge".

# Define split_first_name
#' @export
split_first_name <- function(row) {
  fname_split <- strsplit(row, " ")[[1]]
  fname_split <- fname_split[fname_split != ""]
  if (is.na(fname_split[1])) {
    first <- NA
    middle <- NA
  } else if (is.na(fname_split[2])) {
    first <- fname_split[1]
    middle <- NA
  } else if (stringr::str_length(fname_split[1]) > 1) {
    if (length(unique(fname_split)) != length(fname_split)) {
      fname_split <- rle(fname_split)$values
    }
    first <- fname_split[1]
    middle <- fname_split[2]
  } else if (stringr::str_length(fname_split[2]) > 1) {
    if (length(unique(fname_split)) != length(fname_split)) {
      fname_split <- rle(fname_split)$values
    }
    first <- fname_split[2]
    middle <- fname_split[3]
  } else {
    first <- row
    middle <- fname_split[2]
  }
  return(list(first=first, middle=middle))
}
# NOTES
# 1. To be consistent with the original Stata implementation,
#    last names such as "la duca" are currently parsed as "la".
#    This will likely be changed in the future.

# Define merge_names_no_middle
#' @export
merge_names_no_middle <- function(row) {
  if (is.na(row["name"])) {first <- row["first"]}
  else {first <- row["name"]}
  return(first)
}

#' Clean names
#'
#' This function cleans raw first and last names by removing titles (e.g., Dr.)
#' and replacing nicknames with standard names (e.g., Billy to William).
#' It creates a new dataset with cleaned first names, middle initials, and last names.
#'
#' @param in_path    Path to the directory where the input file is located
#' @param file_name  Input filename
#' @param lname_col  Raw last name column in the input file
#' @param fname_col  Raw first name column in the input file
#' @param sex_col    Sex column in the input file (1=male, 2=female)
#' @param other_cols Other columns to be kept from the input file
#' @param out_path   Path to the directory where the output file will be saved
#' @param middle     Middle initial column in the input file
#' @return           NULL
#' @export
clean_names <- function(in_path, file_name, lname_col, fname_col, sex_col, other_cols, out_path, middle) {

  # Read data
  path_to_data <- paste(in_path, file_name, sep="/")
  data <- readr::read_csv(path_to_data)

  # Copy raw names before processing
  data$lname_clean <- data[[lname_col]]
  data$fname_clean <- data[[fname_col]]

  # Concatenate middle name to first name if middle name exists
  if (middle != "") {
    concat_middle_to_first <- function(row) {
      if (is.na(row["fname_clean"])) {first <- NA}
      else if (is.na(row[[middle]])) {first <- row["fname_clean"]}
      else {first <- paste(row["fname_clean"], row[[middle]])}
      return(first)
    }
    data$fname_clean <- apply(data, 1, concat_middle_to_first) # SLOW
    data$middle_raw <- data[[middle]]
    data[[middle]] <- NULL
  }
  # NOTES
  # 1. This statement is here to make sure that there aren't any inconsistencies
  #    in relation to middle initials. For example, in the case of CenSoc, BUNMD already
  #    has middle initials but the way the ABE algorithm processes first names and
  #    initials are sometimes different from the way BUNMD stores first names and initials.
  #    ABE turns "A J" into "A J" as first name and "J" as initial, while in BUNMD "A J"
  #    would be first name "A" and initial "J".

  # Convert unicode to ascii to remove accents
  data$lname_clean <- iconv(data$lname_clean, to="ASCII//TRANSLIT")
  data$fname_clean <- iconv(data$fname_clean, to="ASCII//TRANSLIT")

  # Convert to lower case
  data$lname_clean <- tolower(data$lname_clean)
  data$fname_clean <- tolower(data$fname_clean)

  # Trim whitespace
  data$lname_clean <- stringr::str_trim(data$lname_clean)
  data$fname_clean <- stringr::str_trim(data$fname_clean)

  # Replace . with " "
  data$lname_clean <- gsub("\\.", " ", data$lname_clean)
  data$fname_clean <- gsub("\\.", " ", data$fname_clean)

  # Keep only letters and spaces (accents don't work)
  data$lname_clean <- gsub("[^a-z ]", "", data$lname_clean)
  data$fname_clean <- gsub("[^a-z ]", "", data$fname_clean)

  # Get rid of titles
  data$fname_clean <- paste0(" ", data$fname_clean)
  data$fname_clean <- remove_titles(data$fname_clean)

  # Fix two-word names (e.g., mc donnell --> mcdonnell)
  data$lname_clean <- paste0(" ", data$lname_clean)
  data$lname_clean <- gsub(" st ",    " st",    data$lname_clean)
  data$lname_clean <- gsub(" ste ",   " ste",   data$lname_clean)
  data$lname_clean <- gsub(" saint ", " saint", data$lname_clean)
  data$lname_clean <- gsub(" virg ",  " virg",  data$lname_clean)
  data$lname_clean <- gsub(" mac ",   " mac",   data$lname_clean)
  data$lname_clean <- gsub(" mc ",    " mc",    data$lname_clean)
  data$lname_clean <- stringr::str_trim(data$lname_clean)
  data$fname_clean <- gsub(" st ",    " st",    data$fname_clean)
  data$fname_clean <- gsub(" ste ",   " ste",   data$fname_clean)
  data$fname_clean <- gsub(" saint ", " saint", data$fname_clean)
  data$fname_clean <- gsub(" virg ",  " virg",  data$fname_clean)
  data$fname_clean <- gsub(" mac ",   " mac",   data$fname_clean)
  data$fname_clean <- gsub(" mc ",    " mc",    data$fname_clean)
  data$fname_clean <- stringr::str_trim(data$fname_clean)
  # NOTES
  # 1. Many more words can be added here, such as:
  #    la, van, du, de, del, o, pal, le, de la

  # Split first name (i.e., deal with middle names and initials)
  res <- lapply(data$fname_clean, split_first_name) # SLOW (~60s with ~3M obs)
  data$first  <- unlist(res)[attr(unlist(res),"names") == "first"]
  data$middle <- unlist(res)[attr(unlist(res),"names") == "middle"]
  rm(res)

  # Merge nicknames
  data_final <- merge(data, nicknames, by.x = c("first", sex_col),
                      by.y = c("nickname", "sex"), all.x = TRUE) # SLOW (~30s with ~3M obs)
  data_final$fname_final <- apply(data_final, 1, merge_names_no_middle) # SLOW (~45s with ~3M obs)

  # Keep only the first multicharacter part of lname_clean
  res <- lapply(data_final$lname_clean, split_first_name) # SLOW (~60s with ~3M obs)
  data_final$lname_final <- unlist(res)[attr(unlist(res),"names") == "first"]
  rm(res)

  # Reorder columns, drop unnecessary ones (fname_clean, lname_clean, first, name)
  if (middle == "") {
    cols_to_keep <- c("fname_final", "middle", "lname_final", sex_col, fname_col, lname_col, other_cols)
    data_final <- data_final[,cols_to_keep]
    colnames(data_final) <- c("fname", "middle", "lname", "sex", "fname_raw", "lname_raw", other_cols)
  } else {
    cols_to_keep <- c("fname_final", "middle", "lname_final", sex_col, fname_col, "middle_raw", lname_col, other_cols)
    data_final <- data_final[,cols_to_keep]
    colnames(data_final) <- c("fname", "middle", "lname", "sex", "fname_raw", "middle_raw", "lname_raw", other_cols)
  }

  # Do a final trim
  data_final$lname <- stringr::str_trim(data_final$lname)
  data_final$fname <- stringr::str_trim(data_final$fname)

  # Save dataset
  dir.create(out_path, showWarnings = FALSE)
  path_to_processed_data <- paste(out_path, gsub(".csv", "_cleaned.csv", file_name), sep="/")
  readr::write_csv(data_final, path_to_processed_data)

  # Clean up after yourself
  rm(list = ls())

}
