#########################
# AUTHOR: UGUR YILDIRIM #
# DATE:   2020-07-17    #
#########################

# Define %>%
`%>%` <- magrittr::`%>%`

# Define preprocess_A
#' @export
preprocess_A <- function(file_A, fname_var, lname_var, time_var, id_var) {
  # Keep fname_var, lname_var, time_var, id_var
  file_A <- file_A[,c(fname_var, lname_var, time_var, id_var)]

  # Create file_A flag
  file_A$file_A <- 1

  # Order rows based on fname_var, lname_var, time_var
  file_A <- file_A[order(file_A[[fname_var]], file_A[[lname_var]], file_A[[time_var]]), ]

  # Create nA column
  file_A$nA <- 1:nrow(file_A)

  # Create ID_A column
  file_A$ID_A <- file_A$nA

  # Remove duplicate rows
  file_A <- file_A %>%
    dplyr::group_by_(fname_var, lname_var, time_var) %>%
    dplyr::mutate(unique_all = sum(file_A)) %>%
    dplyr::ungroup()
  file_A <- subset(file_A, unique_all == 1)
  file_A$unique_all <- NULL

  # Rename columns
  colnames(file_A) <- c("fname", "lname", "byear", "id_A", "file_A", "nA", "ID_A")

  # Return file_A
  return(file_A)
}
# NOTES
# 1. Double check that the ordering given by order(...) is unique.
# 2. Add code chunk to check that id_var uniquely identifies rows.

# Define preprocess_B
#' @export
preprocess_B <- function(file_B, fname_var, lname_var, time_var, id_var) {
  # Keep fname_var, lname_var, time_var, id_var
  file_B <- file_B[,c(fname_var, lname_var, time_var, id_var)]

  # Create file_B flag
  file_B$file_B <- 1

  # Order rows based on fname_var, lname_var, time_var
  file_B <- file_B[order(file_B[[fname_var]], file_B[[lname_var]], file_B[[time_var]]), ]

  # Create nB column
  file_B$nB <- 1:nrow(file_B)

  # Create ID_B column
  file_B$ID_B <- file_B$nB

  # Rename columns
  colnames(file_B) <- c("fname", "lname", "byear", "id_B", "file_B", "nB", "ID_B")

  # Return file_B
  return(file_B)
}
# NOTES
# 1. Double check that the ordering given by order(...) is unique.
# 2. Add code chunk to check that id_var uniquely identifies rows.

# Define append_A_to_B
#' @export
append_A_to_B <- function(file_A, file_B, uniqueband_file=2, backward=FALSE) {
  # Append B to A
  file_AB <- dplyr::bind_rows(file_B, file_A)

  # Fill in missing file flags
  file_AB$file_B <- ifelse(is.na(file_AB$file_A), 1, 0)
  file_AB$file_A <- ifelse(file_AB$file_B == 0,   1, 0)

  # Initialize matched_at_A, exactmatch1
  file_AB$matched_at_A <- NA
  file_AB$exactmatch1  <- 0

  # Create count_A, count_B
  if (backward) {
    file_AB <- file_AB %>%
      dplyr::group_by(fname, lname, byear) %>%
      dplyr::mutate(count_A = sum(file_B), count_B = sum(file_A)) %>%
      dplyr::ungroup()
  } else {
    file_AB <- file_AB %>%
      dplyr::group_by(fname, lname, byear) %>%
      dplyr::mutate(count_A = sum(file_A), count_B = sum(file_B)) %>%
      dplyr::ungroup()
  }

  # Conservative ABE
  if (backward) {
    file_AB <- file_AB[with(file_AB, order(file_B, fname, lname, byear)), ]
    for (i in 1:uniqueband_file) {
      file_AB$temp <- 0
      file_AB <- file_AB %>%
        dplyr::group_by(file_B, fname, lname) %>%
        dplyr::mutate(idx = dplyr::row_number(),
                      tot = dplyr::n()) %>%
        dplyr::ungroup()
      file_AB$byear_lag  <- data.table::shift(file_AB$byear, n=1, fill=NA, type="lag")
      file_AB$byear_lead <- data.table::shift(file_AB$byear, n=1, fill=NA, type="lead")
      file_AB$temp <- ifelse((file_AB$byear - i <= file_AB$byear_lag  & file_AB$idx > 1) |
                             (file_AB$byear + i >= file_AB$byear_lead & file_AB$idx < file_AB$tot), 1, file_AB$temp)
      file_AB[[paste0("uniquestub_file", i)]] <- 1 - file_AB$temp
      file_AB$temp <- NULL
    }
    file_AB$idx <- NULL
    file_AB$tot <- NULL
    file_AB$byear_lag <- NULL
    file_AB$byear_lead <- NULL
  } else {
    file_AB <- file_AB[with(file_AB, order(file_A, fname, lname, byear)), ]
    for (i in 1:uniqueband_file) {
      file_AB$temp <- 0
      file_AB <- file_AB %>%
        dplyr::group_by(file_A, fname, lname) %>%
        dplyr::mutate(idx = dplyr::row_number(),
                      tot = dplyr::n()) %>%
        dplyr::ungroup()
      file_AB$byear_lag  <- data.table::shift(file_AB$byear, n=1, fill=NA, type="lag")
      file_AB$byear_lead <- data.table::shift(file_AB$byear, n=1, fill=NA, type="lead")
      file_AB$temp <- ifelse((file_AB$byear - i <= file_AB$byear_lag  & file_AB$idx > 1) |
                             (file_AB$byear + i >= file_AB$byear_lead & file_AB$idx < file_AB$tot), 1, file_AB$temp)
      file_AB[[paste0("uniquestub_file", i)]] <- 1 - file_AB$temp
      file_AB$temp <- NULL
    }
    file_AB$idx <- NULL
    file_AB$tot <- NULL
    file_AB$byear_lag <- NULL
    file_AB$byear_lead <- NULL
  }

  # Update exactmatch1
  file_AB$exactmatch1 <- ifelse(file_AB$count_A == 1 & file_AB$count_B == 1, 1, file_AB$exactmatch1)

  # Drop rows in A that correspond to multiple rows in B
  if (backward) {
    file_AB$drop <- ifelse(file_AB$count_A > 1 & file_AB$count_B == 1 & file_AB$file_A == 1, 1, 0)
  } else {
    file_AB$drop <- ifelse(file_AB$count_B > 1 & file_AB$count_A == 1 & file_AB$file_A == 1, 1, 0)
  }
  file_AB         <- subset(file_AB, drop == 0)
  file_AB$drop    <- NULL
  file_AB$count_A <- NULL
  file_AB$count_B <- NULL

  # Update matched_at_A
  file_AB$matched_at_A <- ifelse(file_AB$exactmatch1 == 1 & file_AB$file_A == 1, 0, file_AB$matched_at_A)

  # Conservative ABE
  file_AB <- file_AB %>%
    dplyr::group_by(fname, lname, byear) %>%
    dplyr::mutate(uniquestub_match0 = sum(file_B)) %>%
    dplyr::ungroup()

  # Return file_AB
  return(file_AB)
}
# NOTES
# 1. Add a timediff parameter to be able to work with alternative time variables such as age.

# Define find_nonexact_matches
#' @export
find_nonexact_matches <- function(file_AB, timeband=2, uniqueband_match=2, backward=FALSE) {
  # Check if user asked for non-exact matches
  if (timeband > 0) {

    # Initialize already
    file_AB$already <- file_AB$exactmatch1

    # Loop over timeband values
    for (i in 1:timeband) {

      # Create byear_mi, byear_pi
      file_AB[[paste0("byear_m", i)]] <- ifelse(file_AB$file_B == 1, file_AB$byear, file_AB$byear - i)
      file_AB[[paste0("byear_p", i)]] <- ifelse(file_AB$file_B == 1, file_AB$byear, file_AB$byear + i)

      # Create unmatched_A
      file_AB$unmatched_A <- ifelse(file_AB$file_A == 1 & file_AB$already == 0, 1, 0)

      # Search -i
      fname    <- "fname"
      lname    <- "lname"
      byear_mi <- paste0("byear_m", i)
      file_AB <- file_AB %>%
        dplyr::group_by_(fname, lname, byear_mi) %>%
        dplyr::mutate(mcount_A = sum(unmatched_A), mcount_B = sum(file_B), existing_matches = sum(already)) %>%
        dplyr::ungroup()

      # Create exactmatch1_mi
      file_AB[[paste0("exactmatch1_m", i)]] <- ifelse(file_AB$mcount_A == 1 &
                                                      file_AB$mcount_B == 1 &
                                                      file_AB$existing_matches == 0, 1, NA)

      # Drop rows in A-i that correspond to multiple rows in B
      file_AB$drop <- ifelse(file_AB$mcount_B > 1  &
                             file_AB$mcount_A == 1 &
                             file_AB$file_A == 1   &
                             file_AB$existing_matches == 0, 1, 0)
      file_AB      <- subset(file_AB, drop == 0)
      file_AB$drop <- NULL
      file_AB$existing_matches <- NULL

      # Search +i
      fname    <- "fname"
      lname    <- "lname"
      byear_pi <- paste0("byear_p", i)
      file_AB <- file_AB %>%
        dplyr::group_by_(fname, lname, byear_pi) %>%
        dplyr::mutate(pcount_A = sum(unmatched_A), pcount_B = sum(file_B), existing_matches = sum(already)) %>%
        dplyr::ungroup()

      # Create exactmatch1_pi
      file_AB[[paste0("exactmatch1_p", i)]] <- ifelse(file_AB$pcount_A == 1 &
                                                      file_AB$pcount_B == 1 &
                                                      file_AB$existing_matches == 0, 1, NA)

      # Drop rows in A+i that correspond to multiple rows in B
      file_AB$drop <- ifelse(file_AB$pcount_B > 1  &
                             file_AB$pcount_A == 1 &
                             file_AB$file_A == 1   &
                             file_AB$existing_matches == 0, 1, 0)
      file_AB      <- subset(file_AB, drop == 0)
      file_AB$drop <- NULL
      file_AB$existing_matches <- NULL

      # Clean up exactmatch1_m, exactmatch1_p, matched_at_A
      file_AB[[paste0("exactmatch1_m", i)]] <- ifelse(is.na(file_AB[[paste0("exactmatch1_m", i)]]),
                                                      0,
                                                      file_AB[[paste0("exactmatch1_m", i)]])
      file_AB[[paste0("exactmatch1_p", i)]] <- ifelse(is.na(file_AB[[paste0("exactmatch1_p", i)]]),
                                                      0,
                                                      file_AB[[paste0("exactmatch1_p", i)]])
      file_AB$matched_at_A <- ifelse(file_AB[[paste0("exactmatch1_p", i)]] == 1 &
                                     file_AB[[paste0("exactmatch1_m", i)]] == 0 &
                                     file_AB$already != 1 &
                                     file_AB$file_A == 1, i, file_AB$matched_at_A)
      file_AB$matched_at_A <- ifelse(file_AB[[paste0("exactmatch1_p", i)]] == 0 &
                                     file_AB[[paste0("exactmatch1_m", i)]] == 1 &
                                     file_AB$already != 1 &
                                     file_AB$file_A == 1, -i, file_AB$matched_at_A)

      # Update already
      file_AB$already <- ifelse(file_AB[[paste0("exactmatch1_p", i)]] == 1 &
                                file_AB[[paste0("exactmatch1_m", i)]] == 0, 1, file_AB$already)
      file_AB$already <- ifelse(file_AB[[paste0("exactmatch1_p", i)]] == 0 &
                                file_AB[[paste0("exactmatch1_m", i)]] == 1, 1, file_AB$already)

      # Conservative ABE
      file_AB[[paste0("uniquestub_match", i)]] <- file_AB$pcount_B + file_AB$mcount_B
      file_AB[[paste0("uniquestub_match", i)]] <- ifelse(file_AB$file_A != 1,
                                                         NA,
                                                         file_AB[[paste0("uniquestub_match", i)]])

      # Drop temporary columns
      file_AB$pcount_B    <- NULL
      file_AB$pcount_A    <- NULL
      file_AB$mcount_B    <- NULL
      file_AB$mcount_A    <- NULL
      file_AB$unmatched_A <- NULL
    }
  }

  # Drop unmatched in A
  file_AB$drop <- ifelse(is.na(file_AB$matched_at_A) & file_AB$file_A == 1, 1, 0)
  file_AB      <- subset(file_AB, drop == 0)
  file_AB$drop <- NULL

  # Conservative ABE
  if (uniqueband_match > timeband) {
    start <- timeband + 1
    for (i in start:uniqueband_match) {
      file_AB[[paste0("ubyear_m", i)]] <- file_AB$byear - i
      file_AB[[paste0("ubyear_p", i)]] <- file_AB$byear + i
      file_AB[[paste0("ubyear_m", i)]] <- ifelse(file_AB$file_B == 1,
                                                 file_AB$byear,
                                                 file_AB[[paste0("ubyear_m", i)]])
      file_AB[[paste0("ubyear_p", i)]] <- ifelse(file_AB$file_B == 1,
                                                 file_AB$byear,
                                                 file_AB[[paste0("ubyear_p", i)]])
      fname <- "fname"
      lname <- "lname"
      ubyear_mi <- paste0("ubyear_m", i)
      ubyear_pi <- paste0("ubyear_p", i)
      file_AB <- file_AB %>%
        dplyr::group_by_(fname, lname, ubyear_mi) %>%
        dplyr::mutate(unique_m = sum(file_B)) %>%
        dplyr::ungroup()
      file_AB <- file_AB %>%
        dplyr::group_by_(fname, lname, ubyear_pi) %>%
        dplyr::mutate(unique_p = sum(file_B)) %>%
        dplyr::ungroup()
      file_AB[[paste0("uniquestub_match", i)]] <- file_AB$unique_m + file_AB$unique_p
      file_AB[[paste0("uniquestub_match", i)]] <- ifelse(file_AB$file_A != 1,
                                                         NA,
                                                         file_AB[[paste0("uniquestub_match", i)]])
      file_AB[[paste0("ubyear_m", i)]] <- NULL
      file_AB[[paste0("ubyear_p", i)]] <- NULL
      file_AB$unique_m <- NULL
      file_AB$unique_p <- NULL
    }
  }
  # NOTES
  # 1. This chunk is not tested yet.

  # Conservative ABE
  for (i in 1:uniqueband_match) {
    j <- i - 1
    file_AB[[paste0("uniquestub_match", i)]] <- file_AB[[paste0("uniquestub_match", i)]] +
                                                file_AB[[paste0("uniquestub_match", j)]]
  }
  for (i in 1:uniqueband_match) {
    file_AB[[paste0("uniquestub_match", i)]] <-
      ifelse(file_AB$file_A == 1,
             file_AB[[paste0("uniquestub_match", i)]] <= 1,
             file_AB[[paste0("uniquestub_match", i)]])
  }
  file_AB$uniquestub_match0 <- NULL

  # Drop unmatched in A
  file_AB$drop <- ifelse(is.na(file_AB$matched_at_A) & file_AB$file_A == 1, 1, 0)
  file_AB      <- subset(file_AB, drop == 0)
  file_AB$drop <- NULL

  # Create timevar_keep1
  file_AB$timevar_keep1 <- ifelse(file_AB$file_A == 1, file_AB$byear + file_AB$matched_at_A, file_AB$byear)

  # Make sure only two individuals per matched pair
  file_AB <- file_AB %>%
    dplyr::group_by(fname, lname, timevar_keep1) %>%
    dplyr::mutate(count_A = sum(file_A), count_B = sum(file_B)) %>%
    dplyr::ungroup()
  file_AB <- subset(file_AB, count_A == 1 & count_B == 1)
  file_AB$count_A <- NULL
  file_AB$count_B <- NULL

  # Rename columns if backward direction (list updated for conservative ABE)
  if (backward) {
    colnames(file_AB) <- c("fname", "lname", "byear",
                           "id_A", "file_A", "nA", "ID_A", "id_B", "file_B", "nB", "ID_B",
                           "matched_at_B", "exactmatch2", "uniquestub_file1", "uniquestub_file2", "already",
                           "byear_m1", "byear_p1", "exactmatch2_m1", "exactmatch2_p1", "uniquestub_match1",
                           "byear_m2", "byear_p2", "exactmatch2_m2", "exactmatch2_p2", "uniquestub_match2",
                           "timevar_keep2")
  }

  # Return file_AB
  return(file_AB)
}
# NOTES
# 1. Count number of matches at the end.

# Define fix_ids
#' @export
fix_ids <- function(file_AB_processed, timevar_keep, uniqueband_file=2, uniqueband_match=2, backward=FALSE) {
  # Order rows based on fname, lname, timevar_keep, file_A
  file_AB_processed <- file_AB_processed[order(file_AB_processed$fname,           file_AB_processed$lname,
                                               file_AB_processed[[timevar_keep]], file_AB_processed$file_A), ]

  # Fix ID's
  file_AB_processed$ID_A <- ifelse(file_AB_processed$file_B == 1,
                                   data.table::shift(file_AB_processed$ID_A, n=1, fill=NA, type="lead"),
                                   file_AB_processed$ID_A)
  file_AB_processed$ID_B <- ifelse(file_AB_processed$file_A == 1,
                                   data.table::shift(file_AB_processed$ID_B, n=1, fill=NA, type="lag"),
                                   file_AB_processed$ID_B)

  # Conservative ABE
  if (backward) {
    for (i in 1:uniqueband_match) {
      file_AB_processed$temp <- file_AB_processed[[paste0("uniquestub_match", i)]]
      file_AB_processed$min <- NA
      file_AB_processed <- file_AB_processed %>%
        dplyr::group_by(ID_B) %>%
        dplyr::mutate(min = min(temp, na.rm=TRUE)) %>%
        dplyr::ungroup()
      file_AB_processed[[paste0("uniquestub_match", i)]] = file_AB_processed$min
      file_AB_processed$temp <- NULL
      file_AB_processed$min <- NULL
    }
    for (i in 1:uniqueband_file) {
      file_AB_processed$temp <- file_AB_processed[[paste0("uniquestub_file", i)]]
      file_AB_processed$min <- NA
      file_AB_processed <- file_AB_processed %>%
        dplyr::group_by(ID_B) %>%
        dplyr::mutate(min = min(temp, na.rm=TRUE)) %>%
        dplyr::ungroup()
      file_AB_processed[[paste0("uniquestub_file", i)]] = file_AB_processed$min
      file_AB_processed$temp <- NULL
      file_AB_processed$min <- NULL
    }
  } else {
    for (i in 1:uniqueband_match) {
      file_AB_processed$temp <- file_AB_processed[[paste0("uniquestub_match", i)]]
      file_AB_processed$min <- NA
      file_AB_processed <- file_AB_processed %>%
        dplyr::group_by(ID_A) %>%
        dplyr::mutate(min = max(temp, na.rm=TRUE)) %>%
        dplyr::ungroup()
      file_AB_processed[[paste0("uniquestub_match", i, "A")]] = file_AB_processed$min
      file_AB_processed[[paste0("uniquestub_match", i)]] <- NULL
      file_AB_processed$temp <- NULL
      file_AB_processed$min <- NULL
    }
    for (i in 1:uniqueband_file) {
      file_AB_processed$temp <- file_AB_processed[[paste0("uniquestub_file", i)]]
      file_AB_processed$min <- NA
      file_AB_processed <- file_AB_processed %>%
        dplyr::group_by(ID_A) %>%
        dplyr::mutate(min = min(temp, na.rm=TRUE)) %>%
        dplyr::ungroup()
      file_AB_processed[[paste0("uniquestub_file", i, "A")]] = file_AB_processed$min
      file_AB_processed[[paste0("uniquestub_file", i)]] <- NULL
      file_AB_processed$temp <- NULL
      file_AB_processed$min <- NULL
    }
  }

  # Return file_AB_processed
  return(file_AB_processed)
}
# NOTES
# 1. Add code chunk to check whether either direction has 0 rows.

#' Match records
#'
#' This function matches records in dataset A to records in dataset B
#' using the ABE method. The matched dataset includes both standard
#' and conservative ABE matches.
#'
#' @param file_A           Dataset A
#' @param fname_var_A      First name column in dataset A
#' @param lname_var_A      Last name column in dataset A
#' @param time_var_A       Time column in dataset A
#' @param id_var_A         ID column in dataset A
#' @param vars_to_keep_A   Other columns to be kept from dataset A
#' @param file_B           Dataset B
#' @param fname_var_B      First name column in dataset B
#' @param lname_var_B      Last name column in dataset B
#' @param time_var_B       Time column in dataset B
#' @param id_var_B         ID column in dataset B
#' @param vars_to_keep_B   Other columns to be kept from dataset B
#' @param out_path         Path to the directory where the output file will be saved
#' @param out_file_name    Output filename
#' @param timeband         Time band used when searching for nonexact matches
#' @param uniqueband_file  Uniqueness band used for conservative ABE (within)
#' @param uniqueband_match Uniqueness band used for conservative ABE (between)
#' @return               NULL
#' @export
match_records <- function(file_A, fname_var_A, lname_var_A, time_var_A, id_var_A, vars_to_keep_A,
                          file_B, fname_var_B, lname_var_B, time_var_B, id_var_B, vars_to_keep_B,
                          out_path, out_file_name, timeband=2, uniqueband_file=2, uniqueband_match=2) {
  # Forward pass
  file_A_forward            <- preprocess_A(file_A, fname_var_A, lname_var_A, time_var_A, id_var_A)
  file_B_forward            <- preprocess_B(file_B, fname_var_B, lname_var_B, time_var_B, id_var_B)
  file_AB_forward           <- append_A_to_B(file_A_forward, file_B_forward, uniqueband_file, FALSE)
  file_AB_processed_forward <- find_nonexact_matches(file_AB_forward, timeband, uniqueband_match, FALSE)

  # Backward pass
  file_A_backward            <- preprocess_A(file_B, fname_var_B, lname_var_B, time_var_B, id_var_B)
  file_B_backward            <- preprocess_B(file_A, fname_var_A, lname_var_A, time_var_A, id_var_A)
  file_AB_backward           <- append_A_to_B(file_A_backward, file_B_backward, uniqueband_file, TRUE)
  file_AB_processed_backward <- find_nonexact_matches(file_AB_backward, timeband, uniqueband_match, TRUE)

  # Fix ID's
  forward  <- fix_ids(file_AB_processed_forward,  "timevar_keep1", uniqueband_file, uniqueband_match, FALSE)
  backward <- fix_ids(file_AB_processed_backward, "timevar_keep2", uniqueband_file, uniqueband_match, TRUE)

  # Keep only necessary columns (list updated for conservative ABE)
  forward  <- forward[, c("ID_A", "ID_B", "file_A",           "matched_at_A",
                          "uniquestub_match1A", "uniquestub_file1A", "uniquestub_match2A", "uniquestub_file2A")]
  backward <- backward[,c("ID_A", "ID_B", "file_A", "file_B", "matched_at_B",
                          "uniquestub_match1",  "uniquestub_file1",  "uniquestub_match2",  "uniquestub_file2")]

  # Join backward to forward
  final <- dplyr::inner_join(backward, forward, by = c("ID_A", "ID_B", "file_A"))
  # NOTES
  # 1. Add code chunk to check whether final has 0 rows.

  # Split final to A and B and create timediff
  final_A <- subset(final, file_A == 1)
  final_B <- subset(final, file_B == 1)
  final_A$timediff_A <- final_A$matched_at_A
  final_B$timediff_B <- final_B$matched_at_B
  final_A$matched_at_A <- NULL
  final_A$matched_at_B <- NULL
  final_B$matched_at_A <- NULL
  final_B$matched_at_B <- NULL
  final_A$file_A       <- NULL
  final_A$file_B       <- NULL
  final_B$file_A       <- NULL
  final_B$file_B       <- NULL

  # Merge back A
  file_A           <- file_A[,c(fname_var_A, lname_var_A, time_var_A, id_var_A, vars_to_keep_A)]
  file_A           <- file_A[order(file_A[[fname_var_A]], file_A[[lname_var_A]], file_A[[time_var_A]]), ]
  file_A$nA        <- 1:nrow(file_A)
  file_A$ID_A      <- file_A$nA
  file_A           <- file_A[,c("ID_A", id_var_A, fname_var_A, lname_var_A, time_var_A, vars_to_keep_A)]
  colnames(file_A) <- c("ID_A", "id_A", fname_var_A, lname_var_A, time_var_A, paste0(vars_to_keep_A, "_A"))
  file_A           <- dplyr::inner_join(file_A, final_A, by = c("ID_A"))

  # Merge back B
  file_B           <- file_B[,c(fname_var_B, lname_var_B, time_var_B, id_var_B, vars_to_keep_B)]
  file_B           <- file_B[order(file_B[[fname_var_B]], file_B[[lname_var_B]], file_B[[time_var_B]]), ]
  file_B$nB        <- 1:nrow(file_B)
  file_B$ID_B      <- file_B$nB
  file_B           <- file_B[,c("ID_B", id_var_B, vars_to_keep_B)]
  colnames(file_B) <- c("ID_B", "id_B", paste0(vars_to_keep_B, "_B"))
  file_B           <- dplyr::inner_join(file_B, final_B, by = c("ID_B"))

  # Merge A to B
  file_A           <- file_A[,c("ID_A", "id_A", fname_var_A, lname_var_A, time_var_A, paste0(vars_to_keep_A, "_A"), "timediff_A",
                                "uniquestub_match1A", "uniquestub_file1A", "uniquestub_match2A", "uniquestub_file2A",
                                "uniquestub_match1",  "uniquestub_file1",  "uniquestub_match2",  "uniquestub_file2")]
  file_B           <- file_B[,c("ID_A", "id_B",                                       paste0(vars_to_keep_B, "_B"), "timediff_B")]
  res              <- dplyr::inner_join(file_B, file_A, by = c("ID_A"))
  res$ID_A         <- NULL

  # Conservative ABE
  for (i in 1:uniqueband_match) {
    res[[paste0("uniquestub_match", i)]] <-
      ifelse(res[[paste0("uniquestub_match", i, "A")]] < res[[paste0("uniquestub_match", i)]],
             res[[paste0("uniquestub_match", i, "A")]],
             res[[paste0("uniquestub_match", i)]])
    res[[paste0("uniquestub_match", i, "A")]] <- NULL
  }
  for (i in 1:uniqueband_file) {
    res[[paste0("uniquestub_file", i)]] <-
      ifelse(res[[paste0("uniquestub_file", i, "A")]] < res[[paste0("uniquestub_file", i)]],
             res[[paste0("uniquestub_file", i, "A")]],
             res[[paste0("uniquestub_file", i)]])
    res[[paste0("uniquestub_file", i, "A")]] <- NULL
  }

  # Save dataset
  dir.create(out_path, showWarnings = FALSE)
  path_to_out_file <- paste(out_path, out_file_name, sep="/")
  readr::write_csv(res, path_to_out_file)

  # Clean up after yourself
  rm(list = ls())

}
