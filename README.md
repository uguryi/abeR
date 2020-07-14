# abeR

This package implements the ABE method for historical record linking ([2012](https://ranabr.people.stanford.edu/sites/g/files/sbiybj5391/f/abe_ageofmassmigration.pdf), [2014](https://ranabr.people.stanford.edu/sites/g/files/sbiybj5391/f/abe_assimilation_1.pdf), [2017](https://ranabr.people.stanford.edu/sites/g/files/sbiybj5391/f/return-migrants.pdf)). The original Stata implementation of the method can be found [here](https://ranabr.people.stanford.edu/matching-codes).

NYSIIS name standardization and Jaro-Winkler adjustment are currently not supported by this package.

See `vignettes/abeR-demo.Rmd` for a demonstration of how to use the `abeR` package.

The package can be installed using `devtools::install_github("uguryi/abeR")`.
