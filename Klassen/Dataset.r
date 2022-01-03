# Eigene Klasse definieren, die per Referenz übergeben und somit
# in-place bearbeitet werden kann
library("data.table")
setRefClass(
    "Dataset", 
    fields=list(
        Exchange = "character",
        CurrencyPair = "character",
        PathPrefix = "character",
        EndDate = "POSIXct",
        data = "data.table"
    )
)
