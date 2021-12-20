#' Datum auf den ersten Tag des nächsten Monats setzen
addOneMonth <- function(date)
{
    # Bibliotheken laden
    library("lubridate") # is.POSIXct
    
    # Parameter validieren
    stopifnot(
        length(date) == 1,
        is.POSIXct(date)
    )
    
    # Datum auf den ersten Tag des nächsten Monats setzen
    if (month(date) == 12) {
        return(as.POSIXct(paste0(year(date) + 1, "-01-01")))
    } else {
        return(as.POSIXct(paste0(year(date), "-", sprintf("%02d", month(date) + 1), "-01")))
    }
}
