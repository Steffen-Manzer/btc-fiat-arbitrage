#' Datum auf den ersten Tag des nächsten Monats setzen
#' 
#' @author Steffen Manzer
#' @param date Ein beliebiges Datum als `POSIXct`.
#' @return `POSIXct` Mitternacht des ersten Tages des auf `date` folgenden Monats
addOneMonth <- function(date)
{
    # Bibliotheken laden
    library("lubridate") # is.POSIXct
    
    # Parameter validieren
    stopifnot(length(date) == 1, is.POSIXct(date))
    
    # Datum auf den ersten Tag des nächsten Monats setzen
    if (month(date) == 12) {
        return(as.POSIXct(sprintf("%d-01-01", year(date) + 1)))
    } else {
        return(as.POSIXct(sprintf("%d-%02d-01", year(date), month(date) + 1)))
    }
}
