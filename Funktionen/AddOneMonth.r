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
        return(as.POSIXct(paste0(year(date) + 1, "-01-01")))
    } else {
        return(as.POSIXct(paste0(year(date), "-", sprintf("%02d", month(date) + 1), "-01")))
    }
}
