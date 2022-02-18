# Bibliotheken laden
library("data.table") # .[ (Gruppierungsfunktion)

#' Mehrfache Ticks mit der exakt selben Zeit zusammenfassen
#' 
#' Falls `dataset` partiell eingelesen wird, muss zwingend darauf
#' geachtet werden, dass immer sämtliche Ticks der selben Zeit
#' vorliegen.
#' 
#' @param dataset Eine `data.table` mit den Spalten
#'                `ID`, `Time`, `Price`, `Exchange` und `RowNum`
#' @return `data.table` Wie `dataset`, nur mit gruppierten Zeitpunkten
summariseMultipleTicksAtSameTime <- function(dataset) {
    return(dataset[
        j=.(
            IDLow = ID[which.min(Price)],
            PriceLow = min(Price),
            IDHigh = ID[which.max(Price)],
            PriceHigh = max(Price),
            Exchange = last(Exchange),
            RowNum = last(RowNum),
            n = .N
        ), 
        by=Time
    ])
}
