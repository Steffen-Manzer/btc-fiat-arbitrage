# Bibliotheken laden
library("data.table") # .[ (Gruppierungsfunktion)

#' Mehrfache Ticks mit der exakt selben Zeit zusammenfassen
#' 
#' Falls `dataset` partiell eingelesen wird, muss darauf geachtet 
#' werden, dass stets sämtliche Ticks der selben Zeit vorliegen.
#' 
#' @param dataset Eine `data.table` mit den Spalten `Time`, `Price`,
#'                `Exchange` oder `CurrencyPair` sowie `RowNum`
#' @param idColumn Name der Spalte, die diesen Datensatz identifiziert
#' @return `data.table` Wie `dataset`, nur mit gruppierten Zeitpunkten
summariseMultipleTicksAtSameTime <- function(dataset, idColumn = "Exchange") {
    if (idColumn == "Exchange") {
        return(dataset[
            j = .(
                PriceLow = min(Price),
                PriceHigh = max(Price),
                Exchange = last(Exchange),
                RowNum = last(RowNum),
                n = .N
            ),
            by = Time
        ])
    } else if (idColumn == "CurrencyPair") {
        return(dataset[
            j = .(
                PriceLow = min(Price),
                PriceHigh = max(Price),
                CurrencyPair = last(CurrencyPair),
                RowNum = last(RowNum),
                n = .N
            ),
            by = Time
        ])
    } else {
        stop("Unbekannte ID-Spalte.")
    }
}
