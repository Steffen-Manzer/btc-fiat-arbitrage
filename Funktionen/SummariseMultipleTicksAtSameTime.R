# Bibliotheken laden
library("data.table") # .[ (Gruppierungsfunktion)

#' Mehrfache Ticks mit der exakt selben Zeit zusammenfassen
#' 
#' Falls `dataset` partiell eingelesen wird, muss darauf geachtet 
#' werden, dass stets sämtliche Ticks der selben Zeit vorliegen.
#' 
#' @param dataset Eine `data.table` mit den Spalten `Time`, `Price`,
#'                `Exchange` oder `CurrencyPair` sowie `RowNum`
#' @return `data.table` Wie `dataset`, nur mit gruppierten Zeitpunkten
summariseMultipleTicksAtSameTime <- function(dataset) {
    if (!is.null(dataset$Exchange)) {
        return(dataset[
            j = .(
                PriceLow = min(Price), PriceHigh = max(Price),
                Exchange = last(Exchange),
                RowNum = last(RowNum), n = .N
            ),
            by = Time
        ])
    } else if (!is.null(dataset$CurrencyPair)) {
        return(dataset[
            j = .(
                PriceLow = min(Price), PriceHigh = max(Price),
                CurrencyPair = last(CurrencyPair),
                RowNum = last(RowNum), n = .N
            ),
            by = Time
        ])
    } else {
        return(dataset[
            j = .(
                PriceLow = min(Price), PriceHigh = max(Price),
                RowNum = last(RowNum), n = .N
            ),
            by = Time
        ])
    }
}
