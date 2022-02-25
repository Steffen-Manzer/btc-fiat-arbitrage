loadSuspiciousPeriods <- function(exchange, currencyPair)
{
    allExchangeMetadata <- fromJSON(file="Daten/bitcoin-metadata.json")
    
    exchangeMetadata <- allExchangeMetadata[[exchange]]
    stopifnot(!is.null(exchangeMetadata))
    
    pairMetadata <- exchangeMetadata[[currencyPair]]
    stopifnot(!is.null(pairMetadata))
    
    suspiciousPeriods <- data.table()
    for (i in seq_along(pairMetadata$suspiciousPeriods)) {
        period <- pairMetadata$suspiciousPeriods[[i]]
        if (isFALSE(period$filter)) {
            next
        }
        suspiciousPeriods <- rbind(suspiciousPeriods, data.table(
            startDate = as.POSIXct(period$startDate, tz="UTC"),
            endDate = as.POSIXct(period$endDate, tz="UTC")
        ))
    }
    
    return(suspiciousPeriods)
}
