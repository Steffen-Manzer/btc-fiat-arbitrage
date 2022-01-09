#' Versucht, Anomalien in Bitcoin-Kursverläufen automatisch
#' zu erkennen und zu bereinigen.
#' Zur exemplarischen Veranschaulichung solcher Anomalien siehe bspw.
#'   `Auffälligkeiten visualisieren.r`.


# Bibliotheken und Funktionen laden ===========================================
source("Klassen/Dataset.r")
source("Funktionen/AppendToDataTable.r")
source("Funktionen/FormatCurrencyPair.r")
source("Funktionen/FormatDuration.r")
source("Funktionen/FormatNumber.r")
source("Funktionen/ReadAndAppendNewTickData.r")
source("Funktionen/printf.r")
library("fst")
library("data.table")
library("zoo") # rollapply


# Konfiguration
relativeThreshold <- 0.3
absoluteThreshold <- 2000
absoluteThresholdJPY <- 200000


# Hauptfunktion ===============================================================

#' Anomalien an einer Börse finden
#' 
#' @param exchange Name der Börse
#' @param currencyPair Kurspaar (z.B. btcusd)
#' @param startDate Beginne Analyse ab diesem Datum
#'                  (= Zeitpunkt des ersten Datensatzes)
#' @param readFromCache Zwischengespeicherte Ergebnisse laden, falls vorhanden
#' @return `data.table` mit auffälligen Zeitpunkten
findPriceAnomalies <- function(
    exchange, 
    currencyPair,
    startDate,
    readFromCache = TRUE
) {
    
    # Parameter validieren
    stopifnot(
        is.character(exchange), length(exchange) == 1L,
        is.character(currencyPair), length(currencyPair) == 1L,
        is.POSIXct(startDate), length(startDate) == 1L
    )
    
    # In Cache vorhanden
    cacheFile <- sprintf("Cache/Anomalien/%s-%s.fst", exchange, currencyPair)
    if (isTRUE(readFromCache) && file.exists(cacheFile)) {
        printf("Lese aus Cache: %s\n", cacheFile)
        return(read_fst(cacheFile, as.data.table=TRUE))
    }
    
    # Bis einschließlich vergangenen Monat vergleichen
    endDate <- as.POSIXct(format(Sys.time(), "%Y-%m-01 00:00:00")) - 1
    stopifnot(startDate < endDate)
    
    # Datenobjekte initialisieren, die später per Referenz übergeben
    # werden können. So kann direkt an den Daten gearbeitet werden,
    # ohne dass immer eine Kopie angelegt werden muss.
    dataset <- new("Dataset",
                   Exchange = exchange,
                   CurrencyPair = currencyPair,
                   PathPrefix = sprintf("Cache/%s/%s/tick/%1$s-%2$s-tick",
                                        exchange, tolower(currencyPair)),
                   EndDate = endDate,
                   data = data.table()
    )
    
    # Schwellwert bestimmen
    if (tolower(substr(currencyPair, 4, 6)) == "jpy") {
        printf("JPY erkannt: Verwende einen Schwellwert von %s\n", 
               format.number(absoluteThresholdJPY))
        thisAbsoluteThreshold <- absoluteThresholdJPY
    } else {
        thisAbsoluteThreshold <- absoluteThreshold
    }
    
    # Flag: Ende der verfügbaren Daten erreicht, keine weiteren Dateien mehr lesen
    endAfterCurrentDataset <- FALSE
    
    # Ergebnisvektor
    result <- data.table()
    
    # Fortschritt aufzeichnen und ausgeben
    processedDatasets <- 0L
    now <- proc.time()["elapsed"]
    
    # Analyse beginnen
    printf("\n  Beginne Anomalie-Analyse für %s der Börse %s ab %s.\n", 
           format.currencyPair(currencyPair), exchange, format(startDate, "%d.%m.%Y %H:%M:%S"))
    printf("  % 13s   % 11s   %-26s   % 10s   % 10s\n",
           "Laufzeit", "Verarbeitet", "Aktueller Datensatz",
           "Ergebnisse", "Geschw.")
    
    while (TRUE) {
        
        # Daten laden
        loadUntil <- startDate + 60 * 60
        
        # Ende erreicht
        if (loadUntil > endDate) {
            loadUntil <- endDate
            endAfterCurrentDataset <- TRUE
            printf.debug("Datenende erreicht, Stop nach aktuellem Monat.\n")
        }
        
        readAndAppendNewTickData(dataset, startDate, loadUntil, numDatasetsPerRead = 20000L)
        runtime <- as.integer(proc.time()["elapsed"] - now)
        #           Runtime nInput  Time    nResult Speed  
        printf("\r  % 13s   % 11s   % 26s   % 10s   % 6s T/s",
               format.duration(runtime),
               format.number(processedDatasets),
               format(last(dataset$data$Time), "%d.%m.%Y %H:%M:%OS"),
               format.number(nrow(result)),
               format.number(round(processedDatasets/runtime, 0))
        )
        
        if (nrow(dataset$data) < 10) {
            printf("\nWeniger als 10 Datensätze ab %s, vermutlich Ende erreicht.\n",
                   format(startDate, "%d.%m.%Y, %H:%M:%S"))
            break
        }
        
        # Verdächtige Datensätze finden:
        # Mehr als 30 % / 2.000 Einheiten Preisänderung innerhalb von 9 Ticks
        conspicuous <-c(
            rep(FALSE, 4), # Erste Ticks können nicht richtig analysiert werden
            rollapply(
                dataset$data$Price,
                width = 9,
                FUN = function(prices)
                    ((max(prices) - min(prices)) / max(prices) > relativeThreshold) ||
                    (max(prices) - min(prices)) > thisAbsoluteThreshold
            ),
            rep(FALSE, 4) # Letzte Ticks können nicht richtig analysiert werden
        )
        
        # Verdächtige Preisänderungen gefunden
        if (any(conspicuous)) {
            result <- rbindlist(list(result, dataset$data[conspicuous]))
        }
        
        processedDatasets = processedDatasets + nrow(dataset$data)
        
        if (endAfterCurrentDataset) {
            break
        }
        
        # Neuen Startzeitpunkt festlegen
        startDate <- last(dataset$data$Time)
        printf.debug("\n")
    }
    
    # Erfreulich: Keine auffälligen Daten gefunden
    if (nrow(result) == 0L) {
        printf("\n  Keine auffälligen Zeitpunkte / Ticks gefunden.\n")
        
        # Leere data.table in Cache speichern
        write_fst(data.table(Time=double()), cacheFile, compress=100)
        
        return(result)
    }
    
    # Ergebnisse auf Minutenbasis aggregieren
    result <- result[
        j=.(
            PriceLow = min(Price),
            PriceHigh = max(Price)
        ), 
        by=floor_date(Time, unit = "minutes")
    ]
    setnames(result, 1, "Time")
    
    printf("\n  %s auffällige Zeitpunkte / Ticks gefunden.\n",
           format.number(nrow(result)))
    
    # In Cache speichern
    write_fst(result, cacheFile, compress=100)
    
    return(result)
}


# Anomalien finden und speichern ==============================================

# Bitfinex
# BTC/USD: 171.633.442 Ticks, davon 54 auffällige Zeitpunkte
findPriceAnomalies("bitfinex", "btcusd", as.POSIXct("2013-01-01"))

# BTC/EUR: 21.291.120 Ticks, dabei keine auffälligen Zeitpunkte
findPriceAnomalies("bitfinex", "btceur", as.POSIXct("2019-09-01"))

# BTC/GBP: 15.128.461 Ticks, dabei keine auffälligen Zeitpunkte
findPriceAnomalies("bitfinex", "btcgbp", as.POSIXct("2018-03-01"))

# BTC/JPY: 21.090.315, davon 3 auffällige Zeitpunkte
findPriceAnomalies("bitfinex", "btcjpy", as.POSIXct("2018-03-01"))


# Bitstamp
# BTC/USD: 57.645.761 Ticks, davon 55 auffällige Zeitpunkte
findPriceAnomalies("bitstamp", "btcusd", as.POSIXct("2011-09-01"))

# BTC/EUR: 20.052.494 Ticks, davon 5 auffällige Zeitpunkte
findPriceAnomalies("bitstamp", "btceur", as.POSIXct("2017-12-01"))

# BTC/GBP: 26.596 Ticks, dabei keine auffälligen Zeitpunkte
findPriceAnomalies("bitstamp", "btcgbp", as.POSIXct("2021-12-01"))


# Coinbase Pro
# BTC/USD: 262.356.683 Ticks, davon 453 auffällige Zeitpunkte
findPriceAnomalies("coinbase", "btcusd", as.POSIXct("2014-12-01"))

# BTC/EUR: 58.033.711 Ticks, davon 5 auffällige Zeitpunkte
findPriceAnomalies("coinbase", "btceur", as.POSIXct("2015-04-01"))

# BTC/GBP: 26.637.863 Ticks, davon 11 auffällige Zeitpunkte
findPriceAnomalies("coinbase", "btcgbp", as.POSIXct("2015-04-01"))


# Kraken
# BTC/USD: 45.031.172 Ticks, davon 28 auffällige Zeitpunkte
findPriceAnomalies("kraken", "btcusd", as.POSIXct("2013-10-01"))

# BTC/EUR: 64.523.617 Ticks, davon 16 auffällige Zeitpunkte
findPriceAnomalies("kraken", "btceur", as.POSIXct("2013-09-01"))

# BTC/GBP: 1.754.149 Ticks, davon 214 auffällige Zeitpunkte
# Siehe zB: https://status.kraken.com/incidents/nswthr1lyx72
findPriceAnomalies("kraken", "btcgbp", as.POSIXct("2014-11-01"))

# BTC/JPY: 530.055 Ticks, davon 234 auffällige Zeitpunkte
findPriceAnomalies("kraken", "btcjpy", as.POSIXct("2014-11-01"))

# BTC/CAD: 2.076.761 Ticks, davon 205 auffällige Zeitpunkte
findPriceAnomalies("kraken", "btccad", as.POSIXct("2015-06-01"))

# BTC/CHF: 950.639 Ticks, 49 auffällige Zeitpunkte
findPriceAnomalies("kraken", "btcchf", as.POSIXct("2019-12-01"))

# BTC/AUD: 387.311 Ticks, 114 auffällige Zeitpunkte
findPriceAnomalies("kraken", "btcaud", as.POSIXct("2020-06-01"))
