#' Versucht, Anomalien in Bitcoin-Kursverläufen automatisch
#' zu erkennen und zu bereinigen.
#' Zur exemplarischen Veranschaulichung solcher Anomalien siehe bspw.
#'   `Auffälligkeiten visualisieren.r`.


# Bibliotheken und Funktionen laden -------------------------------------------
source("Klassen/Dataset.r")
source("Funktionen/AppendToDataTable.r")
source("Funktionen/FormatCurrencyPair.r")
source("Funktionen/FormatDuration.r")
source("Funktionen/FormatNumber.r")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.r")
source("Funktionen/ReadTickDataAsMovingWindow.r")
source("Funktionen/printf.r")
library("fst")
library("data.table")
library("ggplot2")
library("ggthemes")
library("zoo") # rollapply


# Konfiguration  --------------------------------------------------------------
relativeThreshold <- 0.3
absoluteThreshold <- 2000
absoluteThresholdJPY <- 200000


# Hilfsfunktionen

#' Zeitraum einer Börse betrachten und mit anderen Börsen vergleichen
#'
#' Lädt Daten um den angegebenen Zeitpunkt herum und stellt diese
#' grafisch dar.
#' 
#' @param exchange Name der Börse
#' @param referenceExchange Name einer Vergleichsbörse
#' @param currencyPair Kurspaar (z.B. btcusd)
#' @param appearanceDate Datum/Zeitpunkt der Anomalie
#' @param interval Betrachtungsintervall. Standard: Eine Minute
analyseAnomaly <- function(
    exchange,
    referenceExchange,
    currencyPair,
    appearanceDate,
    interval = 60L
) {
    
    # Parameter validieren
    stopifnot(
        is.character(exchange), length(exchange) == 1L,
        is.character(referenceExchange), length(referenceExchange) == 1L,
        is.character(currencyPair), length(currencyPair) == 1L,
        is.POSIXct(appearanceDate), length(appearanceDate) == 1L,
        is.numeric(interval), length(interval) == 1L
    )
    
    # Daten laden
    loadFrom <- appearanceDate - interval
    loadTo <- appearanceDate + interval + 60
    
    
    # Untersuchte Börse
    sourceFile <- sprintf(
        "Cache/%s/%s/tick/%1$s-%2$s-tick-%d-%02d.fst",
        exchange, tolower(currencyPair), year(loadFrom), month(loadFrom)
    )
    printf("Lade %s\n", sourceFile)
    dataset_exchange <- read_fst(
        sourceFile,
        columns=c("Time", "Price"),
        as.data.table=TRUE
    )
    
    # Nächsten Monat auch noch laden
    if (month(loadTo) != month(loadFrom)) {
        sourceFile <- sprintf(
            "Cache/%s/%s/tick/%1$s-%2$s-tick-%d-%02d.fst",
            exchange, tolower(currencyPair), year(loadTo), month(loadTo)
        )
        printf("Lade %s\n", sourceFile)
        dataset_exchange_next_month <- read_fst(
            sourceFile,
            columns=c("Time", "Price"),
            as.data.table=TRUE
        )
        dataset_exchange <- rbindlist(list(dataset_exchange, dataset_exchange_next_month))
        rm(dataset_exchange_next_month)
    }
    
    # Filtern
    dataset_exchange <- dataset_exchange[Time %between% c(loadFrom, loadTo)]
    
    # Börse hinterlegen
    dataset_exchange[, Exchange:=exchange]
    
    
    # Vergleichsbörse
    sourceFile <- sprintf(
        "Cache/%s/%s/tick/%1$s-%2$s-tick-%d-%02d.fst",
        referenceExchange, tolower(currencyPair), year(loadFrom), month(loadFrom)
    )
    printf("Lade %s\n", sourceFile)
    dataset_reference <- read_fst(
        sourceFile,
        columns=c("Time", "Price"),
        as.data.table=TRUE
    )
    
    # Nächsten Monat auch noch laden
    if (month(loadTo) != month(loadFrom)) {
        sourceFile <- sprintf(
            "Cache/%s/%s/tick/%1$s-%2$s-tick-%d-%02d.fst",
            referenceExchange, tolower(currencyPair), year(loadTo), month(loadTo)
        )
        printf("Lade %s\n", sourceFile)
        dataset_reference_next_month <- read_fst(
            sourceFile,
            columns=c("Time", "Price"),
            as.data.table=TRUE
        )
        dataset_reference <- rbindlist(list(dataset_reference, dataset_reference_next_month))
        rm(dataset_reference_next_month)
    }
    
    # Filtern
    dataset_reference <- dataset_reference[Time %between% c(loadFrom, loadTo)]
    
    # Börse hinterlegen
    dataset_reference[, Exchange:=referenceExchange]
    
    
    # Zu einem gemeinsamen Datensatz verbinden
    dataset <- rbindlist(list(dataset_exchange, dataset_reference))
    rm(dataset_reference)
    gc()
    
    # Sortieren
    setorder(dataset, Time)
    dataset[, Exchange:=factor(Exchange, levels=c(exchange, referenceExchange))]
    
    # Grafik zeichnen
    print(
        ggplot(dataset, aes(x=Time, y=Price)) +
            # Referenzlinie 1
            geom_vline(
                xintercept=appearanceDate, 
                color="red", 
                alpha=.3,
                linetype="dashed",
                size=.5
            ) +
            # Referenzlinie 2, da auf Minutenbasis gerundet
            geom_vline(
                xintercept=appearanceDate + 60, 
                color="red", 
                alpha=.3,
                linetype="dashed",
                size=.5
            ) +
            geom_line(aes(colour=Exchange, linetype=Exchange)) +
            theme_minimal() +
            scale_x_datetime(
                #date_breaks="10 min",
                #date_minor_breaks="1 minute",
                date_labels="%H%:%M",
                expand = expansion(mult = c(.01, .03))
            ) +
            scale_y_continuous(
                labels = function(x) format.money(x, digits=0)
            ) +
            scale_color_ptol() +
            labs(
                title=sprintf("%s %s zwischen %s und %s",
                              exchange,
                              format.currencyPair(currencyPair),
                              format(appearanceDate, "%d.%m.%Y %H:%M:%S"),
                              format(appearanceDate+60, "%d.%m.%Y %H:%M:%S")),
                x="Zeit",
                y="Preis"
            )
    )
    
    return(dataset_exchange)
}


# Hauptfunktion ---------------------------------------------------------------

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
    dataset <- new(
        "Dataset",
        Exchange = exchange,
        CurrencyPair = currencyPair,
        PathPrefix = sprintf("Cache/%s/%s/tick/%1$s-%2$s-tick",
                             exchange, tolower(currencyPair)),
        EndDate = endDate,
        data = data.table()
        # Keine Ausreißer entfernen
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
           format.currencyPair(currencyPair), exchange, 
           format(startDate, "%d.%m.%Y %H:%M:%S"))
    printf("  % 13s   % 11s   %-26s   % 10s   % 10s\n",
           "Laufzeit", "Verarbeitet", "Aktueller Datensatz",
           "Auffällig", "Geschw.")
    
    while (TRUE) {
        
        # Daten laden
        loadUntil <- startDate + 60 * 60
        
        # Ende erreicht
        if (loadUntil > endDate) {
            loadUntil <- endDate
            endAfterCurrentDataset <- TRUE
            printf.debug("Datenende erreicht, Stop nach aktuellem Monat.\n")
        }
        
        readTickDataAsMovingWindow(dataset, startDate, loadUntil, numDatasetsPerRead = 20000L)
        runtime <- as.integer(proc.time()["elapsed"] - now)
        #           Runtime nInput  Time    nResult Speed  
        printf("\r  % 13s   % 11s   % 26s   % 10s   % 6s T/s",
               format.duration(runtime),
               format.number(processedDatasets),
               formatPOSIXctWithFractionalSeconds(last(dataset$data$Time), "%d.%m.%Y %H:%M:%OS"),
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


# Anomalien finden und speichern ----------------------------------------------

# Händisch starten
if (FALSE) {
    
    # Bitfinex
    # BTC/USD: 171.633.442 Ticks, davon 54 auffällige Zeitpunkte
    bitfinex.btcusd <- findPriceAnomalies("bitfinex", "btcusd", as.POSIXct("2013-01-01"))
    
    # BTC/EUR: 21.291.120 Ticks, dabei keine auffälligen Zeitpunkte
    bitfinex.btceur <- findPriceAnomalies("bitfinex", "btceur", as.POSIXct("2019-09-01"))
    
    # BTC/GBP: 15.128.461 Ticks, dabei keine auffälligen Zeitpunkte
    bitfinex.btcgbp <- findPriceAnomalies("bitfinex", "btcgbp", as.POSIXct("2018-03-01"))
    
    # BTC/JPY: 21.090.315, davon 3 auffällige Zeitpunkte
    bitfinex.btcjpy <- findPriceAnomalies("bitfinex", "btcjpy", as.POSIXct("2018-03-01"))
    
    
    # Bitstamp
    # BTC/USD: 58.123.079 Ticks, davon 63 auffällige Zeitpunkte
    bitstamp.btcusd <- findPriceAnomalies("bitstamp", "btcusd", as.POSIXct("2011-08-18"))
    
    # BTC/EUR: 22.704.977 Ticks, davon 5 auffällige Zeitpunkte
    bitstamp.btceur <- findPriceAnomalies("bitstamp", "btceur", as.POSIXct("2016-04-16"))
    
    # BTC/GBP: 1.084.564 Ticks, davon 8 auffällige Zeitpunkte
    bitstamp.btcgbp <- findPriceAnomalies("bitstamp", "btcgbp", as.POSIXct("2020-05-28"))
    
    
    # Coinbase Pro
    # BTC/USD: 262.356.683 Ticks, davon 453 auffällige Zeitpunkte
    coinbase.btcusd <- findPriceAnomalies("coinbase", "btcusd", as.POSIXct("2014-12-01"))
    
    # BTC/EUR: 58.033.711 Ticks, davon 5 auffällige Zeitpunkte
    coinbase.btceur <- findPriceAnomalies("coinbase", "btceur", as.POSIXct("2015-04-01"))
    
    # BTC/GBP: 26.637.863 Ticks, davon 11 auffällige Zeitpunkte
    coinbase.btcgbp <- findPriceAnomalies("coinbase", "btcgbp", as.POSIXct("2015-04-01"))
    
    
    # Kraken
    # BTC/USD: 45.031.172 Ticks, davon 28 auffällige Zeitpunkte
    kraken.btcusd <- findPriceAnomalies("kraken", "btcusd", as.POSIXct("2013-10-01"))
    
    # BTC/EUR: 64.523.617 Ticks, davon 16 auffällige Zeitpunkte
    kraken.btceur <- findPriceAnomalies("kraken", "btceur", as.POSIXct("2013-09-01"))
    
    # BTC/GBP: 1.754.149 Ticks, davon 214 auffällige Zeitpunkte
    # Siehe zB: https://status.kraken.com/incidents/nswthr1lyx72
    kraken.btcgbp <- findPriceAnomalies("kraken", "btcgbp", as.POSIXct("2014-11-01"))
    
    # BTC/JPY: 530.055 Ticks, davon 234 auffällige Zeitpunkte
    kraken.btcjpy <- findPriceAnomalies("kraken", "btcjpy", as.POSIXct("2014-11-01"))
    
    # BTC/CAD: 2.076.761 Ticks, davon 205 auffällige Zeitpunkte
    kraken.btccad <- findPriceAnomalies("kraken", "btccad", as.POSIXct("2015-06-01"))
    
    # BTC/CHF: 950.639 Ticks, 49 auffällige Zeitpunkte
    kraken.btcchf <- findPriceAnomalies("kraken", "btcchf", as.POSIXct("2019-12-01"))
    
    # BTC/AUD: 387.311 Ticks, 114 auffällige Zeitpunkte
    kraken.btcaud <- findPriceAnomalies("kraken", "btcaud", as.POSIXct("2020-06-01"))
}
