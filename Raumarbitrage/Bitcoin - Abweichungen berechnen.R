#' Finde Kurspaare zur (späteren) Analyse von Raumarbitrage.
#' 
#' Grundablauf: Immer paarweiser Vergleich zweier Börsen mit "Moving Window",
#' ähnlich zum Verfahren des `mergesort`-Algorithmus. Lese neue Daten sequentiell
#' und lösche verarbeitete Daten regelmäßig, um den Bedarf an Arbeitsspeicher
#' verhältnismäßig gering zu halten und somit eine gute Parallelisierbarkeit
#' zu ermöglichen.
#' 
#' Hinweis: Mit der aktuellen Einstellung wird erst nach 100 Mio. gefundenen
#' Preis-Paaren eine neue Datei begonnen und Arbeitsspeicher freigegeben.
#' Damit wird im Maximum bis zu ca. 5 GB Arbeitsspeicher belegt.
#'
#' - Erste x Daten beider Börsen laden = "Betrachtungsfenster" initialisieren:
#'   [t = 0, ..., t = 1h], mindestens jedoch je 10.000 Datenpunkte.
#' - Filtere beide Datensätze auf gemeinsamen Zeitraum.
#' - Liste beider Kurse in eine einzelne Liste vereinen, nach Datum sortieren
#'   und filtern (siehe unten).
#' - Solange Daten für beide Börsen vorhanden sind:
#'   - Nächsten Datensatz vergleichen und Ergebnis speichern
#'   - Prüfen, ob noch genug Daten beider Börsen für Vergleich vorhanden sind, sonst:
#'       - neues Datenfenster laden: [t = 1h, ..., t = 2h]
#'       - Speicher freigeben: Daten des ersten Betrachtungsfensters entfernen (0...1h)
#'       - Letzte paar Datenpunkte des vorherigen Betrachtungsfensters behalten, um
#'         korrekt filtern/vergleichen zu können
#' - Alle 100 Millionen Datenpunkte: Teilergebnisse in einer eigenen Datei speichern,
#'   um Arbeitsspeicher freizugeben.


# Bibliotheken und externe Hilfsfunktionen laden ------------------------------
source("Klassen/Dataset.R")
source("Funktionen/AppendToDataTable.R")
source("Funktionen/FilterTwoDatasetsByCommonInterval.R")
source("Funktionen/FormatCurrencyPair.R")
source("Funktionen/FormatDuration.R")
source("Funktionen/FormatNumber.R")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.R")
source("Funktionen/LoadSuspicousBitcoinPeriods.R")
source("Funktionen/MergeSortAndFilter.R")
source("Funktionen/ReadTickDataAsMovingWindow.R")
source("Funktionen/SummariseMultipleTicksAtSameTime.R")
source("Funktionen/printf.R")
library("fst")
library("data.table")
library("lubridate") # floor_date


# Hilfsfunktionen -------------------------------------------------------------

#' Teilergebnis speichern, um Arbeitsspeicher wieder freizugeben
#' 
#' @param result Eine data.table
#' @param index Nummer dieses Teilergebnisses
#' @param exchange_a Name der ersten Börse
#' @param exchange_b Name der zweiten Börse
#' @param currencyPair Name des Kurspaares
#' @param comparisonThreshold Zeitliche Differenz zweier Ticks in Sekunden,
#'                            ab der das Tick-Paar verworfen wird.
savePartialResult <- function(
    result,
    index,
    exchange_a,
    exchange_b,
    currencyPair,
    comparisonThreshold
) {
    # Parameter validieren
    stopifnot(
        is.data.table(result), nrow(result) >= 1L,
        is.integer(index), length(index) == 1L,
        is.character(exchange_a), length(exchange_a) == 1L,
        is.character(exchange_b), length(exchange_b) == 1L,
        is.character(currencyPair), length(currencyPair) == 1L,
        is.integer(comparisonThreshold), length(comparisonThreshold) == 1L
    )
    
    # Zieldatei bestimmen
    outFile <- sprintf(
        "Cache/Raumarbitrage/%ds/%s-%s-%s-%d.fst",
        comparisonThreshold, tolower(currencyPair), exchange_a, exchange_b, index
    )
    stopifnot(!file.exists(outFile))
    
    # Ergebnis um zwischenzeitlich eingefügte `NA`s bereinigen
    # (= reservierter Speicher für weitere Ergebnisse)
    result <- cleanupDT(result)
    
    # Um Probleme mit appendDT (NA+POSIXct) zu umgehen, wurde der Zeitstempel
    # unten in ein double umgewandelt und wird an dieser Stelle wieder in 
    # POSIXct konvertiert.
    result[, Time:=as.POSIXct(Time, origin="1970-01-01")]
    
    printf("\n\n%s Datensätze in %s (unkomprimiert).",
           format.number(nrow(result)),
           format(object.size(result), units="auto", standard="SI"))
    
    # Ergebnis speichern
    if (!dir.exists(dirname(outFile))) {
        dir.create(dirname(outFile), recursive=TRUE)
    }
    write_fst(result, outFile, compress=100)
    
    return(invisible(NULL))
}


# Haupt-Auswertungsfunktion ---------------------------------------------------

#' Preise zweier Börsen vergleichen
#' 
#' @param exchange_a Name der ersten Börse
#' @param exchange_b Name der zweiten Börse
#' @param currencyPair Kurspaar (z.B. btcusd)
#' @param startDate Beginne Vergleich ab diesem Datum
#'                  (= Zeitpunkt des ersten gemeinsamen Datensatzes)
#' @param comparisonThreshold Zeitliche Differenz zweier Ticks in Sekunden,
#'                            ab der das Tick-Paar verworfen wird.
#' @return `NULL` Ergebnisse werden in mehreren Dateien (i = 1...n) unter
#'   Cache/Raumarbitrage/`threshold`s/`currencyPair`-`exchange_a`-`exchange_b`-`i`.fst
#'   gespeichert (siehe `savePartialResult`).
compareTwoExchanges <- function(
    exchange_a,
    exchange_b,
    currencyPair,
    startDate,
    endDate,
    comparisonThreshold = 5L
) {
    if (!is.POSIXct(startDate)) {
        startDate <- as.POSIXct(startDate)
    }
    
    # Parameter validieren
    stopifnot(
        is.character(exchange_a), length(exchange_a) == 1L,
        is.character(exchange_b), length(exchange_b) == 1L,
        is.character(currencyPair), length(currencyPair) == 1L,
        length(startDate) == 1L,
        is.POSIXct(endDate), length(endDate) == 1L,
        startDate < endDate,
        is.integer(comparisonThreshold), length(comparisonThreshold) == 1L
    )
    
    # Ergebnisdatei existiert bereits
    firstOutputFile <- sprintf(
        "Cache/Raumarbitrage/%ds/%s-%s-%s-1.fst",
        comparisonThreshold, tolower(currencyPair), exchange_a, exchange_b
    )
    if (file.exists(firstOutputFile)) {
        printf("Zieldatei %s bereits vorhanden!\n", firstOutputFile)
        return(invisible(NULL))
    }
    
    # Datenobjekte initialisieren, die später per Referenz übergeben
    # werden können. So kann direkt an den Daten gearbeitet werden,
    # ohne dass immer eine Kopie angelegt werden muss.
    dataset_a <- new("Dataset",
        Exchange = exchange_a,
        CurrencyPair = currencyPair,
        PathPrefix = sprintf("Cache/%s/%s/tick/%1$s-%2$s-tick",
                             exchange_a, tolower(currencyPair)),
        EndDate = endDate,
        data = data.table()
        # [27.04.2022] Keine Filterung mehr vornehmen
        #SuspiciousPeriods = loadSuspiciousPeriods(exchange_a, currencyPair)
    )
    
    dataset_b <- new("Dataset",
        Exchange = exchange_b,
        CurrencyPair = currencyPair,
        PathPrefix = sprintf("Cache/%s/%s/tick/%1$s-%2$s-tick",
                             exchange_b, tolower(currencyPair)),
        EndDate = endDate,
        data = data.table()
        # [27.04.2022] Keine Filterung mehr vornehmen
        #SuspiciousPeriods = loadSuspiciousPeriods(exchange_b, currencyPair)
    )
    
    # -- Diese Schritte werden regelmäßig wiederholt, um sequentiell weitere Daten zu laden --
    
    # Daten für die ersten 60 Minuten beider Börsen laden
    loadUntil <- startDate + 60 * 60
    readTickDataAsMovingWindow(dataset_a, startDate, loadUntil)
    readTickDataAsMovingWindow(dataset_b, startDate, loadUntil)
    printf.debug("A: %d Tickdaten von %s bis %s\n",
                 nrow(dataset_a$data), first(dataset_a$data$Time), last(dataset_a$data$Time))
    printf.debug("B: %d Tickdaten von %s bis %s\n",
                 nrow(dataset_b$data), first(dataset_b$data$Time), last(dataset_b$data$Time))
    
    # Begrenze auf gemeinsamen Zeitraum. Beschleunigt anschließendes
    # Merge + Sort + Filter je nach Struktur der Daten deutlich.
    filterTwoDatasetsByCommonTimeInterval(dataset_a, dataset_b)
    printf.debug("A (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n",
                 nrow(dataset_a$data), first(dataset_a$data$Time), last(dataset_a$data$Time))
    printf.debug("B (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n",
                 nrow(dataset_b$data), first(dataset_b$data$Time), last(dataset_b$data$Time))
    
    # Merge + Sort + Filter, danke an Lukas Fischer (@o1oo11oo) für die Idee
    # Zuvor mehrerer Ticks am selben Zeitpunkt zu einem einzigen Datenpunkt
    # zusammenfassen und dabei Mindest-/Höchstpreis berechnen
    dataset_ab <- mergeSortAndFilterTwoDatasets(
        dataset_a$data |> summariseMultipleTicksAtSameTime(),
        dataset_b$data |> summariseMultipleTicksAtSameTime()
    )
    
    printf.debug("A+B: %d Tickdaten von %s bis %s\n",
                 nrow(dataset_ab), first(dataset_ab$Time), last(dataset_ab$Time))
    
    # Aktuelle Position merken
    currentRow <- 0L
    
    # Neue Daten erst unmittelbar vor Ende des Datensatzes nachladen,
    # aber etwas Puffer für `mergeSortAndFilterTwoDatasets` lassen.
    numRows <- nrow(dataset_ab)
    loadNewDataAtRowNumber <- numRows - 5L
    
    # Flag: Ende der verfügbaren Daten erreicht, keine weiteren Dateien mehr lesen
    endAfterCurrentDataset <- FALSE
    
    # -- Ende nötige Wiederholung --
    
    # Ergebnisvektor
    result <- data.table()
    result_set_index <- 1L
    
    # Fortschritt aufzeichnen und ausgeben
    processedDatasets <- 0L
    now <- proc.time()["elapsed"]
    
    # Hauptschleife: Paarweise vergleichen
    printf("\n  Beginne Auswertung für %s der Börsen %s und %s ab %s (Threshold: %d s).\n\n", 
           format.currencyPair(currencyPair),
           exchange_a,
           exchange_b,
           format(startDate, "%d.%m.%Y %H:%M:%S"),
           comparisonThreshold
    )
    printf("  % 13s   % 11s   %-26s   % 10s   % 10s   % 10s   % 3s\n",
           "Laufzeit", "Verarbeitet", "Aktueller Datensatz",
           "Ergebnisse", "Größe", "Geschw.", "Set")
    while (TRUE) {
        
        # Zähler erhöhen
        currentRow <- currentRow + 1L
        nextRow <- currentRow + 1L
        
        # Laufzeit und aktuellen Fortschritt periodisch ausgeben
        processedDatasets <- processedDatasets + 1L
        if (processedDatasets %% 10000 == 0 || currentRow == numRows) {
            runtime <- as.integer(proc.time()["elapsed"] - now)
            #           Runtime nInput  Time    nResult Size    Speed      Set
            printf("\r  % 13s   % 11s   % 26s   % 10s   % 8s   % 6s T/s   % 3d",
                   format.duration(runtime),
                   format.number(processedDatasets),
                   formatPOSIXctWithFractionalSeconds(
                       dataset_ab$Time[currentRow], "%d.%m.%Y %H:%M:%OS"
                   ),
                   format.number(nrowDT(result)),
                   format(object.size(result), units="auto", standard="SI"),
                   format.number(round(processedDatasets/runtime, 0)),
                   result_set_index
            )
        }
        
        # Nur für Benchmark-/Vergleichszwecke: Abbruch nach 500.000 Ticks
        # if (processedDatasets >= 500000) {
        #     printf("\nBenchmark beendet.")
        #     break
        # }
        
        # Ende des Datensatzes erreicht
        if (currentRow == numRows) {
            if (!endAfterCurrentDataset) {
                stop("Keine neuen Daten geladen, obwohl Ende des Datensatzes erreicht wurde!")
            }
            printf("\nZeile %d erreicht, Ende.\n", currentRow)
            break
        }
        
        # Neue Daten laden
        if (currentRow >= loadNewDataAtRowNumber && !endAfterCurrentDataset) {
            
            printf.debug("\nZeile %d/%d (%s) erreicht, lade neue Daten.\n", 
                         currentRow, 
                         numRows,
                         format(dataset_ab$Time[currentRow]))
            currentTick <- dataset_ab[currentRow]
            
            # Lese weitere Daten ab letztem gemeinsamen Datenpunkt
            baseDate <- currentTick$Time
            loadUntil <- last(dataset_ab$Time) + 60 * 60
            
            # Ende erreicht
            if (loadUntil > endDate) {
                loadUntil <- endDate
                endAfterCurrentDataset <- TRUE
                printf.debug("Datenende erreicht, Stop nach aktuellem Monat.\n")
            }
            
            readTickDataAsMovingWindow(dataset_a, baseDate, loadUntil)
            readTickDataAsMovingWindow(dataset_b, baseDate, loadUntil)
            
            printf.debug("A: %d Tickdaten von %s bis %s\n",
                         nrow(dataset_a$data),
                         first(dataset_a$data$Time),
                         last(dataset_a$data$Time))
            printf.debug("B: %d Tickdaten von %s bis %s\n",
                         nrow(dataset_b$data),
                         first(dataset_b$data$Time),
                         last(dataset_b$data$Time))
            
            # Begrenze auf gemeinsamen Zeitraum
            filterTwoDatasetsByCommonTimeInterval(dataset_a, dataset_b)
            
            # Zu wenig gemeinsame Daten (Datenlücke eines Datensatzes!)
            # Weitere Daten nachladen, bis mehr als 50 gemeinsame Daten vorliegen
            # TODO Hier müsste im Grunde ein dynamisches Limit greifen
            # - min. 50 gemeinsame Daten - manchmal aber auch > 100 nötig
            # - nicht zu nah an der *neuen* currentRow! -> Problematisch?
            # Außerdem: Verletzung des DRY-Prinzips
            while (
                !endAfterCurrentDataset &&
                (nrow(dataset_a$data) < 500 || nrow(dataset_b$data) < 500)
            ) {
                printf.debug("Weniger als 500 gemeinsame Daten (Datenlücke!), lade weitere.\n")
                loadUntil <- min(last(dataset_a$data$Time), last(dataset_b$data$Time)) + 60*60
                
                # Ende erreicht
                if (loadUntil > endDate) {
                    loadUntil <- endDate
                    endAfterCurrentDataset <- TRUE
                    printf.debug("Datenende erreicht, Stop nach aktuellem Monat.\n")
                }
                
                readTickDataAsMovingWindow(dataset_b, baseDate, loadUntil)
                readTickDataAsMovingWindow(dataset_a, baseDate, loadUntil)
                
                # Begrenze auf gemeinsamen Zeitraum
                filterTwoDatasetsByCommonTimeInterval(dataset_a, dataset_b)
            }
            
            printf.debug("A (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n",
                         nrow(dataset_a$data),
                         first(dataset_a$data$Time),
                         last(dataset_a$data$Time))
            printf.debug("B (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n",
                         nrow(dataset_b$data), 
                         first(dataset_b$data$Time),
                         last(dataset_b$data$Time))
            
            # Ticks zum selben Zeitpunkt zusammenfassen, dann Merge + Sort + Filter
            dataset_ab <- mergeSortAndFilterTwoDatasets(
                dataset_a$data |> summariseMultipleTicksAtSameTime(),
                dataset_b$data |> summariseMultipleTicksAtSameTime()
            )
            printf.debug("A+B: %d Tickdaten von %s bis %s\n",
                         nrow(dataset_ab), first(dataset_ab$Time), last(dataset_ab$Time))
            
            # Aktuelle Position (`currentRow`) korrigieren, befindet sich nun
            # am Beginn des (neuen) Datensatzes
            currentRow <- dataset_ab[
                Time == currentTick$Time & 
                PriceLow == currentTick$PriceLow &
                PriceHigh == currentTick$PriceHigh &
                Exchange == currentTick$Exchange &
                RowNum == currentTick$RowNum,
                which=TRUE
            ]
            
            # Aktuelle Position in neuem Betrachtungsfenster nicht gefunden!
            if (length(currentRow) == 0) {
                stop(sprintf(
                    "Aktueller Arbeitspunkt (%s) in neu geladenen Daten nicht vorhanden!",
                    format(currentTick$Time)
                ))
            }
            
            printf.debug("Aktueller Datenpunkt nun in Zeile %d.\n", currentRow)
            nextRow <- currentRow + 1L
            
            # Neue Daten erst kurz vor Ende des Datensatzes laden
            numRows <- nrow(dataset_ab)
            loadNewDataAtRowNumber <- numRows - 5L
        }
        
        # Aktuelle und nächste Zeile vergleichen
        tick_a <- dataset_ab[currentRow]
        tick_b <- dataset_ab[nextRow]
        
        # Gleiche Börse, überspringe.
        if (tick_a$Exchange == tick_b$Exchange) {
            next
        }
        
        # Zeitdifferenz zu groß, überspringe.
        if (difftime(tick_b$Time, tick_a$Time, units="secs") > comparisonThreshold) {
            next
        }
        
        # Set in Ergebnisvektor speichern
        # Anmerkung:
        # Um Probleme mit appendDT (NA+POSIXct) zu umgehen, wird der Zeitstempel
        # hier temporär in ein double (= Unixzeit inkl. Sekundenbruchteile) umgewandelt
        # und später wieder in POSIXct konvertiert. Dieses Vorgehen ist ohne
        # Informationsverlust und noch immer signifikant schneller als rbind()
        #
        # Finde größte Preisdifferenz
        if (
            tick_a$PriceHigh - tick_b$PriceLow >=
            tick_b$PriceHigh - tick_a$PriceLow
        ) {
            result <- appendDT(result, list(
                Time = as.double(tick_b$Time),
                PriceHigh = tick_a$PriceHigh,
                ExchangeHigh = tick_a$Exchange,
                PriceLow = tick_b$PriceLow,
                ExchangeLow = tick_b$Exchange
            ))
        } else {
            result <- appendDT(result, list(
                Time = as.double(tick_b$Time),
                PriceHigh = tick_b$PriceHigh,
                ExchangeHigh = tick_b$Exchange,
                PriceLow = tick_a$PriceLow,
                ExchangeLow = tick_a$Exchange
            ))
        }
        
        # 100 Mio. Datenpunkte im Ergebnisvektor: Zwischenspeichern
        if (nrowDT(result) >= 1e8) {
            
            printf.debug(
                "\n100 Mio. Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...\n"
            )
            
            # Ergebnis speichern
            savePartialResult(
                result, 
                result_set_index, 
                exchange_a, 
                exchange_b, 
                currencyPair,
                comparisonThreshold
            )
            result_set_index <- result_set_index + 1L
            
            # Ergebnisspeicher leeren
            result <- data.table()
            gc()
        }
    }
    
    # Rest speichern
    savePartialResult(
        result, 
        result_set_index, 
        exchange_a, 
        exchange_b, 
        currencyPair,
        comparisonThreshold
    )
    
    printf("\n\n  Abgeschlossen.\n")
    
    return(invisible(NULL))
}


# Berechnung starten ----------------------------------------------------------

# Verfügbare Daten nach Börse und Kurspaar:
# Börse     BTC/USD     BTC/EUR     BTC/GBP     BTC/JPY     BTC/CHF     BTC/AUD     BTC/CAD
# Bitfinex  14.01.2013  19.05.2017  29.03.2018  29.03.2018  -           -
# Bitstamp  18.08.2011  16.04.2016  28.05.2020  -           -           -
# Coinbase  01.12.2014  23.04.2015  21.04.2015  -           -           -
# Kraken    06.10.2013  10.09.2013  06.11.2014  05.11.2014  06.12.2019  16.06.2020  29.06.2015

# Abarbeitung händisch parallelisieren, da CPU- und RAM-limitiert
if (FALSE) {
    endDate <- as.POSIXct("2022-01-01 00:00:00") - .000001
    
    # BTC/USD -----------------------------------------------------------------
    
    # Bitfinex - Bitstamp: ~2h45min. 31.615.047 Datensätze in 1.5 GB (unkomprimiert).
    compareTwoExchanges("bitfinex", "bitstamp", "btcusd", "2013-01-14 16:47:23", endDate)
    
    # Bitfinex - Coinbase Pro: ~6h. 63.106.478 Datensätze in 3 GB (unkomprimiert).
    compareTwoExchanges("bitfinex", "coinbase", "btcusd", "2014-12-01 05:33:56", endDate)
    
    # Bitfinex - Kraken: ~2h15min. 23.689.444 Datensätze in 1.1 GB (unkomprimiert).
    compareTwoExchanges("bitfinex", "kraken",   "btcusd", "2013-10-06 21:34:15", endDate)
    
    # Bitstamp - Coinbase Pro: ~3h30min, 37.538.963 Datensätze in 1,6 GB (unkomprimiert).
    compareTwoExchanges("bitstamp", "coinbase", "btcusd", "2014-12-01 05:33:56", endDate)
    
    # Bitstamp - Kraken: ~1h30min. 16.332.564 Datensätze in 784 MB (unkomprimiert).
    compareTwoExchanges("bitstamp", "kraken",   "btcusd", "2013-10-06 21:34:15", endDate)
    
    # Coinbase Pro - Kraken: ~3h. 31.202.746 Datensätze in 1.5 GB (unkomprimiert).
    compareTwoExchanges("coinbase", "kraken",   "btcusd", "2014-12-01 05:33:56", endDate)
    
    
    # BTC/EUR -----------------------------------------------------------------
    
    # Bitfinex - Bitstamp: ~30min. 5.564.218 Datensätze in 222.6 MB (unkomprimiert).
    compareTwoExchanges("bitfinex", "bitstamp", "btceur", "2017-05-19 08:16:21", endDate)
    
    # Bitfinex - Coinbase Pro: ~45min. 9.370.516 Datensätze in 374.8 MB (unkomprimiert).
    compareTwoExchanges("bitfinex", "coinbase", "btceur", "2017-05-19 08:16:21", endDate)
    
    # Bitfinex - Kraken: ~40min. 8.562.289 Datensätze in 342.5 MB (unkomprimiert).
    compareTwoExchanges("bitfinex", "kraken",   "btceur", "2017-05-19 08:16:21", endDate)
    
    # Bitstamp - Coinbase Pro: ~1h15min. 16.767.027 Datensätze in 804.8 MB (unkomprimiert).
    compareTwoExchanges("bitstamp", "coinbase", "btceur", "2016-04-16 16:55:02", endDate)
    
    # Bitstamp - Kraken: ~1h20min. 15.322.141 Datensätze in 735.5 MB (unkomprimiert).
    compareTwoExchanges("bitstamp", "kraken",   "btceur", "2016-04-16 16:55:02", endDate)
    
    # Coinbase Pro - Kraken: ~2h20min. 26.473.543 Datensätze in 1.3 GB (unkomprimiert).
    compareTwoExchanges("coinbase", "kraken",   "btceur", "2015-04-23 01:42:34", endDate)
    
    
    # BTC/GBP -----------------------------------------------------------------
    # Nicht mehr betrachtet
    
    # # Bitfinex - Bitstamp: ~5min. 429.638 Datensätze in 20.6 MB (unkomprimiert).
    # compareTwoExchanges("bitfinex", "bitstamp", "btcgbp", "2020-05-28 09:37:26", endDate)
    # 
    # # Bitfinex - Coinbase Pro: ~18min. 4.517.863 Datensätze in 216.9 MB (unkomprimiert).
    # compareTwoExchanges("bitfinex", "coinbase", "btcgbp", "2018-03-29 14:40:57", endDate)
    # 
    # # Bitfinex - Kraken: ~5min. 610.309 Datensätze in 29.3 MB (unkomprimiert).
    # compareTwoExchanges("bitfinex", "kraken",   "btcgbp", "2018-03-29 14:40:57", endDate)
    # 
    # # Bitstamp - Coinbase Pro: ~6min. 978.408 Datensätze in 47 MB (unkomprimiert).
    # compareTwoExchanges("bitstamp", "coinbase", "btcgbp", "2020-05-28 09:37:26", endDate)
    # 
    # # Bitstamp - Kraken: ~2min30s. 192.105 Datensätze in 9.2 MB (unkomprimiert).
    # compareTwoExchanges("bitstamp", "kraken",   "btcgbp", "2020-05-28 09:37:26", endDate)
    # 
    # # Coinbase Pro - Kraken: ~8min. 1.396.126 Datensätze in 44.7 MB (unkomprimiert).
    # compareTwoExchanges("coinbase", "kraken",   "btcgbp", "2015-04-21 22:22:41", endDate)
    
    
    # BTC/JPY -----------------------------------------------------------------
    # Nicht mehr betrachtet
    
    # # Bitfinex - Kraken: ~2min. 116.057 Datensätze in 3.7 MB (unkomprimiert).
    # compareTwoExchanges("bitfinex", "kraken",   "btcjpy", "2018-03-29 15:55:31", endDate)
    
    
    # BTC/CHF -----------------------------------------------------------------
    # Nur bei Kraken handelbar
    
    
    # BTC/CAD -----------------------------------------------------------------
    # Nur bei Kraken handelbar
}

