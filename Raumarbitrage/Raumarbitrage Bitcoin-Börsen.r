# Bibliotheken und Hilfsfunktionen laden
source("Klassen/Dataset.r")
source("Funktionen/AddOneMonth.r")
source("Funktionen/NumberFormat.r")
source("Funktionen/printf.r")
library("fst")
library("data.table")
library("dplyr") # filter
library("lubridate") # floor_date
library("zoo") # rollapply für Filterfunktion

# Datenbeginn aller Börsen:
# - Bitfinex:
#   BTCUSD enthält Daten von 14.01.2013, 16:47:23 (UTC) bis heute
#   BTCGBP enthält Daten von 29.03.2018, 14:40:57 (UTC) bis heute
#   BTCJPY enthält Daten von 29.03.2018, 15:55:31 (UTC) bis heute
#   BTCEUR enthält Daten von 01.09.2019, 00:00:00 (UTC) bis heute
# - Bitstamp:
#   BTCUSD enthält Daten von 13.09.2011, 13:53:36 (UTC) bis heute
#   BTCEUR enthält Daten von 05.12.2017, 11:43:49 (UTC) bis heute
# - Coinbase Pro:
#   BTCUSD enthält Daten von 01.12.2014, 05:33:56.761199 (UTC) bis heute.
#   BTCEUR enthält Daten von 23.04.2015, 01:42:34.182104 (UTC) bis heute.
#   BTCGBP enthält Daten von 21.04.2015, 22:22:41.294060 (UTC) bis heute.
# - Kraken:
#   BTCUSD enthält Daten von 06.10.2013, 21:34:15 (UTC) bis heute
#   BTCEUR enthält Daten von 10.09.2013, 23:47:11 (UTC) bis heute
#   BTCGBP enthält Daten von 06.11.2014, 16:13:43 (UTC) bis heute
#   BTCJPY enthält Daten von 05.11.2014, 22:21:30 (UTC) bis heute
#   BTCCAD enthält Daten von 29.06.2015, 03:27:41 (UTC) bis heute
#   BTCCHF enthält Daten von 06.12.2019, 16:33:17 (UTC) bis heute
# 
# Zeitpunkt für die ersten gemeinsamen Datensätze:
#  BTCUSD: 14.01.2013, 16:47:23 (Bitfinex + Bitstamp)
#  BTCEUR: 23.04.2015, 01:42:34.182104 (Coinbase Pro + Kraken)
#  BTCGBP: 21.04.2015, 22:22:41.294060 (Coinbase Pro + Kraken)
#  BTCJPY: 29.03.2018, 15:55:31 (Bitfinex + Kraken)
#  BTCCAD: -
#  BTCCHF: -

# Konfiguration
currencyPairs <- data.table(
    CurrencyPair = c("BTCUSD",  "BTCEUR",  "BTCGBP",  "BTCJPY"),
    StartMonth   = c("2013-01", "2015-04", "2015-04", "2018-03")
)
exchanges <- c("bitfinex", "bitstamp", "coinbase", "kraken")

# Grundablauf: Immer paarweiser Vergleich zweier Börsen mit "Moving Window", ähnlich zu mergesort
# - Erste x Daten beider Börsen laden = "Betrachtungsfenster" initialisieren:
#   [t = 0, ..., t = 1h] in Gruppen von je 10.000 Datenpunkten
# - Beschränke Datensatz auf gemeinsame Daten
# - Solange Daten für beide Börsen vorhanden sind:
#   - Nächsten Datensatz vergleichen und Ergebnis speichern
#   - Prüfen, ob noch genug Daten beider Börsen für Vergleich vorhanden sind, sonst:
#       - neues Datenfenster laden: [t = 1h, ..., t = 2h]
#       - Speicher freigeben: Daten des ersten Betrachtungsfensters entfernen (0...1h)
#       - Letzte paar Datenpunkte des vorherigen Betrachtungsfensters behalten, um
#         korrekt filtern/vergleichen zu können


#' Liest eine einzelne angegebene .fst-Datei bis zum Dateiende oder 
#' bis endDate, je nachdem was früher eintritt.
#' 
#' @param dataFile Absoluter Pfad zu einer .fst-Datei
#' @param startRow Zeilennummer, ab der gelesen werden soll
#' @param endDate Zieldatum, bis zu dem mindestens gelesen werden soll
#' @return `data.table` mit den gelesenen Daten
readDataFile <- function(dataFile, startRow, endDate) {
    
    if (!file.exists(dataFile)) {
        stop(sprintf("Datei %s nicht gefunden!", dataFile))
    }
    
    result <- data.table()
    meta <- metadata_fst(dataFile)
    
    # Keine weiteren Daten in dieser Datei: Abbruch
    if (startRow == meta$nrOfRows) {
        return(result)
    }
    
    # Lese Daten iterativ ein
    while (TRUE) {
        
        # Limit bestimmen: 10.000 Datensätze oder bis zum Ende der Datei
        endRow <- min(meta$nrOfRows, startRow + 10000)
        
        # Dateiende bereits erreicht?
        if (startRow > endRow) {
            stop(sprintf("readDataFile(%s): startRow (%d) > endRow (%d)!\n", basename(dataFile), startRow, endRow))
        }
        
        # Datei einlesen
        #printf("Lese %s von Zeile %d bis %d: ", basename(dataFile), startRow, endRow)
        # TODO Auf off-by-one-Fehler prüfen!
        newlyReadData <- read_fst(dataFile, c("Time", "Price"), startRow, endRow, as.data.table=TRUE)
        newlyReadData$RowNum <- startRow:endRow
        
        result <- rbind(result, newlyReadData)
        #printf("%d weitere Datensätze, %d insgesamt.\n", nrow(newlyReadData), nrow(result))
        
        # Letzter Datensatz liegt nach endDate oder Datei ist abgeschlossen
        if (last(result$Time) > endDate || endRow == meta$nrOfRows) {
            break
        }
        
        # Weitere 10.000 Datenpunkte lesen
        startRow <- startRow + 10000
    }
    
    return(result)
}


#' Lade Tickdaten für das gesamte angegebene Intervall, ggf. über
#' mehrere Quelldateien hinweg
#' 
#' Das gewählte Intervall darf dabei einen ganzen Monat (28-31 Tage) 
#' nicht überschreiten.
#' 
#' @param dataset Eine Instanz der Klasse `Dataset`
#' @param baseMonth Datum des zuletzt gelesenen Datenpunktes
#' @param endDate Zieldatum, bis zu dem mindestens gelesen werden soll
#' @param loadFileOfNextMonthIfNotSufficientTicks Sollen auch Daten eines weiteren Monats
#'   (über endDate hinaus) geladen werden, wenn nicht genug Daten in der aktuellen Datei liegen?
#' @return `NA` (`dataset` wird per Referenz verändert)
readAndAppendNewTickData <- function(
    dataset, 
    baseMonth,
    endDate, 
    loadFileOfNextMonthIfNotSufficientTicks = TRUE
) {
    
    # Parameter validieren
    stopifnot(
        inherits(dataset, "Dataset"),
        is.POSIXct(baseMonth),
        is.POSIXct(endDate)
    )
    
    numNewRows <- 0L
    if (nrow(dataset$data) > 0) {
        
        # Speicherbereinigung: Bereits verarbeitete Daten löschen
        # Ansatz 1: Feste Anzahl an Ticks beibehalten
        #dataset$data <- tail(dataset$data, n=5000)
        
        # Ansatz 2 (neu):
        # Einen Zeitraum von fünf Minuten vor dem aktuell 
        # betrachteten Tick beibehalten.
        dataset$data <- dataset$data[Time >= (baseMonth - 5*60),]
        
        # Lese Daten ab dem letzten Tick ein
        baseMonth <- last(dataset$data$Time)
        startRow <- last(dataset$data$RowNum)
        
        # Bereits Daten bis über das angegebene Enddatum hinaus eingelesen
        if (baseMonth > endDate) {
            return()
        }
        
    } else {
        
        # Ab erster Zeile starten
        startRow <- 1L
        
    }
    
    while (TRUE) {
        
        # Beginne immer bei aktuellem Monat
        dataFile <- sprintf(
            "%s-%d-%02d.fst",
            dataset$PathPrefix, year(baseMonth), month(baseMonth)
        )
        if (!file.exists(dataFile)) {
            stop(sprintf("Datei nicht gefunden: %s", dataFile))
        }
        
        printf("Lese %s ab Zeile %s bis ", basename(dataFile), startRow |> numberFormat())
        newData <- readDataFile(dataFile, startRow, endDate)
        numNewRows <- numNewRows + nrow(newData)
        
        # Letzte Zeile nur für Debug-Zwecke speichern
        lastRowNumber <- last(newData$RowNum)
        if (is.null(lastRowNumber)) {
            lastRowNumber <- startRow
        }
        
        if (numNewRows > 0) {
            # Format anpassen und neue Daten anfügen
            newData$Exchange <- dataset$Exchange
            printf("%s (von %s): %s Datensätze.\n", 
                        lastRowNumber |> numberFormat(),
                        metadata_fst(dataFile)$nrOfRows |> numberFormat(),
                        numNewRows |> numberFormat()
            )
            dataset$data <- rbindlist(list(dataset$data, newData))
        } else {
            printf("%s: Keine neuen Datensätze.\n", lastRowNumber |> numberFormat())
        }
        
        # Zieldatum erreicht und mehr als 100 Datensätze geladen:
        # Keine weiteren Daten laden.
        if (
            as.integer(format(endDate, "%Y%m")) <= as.integer(format(baseMonth, "%Y%m")) &&
            (loadFileOfNextMonthIfNotSufficientTicks == FALSE || numNewRows > 100)
        ) {
            break
        }
        
        # Lese zusätzlich nächsten Monat
        baseMonth <- addOneMonth(baseMonth)
        startRow <- 1L
    }
    
    return(NA)
}


#' Zwei Datensätze auf den gemeinsamen Zeitraum beschränken
#' 
#' @param dataset_a Eine Instanz der Klasse `Dataset`
#' @param dataset_b Eine Instanz der Klasse `Dataset`
#' @return `NA` (`dataset_a` und `dataset_b` werden per Referenz verändert)
filterTwoDatasetsByCommonTimeInterval <- function(dataset_a, dataset_b) {
    
    # Parameter validieren
    stopifnot(
        inherits(dataset_a, "Dataset"),
        inherits(dataset_b, "Dataset")
    )
    
    # Ein Datensatz ist leer
    if (nrow(dataset_a$data) == 0L || nrow(dataset_b$data) == 0L) {
        stop("filterTwoDatasetsByCommonTimeInterval: ein Datensatz ist leer!\n")
    }
    
    # Letzten gemeinsamen Zeitpunkt bestimmen
    lastCommonTick <- min(last(dataset_a$data$Time), last(dataset_b$data$Time))
    
    # Anmerkung: Es kann natürlich vorkommen, dass beide Datensätze am exakt
    # gleichen Zeitpunkt enden. In diesem Fall muss nichts gefiltert werden.
    
    # Datensatz A enthält mehr Daten als B
    if (last(dataset_a$data$Time) > lastCommonTick) {
        
        # Behalte nur gemeinsame Daten
        filter_a <- which(dataset_a$data$Time <= lastCommonTick)
        
        # Ergänze Datensatz um einen weiteren Datenpunkt für letzten Vergleich
        filter_a <- c(filter_a, last(filter_a) + 1)
        dataset_a$data <- dataset_a$data[filter_a,]
        
    }
    
    # Datensatz B enthält mehr Daten als A
    if (last(dataset_b$data$Time) > lastCommonTick) {
        
        # Behalte nur gemeinsame Daten
        filter_b <- which(dataset_b$data$Time <= lastCommonTick)
        
        # Ergänze Datensatz um einen weiteren Datenpunkt für letzten Vergleich
        filter_b <- c(filter_b, last(filter_b) + 1)
        dataset_b$data <- dataset_b$data[filter_b,]
        
    }
    
    # Ein resultierender Datensatz ist leer
    if (nrow(dataset_a$data) == 0L || nrow(dataset_b$data) == 0L) {
        stop("filterTwoDatasetsByCommonTimeInterval: ein Datensatz ist nach dem Filtern leer!\n")
    }
    
    return(NA)
}


#' Zwei Datensätze in eine gemeinsame Liste zusammenführen
#' 
#' Verbindet zwei Sätze von Tickdaten in eine gemeinsame Liste,
#' sortiert diese nach Zeit und entfernt die mittleren von
#' drei oder mehr aufeinanderfolgenden Ticks der selben Börse, 
#' da diese für die Auswertung nicht relevant sind.
#' 
#' @param dataset_a Eine Instanz der Klasse `Dataset`
#' @param dataset_b Eine Instanz der Klasse `Dataset`
#' @return `data.table` Eine Tabelle der Tickdaten beider Börsen
mergeSortAndFilterTwoDatasets <- function(dataset_a, dataset_b) {
    
    # Merge, Sort und Filter (nicht: mergesort-Algorithmus)
    
    # Daten zu einer gemeinsamen Liste verbinden
    dataset_ab <- data.table(
        Time = c(dataset_a$data$Time, dataset_b$data$Time),
        Price = c(dataset_a$data$Price, dataset_b$data$Price),
        Exchange = c(
            rep_len(dataset_a$Exchange, nrow(dataset_a$data)), 
            rep_len(dataset_b$Exchange, nrow(dataset_b$data))
        ),
        RowNum = c(dataset_a$data$RowNum, dataset_b$data$RowNum)
    )
    
    # Liste nach Zeit sortieren
    setorder(dataset_ab, Time)
    
    # `dataset_ab` enthält nun beide Datensätze nach Zeit sortiert.
    # Aufeinanderfolgende Daten der selben Börse interessieren nicht, da der Tickpunkt
    # davor bzw. danach immer näher am nächsten Tick der anderen Börse ist.
    # Aufeinanderfolgende Tripel daher herausfiltern.
    #
    # Beispiel:
    # |--------------------------------->   Zeitachse
    #  A A A A B B A B B B A A B B B B A    Originaldaten (Einzelne Ticks der Börse A oder B)
    #         *   * *     *   *       *     Sinnvolle Preisvergleiche
    #  * * *           *         * *        Nicht benötigte Ticks
    #        A B B A B   B A A B     B A    Reduzierter Datensatz
    
    # Filtern
    # Einschränkung: Erste und letzte Zeile werden immer entfernt
    unset <- c(
        T,
        rollapply(
            dataset_ab$Exchange,
            width = 3,
            # Prüfe, ob Börse im vorherigen, aktuellen und nächsten Tick identisch ist
            FUN = function(exchg) (exchg[1] == exchg[2] && exchg[2] == exchg[3])
        ),
        T
    )
    dataset_ab <- dataset_ab[-which(unset),]
    
    return(dataset_ab)
}


#' Preise zweier Börsen vergleichen
#' 
#' TODO Dokumentation ggf. korrigieren
#' 
#' @param exchange_a Name der ersten Börse
#' @param exchange_b Name der zweiten Börse
#' @param currencyPair Kurspaar (z.B. EURUSD)
#' @param startDate Beginne Vergleich ab diesem Datum
#'                  (= Zeitpunkt des ersten gemeinsamen Datensatzes)
compareTwoExchanges <- function(exchange_a, exchange_b, currencyPair, startDate) {
    
    # Bis einschließlich vergangenen Monat vergleichen
    endDate <- as.POSIXct(format(Sys.time(), "%Y-%m-01 00:00:00")) - 1
    
    # Datenobjekte initialisieren
    dataset_a <- new("Dataset",
        Exchange = exchange_a,
        CurrencyPair = currencyPair,
        PathPrefix = sprintf("Cache/%s/%s/tick/%1$s-%2$s-tick",
                             exchange_a, tolower(currencyPair)),
        data = data.table()
    )
    
    dataset_b <- new("Dataset",
        Exchange = exchange_b,
        CurrencyPair = currencyPair,
        PathPrefix = sprintf("Cache/%s/%s/tick/%1$s-%2$s-tick",
                             exchange_b, tolower(currencyPair)),
        data = data.table()
    )
    
    # -- Diese Schritte müssen regelmäßig wiederholt werden, um neue Daten zu laden --
    
    # Daten für die ersten 60 Minuten beider Börsen laden
    loadUntil <- startDate + 60 * 60
    printf("Lade von %s bis mindestens %s\n", startDate, loadUntil)
    
    readAndAppendNewTickData(dataset_a, startDate, loadUntil)
    readAndAppendNewTickData(dataset_b, startDate, loadUntil)
    
    printf("A: %d Tickdaten von %s bis %s\n", 
                nrow(dataset_a$data), first(dataset_a$data$Time), last(dataset_a$data$Time))
    printf("B: %d Tickdaten von %s bis %s\n", 
                nrow(dataset_b$data), first(dataset_b$data$Time), last(dataset_b$data$Time))
    
    # Begrenze auf gemeinsamen Zeitraum
    filterTwoDatasetsByCommonTimeInterval(dataset_a, dataset_b)
    
    printf("A (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n", 
                nrow(dataset_a$data), first(dataset_a$data$Time), last(dataset_a$data$Time))
    printf("B (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n", 
                nrow(dataset_b$data), first(dataset_b$data$Time), last(dataset_b$data$Time))
    
    # Merge + Sort + Filter, danke an Lukas Fischer (@o1oo11oo) für die Idee
    dataset_ab <- mergeSortAndFilterTwoDatasets(dataset_a, dataset_b)
    printf("A+B: %d Tickdaten von %s bis %s\n", 
                nrow(dataset_ab), first(dataset_ab$Time), last(dataset_ab$Time))
    
    # Position merken
    currentRow <- 0L
    
    # Rechtzeitig neue Daten laden
    numRows <- nrow(dataset_ab)
    loadNewDataAtRowNumber <- numRows - 5L
    
    # Ende des Datensatzes erreicht
    endAfterCurrentDataset <- FALSE
    
    # -- Ende nötige Wiederholung --
    
    # Hauptschleife: Paarweise vergleichen
    cat("Beginne Auswertungsschleife (noch: keine Aktion)\n")
    while (TRUE) {
        currentRow <- currentRow + 1L
        
        # Ende des Datensatzes erreicht
        if (currentRow == numRows) {
            printf("Zeile %d erreicht, Ende.\n", currentRow)
            if (!endAfterCurrentDataset) {
                stop("Keine neuen Daten geladen, obwohl Ende des Datensatzes erreicht wurde!")
            }
            break
        }
        
        # Neue Daten laden
        if (currentRow >= loadNewDataAtRowNumber && !endAfterCurrentDataset) {
            
            printf("\nZeile %d/%d erreicht, lade neue Daten.\n", currentRow, numRows)
            currentTick <- dataset_ab[currentRow,]
            
            # Lese weitere Daten ab letztem gemeinsamen Datenpunkt
            baseDate <- currentTick$Time
            loadUntil <- last(dataset_ab$Time) + 60 * 60
            
            # Ende erreicht
            if (loadUntil > endDate) {
                loadUntil <- endDate
                endAfterCurrentDataset <- TRUE
            }
            
            readAndAppendNewTickData(dataset_a, baseDate, loadUntil, 
                                     loadFileOfNextMonthIfNotSufficientTicks=!endAfterCurrentDataset)
            readAndAppendNewTickData(dataset_b, baseDate, loadUntil, 
                                     loadFileOfNextMonthIfNotSufficientTicks=!endAfterCurrentDataset)
            
            printf("A: %d Tickdaten von %s bis %s\n", 
                        nrow(dataset_a$data), first(dataset_a$data$Time), last(dataset_a$data$Time))
            printf("B: %d Tickdaten von %s bis %s\n", 
                        nrow(dataset_b$data), first(dataset_b$data$Time), last(dataset_b$data$Time))
            
            # Begrenze auf gemeinsamen Zeitraum
            filterTwoDatasetsByCommonTimeInterval(dataset_a, dataset_b)
            
            # Zu wenig gemeinsame Daten (Datenlücke eines Datensatzes!)
            # Weitere Daten nachladen, bis mehr als 50 gemeinsame Daten vorliegen
            while (
                !endAfterCurrentDataset &&
                (nrow(dataset_a$data) < 50 || nrow(dataset_b$data) < 50)
            ) {
                loadUntil <- loadUntil + 60*60
                printf("Weniger als 50 gemeinsame Daten, lade weitere.\n")
                readAndAppendNewTickData(dataset_b, baseDate, loadUntil, 
                                         loadFileOfNextMonthIfNotSufficientTicks=!endAfterCurrentDataset)
                readAndAppendNewTickData(dataset_a, baseDate, loadUntil, 
                                         loadFileOfNextMonthIfNotSufficientTicks=!endAfterCurrentDataset)
                
                # Begrenze auf gemeinsamen Zeitraum
                filterTwoDatasetsByCommonTimeInterval(dataset_a, dataset_b)
            }
            
            printf("A (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n", 
                        nrow(dataset_a$data), first(dataset_a$data$Time), last(dataset_a$data$Time))
            printf("B (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n", 
                        nrow(dataset_b$data), first(dataset_b$data$Time), last(dataset_b$data$Time))
            
            # Merge + Sort + Filter, danke an Lukas Fischer (@o1oo11oo) für die Idee
            dataset_ab <- mergeSortAndFilterTwoDatasets(dataset_a, dataset_b)
            printf("A+B: %d Tickdaten von %s bis %s\n", 
                        nrow(dataset_ab), first(dataset_ab$Time), last(dataset_ab$Time))
            
            # Aktuelle Position (`currentRow`) korrigieren, befindet sich nun
            # am Beginn des (neuen) Datensatzes
            currentRow <- dataset_ab[
                Time == currentTick$Time & 
                Price == currentTick$Price &
                Exchange == currentTick$Exchange &
                RowNum == currentTick$RowNum,
                which = TRUE
            ]
            
            # Aktuelle Position in neuem Betrachtungsfenster nicht gefunden!
            if (length(currentRow) == 0) {
                dataset_ab_after_moving_window_loading <<- dataset_ab
                currentTick <<- currentTick
                stop(sprintf("Aktueller Arbeitspunkt (%s) in neu geladenen Daten nicht vorhanden!",
                             format(currentTick$Time)))
            }
            
            printf("Aktueller Datenpunkt nun in Zeile %d.\n", currentRow)
            
            # Neue Daten kurz vor Ende des Datensatzes laden
            numRows <- nrow(dataset_ab)
            loadNewDataAtRowNumber <- numRows - 5L
            
            # TEST
            #break
            
        }
        
        # TODO Zunächst Ladealgorithmus testen
        next
        
        # Vergleiche
        tick_a <- dataset_ab[currentRow,]
        tick_b <- dataset_ab[(currentRow+1),]
        
        # Gleiche Börse, überspringe.
        if (tick_a$Exchange == tick_b$Exchange) {
            next
        }
        
        # Zeitdifferenz zu groß, überspringe.
        timeDifference <- as.double(tick_b$data$Time - tick_a$data$Time, units="secs")
        if (timeDifference > 5) {
            next
        }
        
        # TODO
        # - In zweier-Sets Preise vergleichen
        # [OK] Zeitliche Differenz beider Daten prüfen und ggf. überspringen
        # - Differenz inkl. Zeit und ggf. Preisniveau notieren
        # -> rollapply bietet sich grundsätzlich wieder an? Aber wie/wann neue Daten laden?
        # -> Berechnung eines Arbitrageindex wie in der Literatur?
        
        # Alter Code:
        
        # matchedPriceDifferences <- data.table()
        # ...
        # matchedPriceDifferences <- rbind(matchedPriceDifferences, data.table(
        #     TimeA = tick_a$Time,
        #     TimeB = tick_b$Time,
        #     TimeDifference = tick_a$Time - tick_b$Time,
        #     PriceA = tick_a$Price,
        #     PriceB = tick_b$Price,
        #     PriceDifference = tick_a$Price - tick_b$Price,
        #     ExchangeA = exchange_a,
        #     ExchangeB = exchange_b
        # ))
    }
}


# Alle Währungspaare und alle Börsen untersuchen
for (index in 1:nrow(currencyPairs)) {
    pair <- currencyPairs$CurrencyPair[index]
    startDate <- as.POSIXct(paste0(currencyPairs$StartMonth[index], "-01"))
    
    # Daten bis vor einen Monat verarbeiten
    endDate <- floor_date(floor_date(Sys.Date(), unit = "months") - 1, unit = "months")
    
    printf("======== Untersuche %s ab %s ========\n", pair, format(startDate, "%Y-%m"))
    
    # TEST
    compareTwoExchanges("bitfinex", "bitstamp", pair, startDate)
    
    # TODO Startdatum für jede Börse und jedes Währungspaar separat hinterlegen, s.o.
    # Dann jeweils compareTwoExchanges("a", "b", currencyPair, startDate)
    break
    
    # === ALT ===
    
    # Jedes Börsenpaar vergleichen
    # Bitfinex - Bitstamp
    # Bitfinex - Coinbase Pro
    # Bitfinex - Kraken
    if (length(bitfinex) > 1 && !is.na(bitfinex)) {
        if (length(bitstamp) > 1 && !is.na(bitstamp)) {
            cat("Vergleiche Bitfinex - Bitstamp\n")
            matchedPriceDifferences <- rbind(
                matchedPriceDifferences, 
                compareTwoExchanges(
                    dataset_a = bitfinex,
                    exchange_a = "Bitfinex",
                    dataset_b = bitstamp,
                    exchange_b = "Bitstamp",
                    threshold = 5
                )
            )
        }
        if (length(coinbase) > 1 && !is.na(coinbase)) {
            cat("Vergleiche Bitfinex - Coinbase Pro\n")
            matchedPriceDifferences <- rbind(
                matchedPriceDifferences, 
                compareTwoExchanges(
                    dataset_a = bitfinex,
                    exchange_a = "Bitfinex",
                    dataset_b = coinbase,
                    exchange_b = "Coinbase Pro",
                    threshold = 5
                )
            )
        }
        if (length(kraken) > 1 && !is.na(kraken)) {
            cat("Vergleiche Bitfinex - Kraken\n")
            matchedPriceDifferences <- rbind(
                matchedPriceDifferences, 
                compareTwoExchanges(
                    dataset_a = bitfinex,
                    exchange_a = "Bitfinex",
                    dataset_b = kraken,
                    exchange_b = "Kraken",
                    threshold = 5
                )
            )
        }
    }
    # Bitstamp - Coinbase Pro
    # Bitstamp - Kraken
    if (length(bitstamp) > 1 && !is.na(bitstamp)) {
        if (length(coinbase) > 1 && !is.na(coinbase)) {
            cat("Vergleiche Bitstamp - Coinbase Pro\n")
            matchedPriceDifferences <- rbind(
                matchedPriceDifferences, 
                compareTwoExchanges(
                    dataset_a = bitstamp,
                    exchange_a = "Bitstamp",
                    dataset_b = coinbase,
                    exchange_b = "Coinbase Pro",
                    threshold = 5
                )
            )
        }
        if (length(kraken) > 1 && !is.na(kraken)) {
            cat("Vergleiche Bitstamp - Kraken\n")
            matchedPriceDifferences <- rbind(
                matchedPriceDifferences, 
                compareTwoExchanges(
                    dataset_a = bitstamp,
                    exchange_a = "Bitstamp",
                    dataset_b = kraken,
                    exchange_b = "Kraken",
                    threshold = 5
                )
            )
        }
    }
    # Coinbase Pro - Kraken
    if (length(coinbase) > 1 && !is.na(coinbase)) {
        if (length(kraken) > 1 && !is.na(kraken)) {
            cat("Vergleiche Coinbase Pro - Kraken\n")
            matchedPriceDifferences <- rbind(
                matchedPriceDifferences, 
                compareTwoExchanges(
                    dataset_a = coinbase,
                    exchange_a = "Coinbase Pro",
                    dataset_b = kraken,
                    exchange_b = "Kraken",
                    threshold = 5
                )
            )
        }
    }
}

