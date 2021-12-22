# Bibliotheken und Hilfsfunktionen laden
source("Funktionen/FindNearestDatapoint.r")
source("Funktionen/AddOneMonth.r")
library("lubridate") # floor_date
library("data.table")
library("dplyr") # filter
library("fst")
library("zoo") # rollapply für Filterfunktion

# Datenbeginn aller Börsen:
# - Bitfinex:
#   BTCUSD enthält Daten von 14.01.2013, 16:47:23 (UTC) bis heute
#   BTCEUR enthält Daten von 01.09.2019, 00:00:00 (UTC) bis heute
#   BTCGBP enthält Daten von 29.03.2018, 14:40:57 (UTC) bis heute
#   BTCJPY enthält Daten von 29.03.2018, 15:55:31 (UTC) bis heute
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

# Interne Konfiguration
datasetClassName <- "dataset"
datasetAttributeNames <- list("Exchange", "CurrencyPair", "PathPrefix", "LastRowNumber")

# Grundablauf: Immer paarweiser Vergleich zweier Börsen mit "Moving Window", ähnlich zu mergesort
# - Erste x Daten beider Börsen laden = "Fenster" initialisieren: [t = 0, ..., t = 1h]
# - Verwendung des größeren Datensatzes als "Referenz", damit die geladene Zeit ungefähr konstant ist
# - Zu prüfen: Begrenze Arbeitsfenster auf ~10.000 Daten, da Filter-/Sortier-/Vergleichsaufgaben 
#   exponentiell mit der Größe des Arbeitsfensters wachsen _können_.
# - Wichtig: Daten müssen zeit- und nicht zeilenbasiert geladen werden! Immer 1h-Sets
# - Solange noch Daten für beide Börsen vorhanden sind:
#   - Nächsten Datensatz vergleichen und Ergebnis speichern
#   - Prüfen, ob noch genug Daten beider Börsen für Vergleich vorhanden sind, sonst:
#       - neues Datenfenster laden: [t = 1h, ..., t = 2h]
#       - Daten des ersten Fensters verwerfen (0...1h)
#       - Letzte paar Datenpunkte des vorherigen Fensters behalten, um
#         korrekt filtern/vergleichen zu können

# Hilfsfunktion:
# Neue Daten laden, bis Ende der Datei oder endDate erreicht ist
readDataFile <- function(dataFile, startRow, endDate) {
    result <- data.table()
    meta <- metadata_fst(dataFile)
    while (TRUE) {
        
        # Limit bestimmen: 10.000 Datensätze oder bis zum Ende der Datei
        endRow <- min(meta$nrOfRows, startRow + 10000)
        
        # Dateiende bereits erreicht?
        if (startRow > endRow) {
            stop(sprintf("startRow (%d) > endRow (%d)!\n", startRow, endRow))
        }
        
        # Datei einlesen
        # TODO Auf off-by-one-Fehler prüfen!
        result <- rbind(
            result,
            read_fst(dataFile, c("Time", "Price"), startRow, endRow)
        )
        
        # Letzter Datensatz liegt nach endDate oder Datei ist abgeschlossen
        if (last(result$Time) > endDate || endRow == meta$nrOfRows) {
            break
        }
        
        # Weitere 10.000 Datenpunkte lesen
        startRow <- startRow + 10000
    }
    
    setattr(result, "LastRowNumber", endRow)
    return(result)
}

# Hilfsfunktion: Datensatz x um neue Daten y ergänzen.
# Dabei Attribute und Datensatz-Klasse (s.o.) beibehalten.
# `rbind` und `rbindlist` löschen class + attributes,
# data.tables `full_join` ist wiederum um den Faktor 20+ langsamer.
# Daher `rbindlist` und attribute händisch kopieren...
appendData <- function(x, y) {
    existingAttributes <- attributes(x)
    x <- rbindlist(list(x, y)) # rbindlist ist etwas schneller als rbind
    class(x) <- c(class(x), datasetClassName)
    for (attribute in datasetAttributeNames) {
        if (!is.null(existingAttributes[[attribute]])) {
            setattr(x, attribute, existingAttributes[[attribute]])
        }
    }
    return(x)
}

# Lade Datensatz für das angegebene Intervall.
# Intervall darf einen ganzen Monat (28-31 Tage) nicht überschreiten.
getTickDataByTimeInterval <- function(dataset, startDate, endDate) {
    
    # Wir können nur mit der selbst definierten Klasse "dataset" arbeiten
    stopifnot(inherits(dataset, datasetClassName))
    
    # Alles bis auf letzte 100 Daten löschen
    dataset <- tail(dataset, n = 100)
    
    # Beginne immer bei aktuellem Monat
    dataFile <- sprintf(
        "%s-%d-%02d.fst", 
        attr(dataset, "PathPrefix", TRUE), year(startDate), month(startDate)
    )
    if (!file.exists(dataFile)) {
        stop(sprintf("Datei nicht gefunden: %s", dataFile))
    }
    
    # Lese Datensatz ab dem letzten Datenpunkt ein oder beginne bei 1, falls neu
    startRow <- max(attr(dataset, "LastRowNumber", TRUE), 1)
    newData <- readDataFile(dataFile, startRow, endDate)
    
    # Falls bei 1 gestartet wurde (also kein Zeilenzähler bekannt ist),
    # dann prüfe zusätzlich auf Daten vor dem angegebenen Startzeitpunkt
    # und entferne diese ggf.
    if (startRow == 1) {
        newData <- filter(newData, Time >= startDate)
    }
    
    # Neue Daten anfügen
    dataset <- appendData(dataset, newData)
    setattr(dataset, "LastRowNumber", attr(newData, "LastRowNumber", TRUE))
    
    # Ende liegt im nächsten Monat: Weitere Datei öffnen
    if (month(endDate) != month(startDate)) {
        dataFile <- sprintf(
            "%s-%d-%02d.fst", 
            attr(dataset, "PathPrefix", TRUE), year(endDate), month(endDate)
        )
        if (!file.exists(dataFile)) {
            stop(sprintf("Datei nicht gefunden: %s", dataFile))
        }
        
        newData <- readDataFile(dataFile, 1, endDate)
        dataset <- appendData(dataset, newData)
        setattr(dataset, "LastRowNumber", attr(newData, "LastRowNumber", TRUE))
    }
    
    return(dataset)
}

mergeSortAndFilterTwoDatasets <- function(dataset_a, dataset_b) {
    
    # Merge, Sort und Filter (nicht: mergesort-Algorithmus)
    
    # Listen verbinden
    exchange_a <- attr(dataset_a, "Exchange", TRUE)
    exchange_b <- attr(dataset_b, "Exchange", TRUE)
    dataset_ab <- data.table(
        Time = c(dataset_a$Time, dataset_b$Time),
        Price = c(dataset_a$Price, dataset_b$Price),
        Exchange = c(rep_len(exchange_a, nrow(dataset_a)), rep_len(exchange_b, nrow(dataset_b)))
    )
    
    # Liste nach Zeit sortieren
    setorder(dataset_ab, Time)
    
    # `dataset_ab` enthält nun beide Datensätze nach Zeit sortiert.
    # Aufeinanderfolgende Daten der selben Börse interessieren nicht, da der Tickpunkt
    # davor bzw. danach immer näher am nächsten Tick der anderen Börse ist.
    # Aufeinanderfolgende Tripel daher herausfiltern.
    #
    # Beispiel:
    # |------------------------------->     Zeitachse
    # A A A A B B A B B B A A B B B B A     Originaldaten (Einzelne Ticks der Börse A oder B)
    #        *   * *     *   *       *      Sinnvolle Preisvergleiche
    # * * *           *         * *         Nicht benötigte Ticks
    #       A B B A B   B A A B     B A     Reduzierter Datensatz
    
    # Filtern
    # Einschränkung: Erste und letzte Zeile werden immer beibehalten
    unset <- c(F, rollapply(
        dataset_ab$Exchange,
        width = 3,
        # Prüfe, ob Börse im vorherigen, aktuellen und nächsten Tick identisch ist
        FUN = function(set) (set[1] == set[2] && set[2] == set[3])
    ), F)
    dataset_ab <- dataset_ab[-which(unset),]
    
    return(dataset_ab)
}

compareTwoExchanges <- function(exchange_a, exchange_b, currencyPair, startDate) {
    
    # Leere data.tables mit notwendigen Eigenschaften initialisieren
    dataset_a <- data.table()
    class(dataset_a) <- c(class(dataset_a), datasetClassName)
    setattr(dataset_a, "Exchange", exchange_a)
    setattr(dataset_a, "CurrencyPair", currencyPair)
    setattr(dataset_a, "PathPrefix", 
            sprintf("Cache/%s/%s/tick/%1$s-%2$s-tick", exchange_a, tolower(currencyPair))
    )
    
    dataset_b <- data.table()
    class(dataset_b) <- c(class(dataset_b), datasetClassName)
    setattr(dataset_b, "Exchange", exchange_b)
    setattr(dataset_b, "CurrencyPair", currencyPair)
    setattr(dataset_b, "PathPrefix", 
            sprintf("Cache/%s/%s/tick/%1$s-%2$s-tick", exchange_b, tolower(currencyPair))
    )
    
    # -- Diese Schritte müssen regelmäßig wiederholt werden, um Daten aufzufrischen
    # Daten für die ersten 60 Minuten beider Börsen laden
    endDate <- startDate + 60 * 60
    dataset_a <- getTickDataByTimeInterval(dataset_a, startDate, endDate)
    dataset_b <- getTickDataByTimeInterval(dataset_b, startDate, endDate)
    
    # Merge + Sort + Filter, danke an Lukas Fischer (@o1oo11oo) für die Idee
    dataset_ab <- mergeSortAndFilterTwoDatasets(dataset_a, dataset_b)
    # -- Ende nötige Wiederholung
    
    while (TRUE) {
        # TODO
        # - In zweier-Sets Preise vergleichen
        # - Zeitliche Differenz beider Daten prüfen und ggf. überspringen
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
        
        break
    }
}


# Alle Währungspaare und alle Börsen untersuchen
for (index in 1:nrow(currencyPairs)) {
    pair <- currencyPairs$CurrencyPair[index]
    startDate <- as.POSIXct(paste0(currencyPairs$StartMonth[index], "-01"))
    
    # Daten bis vor einen Monat verarbeiten
    endDate <- floor_date(floor_date(Sys.Date(), unit = "months") - 1, unit = "months")
    
    cat("== Untersuche ", pair, " ab ", format(startDate, "%Y-%m"), "\n", sep="")
    
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

