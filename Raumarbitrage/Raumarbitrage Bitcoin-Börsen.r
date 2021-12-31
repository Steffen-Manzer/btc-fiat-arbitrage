# Bibliotheken und Hilfsfunktionen laden
source("Klassen/Dataset.r")
source("Funktionen/AddOneMonth.r")
source("Funktionen/AppendToDataTable.r")
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

# Anzahl der Datensätze, die eingelesen werden, bevor geprüft wird,
# ob die geforderte Anzahl Daten gelesen wurde
numDatasetsPerRead <- 10000L

# Ende Konfiguration

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
readDataFileChunked <- function(dataFile, startRow, endDate) {
    
    # Umgebungsbedingungen prüfen
    stopifnot(
        exists("numDatasetsPerRead"),
        file.exists(dataFile)
    )
    
    # Metadaten der Datei lesen
    numRowsInFile <- metadata_fst(dataFile)$nrOfRows
    
    # Keine weiteren Daten in dieser Datei: Abbruch
    if (startRow == numRowsInFile) {
        return(data.table())
    }
    
    # Lese Daten iterativ ein
    while (TRUE) {
        
        # Limit bestimmen: `numDatasetsPerRead` Datensätze oder bis zum Ende der Datei
        endRow <- min(numRowsInFile, startRow + numDatasetsPerRead - 1L)
        
        # Datei einlesen
        # printf("Lese %s von Zeile %d bis %d: ", basename(dataFile), startRow, endRow)
        newData <- read_fst(dataFile, c("Time", "Price"), startRow, endRow, as.data.table=TRUE)
        newData[, RowNum:=startRow:endRow]
        
        # Daten anhängen
        if (exists("result")) {
            result <- rbindlist(list(result, newData), use.names=TRUE)
        } else {
            result <- newData
        }
        # printf("%d weitere Datensätze, %d insgesamt.\n", nrow(newData), nrow(result))
        
        # endDate wurde erreicht oder Datei ist abgeschlossen
        if (last(result$Time) > endDate || endRow == numRowsInFile) {
            break
        }
        
        # Weitere `numDatasetsPerRead` Datenpunkte lesen
        startRow <- startRow + numDatasetsPerRead
    }
    
    return(result)
}


#' Lade Tickdaten für das gesamte angegebene Intervall, ggf. über
#' mehrere Quelldateien hinweg
#' 
#' Das gewählte Intervall darf dabei einen ganzen Monat (28-31 Tage) 
#' nicht überschreiten. Daten bis zwei Minuten vor `currentTime` werden
#' entfernt, um Speicher freizugeben.
#' 
#' @param dataset Eine Instanz der Klasse `Dataset`
#' @param currentTime Zeitpunkt des aktuellen (zuletzt verwendeten) Ticks
#' @param endDate Zieldatum, bis zu dem mindestens gelesen werden soll
#' @param loadNextFileIfNotSufficientTicks Sollen auch Daten eines weiteren Monats
#'   (über endDate hinaus) geladen werden, wenn weniger als 100 neue Ticks in der
#'   aktuellen Datei liegen?
#' @return `NULL` (Verändert den angegebenen Datensatz per Referenz.)
readAndAppendNewTickData <- function(
    dataset, 
    currentTime,
    endDate, 
    loadNextFileIfNotSufficientTicks = TRUE
) {
    
    # Parameter validieren
    stopifnot(
        inherits(dataset, "Dataset"),
        is.POSIXct(currentTime),
        is.POSIXct(endDate)
    )
    
    numNewRows <- 0L
    if (nrow(dataset$data) > 0) {
        
        # Speicherbereinigung: Bereits verarbeitete Daten löschen
        # Einen Zeitraum von wenigen Minuten vor dem aktuell 
        # betrachteten Tick beibehalten.
        # `data.table` kann leider noch kein subsetting per Referenz, sodass eine
        # Kopie (`<-`) notwendig ist.
        # printf("Bereinige Daten vor %s.\n", format(currentTime - 2 * 60))
        dataset$data <- dataset$data[Time >= (currentTime - 2 * 60),]
        
        # Lese Daten ab dem letzten Tick ein
        currentTime <- last(dataset$data$Time)
        startRow <- last(dataset$data$RowNum)
        
        # Bereits Daten bis über das angegebene Enddatum hinaus eingelesen
        if (currentTime > endDate) {
            return(invisible())
        }
        
    } else {
        
        # Ab erster Zeile starten
        startRow <- 1L
        
    }
    
    while (TRUE) {
        
        # Beginne immer bei aktuellem Monat
        dataFile <- sprintf(
            "%s-%d-%02d.fst",
            dataset$PathPrefix, year(currentTime), month(currentTime)
        )
        if (!file.exists(dataFile)) {
            stop(sprintf("Datei nicht gefunden: %s", dataFile))
        }
        
        # printf("Lese %s ab Zeile %s bis ", basename(dataFile), startRow |> numberFormat())
        newData <- readDataFileChunked(dataFile, startRow, endDate)
        numNewRows <- numNewRows + nrow(newData)
        
        # Letzte Zeile nur für Debug-Zwecke speichern
        lastRowNumber <- last(newData$RowNum)
        if (is.null(lastRowNumber)) {
            lastRowNumber <- startRow
        }
        
        if (numNewRows > 0) {
            # Börse hinterlegen und an bestehende Daten anfügen
            newData[, Exchange:=dataset$Exchange]
            # printf("%s (von %s): %s Datensätze.\n",
            #             lastRowNumber |> numberFormat(),
            #             metadata_fst(dataFile)$nrOfRows |> numberFormat(),
            #             numNewRows |> numberFormat()
            # )
            dataset$data <- rbindlist(list(dataset$data, newData), use.names=TRUE)
        } else {
            # printf("%s: Keine neuen Datensätze.\n", lastRowNumber |> numberFormat())
        }
        
        # Zieldatum erreicht und mehr als 100 Datensätze geladen:
        # Keine weiteren Daten laden.
        if (
            as.integer(format(endDate, "%Y%m")) <= as.integer(format(currentTime, "%Y%m")) &&
            (isFALSE(loadNextFileIfNotSufficientTicks) || numNewRows > 100)
        ) {
            break
        }
        
        # Lese zusätzlich nächsten Monat
        currentTime <- addOneMonth(currentTime)
        startRow <- 1L
    }
    
    return(invisible())
}


#' Zwei Datensätze auf den gemeinsamen Zeitraum beschränken
#' 
#' Beide Datensätze starten aufgrund der Art und Weise,
#' wie Daten geladen werden, immer ungefähr zum gleichen Zeitpunkt.
#' Es muss also nur ein Enddatum bestimmt werden.
#' 
#' @param dataset_a Eine Instanz der Klasse `Dataset`
#' @param dataset_b Eine Instanz der Klasse `Dataset`
#' @return `NULL` (Verändert die angegebenen Datensätze per Referenz.)
filterTwoDatasetsByCommonTimeInterval <- function(dataset_a, dataset_b) {
    
    # Parameter und Daten validieren
    stopifnot(
        inherits(dataset_a, "Dataset"),
        inherits(dataset_b, "Dataset"),
        nrow(dataset_a$data) > 0L,
        nrow(dataset_b$data) > 0L
    )
    
    # Letzte Tick-Zeitpunkte bestimmen
    last_a <- last(dataset_a$data$Time)
    last_b <- last(dataset_b$data$Time)
    
    # Beide Datensätze enden am exakt gleichen Zeitpunkt.
    # In diesem Fall muss nichts gefiltert werden.
    if (last_a == last_b) {
        return(NA)
    }
    
    # Behalte nur gemeinsame Daten.
    # Ergänze Datensatz um einen weiteren Datenpunkt für letzten Vergleich.
    # `data.table` kann leider noch kein subsetting per Referenz, sodass eine
    # Kopie (`<-`) notwendig ist.
    if (last_a > last_b) {
        
        # Datensatz A enthält mehr Daten als B
        pos_of_last_common_tick <- last(dataset_a$data[Time <= last_b, which=TRUE])
        dataset_a$data <- dataset_a$data[1:(pos_of_last_common_tick+1),]
        
    } else {
        
        # Datensatz B enthält mehr Daten als A
        pos_of_last_common_tick <- last(dataset_b$data[Time <= last_a, which=TRUE])
        dataset_b$data <- dataset_b$data[1:(pos_of_last_common_tick+1),]
        
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
#' @return `data.table` Eine Tabelle der Tickdaten beider Börsen,
#'   bestehend aus den Spalten `Time`, `Price`, `RowNum` und `Exchange`.
mergeSortAndFilterTwoDatasets <- function(dataset_a, dataset_b) {
    
    # Merge, Sort und Filter (nicht: mergesort-Algorithmus)
    
    # Daten zu einer gemeinsamen Liste verbinden.
    # Enthaltene Spalten: Time, Price, RowNum und Exchange.
    dataset_ab <- rbindlist(list(dataset_a$data, dataset_b$data), use.names=TRUE)
    
    # Liste nach Zeit sortieren
    setorder(dataset_ab, Time)
    
    # `dataset_ab` enthält nun Ticks beider Börsen nach Zeit sortiert.
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
    
    # Tripel filtern
    # Einschränkung: Erste und letzte Zeile werden immer entfernt,
    # diese können an dieser Stelle nicht sinnvoll geprüft werden.
    # Aus diesem Grund wird beim Neuladen von Daten immer etwas Puffer
    # in beide Richtungen gelassen.
    triplets <- c(
        T,
        rollapply(
            dataset_ab$Exchange,
            width = 3,
            # Tripel, wenn Börse im vorherigen, aktuellen und nächsten Tick identisch ist
            FUN = function(exchg) (exchg[1] == exchg[2] && exchg[2] == exchg[3])
        ),
        T
    )
    
    # Datensatz ohne gefundene Tripel zurückgeben
    return(dataset_ab[!triplets,])
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
#' @return `data.table` Eine Tabelle mit gemeinsamen Tickdaten innerhalb einer Zeitspanne von 5s
compareTwoExchanges <- function(exchange_a, exchange_b, currencyPair, startDate) {
    
    # Bis einschließlich vergangenen Monat vergleichen
    endDate <- as.POSIXct(format(Sys.time(), "%Y-%m-01 00:00:00")) - 1
    
    # Datenobjekte initialisieren, die später per Referenz übergeben
    # werden können. So kann direkt an den Daten gearbeitet werden,
    # ohne dass immer eine Kopie angelegt werden muss.
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
    
    # -- Diese Schritte werden regelmäßig wiederholt, um sequentiell weitere Daten zu laden --
    
    # Daten für die ersten 60 Minuten beider Börsen laden
    loadUntil <- startDate + 60 * 60
    readAndAppendNewTickData(dataset_a, startDate, loadUntil)
    readAndAppendNewTickData(dataset_b, startDate, loadUntil)
    # printf("A: %d Tickdaten von %s bis %s\n", 
    #             nrow(dataset_a$data), first(dataset_a$data$Time), last(dataset_a$data$Time))
    # printf("B: %d Tickdaten von %s bis %s\n", 
    #             nrow(dataset_b$data), first(dataset_b$data$Time), last(dataset_b$data$Time))
    
    # Begrenze auf gemeinsamen Zeitraum
    filterTwoDatasetsByCommonTimeInterval(dataset_a, dataset_b)
    # printf("A (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n", 
    #             nrow(dataset_a$data), first(dataset_a$data$Time), last(dataset_a$data$Time))
    # printf("B (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n", 
    #             nrow(dataset_b$data), first(dataset_b$data$Time), last(dataset_b$data$Time))
    
    # Merge + Sort + Filter, danke an Lukas Fischer (@o1oo11oo) für die Idee
    dataset_ab <- mergeSortAndFilterTwoDatasets(dataset_a, dataset_b)
    # printf("A+B: %d Tickdaten von %s bis %s\n", 
    #             nrow(dataset_ab), first(dataset_ab$Time), last(dataset_ab$Time))
    
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
    result_index <- data.table() # TODO Nach Tests nur noch diesen Wert behalten
    
    # Fortschritt aufzeichnen und ausgeben
    processedDatasets <- 0L
    now <- proc.time()["elapsed"]
    
    # Hauptschleife: Paarweise vergleichen
    cat("Beginne Auswertung\n")
    while (TRUE) {
        
        # Zähler erhöhen
        currentRow <- currentRow + 1L
        nextRow <- currentRow + 1L
        
        # Laufzeit und aktuellen Stand periodisch ausgeben
        processedDatasets <- processedDatasets + 1L
        if (processedDatasets %% 10000 == 0) {
            runtime <- proc.time()["elapsed"] - now
            printf("\r%s Ticks verarbeitet\tAktuell: %s\t%s Sekunden\t%s Ticks/s ",
                   numberFormat(processedDatasets),
                   format(dataset_ab[currentRow,Time], "%d.%m.%Y %H:%M:%S"),
                   numberFormat(round(runtime, 0)),
                   numberFormat(round(processedDatasets/runtime, 0))
            )
        }
        
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
            
            # printf("\nZeile %d/%d erreicht, lade neue Daten.\n", currentRow, numRows)
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
                                     loadNextFileIfNotSufficientTicks=!endAfterCurrentDataset)
            readAndAppendNewTickData(dataset_b, baseDate, loadUntil, 
                                     loadNextFileIfNotSufficientTicks=!endAfterCurrentDataset)
            
            # printf("A: %d Tickdaten von %s bis %s\n", 
            #             nrow(dataset_a$data), first(dataset_a$data$Time), last(dataset_a$data$Time))
            # printf("B: %d Tickdaten von %s bis %s\n", 
            #             nrow(dataset_b$data), first(dataset_b$data$Time), last(dataset_b$data$Time))
            
            # Begrenze auf gemeinsamen Zeitraum
            filterTwoDatasetsByCommonTimeInterval(dataset_a, dataset_b)
            
            # Zu wenig gemeinsame Daten (Datenlücke eines Datensatzes!)
            # Weitere Daten nachladen, bis mehr als 50 gemeinsame Daten vorliegen
            # TODO Hier müsste im Grunde ein dynamisches Limit greifen
            # - min. 50 gemeinsame Daten - manchmal aber auch 500 nötig
            # - nicht zu nah an der *neuen* currentRow! -> Problematisch?
            while (
                !endAfterCurrentDataset &&
                (nrow(dataset_a$data) < 500 || nrow(dataset_b$data) < 500)
            ) {
                loadUntil <- min(last(dataset_a$data$Time), last(dataset_b$data$Time)) + 60*60
                # printf("Weniger als 500 gemeinsame Daten (Datenlücke!), lade weitere.\n")
                readAndAppendNewTickData(dataset_b, baseDate, loadUntil, 
                                         loadNextFileIfNotSufficientTicks=!endAfterCurrentDataset)
                readAndAppendNewTickData(dataset_a, baseDate, loadUntil, 
                                         loadNextFileIfNotSufficientTicks=!endAfterCurrentDataset)
                
                # Begrenze auf gemeinsamen Zeitraum
                filterTwoDatasetsByCommonTimeInterval(dataset_a, dataset_b)
            }
            
            # printf("A (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n", 
            #             nrow(dataset_a$data), first(dataset_a$data$Time), last(dataset_a$data$Time))
            # printf("B (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n", 
            #             nrow(dataset_b$data), first(dataset_b$data$Time), last(dataset_b$data$Time))
            
            # Merge + Sort + Filter, danke an Lukas Fischer (@o1oo11oo) für die Idee
            dataset_ab <- mergeSortAndFilterTwoDatasets(dataset_a, dataset_b)
            # printf("A+B: %d Tickdaten von %s bis %s\n", 
            #             nrow(dataset_ab), first(dataset_ab$Time), last(dataset_ab$Time))
            
            # Aktuelle Position (`currentRow`) korrigieren, befindet sich nun
            # am Beginn des (neuen) Datensatzes
            currentRow <- dataset_ab[
                Time == currentTick$Time & 
                Price == currentTick$Price &
                Exchange == currentTick$Exchange &
                RowNum == currentTick$RowNum,
                which=TRUE
            ]
            
            # Aktuelle Position in neuem Betrachtungsfenster nicht gefunden!
            if (length(currentRow) == 0) {
                stop(sprintf("Aktueller Arbeitspunkt (%s) in neu geladenen Daten nicht vorhanden!",
                             format(currentTick$Time)))
            }
            
            # printf("Aktueller Datenpunkt nun in Zeile %d.\n", currentRow)
            nextRow <- currentRow + 1L
            
            # Neue Daten erst kurz vor Ende des Datensatzes laden
            numRows <- nrow(dataset_ab)
            loadNewDataAtRowNumber <- numRows - 5L
        }
        
        # Aktuelle und nächste Zeile lesen
        tick_a <- dataset_ab[currentRow,]
        tick_b <- dataset_ab[nextRow,]
        
        # Gleiche Börse, überspringe.
        if (tick_a$Exchange == tick_b$Exchange) {
            next
        }
        
        # Zeitdifferenz zu groß, überspringe.
        # TODO Variabel statt hartkodiert
        timeDifference <- as.double(tick_a$Time - tick_b$Time, units="secs")
        if (timeDifference > 5) {
           next
        }
        
        # Vergleich bzw. Index speichern
        # result <- appendDT(result, list(
        #     Time_A = as.double(tick_a$Time), # TODO Temporär: Probleme mit appendDT vermeiden
        #     Time_B = as.double(tick_b$Time), # Dito
        #     Price_A = tick_a$Price,
        #     Price_B = tick_b$Price,
        #     Exchange_A = tick_a$Exchange,
        #     Exchange_B = tick_b$Exchange
        # ))
        
        result_index <- appendDT(result_index, list(
            Time = as.double(tick_b$Time), # TODO Temporär: Probleme mit appendDT vermeiden
            Diff = abs(tick_a$Price - tick_b$Price),
            MaxPrice = max(tick_a$Price, tick_b$Price)
            #DiffIndex = abs(tick_a$Price - tick_b$Price) / max(tick_a$Price, tick_b$Price)
        ))
        
        # TODO
        # Berechnung eines Arbitrageindex wie in der Literatur?
        # Begrenze auf maximale Differenz innerhalb einer Sekunde/fünf Sekunden/einer Minute?
    }
    
    # Ergebnis um zwischenzeitlich eingefügte NAs bereinigen
    result_index <- cleanupDT(result_index)
    
    # TODO Temporär: double wieder zurück zu POSIXct
    result_index[,Time:=as.POSIXct(Time,origin="1970-01-01")]
    
    # Index berechnen
    result_index[,Index:=Diff/MaxPrice]
    
    return(result_index)
}

#compareTwoExchanges("bitfinex", "bitstamp", "btcusd", as.POSIXct("2013-10-14 00:00:00"))
result <- compareTwoExchanges("bitfinex", "bitstamp", "btcusd", as.POSIXct("2021-11-01 00:00:00"))

# Alle Währungspaare und alle Börsen untersuchen
# for (index in seq_len(nrow(currencyPairs))) {
#     pair <- currencyPairs$CurrencyPair[index]
#     startDate <- as.POSIXct(paste0(currencyPairs$StartMonth[index], "-01"))
#     
#     # Daten bis vor einen Monat verarbeiten
#     endDate <- floor_date(floor_date(Sys.Date(), unit = "months") - 1, unit = "months")
#     
#     printf("======== Untersuche %s ab %s ========\n", pair, format(startDate, "%Y-%m"))
#     
#     # TODO Startdatum für jede Börse und jedes Währungspaar separat hinterlegen, s.o.
#     # -> for-Schleife überhaupt nötig?
#     # Dann jeweils compareTwoExchanges("a", "b", currencyPair, startDate)
#     # === ALT ===
#     
#     # Jedes Börsenpaar vergleichen
#     # Bitfinex - Bitstamp
#     # Bitfinex - Coinbase Pro
#     # Bitfinex - Kraken
#     if (length(bitfinex) > 1 && !is.na(bitfinex)) {
#         if (length(bitstamp) > 1 && !is.na(bitstamp)) {
#             cat("Vergleiche Bitfinex - Bitstamp\n")
#             matchedPriceDifferences <- rbind(
#                 matchedPriceDifferences, 
#                 compareTwoExchanges(
#                     dataset_a = bitfinex,
#                     exchange_a = "Bitfinex",
#                     dataset_b = bitstamp,
#                     exchange_b = "Bitstamp",
#                     threshold = 5
#                 )
#             )
#         }
#         if (length(coinbase) > 1 && !is.na(coinbase)) {
#             cat("Vergleiche Bitfinex - Coinbase Pro\n")
#             matchedPriceDifferences <- rbind(
#                 matchedPriceDifferences, 
#                 compareTwoExchanges(
#                     dataset_a = bitfinex,
#                     exchange_a = "Bitfinex",
#                     dataset_b = coinbase,
#                     exchange_b = "Coinbase Pro",
#                     threshold = 5
#                 )
#             )
#         }
#         if (length(kraken) > 1 && !is.na(kraken)) {
#             cat("Vergleiche Bitfinex - Kraken\n")
#             matchedPriceDifferences <- rbind(
#                 matchedPriceDifferences, 
#                 compareTwoExchanges(
#                     dataset_a = bitfinex,
#                     exchange_a = "Bitfinex",
#                     dataset_b = kraken,
#                     exchange_b = "Kraken",
#                     threshold = 5
#                 )
#             )
#         }
#     }
#     # Bitstamp - Coinbase Pro
#     # Bitstamp - Kraken
#     if (length(bitstamp) > 1 && !is.na(bitstamp)) {
#         if (length(coinbase) > 1 && !is.na(coinbase)) {
#             cat("Vergleiche Bitstamp - Coinbase Pro\n")
#             matchedPriceDifferences <- rbind(
#                 matchedPriceDifferences, 
#                 compareTwoExchanges(
#                     dataset_a = bitstamp,
#                     exchange_a = "Bitstamp",
#                     dataset_b = coinbase,
#                     exchange_b = "Coinbase Pro",
#                     threshold = 5
#                 )
#             )
#         }
#         if (length(kraken) > 1 && !is.na(kraken)) {
#             cat("Vergleiche Bitstamp - Kraken\n")
#             matchedPriceDifferences <- rbind(
#                 matchedPriceDifferences, 
#                 compareTwoExchanges(
#                     dataset_a = bitstamp,
#                     exchange_a = "Bitstamp",
#                     dataset_b = kraken,
#                     exchange_b = "Kraken",
#                     threshold = 5
#                 )
#             )
#         }
#     }
#     # Coinbase Pro - Kraken
#     if (length(coinbase) > 1 && !is.na(coinbase)) {
#         if (length(kraken) > 1 && !is.na(kraken)) {
#             cat("Vergleiche Coinbase Pro - Kraken\n")
#             matchedPriceDifferences <- rbind(
#                 matchedPriceDifferences, 
#                 compareTwoExchanges(
#                     dataset_a = coinbase,
#                     exchange_a = "Coinbase Pro",
#                     dataset_b = kraken,
#                     exchange_b = "Kraken",
#                     threshold = 5
#                 )
#             )
#         }
#     }
# }

