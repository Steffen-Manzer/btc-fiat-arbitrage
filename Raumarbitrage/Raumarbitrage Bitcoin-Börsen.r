# Finde Kurspaare zur Analyse von Raumarbitrage.
# 
# Grundablauf: Immer paarweiser Vergleich zweier Börsen mit "Moving Window",
# ähnlich zum Verfahren des `mergesort`-Algorithmus.
#
# - Erste x Daten beider Börsen laden = "Betrachtungsfenster" initialisieren:
#   [t = 0, ..., t = 1h], mindestens jedoch je 10.000 Datenpunkte.
# - Filtere beide Datensätze auf gemeinsamen Zeitraum.
# - Liste beider Kurse in eine einzelne Liste vereinen, nach Datum sortieren
#   und filtern (siehe unten).
# - Solange Daten für beide Börsen vorhanden sind:
#   - Nächsten Datensatz vergleichen und Ergebnis speichern
#   - Prüfen, ob noch genug Daten beider Börsen für Vergleich vorhanden sind, sonst:
#       - neues Datenfenster laden: [t = 1h, ..., t = 2h]
#       - Speicher freigeben: Daten des ersten Betrachtungsfensters entfernen (0...1h)
#       - Letzte paar Datenpunkte des vorherigen Betrachtungsfensters behalten, um
#         korrekt filtern/vergleichen zu können
# - Alle fünf Millionen Datenpunkte: Teilergebnisse in einer eigenen Datei speichern,
#   um Arbeitsspeicher freizugeben.


# Bibliotheken und externe Hilfsfunktionen laden ==============================
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


# Hilfsfunktionen =============================================================

#' Liest eine einzelne angegebene .fst-Datei bis zum Dateiende oder 
#' bis endDate, je nachdem was früher eintritt
#' 
#' Dabei wird geprüft, ob mehrere Ticks zum selben Zeitpunkt auftreten.
#' Gegebenenfalls werden weitere Daten geladen, bis alle Ticks des letzten
#' Zeitpunktes im Datensatz enthalten sind.
#' 
#' @param dataFile Absoluter Pfad zu einer .fst-Datei
#' @param startRow Zeilennummer, ab der gelesen werden soll
#' @param endDate Zieldatum, bis zu dem mindestens gelesen werden soll
#' @param numDatasetsPerRead Datensätze, die an einem Stück gelesen werden,
#'   bevor geprüft wird, ob ein Abbruchkriterium erreicht wurde
#' @return `data.table` mit den gelesenen Daten (`Time`, `Price`, `RowNum`)
readDataFileChunked <- function(dataFile, startRow, endDate, numDatasetsPerRead = 10000L) {
    
    # Umgebungsbedingungen prüfen
    stopifnot(
        is.integer(numDatasetsPerRead), length(numDatasetsPerRead) == 1L,
        file.exists(dataFile)
    )
    
    # Metadaten der Datei lesen
    numRowsInFile <- metadata_fst(dataFile)$nrOfRows
    
    # Keine weiteren Daten in dieser Datei: Abbruch
    if (startRow == numRowsInFile) {
        return(data.table())
    }
    
    # Lese Daten iterativ ein, bis ein Abbruchkriterium erfüllt ist
    while (TRUE) {
        
        # Limit bestimmen: `numDatasetsPerRead` Datensätze oder bis zum Ende der Datei
        endRow <- min(numRowsInFile, startRow + numDatasetsPerRead - 1L)
        
        # Datei einlesen
        # printf.debug("Lese %s von Zeile %d bis %d: ", basename(dataFile), startRow, endRow)
        newData <- read_fst(dataFile, c("Time", "Price"), startRow, endRow, as.data.table=TRUE)
        newData[, RowNum:=startRow:endRow]
        
        # Daten anhängen
        if (exists("result")) {
            result <- rbindlist(list(result, newData), use.names=TRUE)
        } else {
            result <- newData
        }
        # printf.debug("%d weitere Datensätze, %d insgesamt.\n", nrow(newData), nrow(result))
        
        # Dateiende wurde erreicht
        # Da die Daten monatsweise sortiert sind, ist der nächste Tick
        # immer zu einem anderen Zeitpunkt. Eine Prüfung, ob der nächste
        # Tick die selbe Zeit aufweist, ist hier also nicht erforderlich
        if (endRow == numRowsInFile) {
            break
        }
        
        # endDate wurde erreicht: Prüfe zusätzlich, ob noch weitere
        # Ticks mit der exakt selben Zeit vorliegen und lade alle
        # solchen Ticks, sonst kommt es zu Fehlern in der Auswertung
        lastTime <- last(result$Time)
        if (lastTime > endDate) {
            
            # Prüfe weitere Ticks nur, solange Dateieende nicht erreicht wurde
            while (endRow < numRowsInFile) {
                
                endRow <- endRow + 1L
                oneMoreRow <- read_fst(dataFile, c("Time", "Price"), endRow, endRow, as.data.table=TRUE)
                
                # Nächster Tick ist nicht in der selben Sekunde:
                # Einlesen abgeschlossen.
                if (oneMoreRow$Time[1] > lastTime) {
                    break
                }
                
                # Nächster Tick ist in der exakt selben Sekunde: anhängen.
                # printf.debug("Ein weiterer Datensatz zum exakt selben Zeitpunkt hinzugefügt.\n")
                oneMoreRow[, RowNum:=endRow]
                result <- rbindlist(list(result, oneMoreRow), use.names=TRUE)
            }
            
            # Fertig.
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
#' @return `NULL` (Verändert den angegebenen Datensatz per Referenz.)
readAndAppendNewTickData <- function(dataset, currentTime, endDate) {
    
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
        printf.debug("Bereinige Daten vor %s.\n", format(currentTime - 2 * 60))
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
        
        printf.debug("Lese %s ab Zeile %s bis ", basename(dataFile), startRow |> numberFormat())
        newData <- readDataFileChunked(dataFile, startRow, endDate)
        numNewRows <- numNewRows + nrow(newData)
        
        # Letzte Zeile nur für Debug-Zwecke speichern
        if (exists("DEBUG_PRINT") && isTRUE(DEBUG_PRINT)) {
            lastRowNumber <- last(newData$RowNum)
            if (is.null(lastRowNumber)) {
                lastRowNumber <- startRow
            }
        }
        
        if (numNewRows > 0) {
            # Börse hinterlegen und an bestehende Daten anfügen
            newData[, Exchange:=dataset$Exchange]
            printf.debug("%s (von %s): %s Datensätze.\n",
                        lastRowNumber |> numberFormat(),
                        metadata_fst(dataFile)$nrOfRows |> numberFormat(),
                        numNewRows |> numberFormat()
            )
            dataset$data <- rbindlist(list(dataset$data, newData), use.names=TRUE)
        } else {
            printf.debug("%s: Keine neuen Datensätze.\n", lastRowNumber |> numberFormat())
        }
        
        # Zieldatum erreicht und mehr als 100 Datensätze geladen:
        # Keine weiteren Daten laden.
        if (
            as.integer(format(endDate, "%Y%m")) <= as.integer(format(currentTime, "%Y%m")) &&
            numNewRows > 100
        ) {
            break
        }
        
        # Lese zusätzlich nächsten Monat
        currentTime <- addOneMonth(currentTime)
        startRow <- 1L
        
        # Nächster Monat liegt außerhalb des verfügbaren Datenbereichs, abbrechen.
        if (currentTime > dataset$EndDate) {
            break
        }
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


#' Mehrfache Ticks mit der exakt selben Zeit zusammenfassen
#' 
#' Beim Einlesen der Daten muss zwingend darauf geachtet werden, dass sämtliche
#' Ticks der exakt selben Zeit vollständig geladen werden.
#' Diese Voraussetzung wird in `readDataFileChunked` sichergestellt.
#' 
#' @param dataset Eine `data.table` mit den Spalten
#'                `Time`, `Price`, `Exchange` und `RowNum`
#' @return `data.table` Wie `dataset`, nur mit gruppierten Zeitpunkten
summariseMultipleTicksAtSameTime <- function(dataset) {
    return(dataset[, 
        .(
            PriceLow = min(Price), 
            PriceHigh = max(Price), 
            Exchange = last(Exchange),
            RowNum = last(RowNum),
            n = .N
        ), 
        by=Time
    ])
}


#' Zwei Datensätze in eine gemeinsame Liste zusammenführen
#' 
#' Verbindet zwei Sätze von Tickdaten in eine gemeinsame Liste,
#' sortiert diese nach Zeit und entfernt die mittleren von
#' drei oder mehr aufeinanderfolgenden Ticks der selben Börse, 
#' da diese für die Auswertung nicht relevant sind.
#' 
#' @param dataset_a `data.table` mit mindestens den Spalten `Time` und `Exchange`
#' @param dataset_b Wie `dataset_a`.
#' @return `data.table` Eine Tabelle der Tickdaten beider Börsen
mergeSortAndFilterTwoDatasets <- function(dataset_a, dataset_b) {
    
    # Merge, Sort und Filter (nicht: mergesort-Algorithmus)
    
    # Daten zu einer gemeinsamen Liste verbinden
    dataset_ab <- rbindlist(list(dataset_a, dataset_b), use.names=TRUE)
    
    # Liste nach Zeit sortieren
    setorder(dataset_ab, Time)
    
    # `dataset_ab` enthält nun Ticks beider Börsen nach Zeit sortiert.
    # Aufeinanderfolgende Daten der selben Börse interessieren nicht, da der Tickpunkt
    # davor bzw. danach immer näher am nächsten Tick der anderen Börse ist.
    # Aufeinanderfolgende Tripel daher herausfiltern, dies beschleunigt
    # die weitere Verarbeitung signifikant (etwa um den Faktor 5).
    #
    # Beispiel:
    # |--------------------------------->   Zeitachse
    #  A A A A B B A B B B A A B B B B A    Originaldaten (Einzelne Ticks der Börse A oder B)
    #         *   * *     *   *       *     Sinnvolle Preisvergleiche
    #  * * *           *         * *        Nicht benötigte Ticks
    #        A B B A B   B A A B     B A    Reduzierter Datensatz
    
    # Tripel filtern
    # Einschränkung: Erste und letzte Zeile werden nie entfernt,
    # diese können an dieser Stelle nicht sinnvoll geprüft werden.
    # Im weiteren Verlauf wird erneut auf die gleiche Börse 
    # aufeinanderfolgender Ticks geprüft.
    triplets <- c(
        FALSE,
        rollapply(
            dataset_ab$Exchange,
            width = 3,
            # Es handelt sich um ein zu entfernendes Tripel, wenn  die
            # Börse im vorherigen, aktuellen und nächsten Tick identisch ist
            FUN = function(exchg) (exchg[1] == exchg[2] && exchg[2] == exchg[3])
        ),
        FALSE
    )
    
    # Datensatz ohne gefundene Tripel zurückgeben
    return(dataset_ab[!triplets,])
}


#' Teilergebnis speichern, um Arbeitsspeicher wieder freizugeben
#' 
#' @param result Eine data.table mit den Spalten `Time`, `Diff` und `MaxPrice`
#' @param index Nummer dieses Teilergebnisses
#' @param exchange_a Name der ersten Börse
#' @param exchange_b Name der zweiten Börse
#' @param currencyPair Name des Kurspaares
saveInterimResult <- function(result, index, exchange_a, exchange_b, currencyPair) {
    
    # Parameter validieren
    stopifnot(
        is.data.table(result), nrow(result) >= 1L,
        is.integer(index), length(index) == 1L,
        is.character(exchange_a), length(exchange_a) == 1L,
        is.character(exchange_b), length(exchange_b) == 1L,
        is.character(currencyPair), length(currencyPair) == 1L
    )
    
    # Zieldatei bestimmen
    outFile <- sprintf("Cache/Raumarbitrage/%s-%s-%s-%d.fst",
                       tolower(currencyPair), exchange_a, exchange_b, index)
    stopifnot(!file.exists(outFile))
    
    # Ergebnis um zwischenzeitlich eingefügte `NA`s
    # (= reservierter Speicher für weitere Ergebnisse) bereinigen
    result <- cleanupDT(result)
    
    # Um Probleme mit appendDT (NA+POSIXct) zu umgehen, wurde der Zeitstempel
    # unten in ein double umgewandelt und wird an dieser Stelle (nachdem der
    # reservierte Speicher in Form von `NA`s entfernt wurde) wieder in 
    # POSIXct konvertiert.
    result[, Time:=as.POSIXct(Time, origin="1970-01-01")]
    
    # Index berechnen
    result[, IndexLow:=DiffLow/PriceHigh]
    result[, IndexHigh:=DiffHigh/PriceHigh]
    
    # Ergebnis speichern
    write_fst(result, outFile, compress=100)
    
}


# Haupt-Auswertungsfunktion ===================================================

#' Preise zweier Börsen vergleichen
#' 
#' @param exchange_a Name der ersten Börse
#' @param exchange_b Name der zweiten Börse
#' @param currencyPair Kurspaar (z.B. EURUSD)
#' @param startDate Beginne Vergleich ab diesem Datum
#'                  (= Zeitpunkt des ersten gemeinsamen Datensatzes)
#' @param comparisonThreshold Zeitliche Differenz zweier Ticks in Sekunden,
#'                            ab der das Tick-Paar verworfen wird.
#' @return `NULL` Ergebnisse werden in mehreren Dateien (i = 1...n) unter
#'   Cache/Raumarbitrage/`currencyPair`-`exchange_a`-`exchange_b`-`i`.fst
#'   gespeichert (siehe `saveInterimResult`).
compareTwoExchanges <- function(
    exchange_a, 
    exchange_b, 
    currencyPair, 
    startDate, 
    comparisonThreshold = 5L
) {
    
    # Parameter validieren
    stopifnot(
        is.character(exchange_a), length(exchange_a) == 1L,
        is.character(exchange_b), length(exchange_b) == 1L,
        is.character(currencyPair), length(currencyPair) == 1L,
        is.POSIXct(startDate), length(startDate) == 1L,
        is.integer(comparisonThreshold), length(comparisonThreshold) == 1L
    )
    
    # Bis einschließlich vergangenen Monat vergleichen
    endDate <- as.POSIXct(format(Sys.time(), "%Y-%m-01 00:00:00")) - 1
    stopifnot(startDate < endDate)
    
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
    )
    
    dataset_b <- new("Dataset",
        Exchange = exchange_b,
        CurrencyPair = currencyPair,
        PathPrefix = sprintf("Cache/%s/%s/tick/%1$s-%2$s-tick",
                             exchange_b, tolower(currencyPair)),
        EndDate = endDate,
        data = data.table()
    )
    
    # -- Diese Schritte werden regelmäßig wiederholt, um sequentiell weitere Daten zu laden --
    
    # Daten für die ersten 60 Minuten beider Börsen laden
    loadUntil <- startDate + 60 * 60
    readAndAppendNewTickData(dataset_a, startDate, loadUntil)
    readAndAppendNewTickData(dataset_b, startDate, loadUntil)
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
    printf("Beginne Auswertung für %s der Börsen %s und %s ab %s.\n", 
           toupper(currencyPair), exchange_a, exchange_b, format(startDate, "%d.%m.%Y %H:%M:%S"))
    while (TRUE) {
        
        # Zähler erhöhen
        currentRow <- currentRow + 1L
        nextRow <- currentRow + 1L
        
        # Laufzeit und aktuellen Fortschritt periodisch ausgeben
        processedDatasets <- processedDatasets + 1L
        if (processedDatasets %% 10000 == 0) {
            runtime <- as.integer(proc.time()["elapsed"] - now)
            # Zeile leeren, denn in manchen Terminals überschreibt \t bisherige Ausgaben nicht
            cat("\r", rep(" ", 150), sep="")
            printf("\r%s verarbeitet\t%s im Ergebnisvektor\tAktuell: %s\t%s Sekunden\t%s Ticks/s        ",
                   numberFormat(processedDatasets),
                   numberFormat(nrowDT(result)),
                   format(dataset_ab[currentRow,Time], "%d.%m.%Y %H:%M:%OS"),
                   numberFormat(round(runtime, 0)),
                   numberFormat(round(processedDatasets/runtime, 0))
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
            currentTick <- dataset_ab[currentRow,]
            
            # Lese weitere Daten ab letztem gemeinsamen Datenpunkt
            baseDate <- currentTick$Time
            loadUntil <- last(dataset_ab$Time) + 60 * 60
            
            # Ende erreicht
            if (loadUntil > endDate) {
                loadUntil <- endDate
                endAfterCurrentDataset <- TRUE
                printf.debug("Datenende erreicht, Stop nach aktuellem Monat.\n")
            }
            
            readAndAppendNewTickData(dataset_a, baseDate, loadUntil)
            readAndAppendNewTickData(dataset_b, baseDate, loadUntil)
            
            printf.debug("A: %d Tickdaten von %s bis %s\n",
                        nrow(dataset_a$data), first(dataset_a$data$Time), last(dataset_a$data$Time))
            printf.debug("B: %d Tickdaten von %s bis %s\n",
                        nrow(dataset_b$data), first(dataset_b$data$Time), last(dataset_b$data$Time))
            
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
                
                readAndAppendNewTickData(dataset_b, baseDate, loadUntil)
                readAndAppendNewTickData(dataset_a, baseDate, loadUntil)
                
                # Begrenze auf gemeinsamen Zeitraum
                filterTwoDatasetsByCommonTimeInterval(dataset_a, dataset_b)
            }
            
            printf.debug("A (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n",
                        nrow(dataset_a$data), first(dataset_a$data$Time), last(dataset_a$data$Time))
            printf.debug("B (auf gemeinsame Daten begrenzt): %d Tickdaten von %s bis %s\n",
                        nrow(dataset_b$data), first(dataset_b$data$Time), last(dataset_b$data$Time))
            
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
                stop(sprintf("Aktueller Arbeitspunkt (%s) in neu geladenen Daten nicht vorhanden!",
                             format(currentTick$Time)))
            }
            
            printf.debug("Aktueller Datenpunkt nun in Zeile %d.\n", currentRow)
            nextRow <- currentRow + 1L
            
            # Neue Daten erst kurz vor Ende des Datensatzes laden
            numRows <- nrow(dataset_ab)
            loadNewDataAtRowNumber <- numRows - 5L
        }
        
        # Aktuelle und nächste Zeile speichern
        tick_a <- dataset_ab[currentRow,]
        tick_b <- dataset_ab[nextRow,]
        
        # Gleiche Börse, überspringe.
        if (tick_a$Exchange == tick_b$Exchange) {
            next
        }
        
        # Zeitdifferenz zu groß, überspringe.
        if (difftime(tick_b$Time, tick_a$Time, units="secs") > comparisonThreshold) {
            next
        }
        
        # Preisdifferenz berechnen
        # Je nachdem an welcher Börse das Preisniveau höher ist, ist
        # priceDifference_1 oder _2 höher/niedriger.
        priceDifference_1 <- abs(tick_a$PriceHigh - tick_b$PriceLow)
        priceDifference_2 <- abs(tick_a$PriceLow - tick_b$PriceHigh)
        
        # Set in Ergebnisvektor speichern
        # Anmerkung:
        # Um Probleme mit appendDT (NA+POSIXct) zu umgehen, wird der Zeitstempel
        # hier temporär in ein double (= Unixzeit inkl. Sekundenbruchteile) umgewandelt
        # und später wieder in POSIXct konvertiert. Dieses Vorgehen ist ohne
        # Informationsverlust und noch immer signifikant schneller als rbind()
        result <- appendDT(result, list(
            Time = as.double(tick_b$Time),
            DiffLow = min(priceDifference_1, priceDifference_2),
            DiffHigh = max(priceDifference_1, priceDifference_2),
            PriceHigh = max(tick_a$PriceHigh, tick_b$PriceHigh)
        ))
        
        # Alle 5 Mio. Datenpunkte: Ergebnis speichern
        if (nrowDT(result) > 5e6) {
            
            printf("\n5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...\n")
            
            # Ergebnis speichern
            saveInterimResult(result, result_set_index, exchange_a, exchange_b, currencyPair)
            result_set_index <- result_set_index + 1L
            
            # Ergebnisspeicher leeren
            result <- data.table()
            gc()
        }
    }
    
    # Rest speichern
    saveInterimResult(result, result_set_index, exchange_a, exchange_b, currencyPair)
    
    return(NULL)
}

# Nur Testlauf
#compareTwoExchanges("bitfinex", "bitstamp", "btcusd", as.POSIXct("2013-01-14 00:00:00"))

# Abarbeitung händisch parallelisieren
if (FALSE) {
    
    # Bitfinex:
    #   BTCUSD enthält Daten von 14.01.2013, 16:47:23 (UTC) bis heute
    #   BTCGBP enthält Daten von 29.03.2018, 14:40:57 (UTC) bis heute
    #   BTCJPY enthält Daten von 29.03.2018, 15:55:31 (UTC) bis heute
    #   BTCEUR enthält Daten von 01.09.2019, 00:00:00 (UTC) bis heute
    #
    # Bitstamp:
    #   BTCUSD enthält Daten von 13.09.2011, 13:53:36 (UTC) bis heute
    #   BTCEUR enthält Daten von 05.12.2017, 11:43:49 (UTC) bis heute
    #   BTCGBP enthält Daten von 14.12.2021, 14:48:35 (UTC) bis heute
    #
    # Coinbase Pro:
    #   BTCUSD enthält Daten von 01.12.2014, 05:33:56.761199 (UTC) bis heute.
    #   BTCEUR enthält Daten von 23.04.2015, 01:42:34.182104 (UTC) bis heute.
    #   BTCGBP enthält Daten von 21.04.2015, 22:22:41.294060 (UTC) bis heute.
    #
    # Kraken:
    #   BTCUSD enthält Daten von 06.10.2013, 21:34:15 (UTC) bis heute
    #   BTCEUR enthält Daten von 10.09.2013, 23:47:11 (UTC) bis heute
    #   BTCGBP enthält Daten von 06.11.2014, 16:13:43 (UTC) bis heute
    #   BTCJPY enthält Daten von 05.11.2014, 22:21:30 (UTC) bis heute
    #   BTCCAD enthält Daten von 29.06.2015, 03:27:41 (UTC) bis heute
    #   BTCCHF enthält Daten von 06.12.2019, 16:33:17 (UTC) bis heute
    
    # BTC/USD
    compareTwoExchanges("bitfinex", "bitstamp", "btcusd", as.POSIXct("2013-01-14 16:47:23"))
    compareTwoExchanges("bitfinex", "coinbase", "btcusd", as.POSIXct("2014-12-01 05:33:56"))
    compareTwoExchanges("bitfinex", "kraken",   "btcusd", as.POSIXct("2013-10-06 21:34:15"))
    compareTwoExchanges("bitstamp", "coinbase", "btcusd", as.POSIXct("2014-12-01 05:33:56"))
    compareTwoExchanges("bitstamp", "kraken",   "btcusd", as.POSIXct("2013-10-06 21:34:15"))
    compareTwoExchanges("coinbase", "kraken",   "btcusd", as.POSIXct("2014-12-01 05:33:56"))
    
    # BTC/EUR
    compareTwoExchanges("bitfinex", "bitstamp", "btceur", as.POSIXct("2019-09-01 00:00:00"))
    compareTwoExchanges("bitfinex", "coinbase", "btceur", as.POSIXct("2019-09-01 00:00:00"))
    compareTwoExchanges("bitfinex", "kraken",   "btceur", as.POSIXct("2019-09-01 00:00:00"))
    compareTwoExchanges("bitstamp", "coinbase", "btceur", as.POSIXct("2017-12-05 11:43:49"))
    compareTwoExchanges("bitstamp", "kraken",   "btceur", as.POSIXct("2017-12-05 11:43:49"))
    compareTwoExchanges("coinbase", "kraken",   "btceur", as.POSIXct("2015-04-23 01:42:34"))
    
    # BTC/GBP
    compareTwoExchanges("bitfinex", "bitstamp", "btcgbp", as.POSIXct("2021-12-14 14:48:35"))
    compareTwoExchanges("bitfinex", "coinbase", "btcgbp", as.POSIXct("2018-03-29 14:40:57"))
    compareTwoExchanges("bitfinex", "kraken",   "btcgbp", as.POSIXct("2018-03-29 14:40:57"))
    compareTwoExchanges("bitstamp", "coinbase", "btcgbp", as.POSIXct("2021-12-14 14:48:35"))
    compareTwoExchanges("bitstamp", "kraken",   "btcgbp", as.POSIXct("2021-12-14 14:48:35"))
    compareTwoExchanges("coinbase", "kraken",   "btcgbp", as.POSIXct("2015-04-21 22:22:41"))
    
    # BTC/JPY
    compareTwoExchanges("bitfinex", "kraken",   "btcjpy", as.POSIXct("2018-03-29 15:55:31"))
    
    # BTC/CHF: Nur bei Kraken handelbar
    # BTC/CAD: Nur bei Kraken handelbar
}

