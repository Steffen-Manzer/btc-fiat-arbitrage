# Finde Kurspaare zur (späteren) Analyse von Raumarbitrage.
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
        
        # Limit bestimmen: `numDatasetsPerRead` Datensätze, maximal bis zum Ende der Datei
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
    if (nrow(dataset$data) > 0L) {
        
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
        
        if (numNewRows > 0L) {
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
            numNewRows > 100L
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
        dataset_a$data <- dataset_a$data[1L:(pos_of_last_common_tick+1L),]
        
    } else {
        
        # Datensatz B enthält mehr Daten als A
        pos_of_last_common_tick <- last(dataset_b$data[Time <= last_a, which=TRUE])
        dataset_b$data <- dataset_b$data[1L:(pos_of_last_common_tick+1L),]
        
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
    return(dataset[ 
        j=.(
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
        FALSE, # Ersten Tick immer beibehalten
        rollapply(
            dataset_ab$Exchange,
            width = 3,
            # Es handelt sich um ein zu entfernendes Tripel, wenn  die
            # Börse im vorherigen, aktuellen und nächsten Tick identisch ist
            FUN = function(exchg) (exchg[1] == exchg[2] && exchg[2] == exchg[3])
        ),
        FALSE # Letzten Tick immer beibehalten
    )
    
    # Datensatz ohne gefundene Tripel zurückgeben
    return(dataset_ab[!triplets,])
}


#' Teilergebnis speichern, um Arbeitsspeicher wieder freizugeben
#' 
#' @param result Eine data.table mit den Spalten `Time`, `PriceLow` und `PriceHigh`
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
    
    # Arbitrageindex berechnen
    result[, ArbitrageIndex:=PriceHigh/PriceLow]
    
    # Ergebnis speichern
    write_fst(result, outFile, compress=100)
    
}


# Haupt-Auswertungsfunktion ===================================================

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
        
        # Höchst-/Tiefstpreise bestimmen: jeweils von zwei verschiedenen Börsen
        if (tick_a$PriceHigh >= tick_b$PriceHigh) {
            # Preisniveau an Börse A ist (hier) höher als an Börse B
            PriceHigh <- tick_a$PriceHigh
            PriceLow <- tick_b$PriceLow
        } else {
            # Preisniveau an Börse B ist (hier) höher als an Börse A
            PriceHigh <- tick_b$PriceHigh
            PriceLow <- tick_a$PriceLow
        }
        
        # Set in Ergebnisvektor speichern
        # Anmerkung:
        # Um Probleme mit appendDT (NA+POSIXct) zu umgehen, wird der Zeitstempel
        # hier temporär in ein double (= Unixzeit inkl. Sekundenbruchteile) umgewandelt
        # und später wieder in POSIXct konvertiert. Dieses Vorgehen ist ohne
        # Informationsverlust und noch immer signifikant schneller als rbind()
        result <- appendDT(result, list(
            Time = as.double(tick_b$Time),
            PriceLow = PriceLow,
            PriceHigh = PriceHigh
        ))
        
        # Alle 100 Mio. Datenpunkte: Ergebnis speichern
        if (nrowDT(result) > 1e8) {
            
            printf.debug("\n100 Mio. Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...\n")
            
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
    
    printf("Abgeschlossen.\n")
    
    return(invisible(NULL))
}


# Berechnung starten ==========================================================
# Nur Testlauf
#compareTwoExchanges("bitfinex", "bitstamp", "btcusd", as.POSIXct("2013-01-14 00:00:00"))

# Speicher-Warnung anzeigen
printf("Hinweis: Derzeit wird ab 100 Millionen Ergebnisdatensätzen ")
printf("eine weitere Datei begonnen und Arbeitsspeicher freigegeben.\n")
printf("Dies entspricht einer Speicherauslastung von geschätzt 3-4 GB.\n")

# Abarbeitung händisch parallelisieren, da CPU-limitiert
if (FALSE) {
    
    # Verfügbare Daten nach Börse und Kurspaar:
    # Bitfinex:
    #   BTCUSD enthält Daten von 14.01.2013, 16:47:23 (UTC) bis heute
    #   BTCEUR enthält Daten von 01.09.2019, 00:00:00 (UTC) bis heute
    #   BTCGBP enthält Daten von 29.03.2018, 14:40:57 (UTC) bis heute
    #   BTCJPY enthält Daten von 29.03.2018, 15:55:31 (UTC) bis heute
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
    
    
    # BTC/USD =================================================================
    
    # Bitfinex - Bitstamp: Abgeschlossen in ~2h05min. Log:
    # > compareTwoExchanges("bitfinex", "bitstamp", "btcusd", as.POSIXct("2013-01-14 16:47:23"))
    # Beginne Auswertung für BTCUSD der Börsen bitfinex und bitstamp ab 14.01.2013 16:47:23.
    # 10.240.000 verarbeitet  4.996.068 im Ergebnisvektor     Aktuell: 09.10.2017 02:36:14.000000     1.395 Sekunden  7.341 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 16.780.000 verarbeitet  4.993.676 im Ergebnisvektor     Aktuell: 07.02.2018 22:43:23.000000     2.330 Sekunden  7.202 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 24.210.000 verarbeitet  4.998.575 im Ergebnisvektor     Aktuell: 23.11.2018 07:05:59.000000     3.390 Sekunden  7.142 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 32.850.000 verarbeitet  4.994.631 im Ergebnisvektor     Aktuell: 27.01.2020 17:30:28.249000     4.651 Sekunden  7.063 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 41.380.000 verarbeitet  4.996.599 im Ergebnisvektor     Aktuell: 21.01.2021 22:10:12.719000     5.949 Sekunden  6.956 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 49.510.000 verarbeitet  4.996.076 im Ergebnisvektor     Aktuell: 03.10.2021 21:14:55.447999     7.185 Sekunden  6.891 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 51.810.000 verarbeitet  1.302.895 im Ergebnisvektor     Aktuell: 31.12.2021 18:01:21.000000     7.526 Sekunden  6.884 Ticks/s
    # Zeile 68 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitfinex", "bitstamp", "btcusd", as.POSIXct("2013-01-14 16:47:23"))
    
    # Bitfinex - Coinbase Pro: Abgeschlossen in ~3h42min. Log:
    # > compareTwoExchanges("bitfinex", "coinbase", "btcusd", as.POSIXct("2014-12-01 05:33:56"))
    # Beginne Auswertung für BTCUSD der Börsen bitfinex und coinbase ab 01.12.2014 05:33:56.
    # 10.370.000 verarbeitet  4.997.455 im Ergebnisvektor     Aktuell: 19.10.2017 22:59:32.164999     1.406 Sekunden  7.376 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 17.690.000 verarbeitet  4.998.008 im Ergebnisvektor     Aktuell: 01.02.2018 22:49:20.872999     2.403 Sekunden  7.362 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 25.580.000 verarbeitet  4.995.918 im Ergebnisvektor     Aktuell: 14.09.2018 20:33:23.000000     3.497 Sekunden  7.315 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 33.470.000 verarbeitet  4.993.628 im Ergebnisvektor     Aktuell: 29.05.2019 01:07:04.240999     4.603 Sekunden  7.271 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 41.270.000 verarbeitet  4.998.298 im Ergebnisvektor     Aktuell: 18.11.2019 01:39:00.144999     5.689 Sekunden  7.254 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 49.070.000 verarbeitet  4.999.312 im Ergebnisvektor     Aktuell: 10.05.2020 20:43:34.335000     6.752 Sekunden  7.267 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 56.790.000 verarbeitet  4.997.455 im Ergebnisvektor     Aktuell: 24.11.2020 21:47:58.134000     7.817 Sekunden  7.265 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 64.390.000 verarbeitet  4.994.361 im Ergebnisvektor     Aktuell: 23.01.2021 16:20:24.714999     8.805 Sekunden  7.313 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 72.000.000 verarbeitet  4.998.030 im Ergebnisvektor     Aktuell: 21.03.2021 13:19:56.426000     9.768 Sekunden  7.371 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 79.590.000 verarbeitet  4.999.612 im Ergebnisvektor     Aktuell: 21.05.2021 22:18:26.288000     10.702 Sekunden 7.437 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 87.430.000 verarbeitet  4.998.933 im Ergebnisvektor     Aktuell: 08.08.2021 23:49:38.280999     11.661 Sekunden 7.498 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 95.640.000 verarbeitet  4.996.806 im Ergebnisvektor     Aktuell: 10.11.2021 21:27:57.145181     12.705 Sekunden 7.528 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 100.770.000 verarbeitet 3.105.196 im Ergebnisvektor     Aktuell: 31.12.2021 22:05:29.650000     13.358 Sekunden 7.544 Ticks/s                         
    # Zeile 638 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitfinex", "coinbase", "btcusd", as.POSIXct("2014-12-01 05:33:56"))
    
    # Bitfinex - Kraken: Abgeschlossen in ~1h50min. Log:
    # > compareTwoExchanges("bitfinex", "kraken",   "btcusd", as.POSIXct("2013-10-06 21:34:15"))
    # Beginne Auswertung für BTCUSD der Börsen bitfinex und kraken ab 06.10.2013 21:34:15.
    # 9.440.000 verarbeitet   4.995.706 im Ergebnisvektor     Aktuell: 25.03.2018 18:41:10.240200     1.383 Sekunden  6.826 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 19.000.000 verarbeitet  4.998.349 im Ergebnisvektor     Aktuell: 25.10.2019 08:09:35.913000     2.769 Sekunden  6.862 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 28.510.000 verarbeitet  4.999.702 im Ergebnisvektor     Aktuell: 08.01.2021 18:29:23.358000     4.191 Sekunden  6.803 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 36.920.000 verarbeitet  4.995.607 im Ergebnisvektor     Aktuell: 30.05.2021 08:03:57.400000     5.494 Sekunden  6.720 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 43.780.000 verarbeitet  3.689.647 im Ergebnisvektor     Aktuell: 31.12.2021 19:50:55.767199     6.548 Sekunden  6.686 Ticks/s
    # Zeile 96 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitfinex", "kraken",   "btcusd", as.POSIXct("2013-10-06 21:34:15"))
    
    # Bitstamp - Coinbase Pro: Abgeschlossen in ~2h25min. Log:
    # > compareTwoExchanges("bitstamp", "coinbase", "btcusd", as.POSIXct("2014-12-01 05:33:56"))
    # Beginne Auswertung für BTCUSD der Börsen bitstamp und coinbase ab 01.12.2014 05:33:56.
    # 10.400.000 verarbeitet  4.996.918 im Ergebnisvektor     Aktuell: 13.12.2017 20:21:58.000000     1.475 Sekunden  7.051 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 18.330.000 verarbeitet  4.999.880 im Ergebnisvektor     Aktuell: 24.07.2018 03:19:01.000000     2.606 Sekunden  7.034 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 26.190.000 verarbeitet  4.995.338 im Ergebnisvektor     Aktuell: 28.06.2019 10:11:11.000000     3.761 Sekunden  6.964 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 33.990.000 verarbeitet  4.999.769 im Ergebnisvektor     Aktuell: 26.03.2020 12:57:49.000000     4.912 Sekunden  6.920 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 41.570.000 verarbeitet  4.997.077 im Ergebnisvektor     Aktuell: 16.12.2020 14:14:17.069999     6.020 Sekunden  6.905 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 48.970.000 verarbeitet  4.993.443 im Ergebnisvektor     Aktuell: 12.04.2021 11:12:22.000000     7.112 Sekunden  6.886 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 56.410.000 verarbeitet  4.996.428 im Ergebnisvektor     Aktuell: 12.10.2021 15:04:21.000000     8.256 Sekunden  6.833 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 59.640.000 verarbeitet  2.162.608 im Ergebnisvektor     Aktuell: 31.12.2021 19:38:36.892689     8.784 Sekunden  6.790 Ticks/s
    # Zeile 151 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitstamp", "coinbase", "btcusd", as.POSIXct("2014-12-01 05:33:56"))
    
    # Bitstamp - Kraken: Abgeschlossen in ~1h30min. Log:
    # Beginne Auswertung für BTCUSD der Börsen bitstamp und kraken ab 06.10.2013 21:34:15.
    # 10.280.000 verarbeitet  4.994.432 im Ergebnisvektor     Aktuell: 14.11.2018 17:09:17.000000     1.830 Sekunden  5.617 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 20.650.000 verarbeitet  4.994.159 im Ergebnisvektor     Aktuell: 16.11.2020 15:10:49.000000     3.512 Sekunden  5.880 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 29.290.000 verarbeitet  4.999.316 im Ergebnisvektor     Aktuell: 07.09.2021 23:24:44.289299     4.930 Sekunden  5.941 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 31.520.000 verarbeitet  1.152.041 im Ergebnisvektor     Aktuell: 31.12.2021 13:26:55.000000     5.268 Sekunden  5.983 Ticks/s
    compareTwoExchanges("bitstamp", "kraken",   "btcusd", as.POSIXct("2013-10-06 21:34:15"))
    
    # Coinbase Pro - Kraken: Abgeschlossen in ~2h10min. Log:
    # > compareTwoExchanges("coinbase", "kraken",   "btcusd", as.POSIXct("2014-12-01 05:33:56"))
    # Beginne Auswertung für BTCUSD der Börsen coinbase und kraken ab 01.12.2014 05:33:56.
    # 10.070.000 verarbeitet  4.997.447 im Ergebnisvektor     Aktuell: 12.06.2018 21:08:58.258599     1.429 Sekunden  7.047 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 18.800.000 verarbeitet  4.995.315 im Ergebnisvektor     Aktuell: 16.09.2019 00:52:44.920900     2.673 Sekunden  7.033 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 27.310.000 verarbeitet  4.998.373 im Ergebnisvektor     Aktuell: 18.09.2020 15:18:04.752000     3.892 Sekunden  7.017 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 35.520.000 verarbeitet  4.999.568 im Ergebnisvektor     Aktuell: 22.02.2021 15:03:54.328099     5.107 Sekunden  6.955 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 43.680.000 verarbeitet  4.997.697 im Ergebnisvektor     Aktuell: 25.05.2021 02:06:05.625299     6.287 Sekunden  6.948 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 51.850.000 verarbeitet  4.995.829 im Ergebnisvektor     Aktuell: 21.11.2021 14:59:35.730099     7.524 Sekunden  6.891 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 53.800.000 verarbeitet  1.199.576 im Ergebnisvektor     Aktuell: 31.12.2021 19:49:18.461896     7.823 Sekunden  6.877 Ticks/s
    # Zeile 372 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("coinbase", "kraken",   "btcusd", as.POSIXct("2014-12-01 05:33:56"))
    
    
    # BTC/EUR =================================================================
    
    # Bitfinex - Bitstamp: Abgeschlossen in ~20min. Log:
    # > compareTwoExchanges("bitfinex", "bitstamp", "btceur", as.POSIXct("2019-09-01 00:00:00"))
    # Beginne Auswertung für BTCEUR der Börsen bitfinex und bitstamp ab 01.09.2019 00:00:00.
    # 8.140.000 verarbeitet   3.986.002 im Ergebnisvektor     Aktuell: 31.12.2021 08:49:06.043999     1.162 Sekunden  7.005 Ticks/s
    # Zeile 29 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitfinex", "bitstamp", "btceur", as.POSIXct("2019-09-01 00:00:00"))
    
    # Bitfinex - Coinbase Pro: Abgeschlossen in ~31min. Log:
    # > compareTwoExchanges("bitfinex", "coinbase", "btceur", as.POSIXct("2019-09-01 00:00:00"))
    # Beginne Auswertung für BTCEUR der Börsen bitfinex und coinbase ab 01.09.2019 00:00:00.
    # 8.780.000 verarbeitet   4.997.267 im Ergebnisvektor     Aktuell: 03.06.2021 10:27:12.585000     1.272 Sekunden  6.903 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 12.650.000 verarbeitet  2.238.723 im Ergebnisvektor     Aktuell: 31.12.2021 16:46:28.536999     1.858 Sekunden  6.808 Ticks/s
    # Zeile 83 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitfinex", "coinbase", "btceur", as.POSIXct("2019-09-01 00:00:00"))
    
    # Bitfinex - Kraken: Abgeschlossen in ~30min. Log:
    # > compareTwoExchanges("bitfinex", "kraken",   "btceur", as.POSIXct("2019-09-01 00:00:00"))
    # Beginne Auswertung für BTCEUR der Börsen bitfinex und kraken ab 01.09.2019 00:00:00.
    # 9.220.000 verarbeitet   4.996.033 im Ergebnisvektor     Aktuell: 06.07.2021 01:08:55.990999     1.378 Sekunden  6.691 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 11.840.000 verarbeitet  1.346.365 im Ergebnisvektor     Aktuell: 31.12.2021 11:27:57.265100     1.762 Sekunden  6.720 Ticks/s
    # Zeile 70 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitfinex", "kraken",   "btceur", as.POSIXct("2019-09-01 00:00:00"))
    
    # Bitstamp - Coinbase Pro: Abgeschlossen in ~1h05min. Log:
    # > compareTwoExchanges("bitstamp", "coinbase", "btceur", as.POSIXct("2017-12-05 11:43:49"))
    # Beginne Auswertung für BTCEUR der Börsen bitstamp und coinbase ab 05.12.2017 11:43:49.
    # 9.740.000 verarbeitet   4.999.945 im Ergebnisvektor     Aktuell: 21.02.2020 14:23:27.252000     1.365 Sekunden  7.136 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 18.210.000 verarbeitet  4.993.989 im Ergebnisvektor     Aktuell: 09.03.2021 05:23:37.473000     2.584 Sekunden  7.047 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 26.110.000 verarbeitet  4.997.920 im Ergebnisvektor     Aktuell: 06.12.2021 06:46:23.750343     3.705 Sekunden  7.047 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 26.730.000 verarbeitet  385.965 im Ergebnisvektor       Aktuell: 31.12.2021 23:08:30.000000     3.793 Sekunden  7.047 Ticks/s
    # Zeile 41 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitstamp", "coinbase", "btceur", as.POSIXct("2017-12-05 11:43:49"))
    
    # Bitstamp - Kraken: Abgeschlossen in ~1h. Log:
    # Beginne Auswertung für BTCEUR der Börsen bitstamp und kraken ab 05.12.2017 11:43:49.
    # 10.200.000 verarbeitet  4.998.834 im Ergebnisvektor     Aktuell: 28.03.2020 03:27:42.000000     1.628 Sekunden  6.265 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 18.910.000 verarbeitet  4.998.957 im Ergebnisvektor     Aktuell: 06.04.2021 21:59:27.648900     2.851 Sekunden  6.633 Ticks/s                         
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 25.450.000 verarbeitet  3.814.120 im Ergebnisvektor     Aktuell: 31.12.2021 15:22:00.000000     3.667 Sekunden  6.940 Ticks/s
    compareTwoExchanges("bitstamp", "kraken",   "btceur", as.POSIXct("2017-12-05 11:43:49"))
    
    # Coinbase Pro - Kraken: Abgeschlossen in ~1h50min. Log:
    # > compareTwoExchanges("coinbase", "kraken",   "btceur", as.POSIXct("2015-04-23 01:42:34"))
    # Beginne Auswertung für BTCEUR der Börsen coinbase und kraken ab 23.04.2015 01:42:34.
    # 10.330.000 verarbeitet  4.995.579 im Ergebnisvektor     Aktuell: 22.06.2018 14:50:39.602400     1.499 Sekunden  6.891 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 20.550.000 verarbeitet  4.995.821 im Ergebnisvektor     Aktuell: 08.03.2020 16:07:07.574399     2.922 Sekunden  7.033 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 29.390.000 verarbeitet  4.994.554 im Ergebnisvektor     Aktuell: 04.01.2021 15:07:24.138499     4.145 Sekunden  7.090 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 37.460.000 verarbeitet  4.996.529 im Ergebnisvektor     Aktuell: 09.05.2021 23:34:49.388000     5.204 Sekunden  7.198 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 45.780.000 verarbeitet  4.999.794 im Ergebnisvektor     Aktuell: 08.11.2021 21:33:59.143769     6.276 Sekunden  7.294 Ticks/s
    # 5.000.000 Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...
    # 48.190.000 verarbeitet  1.469.499 im Ergebnisvektor     Aktuell: 31.12.2021 20:08:56.862900     6.584 Sekunden  7.319 Ticks/s
    # Zeile 87 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("coinbase", "kraken",   "btceur", as.POSIXct("2015-04-23 01:42:34"))
    
    
    # BTC/GBP =================================================================
    
    # Bitfinex - Bitstamp: Abgeschlossen in wenigen Sekunden.
    # Anmerkung: Daten für BTC/GBP an Bitstamp wurden erst ab 14.12.2021 erfasst und sind daher
    # für eine repräsentative Auswertung nicht geeignet. Log:
    # > compareTwoExchanges("bitfinex", "bitstamp", "btcgbp", as.POSIXct("2021-12-14 14:48:35"))
    # Beginne Auswertung für BTCGBP der Börsen bitfinex und bitstamp ab 14.12.2021 14:48:35.
    # 40.000 verarbeitet      10.801 im Ergebnisvektor        Aktuell: 31.12.2021 14:58:33.269999     6 Sekunden      6.667 Ticks/s
    # Zeile 24 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitfinex", "bitstamp", "btcgbp", as.POSIXct("2021-12-14 14:48:35"))
    
    # Bitfinex - Coinbase Pro: Abgeschlossen in ~22min. Log:
    # > compareTwoExchanges("bitfinex", "coinbase", "btcgbp", as.POSIXct("2018-03-29 14:40:57"))
    # Beginne Auswertung für BTCGBP der Börsen bitfinex und coinbase ab 29.03.2018 14:40:57.
    # 8.710.000 verarbeitet   4.516.061 im Ergebnisvektor     Aktuell: 31.12.2021 19:45:59.525652     1.274 Sekunden  6.837 Ticks/s
    # Zeile 31 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitfinex", "coinbase", "btcgbp", as.POSIXct("2018-03-29 14:40:57"))
    
    # Bitfinex - Kraken: Abgeschlossen in ~6min. Log:
    # > compareTwoExchanges("bitfinex", "kraken",   "btcgbp", as.POSIXct("2018-03-29 14:40:57"))
    # Beginne Auswertung für BTCGBP der Börsen bitfinex und kraken ab 29.03.2018 14:40:57.
    # 1.930.000 verarbeitet   607.616 im Ergebnisvektor       Aktuell: 28.12.2021 16:36:07.904000     314 Sekunden    6.146 Ticks/s
    # Zeile 38 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitfinex", "kraken",   "btcgbp", as.POSIXct("2018-03-29 14:40:57"))
    
    # Bitstamp - Coinbase Pro: Abgeschlossen in wenigen Sekunden.
    # Anmerkung: Daten für BTC/GBP an Bitstamp wurden erst ab 14.12.2021 erfasst und sind daher
    # für eine repräsentative Auswertung nicht geeignet. Log:
    # > compareTwoExchanges("bitstamp", "coinbase", "btcgbp", as.POSIXct("2021-12-14 14:48:35"))
    # Beginne Auswertung für BTCGBP der Börsen bitstamp und coinbase ab 14.12.2021 14:48:35.
    # 50.000 verarbeitet      26.028 im Ergebnisvektor        Aktuell: 31.12.2021 12:20:52.996440     10 Sekunden     5.000 Ticks/s
    # Zeile 596 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitstamp", "coinbase", "btcgbp", as.POSIXct("2021-12-14 14:48:35"))
    
    # Bitstamp - Kraken: Abgeschlossen in wenigen Sekunden.
    # Anmerkung: Daten für BTC/GBP an Bitstamp wurden erst ab 14.12.2021 erfasst und sind daher
    # für eine repräsentative Auswertung nicht geeignet. Log:
    # Beginne Auswertung für BTCGBP der Börsen bitstamp und kraken ab 14.12.2021 14:48:35.
    # 20.000 verarbeitet      2.747 im Ergebnisvektor        Aktuell: 28.12.2021 01:38:13.405900     2 Sekunden      10.000 Ticks/s
    compareTwoExchanges("bitstamp", "kraken",   "btcgbp", as.POSIXct("2021-12-14 14:48:35"))
    
    # Coinbase Pro - Kraken: Abgeschlossen in ~9min. Log:
    # > compareTwoExchanges("coinbase", "kraken",   "btcgbp", as.POSIXct("2015-04-21 22:22:41"))
    # Beginne Auswertung für BTCGBP der Börsen coinbase und kraken ab 21.04.2015 22:22:41.
    # 2.970.000 verarbeitet   1.394.485 im Ergebnisvektor     Aktuell: 31.12.2021 03:36:18.994821     496 Sekunden    5.988 Ticks/s
    # Zeile 12 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("coinbase", "kraken",   "btcgbp", as.POSIXct("2015-04-21 22:22:41"))
    
    
    # BTC/JPY =================================================================
    
    # Bitfinex - Kraken: Abgeschlossen in ~2min. Log:
    # > compareTwoExchanges("bitfinex", "kraken",   "btcjpy", as.POSIXct("2018-03-29 15:55:31"))
    # Beginne Auswertung für BTCJPY der Börsen bitfinex und kraken ab 29.03.2018 15:55:31.
    # 330.000 verarbeitet     113.993 im Ergebnisvektor       Aktuell: 15.12.2021 04:46:52.596600     114 Sekunden    2.895 Ticks/s
    # Zeile 33 erreicht, Ende.
    # Abgeschlossen.
    compareTwoExchanges("bitfinex", "kraken",   "btcjpy", as.POSIXct("2018-03-29 15:55:31"))
    
    
    # BTC/CHF =================================================================
    # Nur bei Kraken handelbar
    
    
    # BTC/CAD =================================================================
    # Nur bei Kraken handelbar
}

