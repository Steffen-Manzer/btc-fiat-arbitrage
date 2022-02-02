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
source("Klassen/Dataset.r")
source("Funktionen/AddOneMonth.r")
source("Funktionen/AppendToDataTable.r")
source("Funktionen/FormatCurrencyPair.r")
source("Funktionen/FormatDuration.r")
source("Funktionen/FormatNumber.r")
source("Funktionen/FormatPOSIXctWithFractionalSeconds.r")
source("Funktionen/ReadTickDataAsMovingWindow.r")
source("Funktionen/printf.r")
library("fst")
library("data.table")
library("lubridate") # floor_date
library("zoo") # rollapply für Filterfunktion


# Hilfsfunktionen -------------------------------------------------------------

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
        return(invisible(NULL))
    }
    
    # Behalte nur gemeinsame Daten.
    # Ergänze Datensatz um einen weiteren Datenpunkt für letzten Vergleich.
    # `data.table` kann leider noch kein subsetting per Referenz, sodass eine
    # Kopie (`<-`) notwendig ist.
    if (last_a > last_b) {
        
        # Datensatz A enthält mehr Daten als B
        pos_of_last_common_tick <- last(dataset_a$data[Time <= last_b, which=TRUE])
        dataset_a$data <- dataset_a$data[1L:(pos_of_last_common_tick+1L)]
        
    } else {
        
        # Datensatz B enthält mehr Daten als A
        pos_of_last_common_tick <- last(dataset_b$data[Time <= last_a, which=TRUE])
        dataset_b$data <- dataset_b$data[1L:(pos_of_last_common_tick+1L)]
        
    }
    
    # Ein resultierender Datensatz ist leer
    if (nrow(dataset_a$data) == 0L || nrow(dataset_b$data) == 0L) {
        stop("filterTwoDatasetsByCommonTimeInterval: ein Datensatz ist nach dem Filtern leer!\n")
    }
    
    return(invisible(NULL))
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
    return(dataset_ab[!triplets])
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
    
    # Ergebnis um zwischenzeitlich eingefügte `NA`s bereinigen
    # (= reservierter Speicher für weitere Ergebnisse)
    result <- cleanupDT(result)
    
    # Um Probleme mit appendDT (NA+POSIXct) zu umgehen, wurde der Zeitstempel
    # unten in ein double umgewandelt und wird an dieser Stelle wieder in 
    # POSIXct konvertiert.
    result[, Time:=as.POSIXct(Time, origin="1970-01-01")]
    
    # Arbitrageindex berechnen
    result[, ArbitrageIndex:=PriceHigh/PriceLow]
    
    # Ergebnis speichern
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
    
    # Ergebnisdatei existiert bereits
    firstOutputFile <- sprintf("Cache/Raumarbitrage/%s-%s-%s-1.fst",
                            tolower(currencyPair), exchange_a, exchange_b)
    if (file.exists(firstOutputFile)) {
        printf("Zieldatei %s bereits vorhanden!\n", firstOutputFile)
        return(invisible(NULL))
    }
    
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
    printf("\n  Beginne Auswertung für %s der Börsen %s und %s ab %s.\n\n", 
           format.currencyPair(currencyPair),
           exchange_a,
           exchange_b,
           format(startDate, "%d.%m.%Y %H:%M:%S")
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
        
        # Aktuelle und nächste Zeile speichern
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
        
        # Finde größte Preisdifferenz
        if (
            tick_a$PriceHigh - tick_b$PriceLow >=
            tick_b$PriceHigh - tick_a$PriceLow
        ) {
            PriceHigh <- tick_a$PriceHigh
            PriceLow <- tick_b$PriceLow
            ExchangeHigh <- tick_a$Exchange
            ExchangeLow <- tick_b$Exchange
        } else {
            PriceHigh <- tick_b$PriceHigh
            PriceLow <- tick_a$PriceLow
            ExchangeHigh <- tick_b$Exchange
            ExchangeLow <- tick_a$Exchange
        }
        
        # Set in Ergebnisvektor speichern
        # Anmerkung:
        # Um Probleme mit appendDT (NA+POSIXct) zu umgehen, wird der Zeitstempel
        # hier temporär in ein double (= Unixzeit inkl. Sekundenbruchteile) umgewandelt
        # und später wieder in POSIXct konvertiert. Dieses Vorgehen ist ohne
        # Informationsverlust und noch immer signifikant schneller als rbind()
        result <- appendDT(result, list(
            Time = as.double(tick_b$Time),
            PriceHigh = PriceHigh,
            PriceLow = PriceLow,
            ExchangeHigh = ExchangeHigh,
            ExchangeLow = ExchangeLow
        ))
        
        # 100 Mio. Datenpunkte im Ergebnisvektor: Zwischenspeichern
        if (nrowDT(result) > 1e8) {
            
            printf.debug(
                "\n100 Mio. Datenpunkte im Ergebnisvektor, Teilergebnis zwischenspeichern...\n"
            )
            
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
    
    printf("\n\n  Abgeschlossen.\n")
    
    return(invisible(NULL))
}


# Berechnung starten ----------------------------------------------------------

# Abarbeitung händisch parallelisieren, da CPU- und RAM-limitiert
if (FALSE) {
    
    # Verfügbare Daten nach Börse und Kurspaar:
    # Bitfinex:
    #   BTCUSD enthält Daten von 14.01.2013, 16:47:23 (UTC) bis heute
    #   BTCEUR enthält Daten von 01.09.2019, 00:00:00 (UTC) bis heute
    #   BTCGBP enthält Daten von 29.03.2018, 14:40:57 (UTC) bis heute
    #   BTCJPY enthält Daten von 29.03.2018, 15:55:31 (UTC) bis heute
    #
    # Bitstamp:
    #   BTCUSD enthält Daten von 18.08.2011, 12:37:25 (UTC) bis heute
    #   BTCEUR enthält Daten von 16.04.2016, 16:55:02 (UTC) bis heute
    #   BTCGBP enthält Daten von 28.05.2020, 09:37:26 (UTC) bis heute
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
    #   BTCAUD enthält Daten von 16.06.2020, 22:30:13 (UTC) bis heute
    
    
    # BTC/USD -----------------------------------------------------------------
    
    # Bitfinex - Bitstamp: ~2h10min. 31.615.047 Datensätze in 805,3 MB (unkomprimiert).
    compareTwoExchanges("bitfinex", "bitstamp", "btcusd", as.POSIXct("2013-01-14 16:47:23"))
    
    # Bitfinex - Coinbase Pro: ~3h22min. 63.107.943 Datensätze in 2 GB (unkomprimiert).
    compareTwoExchanges("bitfinex", "coinbase", "btcusd", as.POSIXct("2014-12-01 05:33:56"))
    
    # Bitfinex - Kraken: ~1h30min. 23.692.297 Datensätze in 758.2 MB (unkomprimiert).
    compareTwoExchanges("bitfinex", "kraken",   "btcusd", as.POSIXct("2013-10-06 21:34:15"))
    
    # Bitstamp - Coinbase Pro: ~?, 37.538.963 Datensätze in 1,6 GB (unkomprimiert).
    compareTwoExchanges("bitstamp", "coinbase", "btcusd", as.POSIXct("2014-12-01 05:33:56"))
    
    # Bitstamp - Kraken: ~2h30min. 16.332.564 Datensätze in 402,7 MB (unkomprimiert).
    compareTwoExchanges("bitstamp", "kraken",   "btcusd", as.POSIXct("2013-10-06 21:34:15"))
    
    # Coinbase Pro - Kraken: ~1h53min. 31.205.136 Datensätze in 998.6 MB (unkomprimiert).
    compareTwoExchanges("coinbase", "kraken",   "btcusd", as.POSIXct("2014-12-01 05:33:56"))
    
    
    # BTC/EUR -----------------------------------------------------------------
    
    # Bitfinex - Bitstamp: ~22min. 4.035.994 Datensätze in 100,7 MB (unkomprimiert).
    compareTwoExchanges("bitfinex", "bitstamp", "btceur", as.POSIXct("2019-09-01 00:00:00"))
    
    # Bitfinex - Coinbase Pro: ~27min. 7.244.291 Datensätze in 231.8 MB (unkomprimiert).
    compareTwoExchanges("bitfinex", "coinbase", "btceur", as.POSIXct("2019-09-01 00:00:00"))
    
    # Bitfinex - Kraken: ~28min. 6.351.131 Datensätze in 203.2 MB (unkomprimiert).
    compareTwoExchanges("bitfinex", "kraken",   "btceur", as.POSIXct("2019-09-01 00:00:00"))
    
    # Bitstamp - Coinbase Pro: ~1h10min. 16.767.027 Datensätze in 402,7 MB (unkomprimiert).
    compareTwoExchanges("bitstamp", "coinbase", "btceur", as.POSIXct("2016-04-16 16:55:02"))
    
    # Bitstamp - Kraken: ~1h11min. 15.322.141 Datensätze in 402,7 MB (unkomprimiert).
    compareTwoExchanges("bitstamp", "kraken",   "btceur", as.POSIXct("2016-04-16 16:55:02"))
    
    # Coinbase Pro - Kraken: ~1h38min. 26.473.562 Datensätze in 847.2 MB (unkomprimiert).
    compareTwoExchanges("coinbase", "kraken",   "btceur", as.POSIXct("2015-04-23 01:42:34"))
    
    
    # BTC/GBP -----------------------------------------------------------------
    
    # Bitfinex - Bitstamp: ~5min. 429.638 Datensätze in 12,6 MB (unkomprimiert).
    compareTwoExchanges("bitfinex", "bitstamp", "btcgbp", as.POSIXct("2020-05-28 09:37:26"))
    
    # Bitfinex - Coinbase Pro: ~18min. 4.517.869 Datensätze in 144.6 MB (unkomprimiert).
    compareTwoExchanges("bitfinex", "coinbase", "btcgbp", as.POSIXct("2018-03-29 14:40:57"))
    
    # Bitfinex - Kraken: ~4min. 610.423 Datensätze in 19.5 MB (unkomprimiert).
    compareTwoExchanges("bitfinex", "kraken",   "btcgbp", as.POSIXct("2018-03-29 14:40:57"))
    
    # Bitstamp - Coinbase Pro: ~6min. 978.408 Datensätze in 25,2 MB (unkomprimiert).
    compareTwoExchanges("bitstamp", "coinbase", "btcgbp", as.POSIXct("2020-05-28 09:37:26"))
    
    # Bitstamp - Kraken: ~2min30s. 192.105 Datensätze in 6,3 MB (unkomprimiert).
    compareTwoExchanges("bitstamp", "kraken",   "btcgbp", as.POSIXct("2020-05-28 09:37:26"))
    
    # Coinbase Pro - Kraken: ~7min. 1.396.126 Datensätze in 44.7 MB (unkomprimiert).
    compareTwoExchanges("coinbase", "kraken",   "btcgbp", as.POSIXct("2015-04-21 22:22:41"))
    
    
    # BTC/JPY -----------------------------------------------------------------
    
    # Bitfinex - Kraken: ~2min. 116.057 Datensätze in 3.7 MB (unkomprimiert).
    compareTwoExchanges("bitfinex", "kraken",   "btcjpy", as.POSIXct("2018-03-29 15:55:31"))
    
    
    # BTC/CHF -----------------------------------------------------------------
    # Nur bei Kraken handelbar
    
    
    # BTC/CAD -----------------------------------------------------------------
    # Nur bei Kraken handelbar
}

