#' Finde Kurstripel zur Analyse von Dreiecksarbitrage.
#' 
#' Grundablauf: Immer paarweiser Vergleich zweier Währungspaare an einer Börse
#' mit "Moving Window", ähnlich zum Verfahren des `mergesort`-Algorithmus.
#' Lese neue Daten sequentiell und lösche verarbeitete Daten regelmäßig, 
#' um den Bedarf an Arbeitsspeicher verhältnismäßig gering zu halten und 
#' somit eine gute Parallelisierbarkeit zu ermöglichen.
#' 
#' Hinweis: Mit der aktuellen Einstellung wird erst nach 100 Mio. gefundenen
#' Preis-Paaren eine neue Datei begonnen und Arbeitsspeicher freigegeben.
#' Damit wird im Maximum bis zu ca. 5 GB Arbeitsspeicher belegt.
#'
#' - Erste x Daten der Börse und des Wechselkurses laden = 
#'   "Betrachtungsfenster" initialisieren:
#'   [t = 0, ..., t = 1h], mindestens jedoch je 10.000 Datenpunkte.
#' - Filtere die Datensätze auf einen gemeinsamen Zeitraum.
#' - Liste beider BTC-Kurse in eine einzelne Liste vereinen,
#'   nach Datum sortieren und filtern (siehe unten).
#' - Solange Daten für beide BTC-Börsen vorhanden sind:
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
source("Funktionen/DetermineCurrencyPairOrder.R")
source("Funktionen/FilterTwoDatasetsByCommonInterval.R")
source("Funktionen/FindLastDatasetBeforeTimestamp.R")
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
library("lubridate") # is.POSIXct, floor_date


# Haupt-Auswertungsfunktion ---------------------------------------------------

#' Dreiecksarbitrage für die Route A -> BTC -> B -> A bestimmen
#' 
#' @param exchange Name der Bitcoin-Börse
#' @param currency_a Gegenwährung 1 (z.B. usd)
#' @param currency_b Gegenwährung 2 (z.B. eur)
#' @param startDate Beginne Vergleich ab diesem Datum
#'    (= Zeitpunkt des ersten gemeinsamen Datensatzes)
#' @param bitcoinComparisonThresholdSeconds Zeitliche Differenz zweier Ticks,
#'    ab der das Tick-Paar verworfen wird.
#' @param forexComparisonThresholdHours Zeitliche Differenz zum nächsten Wechselkurs,
#'     ab der das Tick-Paar verworfen wird.
#' @return `NULL` Ergebnisse werden unter
#'     Cache/Dreiecksarbitrage/`exchange`-`currency_a`-`currency_b`.fst
#'     gespeichert
calculateTriangularArbitragePriceTriples <- function(
    exchange,
    currency_a,
    currency_b,
    startDate,
    endDate,
    bitcoinComparisonThresholdSeconds = 1L,
    forexComparisonThresholdHours = 1L
) {
    if (!is.POSIXct(startDate)) {
        startDate <- as.POSIXct(startDate)
    }
    
    # Parameter validieren
    stopifnot(
        is.character(exchange), length(exchange) == 1L,
        is.character(currency_a), length(currency_a) == 1L,
        is.character(currency_b), length(currency_b) == 1L,
        length(startDate) == 1L,
        is.POSIXct(endDate), length(endDate) == 1L,
        startDate < endDate,
        is.integer(bitcoinComparisonThresholdSeconds),
        length(bitcoinComparisonThresholdSeconds) == 1L,
        is.integer(forexComparisonThresholdHours),
        length(forexComparisonThresholdHours) == 1L
    )
    
    currency_a <- tolower(currency_a)
    currency_b <- tolower(currency_b)
    
    # Ergebnisdatei
    outputFile <- sprintf(
        "Cache/Dreiecksarbitrage/%ds/%s-%s-%s.fst",
        bitcoinComparisonThresholdSeconds, exchange, currency_a, currency_b
    )
    if (file.exists(outputFile)) {
        printf("Zieldatei %s bereits vorhanden!\n", outputFile)
        return(invisible(NULL))
    }
    if (!dir.exists(dirname(outputFile))) {
        dir.create(dirname(outputFile), recursive=TRUE)
    }
    
    # Bezeichnungen normalisieren
    pair_a_b <- determineCurrencyPairOrder(currency_a, currency_b)
    pair_btc_a <- determineCurrencyPairOrder("btc", currency_a)
    pair_btc_b <- determineCurrencyPairOrder("btc", currency_b)
    
    # Handelszeiten des Wechselkurses (z.B. EUR/USD) laden
    # Wird genutzt, um bereits beim Einlesen der Bitcoin-Daten die
    # Abschnitte handelsfreier Zeiten zu überspringen.
    trading_hours_a_b <- read_fst(
        sprintf("Cache/dukascopy/%s/dukascopy-%1$s-hourly.fst", pair_a_b),
        as.data.table = TRUE,
        columns = c("Time")
    )[Time >= startDate, Time]
    
    
    # Datenobjekte initialisieren, die später per Referenz übergeben
    # werden können. So kann direkt an den Daten gearbeitet werden,
    # ohne dass immer eine Kopie angelegt werden muss.
    dataset_a_b <- new(
        "Dataset",
        CurrencyPair = pair_a_b,
        PathPrefix = sprintf(
            "Cache/dukascopy/%1$s/tick/dukascopy-%1$s-tick",
            pair_a_b
        ),
        EndDate = endDate,
        data = data.table()
    )
    dataset_btc_a <- new(
        "Dataset",
        CurrencyPair = pair_btc_a,
        PathPrefix = sprintf("Cache/%s/%s/tick/%1$s-%2$s-tick", exchange, pair_btc_a),
        EndDate = endDate,
        data = data.table(),
        TradingHours = trading_hours_a_b
        # [27.04.2022] Keine Filterung mehr vornehmen
        #SuspiciousPeriods = loadSuspiciousPeriods(exchange, pair_btc_a)
    )
    dataset_btc_b <- new(
        "Dataset",
        CurrencyPair = pair_btc_b,
        PathPrefix = sprintf("Cache/%s/%s/tick/%1$s-%2$s-tick", exchange, pair_btc_b),
        EndDate = endDate,
        data = data.table(),
        TradingHours = trading_hours_a_b
        # [27.04.2022] Keine Filterung mehr vornehmen
        #SuspiciousPeriods = loadSuspiciousPeriods(exchange, pair_btc_b)
    )
    
    # -- Diese Schritte werden regelmäßig wiederholt, um sequentiell weitere Daten zu laden --
    
    # Daten für die ersten 60 Minuten laden
    startDate <- max(startDate, trading_hours_a_b[1])
    loadUntil <- startDate + 60 * 60
    readTickDataAsMovingWindow(dataset_btc_a, startDate, loadUntil)
    readTickDataAsMovingWindow(dataset_btc_b, startDate, loadUntil)
    forexLoadUntil <- min(last(dataset_btc_a$data$Time), last(dataset_btc_b$data$Time))
    readTickDataAsMovingWindow(
        dataset_a_b, startDate, forexLoadUntil, columns = c("Time", "Bid", "Ask")
    )
    printf.debug(
        "%s: %s Tickdaten von %s bis %s\n",
        format.currencyPair(pair_btc_a),
        format.number(nrow(dataset_btc_a$data)),
        first(dataset_btc_a$data$Time),
        last(dataset_btc_a$data$Time)
    )
    printf.debug(
        "%s: %s Tickdaten von %s bis %s\n",
        format.currencyPair(pair_btc_b),
        format.number(nrow(dataset_btc_b$data)),
        first(dataset_btc_b$data$Time),
        last(dataset_btc_b$data$Time)
    )
    printf.debug(
        "%s: %s Tickdaten von %s bis %s\n",
        format.currencyPair(pair_a_b),
        format.number(nrow(dataset_a_b$data)),
        first(dataset_a_b$data$Time),
        last(dataset_a_b$data$Time)
    )
    
    # Begrenze auf gemeinsamen Zeitraum. Beschleunigt anschließendes
    # Merge + Sort + Filter je nach Struktur der Daten deutlich.
    filterTwoDatasetsByCommonTimeInterval(dataset_btc_a, dataset_btc_b)
    printf.debug(
        "%s: %s gemeinsame Tickdaten von %s bis %s\n",
        format.currencyPair(pair_btc_a),
        format.number(nrow(dataset_btc_a$data)),
        first(dataset_btc_a$data$Time),
        last(dataset_btc_a$data$Time)
    )
    printf.debug(
        "%s: %s gemeinsame Tickdaten von %s bis %s\n",
        format.currencyPair(pair_btc_b),
        format.number(nrow(dataset_btc_b$data)),
        first(dataset_btc_b$data$Time),
        last(dataset_btc_b$data$Time)
    )
    
    # Merge + Sort + Filter, danke an Lukas Fischer (@o1oo11oo) für die Idee
    # Zuvor mehrerer Ticks am selben Zeitpunkt zu einem einzigen Datenpunkt
    # zusammenfassen und dabei Mindest-/Höchstpreis berechnen
    #
    # Mögliche Handelsrouten am Beispiel BTC - USD - EUR:
    # 1. BTC -> USD -> EUR -> BTC
    # 2. EUR -> BTC -> USD -> EUR
    # 3. USD -> BTC -> EUR -> USD
    #
    # Hier: Nie ausgehend von BTC, immer A -> BTC -> B -> A
    dataset_btc_ab <- mergeSortAndFilterTwoDatasets(
        dataset_btc_a$data |> summariseMultipleTicksAtSameTime(),
        dataset_btc_b$data |> summariseMultipleTicksAtSameTime(),
        compare_by = "CurrencyPair"
    )
    printf.debug(
        "%s + %s: %s Tickdaten von %s bis %s\n",
        format.currencyPair(pair_btc_a),
        format.currencyPair(pair_btc_b),
        format.number(nrow(dataset_btc_ab)),
        first(dataset_btc_ab$Time),
        last(dataset_btc_ab$Time)
    )
    
    # Aktuelle Position merken
    currentRow <- 0L
    
    # Neue Daten erst unmittelbar vor Ende des Datensatzes nachladen,
    # aber etwas Puffer für `mergeSortAndFilterTwoDatasets` lassen.
    numRows <- nrow(dataset_btc_ab)
    loadNewDataAtRowNumber <- numRows - 5L
    
    # Flag: Ende der verfügbaren Daten erreicht, keine weiteren Dateien mehr lesen
    endAfterCurrentDataset <- FALSE
    
    # -- Ende nötige Wiederholung --
    
    # Ergebnisvektor
    result <- data.table()
    
    # Fortschritt aufzeichnen und ausgeben
    processedDatasets <- 0L
    now <- proc.time()["elapsed"]
    
    # Statistiken
    numDatasetsOutOfBitcoinThreshold <- 0L
    numDatasetsOutOfForexThreshold <- 0L
    
    # Hauptschleife: Paarweise vergleichen
    printf("\n  Beginne Auswertung für %s und %s an der Börse %s ab %s (BTC-Threshold: %ds).\n\n", 
           format.currencyPair(pair_btc_a),
           format.currencyPair(pair_btc_b),
           exchange,
           format(startDate, "%d.%m.%Y %H:%M:%S"),
           bitcoinComparisonThresholdSeconds
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
            #           Runtime nInput  Time    nResult Size    Speed
            printf("\r  % 13s   % 11s   % 26s   % 10s   % 8s   % 6s T/s",
                   format.duration(runtime),
                   format.number(processedDatasets),
                   formatPOSIXctWithFractionalSeconds(
                       dataset_btc_ab$Time[currentRow], "%d.%m.%Y %H:%M:%OS"
                   ),
                   format.number(nrowDT(result)),
                   format(object.size(result), units="auto", standard="SI"),
                   format.number(round(processedDatasets/runtime, 0))
            )
        }
        
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
                         format(dataset_btc_ab$Time[currentRow]))
            currentTick <- dataset_btc_ab[currentRow]
            
            # Lese weitere Daten ab letztem gemeinsamen Datenpunkt
            baseDate <- currentTick$Time
            loadUntil <- last(dataset_btc_ab$Time) + 60 * 60
            
            # Ende erreicht
            if (loadUntil > endDate) {
                loadUntil <- endDate
                endAfterCurrentDataset <- TRUE
                printf.debug("Datenende erreicht, Stop nach aktuellem Monat.\n")
            }
            
            readTickDataAsMovingWindow(dataset_btc_a, baseDate, loadUntil)
            readTickDataAsMovingWindow(dataset_btc_b, baseDate, loadUntil)
            
            printf.debug(
                "%s: %s Tickdaten von %s bis %s\n",
                format.currencyPair(pair_btc_a),
                format.number(nrow(dataset_btc_a$data)),
                first(dataset_btc_a$data$Time),
                last(dataset_btc_a$data$Time)
            )
            printf.debug(
                "%s: %s Tickdaten von %s bis %s\n",
                format.currencyPair(pair_btc_b),
                format.number(nrow(dataset_btc_b$data)),
                first(dataset_btc_b$data$Time),
                last(dataset_btc_b$data$Time)
            )
            
            # Begrenze auf gemeinsamen Zeitraum
            filterTwoDatasetsByCommonTimeInterval(dataset_btc_a, dataset_btc_b)
            
            # Zu wenig gemeinsame Daten (Datenlücke eines Datensatzes!)
            # Weitere Daten nachladen, bis mehr als 50 gemeinsame Daten vorliegen
            # TODO Hier müsste im Grunde ein dynamisches Limit greifen
            # - min. 50 gemeinsame Daten - manchmal aber auch > 100 nötig
            # - nicht zu nah an der *neuen* currentRow! -> Problematisch?
            # Außerdem: Verletzung des DRY-Prinzips
            while (
                !endAfterCurrentDataset &&
                (nrow(dataset_btc_a$data) < 500 || nrow(dataset_btc_b$data) < 500)
            ) {
                loadUntil <- loadUntil + 60*60
                printf.debug(
                    "< 500 gemeinsame Daten (Lücke!), lade weitere bis %s.\n",
                    format(loadUntil)
                )
                
                # Ende erreicht
                if (loadUntil > endDate) {
                    loadUntil <- endDate
                    endAfterCurrentDataset <- TRUE
                    printf.debug("Datenende erreicht, Stop nach aktuellem Monat.\n")
                }
                
                if (last(dataset_btc_a$data$Time < loadUntil)) {
                    readTickDataAsMovingWindow(dataset_btc_a, baseDate, loadUntil)
                }
                if (last(dataset_btc_b$data$Time < loadUntil)) {
                    readTickDataAsMovingWindow(dataset_btc_b, baseDate, loadUntil)
                }
                
                # Begrenze auf gemeinsamen Zeitraum
                filterTwoDatasetsByCommonTimeInterval(dataset_btc_a, dataset_btc_b)
            }
            
            # Erst dann Wechselkurs-Daten nachladen, wenn Bitcoin-Daten vollständig sind
            forexLoadUntil <- min(last(dataset_btc_a$data$Time), last(dataset_btc_b$data$Time))
            readTickDataAsMovingWindow(
                dataset_a_b, baseDate, forexLoadUntil, columns = c("Time", "Bid", "Ask")
            )
            printf.debug(
                "%s: %s Tickdaten von %s bis %s\n",
                format.currencyPair(pair_a_b),
                format.number(nrow(dataset_a_b$data)),
                first(dataset_a_b$data$Time),
                last(dataset_a_b$data$Time)
            )
            
            printf.debug(
                "%s: %s gemeinsame Tickdaten von %s bis %s\n",
                format.currencyPair(pair_btc_a),
                format.number(nrow(dataset_btc_a$data)),
                first(dataset_btc_a$data$Time),
                last(dataset_btc_a$data$Time)
            )
            printf.debug(
                "%s: %s gemeinsame Tickdaten von %s bis %s\n",
                format.currencyPair(pair_btc_b),
                format.number(nrow(dataset_btc_b$data)),
                first(dataset_btc_b$data$Time),
                last(dataset_btc_b$data$Time)
            )
            
            # Ticks zum selben Zeitpunkt zusammenfassen, dann Merge + Sort + Filter
            dataset_btc_ab <- mergeSortAndFilterTwoDatasets(
                dataset_btc_a$data |> summariseMultipleTicksAtSameTime(),
                dataset_btc_b$data |> summariseMultipleTicksAtSameTime(),
                compare_by = "CurrencyPair"
            )
            printf.debug(
                "%s + %s: %s Tickdaten von %s bis %s\n",
                format.currencyPair(pair_btc_a),
                format.currencyPair(pair_btc_b),
                format.number(nrow(dataset_btc_ab)),
                first(dataset_btc_ab$Time),
                last(dataset_btc_ab$Time)
            )
            
            # Aktuelle Position (`currentRow`) korrigieren, befindet sich nun
            # am Beginn des (neuen) Datensatzes
            currentRow <- dataset_btc_ab[
                Time == currentTick$Time & 
                    PriceLow == currentTick$PriceLow &
                    PriceHigh == currentTick$PriceHigh &
                    CurrencyPair == currentTick$CurrencyPair &
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
            numRows <- nrow(dataset_btc_ab)
            loadNewDataAtRowNumber <- numRows - 5L
        }
        
        # Aktuelle und nächste Zeile vergleichen
        tick_btc_1 <- dataset_btc_ab[currentRow]
        tick_btc_2 <- dataset_btc_ab[nextRow]
        
        # Gleiches Kurspaar, überspringe.
        if (tick_btc_1$CurrencyPair == tick_btc_2$CurrencyPair) {
            next
        }
        
        # Zeitdifferenz zwischen den Bitcoin-Ticks zu groß, überspringe.
        bitcoinTimeDifference <- difftime(tick_btc_2$Time, tick_btc_1$Time, units="secs")
        if (bitcoinTimeDifference > bitcoinComparisonThresholdSeconds) {
            numDatasetsOutOfBitcoinThreshold <- numDatasetsOutOfBitcoinThreshold + 1L
            next
        }
        
        # Letzten gültigen Wechselkurs heraussuchen
        # Da die hier betrachteten Wechselkurse
        #   a) eine hervorragende Liquidität aufweisen und
        #   b) Market-Maker aktiv sind,
        # wird unterstellt, dass jederzeit ein Handel möglich ist.
        # Gültig ist die jeweils letzte Kursnotierung.
        tick_ab <- findLastDatasetBeforeTimestamp(dataset_a_b$data, tick_btc_2$Time)
        
        # (Noch) kein passender Wechselkurs gefunden
        if (nrow(tick_ab) == 0L || (length(tick_ab) == 1L && is.na(tick_ab))) {
            next
        }
        
        # Zeitliche Differenz zu groß
        # Dieser Fall kann nach einer handelsfreien Zeit eintreffen, wenn
        # bereits ein Bitcoin-Paar gefunden wurde, aber noch kein neuer Wechselkurs
        # notiert ist.
        forexTickAge <- difftime(tick_btc_2$Time, tick_ab$Time, units="secs")
        if (forexTickAge > forexComparisonThresholdHours * 60**2) {
            numDatasetsOutOfForexThreshold <- numDatasetsOutOfForexThreshold + 1L
            next
        }
        
        # Set in Ergebnisvektor speichern
        # Anmerkung:
        # Um Probleme mit appendDT (NA+POSIXct) zu umgehen, wird der Zeitstempel
        # hier temporär in ein double (= Unixzeit inkl. Sekundenbruchteile) umgewandelt
        # und später wieder in POSIXct konvertiert. Dieses Vorgehen ist ohne
        # Informationsverlust und noch immer signifikant schneller als rbind()
        if (tick_btc_1$CurrencyPair == pair_btc_a) {
            
            # Tick 1 ist BTC/A, Tick 2 ist BTC/B
            # tick_btc_a <- tick_btc_1
            # tick_btc_b <- tick_btc_2
            result <- appendDT(result, list(
                Time = as.double(tick_btc_2$Time),
                firstTick = "a",
                a_PriceLow = tick_btc_1$PriceLow,
                a_PriceHigh = tick_btc_1$PriceHigh,
                b_PriceLow = tick_btc_2$PriceLow,
                b_PriceHigh = tick_btc_2$PriceHigh,
                ab_Bid = tick_ab$Bid,
                ab_Ask = tick_ab$Ask,
                bitcoinTimeDifference = as.numeric(bitcoinTimeDifference),
                forexTickAge = as.numeric(forexTickAge)
            ))
            
        } else {
            
            # Tick 1 ist BTC/B, Tick 2 ist BTC/A
            # tick_btc_a <- tick_btc_2
            # tick_btc_b <- tick_btc_1
            result <- appendDT(result, list(
                Time = as.double(tick_btc_2$Time),
                firstTick = "b",
                a_PriceLow = tick_btc_2$PriceLow,
                a_PriceHigh = tick_btc_2$PriceHigh,
                b_PriceLow = tick_btc_1$PriceLow,
                b_PriceHigh = tick_btc_1$PriceHigh,
                ab_Bid = tick_ab$Bid,
                ab_Ask = tick_ab$Ask,
                bitcoinTimeDifference = as.numeric(bitcoinTimeDifference),
                forexTickAge = as.numeric(forexTickAge)
            ))
            
        }
    }
    
    # Speichern
    # Ergebnis um zwischenzeitlich eingefügte `NA`s bereinigen
    # (= reservierter Speicher für weitere Ergebnisse)
    result <- cleanupDT(result)
    
    # Um Probleme mit appendDT (NA+POSIXct) zu umgehen, wurde der Zeitstempel
    # zuvor in ein double umgewandelt und an dieser Stelle wieder in POSIXct konvertiert.
    result[, Time:=as.POSIXct(Time, origin="1970-01-01")]
    
    printf("\n\n%s Datensätze in %s (unkomprimiert).",
           format.number(nrow(result)),
           format(object.size(result), units="auto", standard="SI"))
    
    # Ergebnis speichern
    write_fst(result, outputFile, compress=100)
    
    printf("\n\n  Abgeschlossen.\n")
    printf("===============\n")
    printf("Statistiken:\n")
    printf(
        "%s Ticks verworfen, da außerhalb des Bitcoin-Zeitfensters von %d s.\n", 
        format.number(numDatasetsOutOfBitcoinThreshold), bitcoinComparisonThresholdSeconds
    )
    printf(
        "%s Ticks verworfen, da Wechselkurs älter als %d h.\n", 
        format.number(numDatasetsOutOfForexThreshold), forexComparisonThresholdHours
    )
    printf("===============\n")
    
    # Statistiken speichern
    statFile <- sprintf(
        "Cache/Dreiecksarbitrage/%ds/%s-%s-%s.stats.fst",
        bitcoinComparisonThresholdSeconds, exchange, currency_a, currency_b
    )
    if (file.exists(statFile)) {
        unlink(statFile)
    }
    write_fst(
        data.table(
            bitcoinComparisonThresholdSeconds = bitcoinComparisonThresholdSeconds,
            numDatasetsOutOfBitcoinThreshold = numDatasetsOutOfBitcoinThreshold,
            forexComparisonThresholdHours = forexComparisonThresholdHours,
            numDatasetsOutOfForexThreshold = numDatasetsOutOfForexThreshold
        ),
        path = statFile,
        compress = 100L
    )
    
    return(invisible(NULL))
}


# Berechnung starten ----------------------------------------------------------

# Verfügbare Daten nach Börse und Kurspaar:
# Börse     BTC/USD     BTC/EUR     BTC/GBP     BTC/JPY     BTC/CHF     BTC/AUD     BTC/CAD
# Bitfinex  14.01.2013  19.05.2017  29.03.2018  29.03.2018  -           -
# Bitstamp  18.08.2011  16.04.2016  28.05.2020  -           -           -
# Coinbase  01.12.2014  23.04.2015  21.04.2015  -           -           -
# Kraken    06.10.2013  10.09.2013  06.11.2014  05.11.2014  06.12.2019  16.06.2020  29.06.2015

# Abarbeitung händisch parallelisieren, da CPU-limitiert
if (FALSE) {
    endDate <- as.POSIXct("2022-01-01 00:00:00") - .000001
    
    # Bitfinex
    # 1s: 2h06min. 18.174.296 Datensätze in 1.5 GB (unkomprimiert).
    #     2.416.344 Ticks verworfen, da außerhalb des Bitcoin-Zeitfensters von 1 s.
    #         2.611 Ticks verworfen, da Wechselkurs älter als 1 h.
    calculateTriangularArbitragePriceTriples("bitfinex", "usd", "eur", "2017-05-21", endDate)
    
    # Bitstamp
    # 1s: 1h11min. 7.984.800 Datensätze in 638.8 MB (unkomprimiert).
    #     6.255.518 Ticks verworfen, da außerhalb des Bitcoin-Zeitfensters von 1 s.
    #           732 Ticks verworfen, da Wechselkurs älter als 1 h.
    calculateTriangularArbitragePriceTriples("bitstamp", "usd", "eur", "2016-04-16", endDate)
    
    # Coinbase Pro
    # 1s: 4h06min. 31.392.364 Datensätze in 2.5 GB (unkomprimiert).
    #     9.414.822 Ticks verworfen, da außerhalb des Bitcoin-Zeitfensters von 1 s.
    #         3.270 Ticks verworfen, da Wechselkurs älter als 1 h.
    calculateTriangularArbitragePriceTriples("coinbase", "usd", "eur", "2015-04-23", endDate)
    
    # Kraken
    # 1s: 1h37min. 9.148.354 Datensätze in 731.9 MB (unkomprimiert).
    #     8.829.345 Ticks verworfen, da außerhalb des Bitcoin-Zeitfensters von 1 s.
    #           836 Ticks verworfen, da Wechselkurs älter als 1 h.
    calculateTriangularArbitragePriceTriples("kraken",   "usd", "eur", "2013-10-06", endDate)
}

