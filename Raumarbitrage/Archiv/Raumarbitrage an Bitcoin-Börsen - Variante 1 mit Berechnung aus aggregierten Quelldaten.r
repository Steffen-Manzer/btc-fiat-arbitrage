#' Berechne auf Monatsbasis:
#' - geringste
#' - mittlere (arith. Mittel) und
#' - höchste
#' Preisunterschiede auf Sekundenbasis
#'
#' ACHTUNG:
#' Es handelt sich bei diesem Verfahren um die experimentelle erste Herangehensweise.
#' Als Datenquelle werden auf 1s aggregierte Daten der Börsen herangezogen, was die
#' realen Tauschmöglichkeiten nicht zwangsläufig korrekt abbildet.
#'
#' Das neue Verfahren verwendet für die Bestimmung von Tauschmöglichkeiten Tickdaten.
#' Siehe dazu
#'   `Raumarbitrage/Arbitrageindex Bitcoin-Börsen auswerten.r`

stop()


# Pakete laden ----------------------------------------------------------------
source("Funktionen/AddOneMonth.r")
source("Funktionen/FormatCurrencyPair.r")
source("Funktionen/FormatNumber.r")
source("Funktionen/printf.r")
library("fst")
library("data.table")
library("dplyr")
library("lubridate")
library("ggplot2")
library("ggthemes")
library("tictoc") # Zeitmessung


# Konfiguration ---------------------------------------------------------------
source("Konfiguration/FilePaths.r")
plotAsLaTeX <- FALSE
texFile <- sprintf("%s/Abbildungen/Krypto_Preisunterschiede.tex", latexOutPath)
outFileTimestamp <- sprintf("%s/Abbildungen/Krypto_Preisunterschiede_Stand.tex",
                            latexOutPath)

exchanges <- c("bitfinex", "bitstamp", "coinbase", "kraken")
currencyPairs <- c("btcusd")

# Vorheriges Aggregationslevel
timeframe <- "60s"

# Frühester Zeitpunkt, bei dem für mehr als eine Börse Kurse bekannt sind
startDate <- as.POSIXct("2013-01-14 16:47:23")
endDate <- ((Sys.Date() |> format("%Y-%m-01") |> as.POSIXct()) - 1) |> format("%Y-%m-01") |> as.POSIXct()


# Berechnungen durchführen ----------------------------------------------------
for (pair in currencyPairs) {
    printf("Verarbeite %s aus %s-Basis...\n", pair, timeframe)
    
    # Cache-Dateien bestimmen
    cacheFile <- sprintf("Cache/Raumarbitrage-v1/%s-%s-tick.fst", pair, timeframe)
    cacheFileWeekly <- sprintf("Cache/Raumarbitrage-v1/%s-%s-weekly.fst", pair, timeframe)
    cacheFileMonthly <- sprintf("Cache/Raumarbitrage-v1/%s-%s-monthly.fst", pair, timeframe)
    if (!dir.exists(dirname(cacheFile))) {
        dir.create(dirname(cacheFile), recursive=TRUE)
    }
    
    # Cache vorhanden, prüfe auf aktuelle Daten
    if (file.exists(cacheFileWeekly)) {
        
        # Cache lesen
        printf("Lese Cache...\n")
        priceDifferencesWeekly <- read_fst(cacheFileWeekly, as.data.table=TRUE)
        
        # Neue Daten einlesen, wenn mindestens ein weiterer Monat vergangen ist
        if (last(priceDifferencesWeekly$Time) >= endDate) {
            rebuildCache <- FALSE
        } else {
            rebuildCache <- TRUE
            startDate <- addOneMonth(last(priceDifferencesWeekly$Time))
            priceDifferences <- read_fst(cacheFile, as.data.table=TRUE)
            priceDifferencesMonthly <- read_fst(cacheFileMonthly, as.data.table=TRUE)
        }
        
    } else {
        rebuildCache <- TRUE
        priceDifferences <- data.table()
        priceDifferencesWeekly <- data.table()
        priceDifferencesMonthly <- data.table()
    }
    
    if (rebuildCache) {
        # Cache neu erstellen.
        # Dauert für einen vollständigen Durchlauf rund 10-15min.
        printf("--- Aktualisiere Cache: %s (%s) ---\n", 
               format.currencyPair(pair), timeframe)
        
        currentDate <- startDate - 1
        while (currentDate < endDate) {
            currentDate <- addOneMonth(currentDate)
            
            # Daten einlesen
            tic()
            dataset <- data.table()
            printf("%02d/%d:", month(currentDate), year(currentDate))
            for (exchange in exchanges) {
                dataFile <- sprintf("Cache/%s/%s/%s/%1$s-%2$s-%3$s-%4$d-%5$02d.fst",
                                    exchange, pair, timeframe,
                                    year(currentDate), month(currentDate))
                if (!file.exists(dataFile)) {
                    next()
                }
                
                printf(" %s...", exchange)
                thisDataset <- read_fst(dataFile, columns = c("Time", "Close"), as.data.table = TRUE)
                
                # Daten speichern
                thisDataset[, Exchange:=exchange]
                dataset <- rbindlist(list(dataset, thisDataset))
                rm(thisDataset)
            }
            
            # Nach Zeit sortieren
            setorder(dataset, Time)
            
            # Exakte Preisdifferenzen je Zeiteinheit. Langsam.
            printf(" Differenzen... ")
            dataset <- dataset[
                j=.(
                    # Niedrigster Preis im Zeitabschnitt
                    priceLow = min(Close),
                    priceLowExchange = Exchange[which.min(Close)],
                    
                    # Höchster Preis im Zeitabschnitt
                    priceHigh = max(Close),
                    priceHighExchange = Exchange[which.max(Close)],
                    
                    # Mittlere Preise im Zeitabschnitt
                    meanPrice = mean(Close),
                    medianPrice = median(Close),
                    
                    # Anzahl Preise
                    numPrices = .N
                ),
                by=Time
            ]
            
            # n <= 1 entfernen
            dataset <- dataset[numPrices > 1]
            
            # Differenzen berechnen
            dataset[, priceHighDifference:=priceHigh-priceLow]
            dataset[, priceHighDifferenceRelative:=priceHighDifference / meanPrice]
            
            # Ausreißer entfernen
            # outliers <- boxplot(thisPriceDifferences$priceHighDifference, plot=FALSE)$out
            # if (length(outliers) > 0) {
            #     thisPriceDifferences <- 
            #         thisPriceDifferences[-which(thisPriceDifferences$priceHighDifference %in% outliers),]
            # }
            priceDifferences <- rbindlist(list(priceDifferences, dataset))
            
            # Preisdifferenzen auf Wochenbasis herunterbrechen für Darstellung
            printf("Aggregiere... ")
            thisPriceDifferencesWeekly <- dataset[
                j=.(
                    # Höchste Preisdifferenz im betrachteten Zeitraum
                    maxOfPriceHighDifferences = max(priceHighDifference),
                    
                    # Niedrigste (maximale) Preisdifferenz im betrachteten Zeitraum
                    minOfPriceHighDifferences = min(priceHighDifference),
                    
                    # Mittlere Preisdifferenzen im betrachteten Zeitraum
                    meanOfPriceHighDifferences = mean(priceHighDifference),
                    medianOfPriceHighDifferences = median(priceHighDifference),
                    
                    # Anzahl Datensätze
                    numDatasets = .N
                ),
                by=floor_date(Time, unit = "week", week_start = 1)
            ]
            setnames(thisPriceDifferencesWeekly, 1, "Time")
            priceDifferencesWeekly <- rbindlist(list(priceDifferencesWeekly, thisPriceDifferencesWeekly))
            
            # Preisdifferenzen auf Monatsbasis herunterbrechen für Darstellung
            thisPriceDifferencesMonthly <- dataset[
                j=.(
                    # Höchste Preisdifferenz im betrachteten Zeitraum
                    maxOfPriceHighDifferences = max(priceHighDifference),
                    
                    # Niedrigste (maximale) Preisdifferenz im betrachteten Zeitraum
                    minOfPriceHighDifferences = min(priceHighDifference),
                    
                    # Mittlere Preisdifferenzen im betrachteten Zeitraum
                    meanOfPriceHighDifferences = mean(priceHighDifference),
                    medianOfPriceHighDifferences = median(priceHighDifference),
                    
                    # Anzahl Datensätze
                    numDatasets = .N
                ),
                by=floor_date(Time, unit = "month")
            ]
            setnames(thisPriceDifferencesMonthly, 1, "Time")
            priceDifferencesMonthly <- rbind(priceDifferencesMonthly, thisPriceDifferencesMonthly)
            
            toc()
        }
        
        # Cache erzeugen
        printf("Speichern...")
        write_fst(priceDifferences, cacheFile, compress=100L)
        write_fst(priceDifferencesWeekly, cacheFileWeekly, compress=100L)
        write_fst(priceDifferencesMonthly, cacheFileMonthly, compress=100L)
    }
    
    
    if (plotAsLaTeX) {
        source("Konfiguration/TikZ.r")
        printf("Ausgabe in Datei %s\n", texFile)
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = 5 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
        xTitle <- "\\footnotesize Datum"
        yTitle <- "\\footnotesize Preisunterschied [USD]"
        lineSize <- 1
    } else {
        xTitle <- "Datum"
        yTitle <- "Preisunterschied [USD]"
        lineSize <- .4
    }
    
    # Ausreißer entfernen
    # Sieht extrem auffällig aus
    # minOut <- boxplot(priceDifferencesWeekly$minOfPriceHighDifferences, plot = FALSE)$out
    # priceDifferencesWeekly <- priceDifferencesWeekly[
    #     -which(priceDifferencesWeekly$minOfPriceHighDifferences %in% minOut),
    # ]
    # medianOut <- boxplot(priceDifferencesWeekly$medianOfPriceHighDifferences, plot = FALSE)$out
    # priceDifferencesWeekly <- priceDifferencesWeekly[
    #     -which(priceDifferencesWeekly$medianOfPriceHighDifferences %in% medianOut),
    # ]
    # maxOut <- boxplot(priceDifferencesWeekly$maxOfPriceHighDifferences, plot = FALSE)$out
    # priceDifferencesWeekly <- priceDifferencesWeekly[
    #     -which(priceDifferencesWeekly$maxOfPriceHighDifferences %in% maxOut),
    # ]
    
    # Plot zeichnen
    print(
        ggplot(priceDifferencesWeekly, aes(x=Time)) +
            geom_ribbon(
                aes(
                    ymin = medianOfPriceHighDifferences,
                    ymax = maxOfPriceHighDifferences
                ),
                fill="grey70"
            ) +
            geom_line(aes(y = medianOfPriceHighDifferences, color="1", linetype="1"), size=lineSize) +
            theme_minimal() +
            theme(
                legend.position = "none",
                axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
                axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
            ) +
            scale_x_datetime(
                date_breaks="1 year",
                #date_minor_breaks="3 months",
                date_labels="%Y",
                expand = expansion(mult = c(.02, .02))
            ) +
            scale_y_log10(
                #labels = function(x) format.money(x, digits=0), # Nur für scale_y_continuous, da 0,1 auf 0 gerundet wird
                labels = function(x) { prettyNum(x, big.mark=".", decimal.mark=",", scientific=F) },
                breaks = c(0.1, 1, 10, 100, 1000, 10000)
            ) +
            scale_color_ptol() +
            labs(
                x=xTitle,
                y=yTitle
            )
    )
    
    if (plotAsLaTeX) {
        dev.off()
        cat(
            trimws(format(Sys.time(), "%B %Y")), "%",
            file = outFileTimestamp,
            sep = ""
        )
    }
}
