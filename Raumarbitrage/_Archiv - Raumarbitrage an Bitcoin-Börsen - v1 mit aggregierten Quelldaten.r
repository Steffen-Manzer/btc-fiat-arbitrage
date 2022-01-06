# Berechne auf Monatsbasis:
# - geringste
# - mittlere (arith. Mittel) und
# - höchste
# Preisunterschiede auf Sekundenbasis
#
# ACHTUNG:
# Es handelt sich bei diesem Verfahren um die experimentelle erste Herangehensweise.
# Als Datenquelle werden auf 1s aggregierte Daten der Börsen herangezogen, was die
# Tauschmöglichkeiten nicht zwangsläufig korrekt abbildet.
#
# Das neue Verfahren verwendet für die Bestimmung von Tauschmöglichkeiten Tickdaten.
# Siehe dazu
#   `Raumarbitrage/Arbitrageindex Bitcoin-Börsen auswerten.r`

stop()


# Pakete laden ----------------------------------------------------------------
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
asTeX <- TRUE
texFile <- "/Users/fox/Documents/Studium - Promotion/TeX/R/Abbildungen/Krypto_Preisunterschiede.tex"
outFileTimestamp <- "/Users/fox/Documents/Studium - Promotion/TeX/R/Abbildungen/Krypto_Preisunterschiede_Stand.tex"

exchanges <- c("bitfinex", "bitstamp", "coinbase", "kraken")
#currencyPairs <- c("btcusd", "btceur")
currencyPairs <- c("btcusd")

# Frühester Zeitpunkt, bei dem für mehr als eine Börse Kurse bekannt sind
earliestDatesWithMultipleExchanges <- list()
earliestDatesWithMultipleExchanges[["btcusd"]] <- as.POSIXct("2013-01-14 16:47:23")
earliestDatesWithMultipleExchanges[["btceur"]] <- as.POSIXct("2015-04-23 01:42:33")


# Berechnungen durchführen ----------------------------------------------------
for (pair in currencyPairs) {
    printf("Verarbeite %s...\n", pair)
    earliestDateWithMultipleExchanges <- earliestDatesWithMultipleExchanges[[pair]]
    
    # Aggregationslevel: 1s, 5s, 60s?
    timeframe <- "5s"
    print("Auf %s-Basis...\n", timeframe)
    cacheFile <- sprintf("Cache/Raumarbitrage-v1/%s-%s-tick.fst", pair, timeframe)
    cacheFileWeekly <- sprintf("Cache/Raumarbitrage-v1/%s-%s-weekly.fst", pair, timeframe)
    cacheFileMonthly <- sprintf("Cache/Raumarbitrage-v1/%s-%s-monthly.fst", pair, timeframe)
    if (!dir.exists(dirname(cacheFile))) {
        dir.create(dirname(cacheFile), recursive=TRUE)
    }
    
    if (file.exists(cacheFileWeekly)) {
        
        # Cache lesen
        printf("Lese Cache...\n")
        priceDifferencesWeekly <- read_fst(cacheFileWeekly, as.data.table=TRUE)
        
        # Erweiterung des Caches nötig?
        lastDataset = last(priceDifferencesWeekly$Time)
        lastMonth = month(lastDataset)
        lastYear = year(lastDataset)
        
        if (lastMonth == 12) {
            startYear <- lastYear + 1
            startMonth <- 1
        } else {
            startYear <- lastYear
            startMonth <- lastMonth + 1
        }
        
        # Neue Daten nur einlesen, wenn mindestens ein weiterer Monat vergangen ist
        if (
            startYear > year(Sys.Date()) ||
            (startYear == year(Sys.Date()) && startMonth >= month(Sys.Date()))
        ) {
            rebuildCache <- FALSE
        } else {
            rebuildCache <- TRUE
            priceDifferences <- read_fst(cacheFile, as.data.table=TRUE)
            priceDifferencesMonthly <- read_fst(cacheFileMonthly, as.data.table=TRUE)
        }
        
    } else {
        rebuildCache <- TRUE
        priceDifferences <- data.table()
        priceDifferencesWeekly <- data.table()
        priceDifferencesMonthly <- data.table()
        startYear = year(earliestDateWithMultipleExchanges)
        startMonth = month(earliestDateWithMultipleExchanges)
    }
    
    if (rebuildCache) {
        # Cache neu erstellen. Langsam beim ersten Mal.
        printf("=== Aktualisiere Cache: %s-%s ===\n", format.currencyPair(pair), timeframe)
        
        # Daten bis vor einem Monat einlesen
        endMonth <- month(Sys.Date())
        endYear <- year(Sys.Date())
        if (endMonth == 1) {
            endYear <- endYear - 1
            endMonth <- 12
        } else {
            endMonth <- endMonth - 1
        }
        
        # Jedes Jahr durchgehen
        for (year in seq(startYear, endYear)) {
            if (year == startYear) {
                thisStartMonth <- startMonth
            } else {
                thisStartMonth <- 1
            }
            if (year == endYear) {
                thisEndMonth <- endMonth
            } else {
                thisEndMonth <- 12
            }
            
            # Jeden Monat durchgehen
            for (month in seq(thisStartMonth, thisEndMonth)) {
                
                # Daten einlesen
                tic()
                dataset <- data.table()
                printf("%02d/%d:", month, year)
                for (exchange in exchanges) {
                    dataFile <- sprintf("Cache/%s/%s/%s/%1$s-%2$s-%3$s-%4$d-%5$02d.fst",
                                        exchange, pair, timeframe, year, month)
                    if (!file.exists(dataFile)) {
                        next()
                    }
                    
                    printf(" %s", exchange)
                    thisDataset <- read_fst(dataFile, as.data.table = TRUE)
                    
                    # Auf Schlusskurs beschränken
                    thisDataset <- thisDataset[, c("Time", "Close")]
                    
                    # Daten speichern
                    thisDataset$Exchange <- exchange
                    dataset <- rbind(dataset, thisDataset)
                    rm(thisDataset)
                }
                
                # Nach Zeit sortieren
                setorder(dataset, Time)
                
                # Auf Datensätze mit mindestens 2 Börsen beschränken. Langsam.
                printf(". %s -> ", format.number(nrow(dataset)))
                #cat("Auf Datensätze mit mindestens zwei Börsen beschränken... ")
                dataset <- dataset %>%
                    group_by(Time) %>%
                    filter(n() > 1)
                cat(prettyNum(nrow(dataset), big.mark=".", decimal.mark=","), ". ", sep="")
                
                # Exakte Preisdifferenzen je Zeiteinheit. Langsam.
                # TODO Umbenennen zu High/Low?
                # TODO data.table-Grouping statt dplyr
                printf("Differenzen... ")
                thisPriceDifferences <- dataset %>%
                    #group_by(Time) %>% # Bereits gruppiert, wenn filter() ohne .preserve=T aufgerufen wird
                    summarise(
                        .groups = "drop",
                        # Niedrigster Preis im Zeitabschnitt
                        minPrice = min(Close),
                        minPriceExchange = Exchange[which.min(Close)],
                        # Höchster Preis im Zeitabschnitt
                        maxPrice = max(Close),
                        maxPriceExchange = Exchange[which.max(Close)],
                        # Mittlere Preise im Zeitabschnitt
                        meanPrice = mean(Close),
                        medianPrice = median(Close),
                        # Anzahl Preise
                        numPrices = n()
                    )
                
                # TODO Assign via data.table
                thisPriceDifferences$maxPriceDifference <- 
                    thisPriceDifferences$maxPrice - thisPriceDifferences$minPrice
                thisPriceDifferences$maxPriceDifferenceRelative <- 
                    thisPriceDifferences$maxPriceDifference / thisPriceDifferences$meanPrice
                
                # Ausreißer entfernen
                #cat("Ausreißer... ")
                printf("Aggregiere... ")
                # outliers <- boxplot(thisPriceDifferences$maxPriceDifference, plot=FALSE)$out
                # if (length(outliers) > 0) {
                #     thisPriceDifferences <- 
                #         thisPriceDifferences[-which(thisPriceDifferences$maxPriceDifference %in% outliers),]
                # }
                priceDifferences <- rbind(priceDifferences, thisPriceDifferences)
                
                # Preisdifferenzen auf Wochenbasis herunterbrechen für Darstellung
                thisPriceDifferencesWeekly <- thisPriceDifferences %>%
                    group_by(floor_date(Time, unit = "week", week_start = 1)) %>%
                    summarise(
                        # Höchste Preisdifferenz im betrachteten Zeitraum
                        maxOfMaxPriceDifferences = max(maxPriceDifference),
                        # Niedrigste (maximale) Preisdifferenz im betrachteten Zeitraum
                        minOfMaxPriceDifferences = min(maxPriceDifference),
                        # Mittlere Preisdifferenzen im betrachteten Zeitraum
                        meanOfMaxPriceDifferences = mean(maxPriceDifference),
                        medianOfMaxPriceDifferences = median(maxPriceDifference),
                        # Anzahl Datensätze
                        numDatasets = n()
                    )
                colnames(thisPriceDifferencesWeekly)[1] = "Time"
                priceDifferencesWeekly <- rbind(priceDifferencesWeekly, thisPriceDifferencesWeekly)
                
                # Preisdifferenzen auf Monatsbasis herunterbrechen für Darstellung
                #cat("Monat... ")
                thisPriceDifferencesMonthly <- thisPriceDifferences %>%
                    group_by(floor_date(Time, unit = "month")) %>%
                    summarise(
                        # Höchste Preisdifferenz im betrachteten Zeitraum
                        maxOfMaxPriceDifferences = max(maxPriceDifference),
                        # Niedrigste (maximale) Preisdifferenz im betrachteten Zeitraum
                        minOfMaxPriceDifferences = min(maxPriceDifference),
                        # Mittlere Preisdifferenzen im betrachteten Zeitraum
                        meanOfMaxPriceDifferences = mean(maxPriceDifference),
                        medianOfMaxPriceDifferences = median(maxPriceDifference),
                        # Anzahl Datensätze
                        numDatasets = n()
                    )
                colnames(thisPriceDifferencesMonthly)[1] = "Time"
                priceDifferencesMonthly <- rbind(priceDifferencesMonthly, thisPriceDifferencesMonthly)
                
                toc()
            }
        }
        
        # Cache erzeugen
        printf("Speichern...")
        write_fst(priceDifferences, cacheFile, compress=100L)
        write_fst(priceDifferencesWeekly, cacheFileWeekly, compress=100L)
        write_fst(priceDifferencesMonthly, cacheFileMonthly, compress=100L)
    }
    
    
    if (asTeX) {
        source("Konfiguration/TikZ.r")
        printf("Ausgabe in Datei %s\n", texFile)
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = 5 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
    # Ausreißer entfernen
    # Sieht extrem auffällig aus
    # minOut <- boxplot(priceDifferencesWeekly$minOfMaxPriceDifferences, plot = FALSE)$out
    # priceDifferencesWeekly <- priceDifferencesWeekly[
    #     -which(priceDifferencesWeekly$minOfMaxPriceDifferences %in% minOut),
    # ]
    # medianOut <- boxplot(priceDifferencesWeekly$medianOfMaxPriceDifferences, plot = FALSE)$out
    # priceDifferencesWeekly <- priceDifferencesWeekly[
    #     -which(priceDifferencesWeekly$medianOfMaxPriceDifferences %in% medianOut),
    # ]
    # maxOut <- boxplot(priceDifferencesWeekly$maxOfMaxPriceDifferences, plot = FALSE)$out
    # priceDifferencesWeekly <- priceDifferencesWeekly[
    #     -which(priceDifferencesWeekly$maxOfMaxPriceDifferences %in% maxOut),
    # ]
    
    # Drei-Monats-Hilfslinien korrekt ausrichten
    priceDifferencesWeekly <- priceDifferencesWeekly[Time > "2013-03-01",]
    
    # Plot zeichnen
    plot <- priceDifferencesWeekly %>%
        ggplot(aes(x=Time)) +
        geom_ribbon(
            aes(
                ymin = medianOfMaxPriceDifferences,
                ymax = maxOfMaxPriceDifferences
            ),
            fill="grey70"
        ) +
        #geom_line(aes(y = maxOfMaxPriceDifferences, color="2", linetype="2")) +
        geom_line(aes(y = medianOfMaxPriceDifferences, color="1", linetype="1"), size=1) +
        theme_minimal() +
        theme(
            legend.position = "none",
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
        ) +
        scale_x_datetime(
            date_breaks="1 year",
            date_minor_breaks="3 months",
            date_labels="%Y",
            expand = expansion(mult = c(.02, .02))
        ) +
        scale_y_log10(
            labels = function(x) { prettyNum(x, big.mark=".", decimal.mark=",", scientific=F) },
            breaks = c(0.1, 1, 10, 100, 1000, 10000)
        ) +
        scale_color_ptol() +
        labs(
            x="\\footnotesize Datum",
            y="\\footnotesize Preisunterschied [USD]"
        ) ; print(plot)
    
    if (asTeX) {
        dev.off()
        cat(
            trimws(format(Sys.time(), "%B %Y")), "%",
            file = outFileTimestamp,
            sep = ""
        )
    }
}
