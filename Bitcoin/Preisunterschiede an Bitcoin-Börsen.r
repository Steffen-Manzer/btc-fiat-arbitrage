
# Berechne auf Monatsbasis:
# - Geringste
# - Mittlere (arith. Mittel)
# - Höchste Preisunterschiede auf Sekundenbasis

(function() {
    
    # Pakete laden ------------------------------------------------------------
    library("fst")
    require("data.table")
    require("dplyr")
    require("lubridate")
    require("ggplot2")
    library("ggthemes")
    library("tictoc") # Zeitmessung
    
    # Konfiguration -----------------------------------------------------------
    asTeX <- TRUE
    texFile <- "/Users/fox/Documents/Studium - Promotion/TeX/R/Abbildungen/Krypto_Preisunterschiede.tex"
    outFileTimestamp <- "/Users/fox/Documents/Studium - Promotion/TeX/R/Abbildungen/Krypto_Preisunterschiede_Stand.tex"
    
    exchanges <- c("bitfinex", "bitstamp", "coinbase", "kraken")
    #currencyPairs <- c("btcusd", "btceur")
    currencyPairs <- c("btcusd")
    
    # Frühester Zeitpunkt, bei dem für mehr als eine Börse Kurse bekannt sind
    earliestDatesWithMultipleExchanges <- list()
    earliestDatesWithMultipleExchanges[["btcusd"]] <- fast_strptime("2013-01-14 16:47:23", format="%Y-%m-%d %H:%M:%S")
    earliestDatesWithMultipleExchanges[["btceur"]] <- fast_strptime("2015-04-23 01:42:33", format="%Y-%m-%d %H:%M:%S")
    
    # Bitfinex BTCUSD: ab 14.01.2013, 16:47:23.000
    # Bitstamp BTCUSD: ab 13.09.2011, 13:53:36
    # Coinbase BTCUSD: ab 01.12.2014, 05:33:56.761199
    #   Kraken BTCUSD: ab 06.10.2013, 21:34:15
    # Bitfinex BTCEUR: ab 19.05.2017, 08:16:21
    # Bitstamp BTCEUR: ab 05.12.2017, 11:43:49
    # Coinbase BTCEUR: ab 23.04.2015, 01:42:34.182104
    #   Kraken BTCEUR: ab 10.09.2013, 23:47:11
    
    
    # Berechnungen durchführen ------------------------------------------------
    for (pair in currencyPairs) {
        cat("Verarbeite", pair, "...\n")
        earliestDateWithMultipleExchanges <- earliestDatesWithMultipleExchanges[[pair]]
        
        # Aggregationslevel: 1s, 5s, 60s?
        timeframe <- "5s"
        cat("Auf ", timeframe, "-Basis...\n", sep="")
        cacheFile <- paste0("Cache/calculations/btc-price-differences/", pair, "-", timeframe, "-raw.fst")
        cacheFileWeekly <- paste0("Cache/calculations/btc-price-differences/", pair, "-", timeframe, "-aggregatedWeekly.fst")
        cacheFileMonthly <- paste0("Cache/calculations/btc-price-differences/", pair, "-", timeframe, "-aggregatedMonthly.fst")
        if (!dir.exists(dirname(cacheFile))) {
            dir.create(dirname(cacheFile), recursive=TRUE)
        }
        
        if (file.exists(cacheFileWeekly)) {
            
            # Cache lesen
            cat("Lese Cache...\n")
            priceDifferencesWeekly <- read_fst(cacheFileWeekly, as.data.table = TRUE)
            
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
                priceDifferences <- read_fst(cacheFile, as.data.table = TRUE)
                priceDifferencesMonthly <- read_fst(cacheFileMonthly, as.data.table = TRUE)
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
            cat("=== Aktualisiere Cache:", toupper(pair), "-", timeframe, " ===\n")
            
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
            for (year in startYear:endYear) {
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
                for (month in thisStartMonth:thisEndMonth) {
                    
                    # Daten einlesen
                    tic()
                    dataset <- data.table()
                    cat(sprintf("%02d", month), "/", year, ":", sep="")
                    for (exchange in exchanges) {
                        dataFile <- paste0(
                            "Cache/",
                            exchange, "/",
                            pair, "/", 
                            timeframe, "/",
                            exchange, "-", pair, "-", timeframe, "-", year, "-", sprintf("%02d", month), ".fst"
                        )
                        if (!file.exists(dataFile)) {
                            next()
                        }
                        
                        cat(" ", exchange, sep="")
                        thisDataset <- read_fst(dataFile, as.data.table = TRUE)
                        
                        # Auf Schlusskurs beschränken
                        thisDataset <- thisDataset[, c("Time", "Close")]
                        
                        # Daten speichern
                        thisDataset$Exchange <- exchange
                        dataset <- rbind(dataset, thisDataset)
                        rm(thisDataset)
                    }
                    
                    # Nach Zeit sortieren (paar Sekunden)
                    dataset <- dataset[order(dataset$Time),]
                    
                    # Auf Datensätze mit mindestens 2 Börsen beschränken. Langsam.
                    cat(". ", prettyNum(nrow(dataset), big.mark=".", decimal.mark=","), " -> ", sep="")
                    #cat("Auf Datensätze mit mindestens zwei Börsen beschränken... ")
                    dataset <- dataset %>%
                        group_by(Time) %>%
                        filter(n() > 1)
                    cat(prettyNum(nrow(dataset), big.mark=".", decimal.mark=","), ". ", sep="")
                    
                    # Exakte Preisdifferenzen je Zeiteinheit. Langsam.
                    # TODO: Umbenennen zu High/Low?
                    cat("Differenzen... ")
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
                    thisPriceDifferences$maxPriceDifference = thisPriceDifferences$maxPrice - thisPriceDifferences$minPrice
                    thisPriceDifferences$maxPriceDifferenceRelative = thisPriceDifferences$maxPriceDifference / thisPriceDifferences$meanPrice
                    
                    # Ausreißer entfernen
                    #cat("Ausreißer... ")
                    cat("Aggregiere... ")
                    # outliers <- boxplot(thisPriceDifferences$maxPriceDifference, plot = FALSE)$out
                    # if (length(outliers) > 0) {
                    #     thisPriceDifferences <- thisPriceDifferences[-which(thisPriceDifferences$maxPriceDifference %in% outliers),]
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
            cat("Speichern...")
            tic()
            write_fst(priceDifferences, cacheFile)
            write_fst(priceDifferencesWeekly, cacheFileWeekly)
            write_fst(priceDifferencesMonthly, cacheFileMonthly)
            toc()
        }
        
        
        if (asTeX) {
            source("Konfiguration/TikZ.r")
            cat("Ausgabe in Datei ", texFile, "\n")
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
    
})()
