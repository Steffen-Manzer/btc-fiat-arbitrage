# Bibliotheken und Hilfsfunktionen laden
source("Funktionen/FindNearestDatapoint.r")
source("Funktionen/AddOneMonth.r")
library("lubridate") # floor_date
library("data.table")

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

# Hilfsfunktion: Datensatz einer Börse laden
getCachedDataset <- function(exchange, currencyPair, yearmonth) {
    # Beispiel: Cache/bitfinex/btcusd/tick/bitfinex-btcusd-tick-2021-11.rds
    dataFile <- paste0(
        "Cache/",
        exchange, "/",
        tolower(currencyPair), "/", 
        "tick/", 
        exchange, "-", tolower(currencyPair), "-tick-", year(yearmonth), "-", sprintf("%02d", month(yearmonth)), ".rds"
    )
    
    if (!file.exists(dataFile)) {
        return(NA)
    }
    
    return(readRDS(dataFile))
}

# Hilfsfunktion: Preise zweier Börsen vergleichen
compareTwoExchanges <- function(
    dataset_a,
    exchange_a,
    dataset_b,
    exchange_b,
    threshold
) {
    # Parameter validieren
    stopifnot(
        is.data.table(dataset_a),
        length(exchange_a) == 1,
        is.character(exchange_a),
        is.data.table(dataset_b),
        length(exchange_b) == 1,
        is.character(exchange_b),
        length(threshold) == 1,
        is.numeric(threshold)
    )
    
    # Ergebnisdatensatz aufbauen. Spalten: Siehe unten
    matchedPriceDifferences <- data.table()
    
    lastTick <- Sys.time()
    speed <- "Berechne Geschwindigkeit..."
    cat("0 %")
    
    # Jeden Datenpunkt abarbeiten
    numRows <- nrow(dataset_a)
    for (i in 1:numRows) {
        
        if (i %% 1000 == 0) {
            duration <- as.double(Sys.time() - lastTick)
            speed <- paste0(round(1000/duration), " Datensätze pro Sekunde")
            lastTick <- Sys.time()
        }
        
        cat("\r", round(i/numRows*100), " % (", i, " von ", numRows, " verarbeitet), ", speed, sep="")
        
        tick_a <- dataset_a[i,]
        tick_b <- findNearestDatapoint(tick_a$Time, dataset_b, threshold=threshold)
        
        # Kein Tick innerhalb von `threshold` gefunden
        if (length(tick_b) == 1 && is.na(tick_b)) {
            next
        }
        
        matchedPriceDifferences <- rbind(matchedPriceDifferences, data.table(
            TimeA = tick_a$Time,
            TimeB = tick_b$Time,
            TimeDifference = tick_a$Time - tick_b$Time,
            PriceA = tick_a$Price,
            PriceB = tick_b$Price,
            PriceDifference = tick_a$Price - tick_b$Price,
            ExchangeA = exchange_a,
            ExchangeB = exchange_b
        ))
    }
    cat("\n")
    return(matchedPriceDifferences)
}


# Alle Währungspaare und alle Börsen untersuchen
for (index in 1:nrow(currencyPairs)) {
    pair <- currencyPairs$CurrencyPair[index]
    startDate <- as.POSIXct(paste0(currencyPairs$StartMonth[index], "-01"))
    
    # Daten bis vor einen Monat verarbeiten
    endDate <- floor_date(floor_date(Sys.Date(), unit = "months") - 1, unit = "months")
    
    cat("== Untersuche ", pair, " ab ", format(startDate, "%Y-%m"), "\n", sep="")
    currentDate <- startDate
    while (currentDate <= endDate) {
        cat(format(currentDate, "%Y-%m"), "\n", sep="")
        
        bitfinex <- getCachedDataset("bitfinex", pair, currentDate)
        bitstamp <- getCachedDataset("bitstamp", pair, currentDate)
        coinbase <- getCachedDataset("coinbase", pair, currentDate)
        kraken <- getCachedDataset("kraken", pair, currentDate)
        
        matchedPriceDifferences <- data.table()
        
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
        
        # Zu nächsten Monat weitergehen
        currentDate <- addOneMonth(currentDate)
    }
}

