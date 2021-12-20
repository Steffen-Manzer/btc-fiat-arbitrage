
source("Funktionen/ReadMonthlyDividedDataset.r")
library("data.table") # fread

# Die Methode, um Daten zu 1s/5s/60s/1d zu aggregieren,
# ist für alle Börsen identisch.
bitcoinSummariseCallback <- function(dataset) {
    dataset |> summarise(
        Amount = sum(abs(Amount)),
        Open = first(Price),
        High = max(Price),
        Low = min(Price),
        Close = last(Price),
        Mean = mean(Price),
        Median = median(Price),
        numTrades = n()
    )
}

# Bitfinex
cat("\n========= Verarbeite Bitfinex =========\n")
readMonthlyDividedDataset(
    exchangeName = "Bitfinex",
    currencyPairs = c("btcusd", "btceur", "btcgbp", "btcjpy"),
    getSourceFileCallback = function(pair, year, month) {
        return(paste0(
            "Daten/",
            "bitfinex/",
            pair, "/",
            "bitfinex-tick-", pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
        ))
    },
    parseSourceFileCallback = function(srcFile) {
        #                            ID         Time         Amount    Price
        sourceFileColumnClasses = c("numeric", "character", "double", "double")
        thisDataset <- fread(srcFile, colClasses = sourceFileColumnClasses)
        thisDataset$Time <- fastPOSIXct(thisDataset$Time, tz="UTC")
        return(thisDataset)
    },
    summariseDataCallback = bitcoinSummariseCallback
)

# Bitstamp via Bitcoincharts
# Diesen Datensatz nur verwenden, wenn keine (detaillierteren) Originaldaten
# vorliegen. Dies ist bis einschließlich August 2019 der Fall.
cat("\n========= Verarbeite Bitstamp (via Bitcoincharts bis August 2019) =========\n")
readMonthlyDividedDataset(
    "Bitstamp via Bitcoincharts",
    targetBasename = "bitstamp",
    currencyPairs = c("btcusd", "btceur"),
    getSourceFileCallback = function(pair, year, month) {
        return(paste0(
            "Daten/",
            "bitcoincharts-bitstamp/",
            pair, "/",
            "bitcoincharts-bitstamp-tick-", pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
        ))
    },
    parseSourceFile = function(srcFile) {
        #                            Time         Price     Amount
        sourceFileColumnClasses = c("character", "double", "double")
        thisDataset <- fread(srcFile, colClasses = sourceFileColumnClasses)
        thisDataset$Time <- fastPOSIXct(thisDataset$Time, tz="UTC")
        return(thisDataset)
    },
    summariseDataCallback = bitcoinSummariseCallback,
    readUntil = as.POSIXct("2019-08-01")
)

# Bitstamp (Originaldaten) ab September 2019
cat("\n========= Verarbeite Bitstamp (Originaldaten ab September 2019) =========\n")
readMonthlyDividedDataset(
    "Bitstamp",
    currencyPairs = c("btcusd", "btceur", "btcgbp"),
    getSourceFileCallback = function(pair, year, month) {
        return(paste0(
            "Daten/",
            "bitstamp/",
            pair, "/",
            "bitstamp-tick-", pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
        ))
    },
    parseSourceFile = function(srcFile) {
        #                            ID         Time         Amount    Price     Type
        sourceFileColumnClasses = c("numeric", "character", "double", "double", "numeric")
        thisDataset <- fread(srcFile, colClasses = sourceFileColumnClasses)
        thisDataset$Time <- fastPOSIXct(thisDataset$Time, tz="UTC")
        return(thisDataset)
    },
    summariseDataCallback = bitcoinSummariseCallback
)

# Coinbase Pro
cat("\n========= Verarbeite Coinbase Pro =========\n")
readMonthlyDividedDataset(
    "Coinbase Pro",
    targetBasename = "coinbase",
    currencyPairs = c("btcusd", "btceur", "btcgbp"),
    getSourceFileCallback = function(pair, year, month) {
        return(paste0(
            "Daten/",
            "coinbase/",
            pair, "/",
            "coinbase-tick-", pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
        ))
    },
    parseSourceFile = function(srcFile) {
        #                           ID         Time         Amount    Price     Type
        sourceFileColumnClasses = c("numeric", "character", "double", "double", "numeric")
        thisDataset <- fread(srcFile, colClasses = sourceFileColumnClasses)
        thisDataset$Time <- fastPOSIXct(thisDataset$Time, tz="UTC")
        return(thisDataset)
    },
    summariseDataCallback = bitcoinSummariseCallback
)

# Kraken
cat("\n========= Verarbeite Kraken =========\n")
readMonthlyDividedDataset(
    "Kraken",
    currencyPairs = c("btcusd", "btceur", "btcgbp", "btcjpy", "btccad", "btcchf"),
    getSourceFileCallback = function(pair, year, month) {
        return(paste0(
            "Daten/",
            "kraken/",
            pair, "/",
            "kraken-tick-", pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
        ))
    },
    parseSourceFile = function(srcFile) {
        #                            Time         Amount    Price     Type         Limit
        sourceFileColumnClasses = c("character", "double", "double", "character", "character")
        thisDataset <- fread(srcFile, colClasses = sourceFileColumnClasses)
        thisDataset$Time <- fastPOSIXct(thisDataset$Time, tz="UTC")
        return(thisDataset)
    },
    summariseDataCallback = bitcoinSummariseCallback
)

cat("\nTipp: R-Session neu starten, um nicht benötigten Speicher freizugeben.\n")
