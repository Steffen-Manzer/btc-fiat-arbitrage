# Lese und aggregiere Rohdaten der Bitcoin-Börsen zu
# Tickdaten, 1s-/5s-/60s/1d, 1mo-OHLC

source("Funktionen/ReadMonthlyDividedDataset.r")

# Bibliotheken laden
library("data.table") # fread
library("fasttime")

# Hilfsfunktion: Daten eines Zeitabschnittes aggregieren
# Die Art und Weise, wie Daten zu 1s/5s/60s/1d aggregiert werden, ist
# für alle Bitcoin-Börsen identisch.
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
        # Dateinamen der Quelldatei erzeugen
        return(paste0(
            "Daten/",
            "bitfinex/",
            pair, "/",
            "bitfinex-tick-", pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
        ))
    },
    parseSourceFileCallback = function(srcFile) {
        # Daten der angegebenen Quelldatei einlesen
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
    currencyPairs = c("btcusd", "btceur"),
    getSourceFileCallback = function(pair, year, month) {
        # Dateinamen der Quelldatei erzeugen
        return(paste0(
            "Daten/",
            "bitcoincharts-bitstamp/",
            pair, "/",
            "bitcoincharts-bitstamp-tick-", pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
        ))
    },
    parseSourceFile = function(srcFile) {
        # Daten der angegebenen Quelldatei einlesen
        #                            Time         Price     Amount
        sourceFileColumnClasses = c("character", "double", "double")
        thisDataset <- fread(srcFile, colClasses = sourceFileColumnClasses)
        thisDataset$Time <- fastPOSIXct(thisDataset$Time, tz="UTC")
        return(thisDataset)
    },
    summariseDataCallback = bitcoinSummariseCallback,
    
    # Unter gemeinsamem "bitstamp"-Datensatz speichern
    targetBasename = "bitstamp",
    
    # Daten nur bis einschließlich August 2019 einlesen
    readUntil = as.POSIXct("2019-08-01")
)


# Bitstamp (Originaldaten) ab September 2019
cat("\n========= Verarbeite Bitstamp (Originaldaten ab September 2019) =========\n")
readMonthlyDividedDataset(
    "Bitstamp",
    currencyPairs = c("btcusd", "btceur", "btcgbp"),
    getSourceFileCallback = function(pair, year, month) {
        # Dateinamen der Quelldatei erzeugen
        return(paste0(
            "Daten/",
            "bitstamp/",
            pair, "/",
            "bitstamp-tick-", pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
        ))
    },
    parseSourceFile = function(srcFile) {
        # Daten der angegebenen Quelldatei einlesen
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
    currencyPairs = c("btcusd", "btceur", "btcgbp"),
    getSourceFileCallback = function(pair, year, month) {
        # Dateinamen der Quelldatei erzeugen
        return(paste0(
            "Daten/",
            "coinbase/",
            pair, "/",
            "coinbase-tick-", pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
        ))
    },
    parseSourceFile = function(srcFile) {
        # Daten der angegebenen Quelldatei einlesen
        #                             ID         Time         Amount    Price     Type
        sourceFileColumnClasses <- c("numeric", "character", "double", "double", "numeric")
        thisDataset <- fread(srcFile, colClasses = sourceFileColumnClasses)
        thisDataset$Time <- fastPOSIXct(thisDataset$Time, tz="UTC")
        return(thisDataset)
    },
    summariseDataCallback = bitcoinSummariseCallback,
    
    # Unter dem Namen "coinbase" abspeichern, ohne Leerzeichen und "Pro"
    targetBasename = "coinbase"
)


# Kraken
cat("\n========= Verarbeite Kraken =========\n")
readMonthlyDividedDataset(
    "Kraken",
    currencyPairs = c("btcusd", "btceur", "btcgbp", "btcjpy", "btccad", "btcchf"),
    getSourceFileCallback = function(pair, year, month) {
        # Dateinamen der Quelldatei erzeugen
        return(paste0(
            "Daten/",
            "kraken/",
            pair, "/",
            "kraken-tick-", pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
        ))
    },
    parseSourceFile = function(srcFile) {
        # Daten der angegebenen Quelldatei einlesen
        #                            Time         Amount    Price     Type         Limit
        sourceFileColumnClasses = c("character", "double", "double", "character", "character")
        thisDataset <- fread(srcFile, colClasses = sourceFileColumnClasses)
        thisDataset$Time <- fastPOSIXct(thisDataset$Time, tz="UTC")
        return(thisDataset)
    },
    summariseDataCallback = bitcoinSummariseCallback
)
