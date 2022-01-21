#' Lese und aggregiere Rohdaten der Bitcoin-Börsen zu
#' Tickdaten, 60s/1d/1mo-OHLC


# Bibliotheken und externe Hilfsfunktionen laden ------------------------------
source("Funktionen/AddOneMonth.r")
source("Funktionen/ReadMonthlyDividedDataset.r")
source("Funktionen/printf.r")
source("Funktionen/FormatCurrencyPair.r")
library("data.table") # fread
library("fasttime")
library("lubridate") # floor_date
library("tictoc")


# Hilfsfunktionen -------------------------------------------------------------

#' Daten eines Zeitabschnittes aggregieren
#' Die Art und Weise, wie Daten zu 60s/1d/1mon aggregiert werden, ist
#' für alle Bitcoin-Börsen identisch.
bitcoinSummariseCallback <- function() {
    return(expression(.(
        Amount = sum(abs(Amount)),
        Open = first(Price),
        High = max(Price),
        Low = min(Price),
        Close = last(Price),
        Mean = mean(Price),
        Median = median(Price),
        numTrades = .N
    )))
}


# Daten einlesen --------------------------------------------------------------

# Bitfinex
printf("\n--------- Verarbeite Bitfinex ---------\n")
readMonthlyDividedDataset(
    exchangeName = "Bitfinex",
    currencyPairs = c("btcusd", "btceur", "btcgbp", "btcjpy"),
    getSourceFileCallback = function(pair, year, month) {
        # Dateinamen der Quelldatei erzeugen
        return(sprintf("Daten/bitfinex/%s/bitfinex-tick-%1$s-%2$d-%3$02d.csv.gz",
                       pair, year, month))
    },
    parseSourceFileCallback = function(srcFile) {
        # Daten der angegebenen Quelldatei einlesen
        #                            ID         Time         Amount    Price
        sourceFileColumnClasses <- c("numeric", "character", "double", "double")
        thisDataset <- fread(srcFile, colClasses=sourceFileColumnClasses, showProgress=FALSE)
        thisDataset[,Time:=fastPOSIXct(Time, tz="UTC")]
        return(thisDataset)
    },
    summariseDataCallback = bitcoinSummariseCallback
)


# Bitstamp: Historische Tickdaten aus .csv extrahieren und für weitere
# Verarbeitung als .fst speichern
printf("\n--------- Verarbeite Bitstamp (historische Daten) ---------\n")
bitstampHistoricalSets <- data.table(
    pair     = c("btcusd",     "btceur",     "btcgbp"),
    endDates = c("2019-09-01", "2019-09-01", "2022-01-01") # Letzter Monat (exkl.)
)
for (i in seq_len(nrow(bitstampHistoricalSets))) {
    pair <- bitstampHistoricalSets$pair[i]
    endDate <- as.POSIXct(bitstampHistoricalSets$endDates[i], tz="UTC")
    
    printf("----- Bitstamp %s -----\n", format.currencyPair(pair))
    
    # Datensatz vorhanden:
    # Historische Daten nur einmalig zu Beginn importieren,
    # nicht an bestehenden Datensatz anhängen
    if (file.exists(sprintf("Cache/bitstamp/%s/bitstamp-%1$s-daily.fst", pair))) {
        printf("Datensatz vorhanden, Überspringe historische Daten.\n")
        next
    }
    
    # Zielverzeichnis
    targetDirTick <- sprintf("Cache/bitstamp/%s/tick", pair)
    if (!dir.exists(targetDirTick)) {
        dir.create(targetDirTick, recursive = TRUE)
    }
    
    # Quelldatei
    sourceFile <- sprintf(
        "Daten/bitstamp/historical/%s_transactions.csv.gz",
        toupper(pair)
    )
    if (!file.exists(sourceFile)) {
        stop(sprintf("Quelldatei %s nicht gefunden!", sourceFile))
    }
    
    # Datei einlesen
    tic()
    printf("Lese Quelldatei... ")
    dataset <- fread(
        file = sourceFile,
        header = TRUE,
        col.names = c("ID", "Time", "Amount", "Price"),
        showProgress = FALSE
    )
    toc()
    
    # Sortieren
    setorder(dataset, Time)
    
    # Zeit als POSIXct speichern
    dataset[,Time:=as.POSIXct(Time, origin="1970-01-01", tz="UTC")]
    
    # Kein Auftragstyp verfügbar
    dataset[,Type:=NA]
    
    # Nach Monat gruppieren
    # Keine Aggregationen vornehmen, sondern nur Tickdaten als .fst speichern.
    # Aggregation nimmt im weiteren Verlauf `readMonthlyDataset` vor.
    currentDate <- first(dataset$Time)
    while (currentDate < endDate) {
        tic()
        nextMonth <- addOneMonth(currentDate)
        
        printf("Bitstamp %s %02d/%d: ", 
               format.currencyPair(pair), month(currentDate), year(currentDate))
        
        subset <- dataset[Time >= currentDate & Time < nextMonth, which=TRUE]
        
        # Monatsdaten separieren
        monthlyData <- dataset[subset]
        
        # Statistiken ausgeben
        printf(
            "%s - %s",
            format(first(monthlyData$Time), format="%d.%m.%Y %H:%M:%S"),
            format(last(monthlyData$Time), format="%d.%m.%Y %H:%M:%S")
        )
        
        # Tickdaten speichern
        write_fst(
            monthlyData, 
            sprintf("%s/bitstamp-%2$s-tick-%3$d-%4$02d.fst",
                    targetDirTick, pair, year(currentDate), month(currentDate)),
            compress=100L
        )
        
        # Aus Originaldaten entfernen
        rm(monthlyData)
        dataset <- dataset[!subset]
        gc()
        
        printf(" - ")
        toc()
        currentDate <- nextMonth
    }
}

# Bitstamp: REST-API ab September 2019 / ab Januar 2022
printf("\n--------- Verarbeite Bitstamp (REST-API) ---------\n")
readMonthlyDividedDataset(
    "Bitstamp",
    currencyPairs = c("btcusd", "btceur", "btcgbp"),
    getSourceFileCallback = function(pair, year, month) {
        # Dateinamen der Quelldatei erzeugen
        return(sprintf("Daten/bitstamp/%s/bitstamp-tick-%1$s-%2$d-%3$02d.csv.gz",
                       pair, year, month))
    },
    parseSourceFile = function(srcFile) {
        # Daten der angegebenen Quelldatei einlesen
        #                            ID         Time         Amount    Price     Type
        sourceFileColumnClasses <- c("numeric", "character", "double", "double", "numeric")
        thisDataset <- fread(srcFile, colClasses=sourceFileColumnClasses, showProgress=FALSE)
        thisDataset[,Time:=fastPOSIXct(Time, tz="UTC")]
        return(thisDataset)
    },
    summariseDataCallback = bitcoinSummariseCallback
)


# Coinbase Pro
printf("\n--------- Verarbeite Coinbase Pro ---------\n")
readMonthlyDividedDataset(
    "Coinbase Pro",
    currencyPairs = c("btcusd", "btceur", "btcgbp"),
    getSourceFileCallback = function(pair, year, month) {
        # Dateinamen der Quelldatei erzeugen
        return(sprintf("Daten/coinbase/%s/coinbase-tick-%1$s-%2$d-%3$02d.csv.gz",
                       pair, year, month))
    },
    parseSourceFile = function(srcFile) {
        # Daten der angegebenen Quelldatei einlesen
        #                             ID         Time         Amount    Price     Type
        sourceFileColumnClasses <- c("numeric", "character", "double", "double", "numeric")
        thisDataset <- fread(srcFile, colClasses=sourceFileColumnClasses, showProgress=FALSE)
        thisDataset[,Time:=fastPOSIXct(Time, tz="UTC")]
        return(thisDataset)
    },
    summariseDataCallback = bitcoinSummariseCallback,
    
    # Unter dem Namen "coinbase" abspeichern, ohne Leerzeichen und "Pro"
    targetBasename = "coinbase"
)


# Kraken
printf("\n--------- Verarbeite Kraken ---------\n")
readMonthlyDividedDataset(
    "Kraken",
    currencyPairs = c("btcusd", "btceur", "btcgbp", "btcjpy",
                      "btccad", "btcchf", "btcaud"),
    getSourceFileCallback = function(pair, year, month) {
        # Dateinamen der Quelldatei erzeugen
        return(sprintf("Daten/kraken/%s/kraken-tick-%1$s-%2$d-%3$02d.csv.gz",
                       pair, year, month))
    },
    parseSourceFile = function(srcFile) {
        # Daten der angegebenen Quelldatei einlesen
        #                            Time         Amount    Price     Type         Limit
        sourceFileColumnClasses <- c("character", "double", "double", "character", "character")
        thisDataset <- fread(srcFile, colClasses=sourceFileColumnClasses, showProgress=FALSE)
        thisDataset[,Time:=fastPOSIXct(Time, tz="UTC")]
        return(thisDataset)
    },
    summariseDataCallback = bitcoinSummariseCallback
)
