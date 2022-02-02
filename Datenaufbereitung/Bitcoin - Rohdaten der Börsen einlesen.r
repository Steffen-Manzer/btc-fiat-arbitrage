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
library("stringr") # str_match
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


# Daten einlesen

# Bitfinex --------------------------------------------------------------------
printf("\n--------- Verarbeite Bitfinex ---------\n")
readMonthlyDividedDataset(
    
    # Börsenname
    exchangeName = "Bitfinex",
    
    # Kurspaare
    currencyPairs = c("btcusd", "btceur", "btcgbp", "btcjpy"),
    
    # Dateiname der Quelldatei für ein Kurspaar/Jahr/Monat erzeugen
    getSourceFileCallback = function(pair, year, month) {
        return(sprintf("Daten/bitfinex/%s/bitfinex-tick-%1$s-%2$d-%3$02d.csv.gz",
                       pair, year, month))
    },
    
    # Rohdaten einlesen und normalisieren
    parseSourceFileCallback = function(srcFile) {
        #                            ID         Time         Amount    Price
        sourceFileColumnClasses <- c("numeric", "character", "double", "double")
        thisDataset <- fread(srcFile, colClasses=sourceFileColumnClasses, showProgress=FALSE)
        thisDataset[,Time:=fastPOSIXct(Time, tz="UTC")]
        return(thisDataset)
    },
    
    # Daten aggregieren
    summariseDataCallback = bitcoinSummariseCallback
)


# Bitstamp --------------------------------------------------------------------

# Schritt 1:
# Historische Tickdaten aus .csv extrahieren und für weitere
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

# Schritt 2: REST-API ab September 2019 / ab Januar 2022 einlesen
printf("\n--------- Verarbeite Bitstamp (REST-API) ---------\n")
readMonthlyDividedDataset(
    
    # Börsenname
    "Bitstamp",
    
    # Kurspaare
    currencyPairs = bitstampHistoricalSets$pair,
    
    # Dateiname der Quelldatei für ein Kurspaar/Jahr/Monat erzeugen
    getSourceFileCallback = function(pair, year, month) {
        return(sprintf("Daten/bitstamp/%s/bitstamp-tick-%1$s-%2$d-%3$02d.csv.gz",
                       pair, year, month))
    },
    
    # Rohdaten einlesen und normalisieren
    parseSourceFile = function(srcFile) {
        #                             ID         Time         Amount    Price     Type
        sourceFileColumnClasses <- c("numeric", "character", "double", "double", "numeric")
        thisDataset <- fread(srcFile, colClasses=sourceFileColumnClasses, showProgress=FALSE)
        thisDataset[,Time:=fastPOSIXct(Time, tz="UTC")]
        return(thisDataset)
    },
    
    # Daten aggregieren
    summariseDataCallback = bitcoinSummariseCallback
)


# Coinbase Pro ----------------------------------------------------------------
printf("\n--------- Verarbeite Coinbase Pro ---------\n")
readMonthlyDividedDataset(
    
    # Börsenname
    "Coinbase Pro",
    
    # Kurspaare
    currencyPairs = c("btcusd", "btceur", "btcgbp"),
    
    # Dateiname der Quelldatei für ein Kurspaar/Jahr/Monat erzeugen
    getSourceFileCallback = function(pair, year, month) {
        return(sprintf("Daten/coinbase/%s/coinbase-tick-%1$s-%2$d-%3$02d.csv.gz",
                       pair, year, month))
    },
    
    # Rohdaten einlesen und normalisieren
    parseSourceFile = function(srcFile) {
        #                             ID         Time         Amount    Price     Type
        sourceFileColumnClasses <- c("numeric", "character", "double", "double", "numeric")
        thisDataset <- fread(srcFile, colClasses=sourceFileColumnClasses, showProgress=FALSE)
        thisDataset[,Time:=fastPOSIXct(Time, tz="UTC")]
        return(thisDataset)
    },
    
    # Daten aggregieren
    summariseDataCallback = bitcoinSummariseCallback,
    
    # Unter dem Namen "coinbase" abspeichern, ohne Leerzeichen und "Pro"
    targetBasename = "coinbase"
)


# Kraken ----------------------------------------------------------------------

krakenCurrencyPairs <- c("btcusd", "btceur", "btcgbp", "btcjpy",
                         "btccad", "btcchf", "btcaud")

# Schritt 1:
# Tickdaten aus CSV extrahieren und um interne ID ergänzen
printf("\n--------- Verarbeite Kraken (CSV) ---------\n")
for (pair in krakenCurrencyPairs) {
    printf("----- Kraken %s -----\n", format.currencyPair(pair))
    
    # Zielverzeichnis
    targetDirTick <- sprintf("Cache/kraken/%s/tick", pair)
    lastFile <- last(list.files(targetDirTick, pattern = "\\.fst$", full.names = TRUE))
    
    if (length(lastFile) == 1L) {
        
        # Daten bereits teilweise eingelesen, lese letzte ID
        meta <- metadata_fst(lastFile)
        ID <- read_fst(
            lastFile, 
            columns = c("ID"), 
            from = meta$nrOfRows,
            to = meta$nrOfRows,
            as.data.table = TRUE
        )$ID
        
        printf("Daten bereits vorhanden, ID: %d\n", ID)
        
    } else {
        
        # Daten noch nicht eingelesen
        if (!dir.exists(targetDirTick)) {
            dir.create(targetDirTick, recursive=TRUE)
        }
        ID <- 1L
        
    }
    
    sourceFiles <- list.files(
        sprintf("Daten/kraken/%s", pair), 
        pattern = "\\.csv\\.gz$",
        full.names = TRUE
    )
    
    for (sourceFile in sourceFiles) {
        
        printf("Lese %s...", sourceFile)
        ym <- str_match(sourceFile, "-(\\d{4})-(\\d{2})\\.csv\\.gz$")
        
        # Zieldatei
        targetFile <- sprintf("Cache/kraken/%1$s/tick/kraken-%1$s-tick-%2$s-%3$s.fst",
                              pair, ym[1,2], ym[1,3])
        
        # Datensatz existiert
        if (file.exists(targetFile)) {
            printf(" Bereits eingelesen.\n")
            next
        }
        
        # Datei einlesen
        dataset <- fread(
            file = sourceFile,
            #              Time         Amount    Price     Type         Limit
            colClasses = c("character", "double", "double", "character", "character"),
            showProgress = FALSE
        )
        
        # Zeit einlesen
        dataset[,Time:=fastPOSIXct(Time, tz="UTC")]
        
        # Sortieren
        setorder(dataset, Time)
        
        # ID ergänzen
        dataset[,ID:=ID:(ID+nrow(dataset)-1L)]
        
        # ID an den Anfang verschieben
        setcolorder(dataset, neworder="ID")
        
        # Speichern
        write_fst(dataset, targetFile, compress=100)
        
        ID <- ID + nrow(dataset)
        printf("\n")
    }
}

# Schritt 2:
# Daten aggregieren
printf("\n--------- Verarbeite Kraken (Aggregation) ---------\n")
readMonthlyDividedDataset(
    
    # Börsenname
    "Kraken",
    
    # Kurspaare
    currencyPairs = krakenCurrencyPairs,
    
    # Dateiname der Quelldatei für ein Kurspaar/Jahr/Monat erzeugen
    getSourceFileCallback = function(pair, year, month) {
        return(sprintf("Daten/kraken/%s/kraken-tick-%1$s-%2$d-%3$02d.csv.gz",
                       pair, year, month))
    },
    
    # Rohdaten einlesen und normalisieren - hier nicht nötig:
    # Alle Tickdaten liegen nach Schritt 1 bereits als präparierte .fst vor,
    # die um eine ID ergänzt wurde. Hier niemals CSV einlesen!
    parseSourceFile = function(srcFile) {
        stop(sprintf("Kraken-CSV (%s) nie direkt einlesen!", srcFile))
    },
    
    # Daten aggregieren
    summariseDataCallback = bitcoinSummariseCallback
)
