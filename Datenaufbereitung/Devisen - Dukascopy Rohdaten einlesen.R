# Lese und aggregiere Dukascopy-Rohdaten zu
# - Tickdaten (vollständig)
# - 60s/1d/1mon: Schlusskurse für Bid-, Ask- und Mittelkurs
# Diese Auswahl erfolgt, um die Dateigröße zu gering wie möglich zu halten.

source("Funktionen/ReadMonthlyDividedDataset.R")

# Bibliotheken laden
library("data.table")
library("fasttime")

# Hilfsfunktion: Eine .csv-Datei einlesen
parseDukascopyTickData <- function(srcFile) {
    
    # Tickdaten aus CSV einlesen
    # Datenstruktur:
    #                       Time     Bid     Ask BidVolume AskVolume
    # 1: 2010-01-01 00:00:03.963 1.43283 1.43293   2300000   3000000
    # 2: 2010-01-01 00:00:05.996 1.43278 1.43290   1400000   4200000
    # Überspringe Leerzeilen (können aufgrund eines Bugs in
    # der Datenerfassung am Wochenende auftreten)
    thisDataset <- fread(
        srcFile,
        showProgress = FALSE, 
        blank.lines.skip = TRUE,
        # Handelsvolumen interessiert derzeit nicht
        drop = c("BidVolume", "AskVolume")
    )
    
    # Zeit einlesen
    thisDataset[,Time:=fastPOSIXct(Time, tz="UTC")]
    
    # Mittelkurs aus Bid und Ask berechnen
    thisDataset[,Mittel:=rowMeans(thisDataset[,c("Bid","Ask")])]
    
    return(thisDataset)
    
}

# Hilfsfunktion: Daten eines Zeitabschnittes aggregieren
summariseDukascopyTickData <- function() {
    return(expression(.(
        CloseBid = last(Bid),
        CloseAsk = last(Ask),
        CloseMittel = last(Mittel),
        numDatasets = .N
    )))
}

# Daten einlesen und aggregieren
readMonthlyDividedDataset(
    "Dukascopy",
    currencyPairs = c("eurusd"),
    getSourceFileCallback = function(pair, year, month) {
        return(sprintf("Daten/dukascopy/%s/dukascopy-%1$s-%2$d-%3$02d.csv.gz",
                       pair, year, month))
    },
    parseSourceFileCallback = parseDukascopyTickData,
    summariseDataCallback = summariseDukascopyTickData
)
