
source("Datenaufbereitung/0 ReadMonthlyDividedDataset.r")

parseDukascopyTickData <- function(srcFile) {
    
    # Bibliotheken laden
    library("data.table")
    library("fasttime")
    
    # Tickdaten aus CSV einlesen
    # Datenstruktur:
    #                       Time     Bid     Ask BidVolume AskVolume
    # 1: 2010-01-01 00:00:03.963 1.43283 1.43293   2300000   3000000
    # 2: 2010-01-01 00:00:05.996 1.43278 1.43290   1400000   4200000
    # Überspringe Leerzeilen (können aufgrund eines Bugs in
    # der Datenerfassung am Wochenende auftreten)
    thisDataset <- fread(srcFile, showProgress = FALSE, blank.lines.skip = TRUE)
    
    # Handelsvolumen interessiert derzeit nicht
    thisDataset$BidVolume <- NULL
    thisDataset$AskVolume <- NULL
    
    # Zeit einlesen
    thisDataset$Time <- fastPOSIXct(thisDataset$Time, tz="UTC")
    
    # Mittelkurs aus Bid und Ask berechnen
    thisDataset$Mittel <- rowMeans(thisDataset[,c("Bid","Ask")])
    
    return(thisDataset)
    
}

# Lese und aggregiere Dukascopy-Rohdaten zu
# - Tickdaten (vollständig)
# - 1s-/5s-/60s/1d: Schlusskurse für Bid-, Ask- und Mittelkurs
# Diese Auswahl erfolgt, um die Dateigröße zu gering wie möglich zu halten.
summariseDukascopyTickData <- function(dataset) {
    dataset |> summarise(
        CloseBid = last(Bid),
        CloseAsk = last(Ask),
        CloseMittel = last(Mittel),
        numDatasets = n(),
    )
}

readMonthlyDividedDataset(
    "Dukascopy",
    currencyPairs = c("eurusd", "gbpusd", "usdcad", "usdchf", "usdjpy", "audusd"),
    getSourceFileCallback = function(pair, year, month) {
        return(paste0(
            "Daten/",
            "dukascopy/",
            pair, "/",
            "dukascopy-", pair, "-", year, "-", sprintf("%02d", month), ".csv.gz"
        ))
    },
    parseSourceFileCallback = parseDukascopyTickData,
    summariseDataCallback = summariseDukascopyTickData
)

cat("\nTipp: R-Session neu starten, um nicht benötigten Speicher freizugeben.\n")
