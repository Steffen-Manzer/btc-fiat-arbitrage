#' Monatlich getrennte Daten einlesen
#' 
#' Einen Datensatz aus monatlich getrennten CSV-Dateien einlesen und auf
#' bestimmte Zeithorizonte aggregieren (1s, 5s, 60s, 1d, 1mo).
#' Funktioniert für Bitcoin-Börsen und Wechselkursdaten.
#' 
#' @author Steffen Manzer
#' @param exchangeName Name der Börse.
#' @param currencyPairs Liste der Gegenwährungen, z.B. `c("btceur", "eurusd")`.
#' @param getSourceFileCallback Callback, das den Dateipfad
#'   der CSV-Quelldatei zurückgibt.
#' @param parseSourceFileCallback Callback, das den Inhalt
#'   der CSV-Quelldatei zurückgibt.
#' @param summariseDataCallback Callback, das die Daten aggregiert (via data.table).
#' @param targetBasename Basisverzeichnis für Zieldateien.
#'   Standard: exchangeName in Kleinbuchstaben, falls NA.
#' @param sourceFileExtension Dateiendung der Rohdaten.
#'   Standard: .csv.gz.
#' @param readUntil Daten bis zu diesem Zeitpunkt einlesen. 
#'   Standard: bis vor einen Monat.
#' @return NULL
#' @examples
#' # NOT RUN {
#' readMonthlyDividedDataset(
#'     exchangeName = "Bitstamp",
#'     currencyPairs = c("btcusd", "btceur"),
#'     getSourceFileCallback = function(pair, year, month) {
#'         return(sprintf(
#'             "Daten/bitstamp/%s/bitstamp-tick-%1$s-%2$d-%3$02d.csv.gz",
#'             pair, year, month
#'         ))
#'     },
#'     parseSourceFileCallback = function(srcFile) {
#'          #                            ID         Time         Amount    Price
#'          sourceFileColumnClasses = c("numeric", "character", "double", "double")
#'          thisDataset <- fread(srcFile, colClasses = sourceFileColumnClasses)
#'          thisDataset$Time <- fastPOSIXct(thisDataset$Time, tz="UTC")
#'         return(thisDataset)
#'     },
#'     readUntil = as.POSIXct("2019-08-01")
#' )
#' # }
readMonthlyDividedDataset <- function(
    exchangeName,
    currencyPairs,
    getSourceFileCallback,
    parseSourceFileCallback,
    summariseDataCallback,
    targetBasename = NA,
    sourceFileExtension = ".csv.gz",
    readUntil = NA
) {
    
    # Bibliotheken und Hilfsfunktionen laden ----------------------------------
    library("fst")
    library("data.table")
    library("fasttime")
    library("tictoc")
    library("lubridate") # floor_date
    source("Funktionen/printf.r")
    
    # Parameter verarbeiten ---------------------------------------------------
    if (is.na(targetBasename)) {
        targetBasename = exchangeName |> tolower()
    }
    
    if (!is.na(readUntil)) {
        
        # Daten nur bis zu einem bestimmten Datum einlesen (einschließlich)
        endMonth = month(readUntil)
        endYear = year(readUntil)
        
    } else {
        
        # Daten bis vor einem Monat einlesen, da aktueller Monat nicht
        # zwingend vollständig vorliegt. Das wäre deshalb problematisch,
        # weil eine Aggregation auf Monatsebene vorgenommen wird.
        endMonth <- month(Sys.time())
        endYear <- year(Sys.time())
        if (endMonth == 1) {
            endYear <- endYear - 1
            endMonth <- 12
        } else {
            endMonth <- endMonth - 1
        }
        
    }
    
    # Daten einlesen ----------------------------------------------------------
    # Jedes Wechselkurspaar dieser Börse durchgehen
    for (pair in currencyPairs) {
        
        pair <- tolower(pair)
        newDataFound <- FALSE
        printf("===== %s %s =====\n", exchangeName, toupper(pair))
        
        # Cache-Basisverzeichnis
        cacheBase <- sprintf("Cache/%s/%s", targetBasename, pair)
        
        # Cache-Verzeichnisse anlegen
        for (aggregationLevel in c("tick", "1s", "5s", "60s")) {
            if (!dir.exists(sprintf("%s/%s", cacheBase, aggregationLevel))) {
                dir.create(sprintf("%s/%s", cacheBase, aggregationLevel), recursive = TRUE)
            }
        }
        
        # Basispfade für alle Aggregationsstufen festlegen
        # Genauere Aggregationsstufen werden nach Monaten getrennt
        # Beispiel-Schema: Cache/bitstamp/btcusd/tick/bitstamp-btcusd-tick-2019-09.fst
        cacheBaseTick <- sprintf("%s/tick/%s-%s-tick", cacheBase, targetBasename, pair)
        cacheBase1s <- sprintf("%s/1s/%s-%s-1s", cacheBase, targetBasename, pair)
        cacheBase5s <- sprintf("%s/5s/%s-%s-5s", cacheBase, targetBasename, pair)
        cacheBase60s <- sprintf("%s/60s/%s-%s-60s", cacheBase, targetBasename, pair)
        
        # Tages- und Monatsdaten werden nicht nach Monaten getrennt,
        # sondern alle in eine Datei geschrieben
        # Beispiel-Schema: Cache/bitstamp/btcusd/bitstamp-btcusd-daily.fst
        targetFileDaily <- sprintf("%s/%s-%s-daily.fst", cacheBase, targetBasename, pair)
        targetFileMonthly <- sprintf("%s/%s-%s-monthly.fst", cacheBase, targetBasename, pair)
        
        
        # Anhand der (sehr kleinen) Tagesdaten prüfen, ob der Datensatz 
        # bereits einmal eingelesen wurde und nur neue Daten
        # angehängt werden müssen.
        if (file.exists(targetFileDaily)) {
            
            if (!file.exists(targetFileMonthly)) {
                printf("Monatsdaten fehlen, obwohl Tagesdaten verfügbar sind. Fehler!\n")
                next
            }
            
            dataset_daily <- read_fst(targetFileDaily, as.data.table = TRUE)
            dataset_monthly <- read_fst(targetFileMonthly, as.data.table = TRUE)
            
            lastDataset = last(dataset_daily$Time)
            lastMonth = month(lastDataset)
            lastYear = year(lastDataset)
            
            printf("Datensatz vorhanden, Stand: %02d/%d ...", lastMonth, lastYear)
            
            # Größer-gleich-Vergleich, da Enddatum vor dem Ende 
            # der bereits eingelesenen Daten liegen kann
            if (lastYear >= endYear && lastMonth >= endMonth) {
                printf(" Überspringe.\n")
                next
            }
            
            if (lastMonth == 12) {
                startYear <- lastYear + 1
                startMonth <- 1
            } else {
                startYear <- lastYear
                startMonth <- lastMonth + 1
            }
            
            printf("\n")
            
        } else {
            
            # Datensatz nicht gefunden, starte von Beginn.
            # Ab 2011 starten, manche Datensätze beginnen so früh.
            dataset_daily <- data.table()
            dataset_monthly <- data.table()
            startYear = 2010
            startMonth = 1
            
        }
        
        # Jedes Jahr der (möglichen) Quelldaten durchgehen
        stop = FALSE
        for (year in startYear:endYear) {
            
            if (stop) {
                break
            }
            
            # Das erste Jahr ab dem oben festgelegten Startmonat einlesen.
            # Alle weiteren Jahre ab Januar einlesen.
            if (year == startYear) {
                thisStartMonth <- startMonth
            } else {
                thisStartMonth <- 1
            }
            
            # Das letzte Jahr nur bis zum oben festgelegten Endmonat einlesen.
            # Alle anderen Jahre bis Dezember einlesen.
            if (year == endYear) {
                thisEndMonth <- endMonth
            } else {
                thisEndMonth <- 12
            }
            
            # Jeden Monat des ausgewählten Zeitraumes durchgehen
            for (month in thisStartMonth:thisEndMonth) {
                
                if (stop) {
                    break
                }
                
                # Quell- und Zieldateien für diesen Monat bestimmen
                srcFile <- getSourceFileCallback(pair, year, month)
                targetFileTick <- sprintf("%s-%d-%02d.fst", cacheBaseTick, year, month)
                targetFile1s <- sprintf("%s-%d-%02d.fst", cacheBase1s, year, month)
                targetFile5s <- sprintf("%s-%d-%02d.fst", cacheBase5s, year, month)
                targetFile60s <- sprintf("%s-%d-%02d.fst", cacheBase60s, year, month)
                
                # Quell-Datensatz existiert nicht
                if (!file.exists(srcFile) && !file.exists(targetFileTick)) {
                    # Es sind neue Daten vorhanden, dieser Datensatz fehlt allerdings.
                    if (newDataFound) {
                        printf("%s nicht gefunden! Ende.\n")
                        stop <- TRUE
                    }
                    next()
                }
                
                # Neue Daten wurden gefunden
                newDataFound <- TRUE
                tic()
                printf("%s %s %02d/%d: ", exchangeName, toupper(pair), month, year)
                
                # Tickdaten verarbeiten
                if (file.exists(targetFileTick)) {
                    
                    # Vorhandene Tickdaten dieses Monats einlesen
                    thisDataset <- read_fst(targetFileTick, as.data.table = TRUE)
                    
                } else {
                    
                    # Tickdaten aus CSV einlesen
                    thisDataset <- parseSourceFileCallback(srcFile)
                    
                    # Tickdaten nach Datum sortieren
                    # Nicht alle Datensätze sind exakt nach Zeit sortiert, beispielsweise
                    # für Coinbase Pro kann es Abweichungen auf ms-Basis geben.
                    setorder(thisDataset, Time)
                    
                    # Tickdaten speichern
                    write_fst(thisDataset, targetFileTick, compress=100)
                }
                
                # Statistiken ausgeben
                printf(
                    "%s - %s",
                    format(first(thisDataset$Time), format="%d.%m.%Y %H:%M:%OS"),
                    format(last(thisDataset$Time), format="%d.%m.%Y %H:%M:%OS")
                )
                
                # Auf 1s aggregieren
                if (!file.exists(targetFile1s)) {
                    printf(" - 1s")
                    thisDataset_1s <- thisDataset[
                        j=eval(summariseDataCallback()),
                        by=floor_date(Time, unit = "second")
                    ]
                    setnames(thisDataset_1s, 1, "Time")
                    write_fst(thisDataset_1s, targetFile1s, compress=100)
                    
                    # Speicher freigeben
                    rm(thisDataset_1s)
                }
                
                # Auf 5s aggregieren
                if (!file.exists(targetFile5s)) {
                    printf(", 5s")
                    thisDataset_5s <- thisDataset[
                        j=eval(summariseDataCallback()),
                        by=floor_date(Time, unit = "5 seconds")
                    ]
                    setnames(thisDataset_5s, 1, "Time")
                    write_fst(thisDataset_5s, targetFile5s, compress=100)
                    
                    # Speicher freigeben
                    rm(thisDataset_5s)
                }
                
                # Auf 60s aggregieren
                if (!file.exists(targetFile60s)) {
                    printf(", 60s")
                    thisDataset_60s <- thisDataset[
                        j=eval(summariseDataCallback()),
                        by=floor_date(Time, unit = "minute")
                    ]
                    setnames(thisDataset_60s, 1, "Time")
                    write_fst(thisDataset_60s, targetFile60s, compress=100)
                    
                    # Speicher freigeben
                    rm(thisDataset_60s)
                }
                
                # Auf 1d aggregieren (einzelne Datei für gesamten Datensatz)
                printf(", 1d ")
                thisDataset_daily <- thisDataset[
                    j=eval(summariseDataCallback()),
                    by=floor_date(Time, unit = "1 day")
                ]
                setnames(thisDataset_daily, 1, "Time")
                dataset_daily <- rbind(dataset_daily, thisDataset_daily)
                write_fst(dataset_daily, targetFileDaily, compress=100)
                rm(thisDataset_daily)
                
                # Auf 1 Monat aggregieren (einzelne Datei für gesamten Datensatz)
                printf(", 1mo ")
                thisDataset_monthly <- thisDataset[
                    j=eval(summariseDataCallback()),
                    by=floor_date(Time, unit = "1 month")
                ]
                setnames(thisDataset_monthly, 1, "Time")
                dataset_monthly <- rbind(dataset_monthly, thisDataset_monthly)
                write_fst(dataset_monthly, targetFileMonthly, compress=100)
                
                rm(thisDataset_monthly, thisDataset)
                toc() # x.xxx sec elapsed\n
                gc()
            } # loop: month
        } # loop: year
    } # loop: pair
}
