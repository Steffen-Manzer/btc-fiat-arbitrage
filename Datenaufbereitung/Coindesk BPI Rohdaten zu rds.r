
(function() {
    
    # Konfiguration -----------------------------------------------------------
    dataSource <- "Daten/coindesk/coindesk-bpi-close-60s.csv.gz"
    dataTargetMonthly <- "Cache/coindesk/bpi-monthly-btcusd.rds"
    dataTarget1d <- "Cache/coindesk/bpi-daily-btcusd.rds"
    dataTarget60s <- "Cache/coindesk/bpi-60s-btcusd.rds"
    
    # Nur einmal pro Monat neu laden
    if (file.exists(dataTarget60s) && difftime(Sys.time(), file.mtime(dataTarget60s), units = "days") < 28) {
        cat("BPI-Daten noch aktuell, keine Aktualisierung.\n")
        return()
    }
    
    # Pakete laden
    library("data.table")
    library("anytime")
    library("lubridate") # floor_date
    library("fasttime") # fastPOSIXct
    library("dplyr")
    library("tictoc")
    
    # summarise: Nicht über ungrouping informieren
    options(dplyr.summarise.inform = FALSE)
    
    # Lese Daten: BPI Tick = 60s
    cat("Reading CSV... ")
    tic()
    bpi.60s <- fread(dataSource, header = TRUE, colClasses = c("character", "numeric"))
    toc()
    
    # Zeit parsen, UTC
    cat("Parsing as 60s data and saving as .rds ... ")
    tic()
    bpi.60s$Time <- fastPOSIXct(bpi.60s$Time)
    
    # Rendite berechnen
    bpi.60s$Rendite <- c(0, diff(log(bpi.60s$Close)))
    
    # Speichern
    saveRDS(bpi.60s, dataTarget60s)
    toc()
    
    # Aggregieren auf Tagesdaten
    cat("Aggregating as daily data and saving as .rds ... ")
    tic()
    bpi.perDay <- bpi.60s %>%
        group_by(Time=as.Date(Time)) %>%
        summarise(
            Mean = mean(Close),
            Open = first(Close),
            High = max(Close),
            Low = min(Close),
            Close = last(Close), # MUSS die letzte Zeile sein
            NumDatasets = n()
        )
    
    # Rendite
    bpi.perDay$Rendite <- c(0, diff(log(bpi.perDay$Close)))
    
    # Speichern
    saveRDS(bpi.perDay, dataTarget1d)
    toc()
    
    # Aggregieren auf Monatsdaten
    cat("Aggregating as monthly data and saving as .rds ... ")
    tic()
    bpi.perMonth <- bpi.60s %>%
        group_by(Time=as.Date(floor_date(Time, "month"))) %>%
        summarise(
            Mean = mean(Close),
            Open = first(Close),
            High = max(Close),
            Low = min(Close),
            Close = last(Close), # MUSS die letzte Zeile sein
            NumDatasets = n()
        )
    
    # Rendite
    bpi.perMonth$Rendite <- c(0, diff(log(bpi.perMonth$Close)))
    
    # Speichern
    saveRDS(bpi.perMonth, dataTargetMonthly)
    toc()
    
})()
