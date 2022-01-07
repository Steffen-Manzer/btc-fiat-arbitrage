# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - setwd + source(.Rprofile)
# - Caching, da 3x pro Kompilierung aufgerufen

(function() {
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    
    # Konfiguration -----------------------------------------------------------
    asTeX <- fromLaTeX || F
    texFile <- "/Users/fox/Documents/Studium - Promotion/TeX/R/Abbildungen/Krypto_Anzahl_BTC_Transaktionen.tex"
    outFileTimestamp <- "/Users/fox/Documents/Studium - Promotion/TeX/R/Abbildungen/Krypto_Anzahl_BTC_Transaktionen_Stand.tex"
    apiSourceFile <- "https://api.blockchain.info/charts/n-transactions?timespan=all&sampled=false&format=csv"
    
    # Nur einmal pro Woche neu laden
    if (fromLaTeX && asTeX && file.exists(texFile) && difftime(Sys.time(), file.mtime(texFile), units = "days") < 8) {
        cat("Grafik Bitcoin-Transaktionen noch aktuell, keine Aktualisierung.\n")
        return()
    }
    
    # Bibliotheken laden ------------------------------------------------------
    setwd("/Users/fox/Documents/Studium - Promotion/Datenanalyse/")
    source(".Rprofile")
    library("data.table")
    library("lubridate") # floor_date
    library("fasttime")
    library("dplyr")
    library("ggplot2")
    library("ggthemes")
    
    # Berechnungen ------------------------------------------------------------
    # Daten einlesen
    rawData <- fread(
        apiSourceFile,
        header = TRUE,
        colClasses = c("character", "numeric"),
        col.names = c("Time", "Volume")
    )
    
    rawData$Time <- fastPOSIXct(rawData$Tim, tz="UTC")
    
    # Wochensummen bilden
    # TODO Gruppieren mittels data.table
    weeklyData <- rawData %>%
        group_by(week = floor_date(Time, unit="week")) %>%
        summarise(
            Volume = sum(Volume)
        )
    setnames(weeklyData, 1, "Time")
    weeklyData$Time <- as.Date(weeklyData$Time)
    
    # Letzten Datensatz entfernen, potentiell unvollständig und damit verzerrend
    weeklyData <- weeklyData[1:nrow(weeklyData)-1,]
    
    # Grafiken erstellen
    if (asTeX) {
        source("Konfiguration/TikZ.r")
        cat("Ausgabe in Datei", texFile, "\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
    # 1-Wochen-Durchschnitt
    plot <- weeklyData %>%
        ggplot(aes(x=Time)) +
        geom_line(aes(y=Volume, color="NumTransaktionen"), size=1) +
        theme_minimal() +
        theme(
            legend.position = "none",
            axis.title.x = element_text(size = 9, margin = margin(t = 10)),
            axis.title.y = element_text(size = 9, margin = margin(r = 10))
        ) +
        scale_fill_ptol() +
        scale_x_date(
            date_breaks="1 year",
            minor_breaks=NULL,
            date_labels="%Y",
            expand = expansion(mult = c(.02, .02))
        ) +
        scale_y_log10(
            labels = function(x) { prettyNum(x, big.mark=".", decimal.mark=",", scientific=F) },
            #breaks = c(10e2, 10e3, 10e4, 10e5, 10e6, 10e7),
            #limits = c(5e3, 5e7),
            minor_breaks = NULL,
            expand = expansion(mult = c(.02, .02))
        ) +
        scale_color_ptol() +
        labs(x="Datum", y="Transaktionen je Woche")
    
    print(plot)
    if (asTeX) {
        dev.off()
    }
    
    cat(
        trimws(format(Sys.time(), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    
})()
