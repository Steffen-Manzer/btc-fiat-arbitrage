# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - setwd + source(.Rprofile)
# - Caching, da 3x pro Kompilierung aufgerufen

(function() {
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    
    # Konfiguration -----------------------------------------------------------
    source("Konfiguration/FilePaths.R")
    texFile <- sprintf("%s/Abbildungen/Krypto_Anzahl_BTC_Transaktionen.tex",
                       latexOutPath)
    outFileTimestamp <- sprintf("%s/Abbildungen/Krypto_Anzahl_BTC_Transaktionen_Stand.tex",
                                latexOutPath)
    apiSourceFile <- 
        "https://api.blockchain.info/charts/n-transactions?timespan=all&sampled=false&format=csv"
    plotAsLaTeX <- fromLaTeX || FALSE
    
    # Nur einmal pro Woche neu laden
    if (
        fromLaTeX && plotAsLaTeX && file.exists(texFile) && 
        difftime(Sys.time(), file.mtime(texFile), units = "days") < 8
    ) {
        cat("Grafik Bitcoin-Transaktionen noch aktuell, keine Aktualisierung.\n")
        return()
    }
    
    
    # Bibliotheken laden ------------------------------------------------------
    source("Funktionen/FormatNumber.R")
    library("data.table")
    library("lubridate") # floor_date
    library("fasttime") # fastPOSIXct
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
    
    rawData[,Time:=fastPOSIXct(rawData$Tim, tz="UTC")]
    
    # Wochensummen bilden
    weeklyData <- rawData[j=.(Volume=sum(Volume)), by=.(Time=floor_date(Time, unit="week"))]
    weeklyData[,Time:=as.Date(weeklyData$Time)]
    
    # Letzten Datensatz entfernen, potentiell unvollständig und damit verzerrend
    weeklyData <- weeklyData[1:nrow(weeklyData)-1]
    
    # Grafiken erstellen
    if (plotAsLaTeX) {
        source("Konfiguration/TikZ.R")
        cat("Ausgabe in Datei", texFile, "\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
    # 1-Wochen-Durchschnitt
    plot <- ggplot(weeklyData, aes(x=Time)) +
        geom_line(aes(y=Volume, color="NumTransaktionen"), size=1) +
        theme_minimal() +
        theme(
            legend.position = "none",
            axis.title.x = element_text(size = 9, margin = margin(t = 10)),
            axis.title.y = element_text(size = 9, margin = margin(r = 10))
        ) +
        scale_fill_ptol() +
        scale_x_date(
            date_breaks = "1 year",
            minor_breaks = NULL,
            date_labels = "%Y",
            expand = expansion(mult = c(.02, .02))
        ) +
        scale_y_log10(
            labels = format.number,
            minor_breaks = NULL,
            expand = expansion(mult = c(.02, .02))
        ) +
        scale_color_ptol() +
        labs(x="Datum", y="Transaktionen je Woche")
    
    print(plot)
    if (plotAsLaTeX) {
        dev.off()
    }
    
    cat(
        trimws(format(Sys.time(), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    
})()
