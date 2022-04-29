# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - setwd + source(.Rprofile)
# - Caching, da 3x pro Kompilierung aufgerufen

(function() {
    
    # Derzeit nicht genutzt.
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    
    
    # Konfiguration -----------------------------------------------------------
    source("Konfiguration/FilePaths.R")
    texFile <- sprintf("%s/Abbildungen/Krypto_Tagesumsatz_BTC.tex", latexOutPath)
    outFileTimestamp <- sprintf("%s/Abbildungen/Krypto_Tagesumsatz_BTC_Stand.tex", latexOutPath)
    dataSource <- "http://data.bitcoinity.org/export_data.csv?c=e&currency=USD&data_type=volume&r=day&t=b&timespan=all"
    plotAsLaTeX <- FALSE
    
    # Nur einmal pro Monat neu laden
    if (
        fromLaTeX && plotAsLaTeX && 
        file.exists(texFile) && difftime(Sys.time(), file.mtime(texFile), units = "days") < 28
    ) {
        cat("Grafik BTC-Handelsvolumen noch aktuell, keine Aktualisierung.\n")
        return()
    }
    
    
    # Bibliotheken laden ------------------------------------------------------
    library("data.table")
    library("fasttime") # fastPOSIXct
    library("ggplot2")
    library("ggthemes")
    library("zoo")
    
    # Daten einlesen
    btcvolume <- fread(dataSource)
    
    btcvolume$Time <- fastPOSIXct(btcvolume$Tim, tz="UTC")
    
    # Entferne letzten Monat, weil der sehr wahrscheinlich unvollständig ist
    lastMonth <- last(btcvolume$Time)
    btcvolume <- btcvolume[btcvolume$Time < paste0(year(lastMonth), "-", month(lastMonth), "-01")]
    
    # Börsen zu einem Gesamtvolumen zusammenfassen
    btcvolume$Volume <- rowSums(btcvolume[,2:ncol(btcvolume)], na.rm = TRUE)
    btcvolume <- btcvolume[, c("Time", "Volume")]
    
    # Auf Jahr+Monat aggregieren und Tagesdurchschnitt berechnen
    btcvolume <- btcvolume[
        j = .(
            meanDailyVolume = mean(Volume),
            medianDailyVolume = median(Volume),
            minDailyVolume = min(Volume),
            maxDailyVolume = max(Volume),
            monthlyVolume = sum(Volume)
        ),
        by = .(Time = as.Date(as.yearmon(Time)))
    ]
    
    
    # Grafiken erstellen ------------------------------------------------------
    if (plotAsLaTeX) {
        source("Konfiguration/TikZ.R")
        cat("Ausgabe in Datei", texFile, "\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = 5 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
    plot <- ggplot(btcvolume, aes(x=Time, y=meanDailyVolume)) +
        geom_line(size=1, aes(color="")) +
        theme_minimal() +
        theme(
            legend.position = "none",
            axis.title.x = element_text(margin = margin(t = 10, r = 0, b = 0, l = 0)),
            axis.title.y = element_text(margin = margin(t = 0, r = 10, b = 0, l = 0))
        ) +
        scale_x_date(
            date_breaks="1 year",
            date_minor_breaks="3 months",
            date_labels="%Y",
            expand = expansion(mult = c(.02, .02))
        ) +
        scale_y_continuous(
            labels = function(x) { prettyNum(x, big.mark=".", decimal.mark=",", scientific=F) },
            #breaks = c(0, 5e6, 10e6),
            #limits = c(0, 10e6),
            minor_breaks = seq(from = 0, to = 10e6, by=1e6),
            expand = expansion(mult = c(0.02, 0.02))
        ) + 
        scale_color_ptol() +
        labs(x="\\footnotesize Datum", y="\\footnotesize Tagesumsatz [BTC]") ;
        print(plot)
    
    
    if (plotAsLaTeX) {
        dev.off
        cat(
            trimws(format(Sys.time(), "%B %Y")), "%",
            file = outFileTimestamp,
            sep = ""
        )
    }
})()
