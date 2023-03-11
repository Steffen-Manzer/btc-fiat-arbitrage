# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - setwd + source(.Rprofile)
# - Caching, da 3x pro Kompilierung aufgerufen

# Derzeit nur händisch aktualisiert

(function() {
    
    ####
    # Verwendet in Arbeit:
    # Kapitel 3.1.2.2 Entwicklung und Marktdurchdringung dezentraler Systeme
    # Label: Grafik:Bitcoin_Preise_Historisch_BPI_Log
    ####
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    
    # Konfiguration -----------------------------------------------------------
    source("Konfiguration/FilePaths.R")
    texFile <- sprintf("%s/Abbildungen/Markteffizienz_Bitcoin_HistorischeKurse.tex", latexOutPath)
    outFileTimestamp <- sprintf(
        "%s/Abbildungen/Markteffizienz_Bitcoin_HistorischeKurse_Stand.tex",
        latexOutPath
    )
    plotAsLaTeX <- fromLaTeX || TRUE
    
    # Nur einmal pro Monat neu laden
    if (
        fromLaTeX && plotAsLaTeX && file.exists(texFile) && 
        difftime(Sys.time(), file.mtime(texFile), units = "days") < 28
    ) {
        cat("Grafik BTCUSD noch aktuell, keine Aktualisierung.\n")
        return()
    }
    
    # Bibliotheken laden ------------------------------------------------------
    library("fst")
    library("data.table")
    library("lubridate") # floor_date
    library("ggplot2")
    library("ggthemes")
    library("gridExtra") # grid.arrange
    
    # Berechnungen durchführen ------------------------------------------------
    # Quelldaten einlesen
    btcusd <- read_fst("Cache/coindesk/bpi-daily-btcusd.fst", as.data.table = TRUE)
    
    # Auf Wochendaten summieren
    btcusd <- btcusd[
        j = .(Close = last(Close)),
        by = .(Time = as.Date(floor_date(Time, unit="week", week_start=1)))
    ]
    
    if (plotAsLaTeX) {
        source("Konfiguration/TikZ.R")
        cat("Ausgabe in Datei ", texFile, "\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
    # Plots
    plotBase <- ggplot(btcusd, aes(x=Time)) +
        geom_line(aes(y=Close, color="BTCUSD"), linewidth=1) +
        theme_minimal() +
        theme(
            legend.position = "none",
            axis.title.x = element_blank(),
            axis.title.y = element_text(size = 9, margin = margin(r = 8))
        ) +
        scale_x_date(
            date_breaks="2 years",
            minor_breaks="1 year",
            date_labels="%Y",
            expand = expansion(mult=c(0,0))
        ) +
        scale_color_ptol() +
        labs(x="Datum", y="Bitcoin-Preis [USD]")
    
    plotLin <- plotBase +
        scale_y_continuous(
            labels = function(x) { prettyNum(x, big.mark=".", decimal.mark=",", scientific=F) }
        )
    
    plotLog <- plotBase +
        scale_y_log10(
            labels = function(x) { prettyNum(x, big.mark=".", decimal.mark=",", scientific=F) },
            breaks = 1*10^(-1:5),
            minor_breaks = NULL#c(5e-1, 5e0, 5e1, 5e2, 5e3, 5e4)
        ) +
        theme(axis.title.y = element_blank())
    
    grid.arrange(
        grobs = list(plotLin, plotLog),
        nrow = 1,
        ncol = 2
    )
    
    if (plotAsLaTeX) {
        dev.off()
    }
    
    cat(
        trimws(format(last(btcusd$Time), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    
})()
