# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - setwd + source(.Rprofile)
# - Caching, da 3x pro Kompilierung aufgerufen

(function() {
    
    #stop("Derzeit nicht genutzt.")
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    
    # Konfiguration -----------------------------------------------------------
    source("Konfiguration/FilePaths.R")
    texFile <- sprintf("%s/Abbildungen/Bitcoin_Energieverbrauch.tex", latexOutPath)
    outFileTimestamp <- sprintf("%s/Abbildungen/Bitcoin_Energieverbrauch_Stand.tex", latexOutPath)
    asTeX <- fromLaTeX || FALSE
    
    
    # Berechnungen ------------------------------------------------------------
    # Jahresenergiebedarf Bitcoin
    # https://static.dwcdn.net/data/cFnri.csv
    # via https://datawrapper.dwcdn.net/cFnri/8/
    #   via https://digiconomist.net/bitcoin-energy-consumption/
    apiSourceFile <- "https://static.dwcdn.net/data/cFnri.csv"
    
    # Nur einmal pro Woche neu laden
    if (
        fromLaTeX && asTeX && file.exists(texFile) && 
        difftime(Sys.time(), file.mtime(texFile), units = "days") < 8
    ) {
        cat("Grafik Bitcoin-Energieverbrauch noch aktuell, keine Aktualisierung.\n")
        return()
    }
    
    
    # Bibliotheken und Hilfsfunktionen laden ----------------------------------
    source("Funktionen/R_in_LaTeX_Warning.R")
    library("data.table")
    library("lubridate") # floor_date
    library("ggplot2")
    library("ggthemes")
    
    # Daten einlesen: Bitcoin
    rawData <- fread(
        apiSourceFile,
        header = TRUE,
        colClasses = c("character", "numeric", "numeric"),
        col.names = c("Time", "Geschätzter Jahresenergiebedarf", "Minimaler Jahresenergiebedarf")
    )
    cat(nrow(rawData), "Datensaetze geladen.\n")
    rawData[, Time := as.Date(parse_date_time2(rawData$Time, order = "Y/m/d", tz="UTC"))]
    rawData <- melt(
        rawData,
        id.vars = "Time",
        variable.name = "Type",
        value.name = "Energiemenge_in_TWh"
    )
    
    
    # Grafiken erstellen
    if (asTeX) {
        source("Konfiguration/TikZ.R")
        cat("Ausgabe in Datei", texFile, "\n")
        tikz(
            file = texFile,
            width = documentPageWidth,
            height = 6 / 2.54, # cm -> Zoll
            sanitize = TRUE
        )
    }
    
    plot <- ggplot(rawData, aes(x=Time)) +
        geom_line(
            aes(y=Energiemenge_in_TWh, color=Type, linetype=Type),
            size=1
        ) +
        theme_minimal() +
        theme(
            legend.title = element_blank(),
            legend.position = "bottom",
            legend.margin = margin(t=-5),
            legend.text = element_text(margin = margin(r = 15)),
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
        scale_y_continuous(
            labels = function(x) { prettyNum(x, big.mark=".", decimal.mark=",", scientific=F) },
            expand = expansion(mult = c(.02, .02))
        ) +
        expand_limits(y = 0) + # 0 mit enthalten
        scale_color_ptol() +
        labs(x="Datum", y="Energiemenge [TWh]") ; print(plot)
    
    if (asTeX) {
        dev.off()
    }
    
    cat(
        trimws(format(Sys.time(), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    
})()
