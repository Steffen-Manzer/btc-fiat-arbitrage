# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - setwd + source(.Rprofile)
# - Caching, da 3x pro Kompilierung aufgerufen
# - Keine Umlaute im Dateinamen

(function() {
    
    # Konfiguration -----------------------------------------------------------
    source("Konfiguration/FilePaths.R")
    outFile <- sprintf("%s/Daten/Bitcoin_Blockchain_Groesse.tex", latexOutPath)
    outFileTimestamp <- sprintf("%s/Daten/Bitcoin_Blockchain_Groesse_Stand.tex", latexOutPath)
    
    # Mögliche Daten:
    # https://www.blockchain.com/api/q
    # https://www.blockchain.com/api/charts_api
    sourceURL <- "https://api.blockchain.info/charts/blocks-size?timespan=1week&format=csv"
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    
    # Nur alle zwei Wochen neu laden
    if (fromLaTeX && file.exists(outFile) && difftime(Sys.time(), file.mtime(outFile), units = "days") < 14) {
        cat("Daten noch aktuell, keine Aktualisierung der Blockgroesse.\n")
        return()
    }
    
    
    # Bibliotheken und Hilfsfunktionen laden ----------------------------------
    source("Funktionen/R_in_LaTeX_Warning.R")
    library("data.table")
    
    
    # Daten abrufen -----------------------------------------------------------
    # TODO trycatch?
    rawData <- fread(sourceURL)
    
    if (ncol(rawData) != 2 || nrow(rawData) == 0 || !is.double(rawData$V2)) {
        cat(latexWarning("Konnte Blockchain-Groesse nicht lesen"), file = outFile)
        warning("Konnte Blockchain-Größe nicht lesen.")
        return()
    }
    
    blocksize <- last(rawData$V2)
    
    # Auf nächste 5 GB abrunden
    blocksize <- floor(blocksize / 5000) * 5
    cat(blocksize, "~GB%", file = outFile, sep="")
    cat(
        trimws(format(Sys.time(), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    cat("Blockchain-Groesse von", blocksize, "GB in Datei", outFile, "geschrieben.")
    
})()
