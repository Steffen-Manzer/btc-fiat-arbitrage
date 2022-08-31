# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - setwd + source(.Rprofile)
# - Caching, da 3x pro Kompilierung aufgerufen

(function() {
    
    # Konfiguration -----------------------------------------------------------
    source("Konfiguration/FilePaths.R")
    outFile <- sprintf("%s/Daten/Bitcoin_Blockchain_Blockcount.tex", latexOutPath)
    outFileTimestamp <- sprintf("%s/Daten/Bitcoin_Blockchain_Blockcount_Stand.tex", latexOutPath)
    sourceURL <- "https://blockchain.info/q/getblockcount"
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    
    # Nur alle zwei Wochen neu laden
    if (fromLaTeX && file.exists(outFile) && difftime(Sys.time(), file.mtime(outFile), units = "days") < 14) {
        cat("Daten noch aktuell, keine Aktualisierung der Blockanzahl\n")
        return()
    }
    
    # Arbeitsverzeichnis und Pakete
    source("Funktionen/R_in_LaTeX_Warning.R")
    library("data.table")
    
    # Mögliche Daten:
    # https://www.blockchain.com/api/q
    # https://www.blockchain.com/api/charts_api
    
    # Quelldaten
    # TODO trycatch?
    rawData <- fread(sourceURL, nrows=1, header=FALSE, colClasses=c("integer"))
    
    if (ncol(rawData) != 1 || nrow(rawData) != 1 || !is.integer(rawData$V1)) {
        cat(latexWarning("Konnte Blockchain-Blockanzahl nicht lesen"), file = outFile)
        warning("Konnte Blockanzahl nicht lesen.")
        return()
    }
    
    numBlocks <- rawData$V1
    
    # Auf nächste 10.000 abrunden
    numBlocks <- floor(numBlocks / 10000) * 10000
    numBlocks <- prettyNum(numBlocks, big.mark=".", decimal.mark=",", scientific = FALSE)
    cat(numBlocks, "%", file = outFile, sep="")
    cat(
        trimws(format(Sys.time(), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    cat("Blockchain-Anzahl von ", numBlocks, " in Datei ", outFile, " geschrieben.\n")
    
})()
