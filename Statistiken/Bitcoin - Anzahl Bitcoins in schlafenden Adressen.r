# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - setwd + source(.Rprofile)
# - Caching, da 3x pro Kompilierung aufgerufen

(function() {
    
    # Konfiguration -----------------------------------------------------------
    source("Konfiguration/FilePaths.r")
    outFile <- sprintf("%s/Daten/Bitcoin_Schlafend.tex", latexOutPath)
    outFileTimestamp <- sprintf("%s/Daten/Bitcoin_Schlafend_Stand.tex", latexOutPath)
    sourceURL <- "https://bitinfocharts.com/top-100-dormant_1y-bitcoin-addresses.html"
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    
    # Nur einmal pro Monat neu laden
    if (fromLaTeX && file.exists(outFile) && difftime(Sys.time(), file.mtime(outFile), units = "days") < 28) {
        cat("Daten noch aktuell, keine Aktualisierung der Anzahl schlafender Bitcoins\n")
        return()
    }
    
    
    # Bibliotheken und Hilfsfunktionen laden ----------------------------------
    source("Funktionen/R_in_LaTeX_Warning.r")
    library("curl")
    library("stringr")
    library("data.table")
    
    
    # Anzahl schlafender Bitcoins abrufen -------------------------------------
    failed <- FALSE
    tryCatch(
        {
            con <- curl(sourceURL)
            sourceHTML <- readLines(con, warn = FALSE)
            close(con)
            
            dydata <- str_extract(sourceHTML, regex("var dydata = \\[(.+?)\\];"))
            if (is.na(dydata)) {
                simpleError("Could not read dydata from input HTML.")
            }
            
            fragments <- str_match_all(dydata, "new Date\\('([0-9]{4}/[0-9]{2}/[0-9]{2})'\\),([0-9]+)")
            
            if (length(fragments[[1]]) == 0) {
                simpleError("Could not parse values from dydata.")
            }
            fragments <- fragments[[1]]
            
            dataset <- data.table(
                Time = as.POSIXct(fragments[,2], format="%Y/%m/%d", tz="UTC"),
                NumDormant = as.integer(fragments[,3])
            )
            
            # Prüfe Validität
            if (length(dataset) != 2 | nrow(dataset) == 0) {
                simpleError("Invalid response length.")
            }
            
            dataset <<- dataset
            
        },
        error = function(err) {
            failed <<- TRUE
            warning(err)
        }
    )
    
    if (failed) {
        cat(latexWarning("Konnte Anzahl schlafender Bitcoins nicht lesen!"), file = outFile)
        return()
    }
    
    
    # Daten aufbereiten und Ergebnis speichern --------------------------------
    monthOneYearBack <- format(as.Date(Sys.time()) - 365, "%Y-%m-01")
    
    numDormant <- dataset$NumDormant[dataset$Time == monthOneYearBack]
    
    # Abrunden
    numDormant <- floor(numDormant / 1000000) * 1000000
    
    if (numDormant > 1e6) {
        numDormant <- paste0(prettyNum(numDormant / 1e6, big.mark="", decimal.mark=","), "~Millionen")
    } else {
        numDormant <- prettyNum(numDormant, big.mark=".", decimal.mark=",")
    }
    
    cat(numDormant, "%", file = outFile, sep="")
    cat(
        trimws(format(Sys.time(), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    cat("\nSchlafende Bitcoins i.H.v.", numDormant, "in Datei", outFile, "geschrieben.\n")
    
})()
