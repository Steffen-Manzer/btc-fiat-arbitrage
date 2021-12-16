# Aufruf aus LaTeX heraus via \executeR{...}
# Besonderheiten gegenüber normalen Skripten:
# - setwd + source(.Rprofile)
# - Caching, da 3x pro Kompilierung aufgerufen

(function() {
    
    # Aufruf durch LaTeX, sonst direkt aus RStudio
    fromLaTeX <- (commandArgs(T)[1] == "FromLaTeX") %in% TRUE
    latexWarning <- function(x) {
        context <- ""
        if (sys.nframe() > 0) {
            context <- paste0(sys.frame(1)$ofile, " ")
        }
        cat(paste0(context, Sys.time(), ": ", x, "\n"), file="R_in_LaTeX_Errors.log", append=TRUE)
        paste0("\\textcolor{red}{\\HUGE\\textbf{!!! ", x, " !!!}}%")
    }
    
    # Konfiguration -----------------------------------------------------------
    outFile <- "/Users/fox/Documents/Studium - Promotion/TeX/R/Daten/Bitcoin_Marktkapitalisierung.tex"
    outFileTimestamp <- "/Users/fox/Documents/Studium - Promotion/TeX/R/Daten/Bitcoin_Marktkapitalisierung_Stand.tex"
    
    # Marktkapitalisierung
    # API-Doku:
    # https://coinmarketcap.com/api/documentation/v1/#operation/getV1CryptocurrencyQuotesLatest
    sourceURL <- "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
    sourceParams <- "slug=bitcoin&aux=cmc_rank"
    
    # Nur einmal pro Monat neu laden
    if (fromLaTeX && file.exists(outFile) && difftime(Sys.time(), file.mtime(outFile), units = "days") < 28) {
        cat("Marktkapitalisierung noch aktuell, keine Aktualisierung.\n")
        return()
    }
    
    # Bibliotheken laden ------------------------------------------------------
    setwd("/Users/fox/Documents/Studium - Promotion/Datenanalyse/")
    source(".Rprofile")
    source("Konfiguration/CoinMarketCap_API_KEY.r")
    library("data.table")
    library("rjson")
    
    # Daten laden -------------------------------------------------------------
    failed <- FALSE
    tryCatch(
        {
            apiResponse <- system2("curl", c(
                paste0('-H "X-CMC_PRO_API_KEY: ', coinMarketCap_API_Key, '"'),
                '-H "Accept: application/json"',
                paste0('-d "', sourceParams, '"'),
                '-G',
                sourceURL
            ), stdout = TRUE, timeout = 60)
            
            if (nchar(apiResponse) < 10) {
                simpleError("Invalid response.")
            }
            
            apiResponse <- do.call(paste0, as.list(apiResponse))
            apiResponse <- fromJSON(apiResponse)
            
            # Prüfe Validität
            if (length(apiResponse) != 2) {
                simpleError("Invalid response length.")
            }
            if (apiResponse$status$error_code > 0) {
                simpleError(apiResponse$status$error_message)
            }
            
            apiResponse <<- apiResponse
            
        },
        error = function(err) {
            failed <<- TRUE
            warning(err)
        }
    )
    
    if (failed) {
        cat(latexWarning("Konnte Bitcoin-Marktkapitalisierung nicht lesen!"), file = outFile)
        return()
    }
    
    # Berechnungen durchführen ------------------------------------------------
    # Auf Billionen runden
    marketCap <- round(first(apiResponse$data)$quote$USD$market_cap / 1e10) * 10
    marketCap <- prettyNum(marketCap, big.mark=".", decimal.mark=",")
    
    cat(marketCap, "~Mrd.\\,USD%", file = outFile, sep="")
    cat(
        trimws(format(Sys.time(), "%B %Y")), "%",
        file = outFileTimestamp,
        sep = ""
    )
    cat("Bitcoin-Marktkapitalisierung von", marketCap, "Mrd. USD in Datei", outFile, " geschrieben.\n")
    
})()
