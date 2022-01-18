#' Bei automatischer Aktualisierung von Daten im Hintergrund durch
#' LaTeX (`\executeR`) können Fehler auftreten.
#' Diese Fehler in eine Datei loggen und großflächig anzeigen.
latexWarning <- function(message) {
    context <- ""
    if (sys.nframe() > 0) {
        context <- paste0(sys.frame(1)$ofile, " ")
    }
    cat(paste0(context, Sys.time(), ": ", message, "\n\n"), file="R_in_LaTeX_Errors.log", append=TRUE)
    return(paste0("\\textcolor{red}{\\HUGE\\textbf{!!! ", message, " !!!}}%"))
}
