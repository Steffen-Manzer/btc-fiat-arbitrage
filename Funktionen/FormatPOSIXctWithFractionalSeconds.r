library("lubridate")
library("stringr")

#' Korrekte Ausgabe von `POSIXct`-Objekten mit Sekundenbruchteilen,
#' maximal auf sechs Nachkommastellen
#' 
#' @param d Eines oder mehrere `POSIXct`-Objekte
#' @param format Das gewünschte Format, äquivalent zu format.POSIXct
#' @param ... Weitere Parameter, die an format.POSIXct übergeben werden
#' @return Formatierter String
formatPOSIXctWithFractionalSeconds <- function(d, format = "%Y-%m-%d %H:%M:%OS", ...)
{
    stopifnot(
        is.POSIXct(d),
        is.character(format), length(format) == 1L
    )
    
    # Keine Sekundenbruchteile vorhanden oder keine Ausgabe gewünscht
    if (
        !str_detect(format, fixed("%OS")) ||
        str_detect(format, fixed("%OS0")) ||
        (length(d) == 1L && str_detect(sprintf("%f", d), fixed(".000000")))
    ) {
        return(format.POSIXct(d, format, ...))
    }
    
    # Gewünschte Ausgabelänge, Standard: 6
    fractionLengthMatched <- str_match(format, "%OS(\\d)")
    if (!is.na(fractionLengthMatched[[2]])) {
        fractionLength <- as.integer(fractionLengthMatched[[2]])
        formatFixed <- str_replace(format, "%OS\\d", "%S.{OS}")
    } else {
        fractionLength <- 6L
        formatFixed <- str_replace(format, fixed("%OS"), "%S.{OS}")
    }
    
    # Sekundenbruchteile bestimmen
    dAsString <- sprintf(paste0("%.", fractionLength, "f"), d)
    fractions <- str_split_fixed(dAsString, fixed("."), n=2L)[,2]
    
    d |>
        format.POSIXct(formatFixed, ...) |>
        str_replace(fixed("{OS}"), fractions) -> result
    return(result)
}
