library("lubridate") # is.datetime

#' Zeitraum als menschenlesbare Zeitspanne formatieren, beispielsweise
#' x Stunden y Minuten
#' 
#' @param x Zeitspanne als `difftime` oder in Sekunden
#' @param form Kurzes (short) oder langes (long) Format
#' @return Als `character` formatierte menschenlesbare Zeitspanne
format.duration <- function(x, form="short")
{
    stopifnot(form %in% c("short", "long"))
    
    if (is.difftime(x)) {
        secs <- floor(as.double(x, units="secs"))
    } else if (is.numeric(x)) {
        secs <- x
    } else {
        stop("x must be a difftime or a numeric")
    }
    
    # Format definieren
    if (form == "short") {
        format.secs <- function(s) sprintf("%d s", s)
        format.mins <- function(m) sprintf("%d min", m)
        format.hours <- function(h) sprintf("%d h", h)
        format.days <- function(d) sprintf("%d d", d)
    } else {
        format.secs <- function(s) {
            if (s != 1) {
                return(sprintf("%d Sekunden", s))
            } else {
                return(sprintf("%d Sekunde", s))
            }
        }
        format.mins <- function(m) {
            if (m != 1) {
                return(sprintf("%d Minuten", m))
            } else {
                return(sprintf("%d Minute", m))
            }
        }
        format.hours <- function(h) {
            if (h != 1) {
                return(sprintf("%d Stunden", h))
            } else {
                return(sprintf("%d Stunde", h))
            }
        }
        format.days <- function(d) {
            if (d != 1) {
                return(sprintf("%d Tage", d))
            } else {
                return(sprintf("%d Tag", d))
            }
        }
    }
    
    # Weniger als eine Minute
    if (secs < 60) {
        return(format.secs(secs))
    }
    
    # Weniger als eine Stunde
    if (secs < 60 * 60) {
        mins <- floor(secs/60)
        remaining_secs <- secs %% 60
        if (remaining_secs > 0) {
            return(paste0(format.mins(mins), ", ", format.secs(remaining_secs)))
        } else {
            return(format.mins(mins))
        }
    }
    
    # Weniger als ein Tag
    if (secs < 24 * 60 * 60) {
        hours <- floor(secs / 60^2)
        remaining_minutes <- floor(secs %% 60^2 / 60)
        if (remaining_minutes > 0) {
            return(paste0(format.hours(hours), ", ", format.mins(remaining_minutes)))
        } else {
            return(format.hours(hours))
        }
    }
    
    # Mehr als ein Tag
    days <- floor(secs / (24*60^2))
    remaining_hours <- floor(secs %% (24*60^2) / 60^2)
    if (remaining_hours > 0) {
        return(paste0(format.days(days), ", ", format.hours(remaining_hours)))
    } else {
        return(format.days(days))
    }
}
