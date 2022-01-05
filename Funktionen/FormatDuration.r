format.duration <- function(x) {
    if (is.difftime(x)) {
        secs <- floor(as.double(x, units="secs"))
    } else if (is.integer(x)) {
        secs <- x
    } else {
        stop("x must be a difftime or an integer.")
    }
    
    # Weniger als eine Minute
    if (secs < 60) {
        return(sprintf("%d s", secs))
    }
    
    # Weniger als eine Stunde
    if (secs < 60 * 60) {
        mins <- floor(secs/60)
        remaining_secs <- secs %% 60
        return(sprintf("%d min, %d s", mins, remaining_secs))
    }
    
    # Mehr als eine Stunde
    hours <- floor(secs / 60^2)
    minutes <- floor(secs %% 60^2 / 60)
    return(sprintf("%d h, %d min", hours, minutes))
}
