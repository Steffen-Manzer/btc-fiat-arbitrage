# Wrapper für cat(sprintf(...))
printf <- function(...) cat(sprintf(...))

# Das gleiche, eine Ausgabe erfolgt jedoch nur, wenn im aktuellen
# env die Variable DEBUG_PRINT auf TRUE gesetzt ist.
printf.debug <- function(...) {
    if (exists("DEBUG_PRINT") && isTRUE(DEBUG_PRINT)) {
        printf(...)
    }
}
