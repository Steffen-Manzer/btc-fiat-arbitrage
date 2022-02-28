
# tikz-Einstellungen
# Seitenbreite: \the\textwidth = 455,24408pt
# Abzüglich 2pt Padding und .3pt Rand Links+Rechts in der fbox
# Abzüglich eines seltsamen weiteren Abstandes?
# Umrechnen in Zoll
documentPageWidth <- (455.24408 - 2 * (2 + .3) - 5) / 72
defaultImageHeight <- 3 # 2,84528pt = ca. 1mm

# Pfade
options(tikzLatex="/Library/TeX/texbin/pdflatex")

# UTF-8 aktivieren
options(tikzPdftexWarnUTF = F)
options(tikzMetricPackages = c(
    "\\usepackage[T1]{fontenc}\n",
    "\\usetikzlibrary{calc}\n",
    "\\usepackage[utf8]{inputenc}\n"
))
library("tikzDevice") # R -> TeX Graphics

# Schriftart setzen
options(tikzLatexPackages = c(
    getOption("tikzLatexPackages"),
    "\\renewcommand\\familydefault{\\sfdefault}"
))
# -- ACHTUNG: Sämtliche nicht-ASCII und Kontrollzeichen müssen für tikz escaped werden

