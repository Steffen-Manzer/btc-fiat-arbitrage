# Arbitrage zwischen Bitcoin- und Devisenmärkten

Crawler, Aggregatoren und R-Skripte zur Analyse von Arbitragemöglichkeiten zwischen Bitcoin- und Devisenmärkten.

## Verbindung zu LaTeX
Für eine automatische Datenaktualisierung aus LaTeX heraus wird in einigen Skripten der
LaTeX-Befehl `executeR` erwähnt:

```latex
% R aus LaTeX heraus ausführen
\newcommand{\executeR}[1]{%
    \ignorespaces%
    \immediate%
    \write18{echo; echo "executeR: #1:"; R --slave --vanilla -f '#1' --args FromLaTeX; echo}%
    \ignorespaces%
}
```
