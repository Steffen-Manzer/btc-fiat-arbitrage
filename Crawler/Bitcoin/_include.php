<?php

// Direkten Zugriff auf diese Datei unterbinden
if (__FILE__ === $_SERVER['SCRIPT_FILENAME']) die('Diese Datei stellt gemeinsam genutzte Methoden fuer Cronskripte bereit und ist nicht fuer den oeffentlichen Zugriff eingerichtet.');

require '_config.php';

// Aktuell aufgerufenes Skript für Logging
define('CRON_NAME', basename($_SERVER['SCRIPT_NAME'], '.php'));

// ISO 8601-Zeitformat mit Mikrosekunden
// Zeit = YYYY-MM-DDTHH:MM:SS.mmmmmmPPPPPP
// Zeit = 2018-01-13T13:30:21.029304+01:00
define('TIMEFORMAT', 'Y-m-d\TH:i:s.uP');

// ISO 8601-Zeitformat ohne Mikrosekunden
// Zeit = YYYY-MM-DDTHH:MM:SSPPPPPP
// Zeit = 2018-01-13T13:30:21+01:00
define('TIMEFORMAT_SECONDS', 'Y-m-d\TH:i:sP');

// Zeitzone immer UTC
date_default_timezone_set('UTC');

// Ausgabeoptionen
header('Content-Type: text/plain; charset=UTF-8');
ini_set('html_errors', 0);
register_shutdown_function(function() { echo PHP_EOL; });

// Zeitstempel für Logging erhalten
function getLogTime() : string
{
    return preg_replace('~\+00:00$~', 'Z', (new \DateTime())->format('H:i:s.u')) . ' ';
}

// Nachricht mit Zeitstempel ausgeben
function printLog(...$args) : void
{
    if (empty($args)) {
        return;
    }
    // Last argument determines new line after log message
    if (end($args) === false) {
        array_pop($args);
    } else {
        $args[] = PHP_EOL;
    }
    echo getLogTime() . implode(' ', $args);
}

// Nachricht anzeigen und speichern
function infoLog(string $msg) : void
{
    echo $msg . PHP_EOL;
    file_put_contents(
        __DIR__ . '/' . LOG_DIR . CRON_NAME . '-' . date('Ym') . '.log',
        ((new \DateTime())->format('[Y-m-d H:i:s.u] ')) . $msg . PHP_EOL,
        FILE_APPEND
    );
}

// Über kritischen Fehler benachrichtigen
function notifyAboutCriticalError(string $msg) : void
{
    mail(
        CONTACT_MAIL,
        'Fehler in Bitcoin-Crawler: ' . basename($_SERVER['SCRIPT_FILENAME']),
        $msg
    );
}

// Währungsbetrag nach deutschem Format ausgeben
function asPrice($string) : string
{
    return number_format($string, 2, ',', '.');
}

// ISO-Datum einlesen
function readISODate(string $isoDate): \DateTime
{
    $dateTime = \DateTime::createFromFormat(TIMEFORMAT, $isoDate);
    if ($dateTime === false) {
        throw new Exception('Could not parse time.');
    }
    return $dateTime;
}

// ISO-Datum mit Sekundenbruchteilen ausgeben, nutze dabei Format "Z" für UTC-Zeitzone
function getISODate(\DateTime $dateTime) : string
{
    return preg_replace('~\+00:00$~', 'Z', $dateTime->format(TIMEFORMAT));
}

// ISO-Datum ohne Sekundenbruchteile ausgeben, nutze dabei Format "Z" für UTC-Zeitzone
function getISODateSeconds(\DateTime $dateTime) : string
{
    return preg_replace('~\+00:00$~', 'Z', $dateTime->format(TIMEFORMAT_SECONDS));
}

// Als Unix-Zeitstempel inklusive Millisekunden ausgeben
function getUnixTimeWithMilliseconds(\DateTime $time, int $decimals = 3) : string
{
    $decimals = max(min($decimals, 6), 0);
    return $time->format('U') . substr($time->format('u'), 0, $decimals);
}

// Erste $lines Zeilen eines Strings zurückgeben
function head(string $str, int $lines = 1, string $newLine = "\n") : string
{
    $result = strtok($str, $newLine);
    for ($i = 1; $i < $lines; $i++) {
        $result .= $newLine . strtok($newLine);
    }
    return $result;
}

// Read last chunk of concatenated gzip file: Search for magic number 1f 8b
// Poor performance. Use only for smaller chunks of data.
// Limited to maximum 1MB of compressed data (uncompressed data may be larger)
function gzfile_get_last_chunk_of_concatenated_file(string $file, int $readLimit = 1000000) : string
{
    // Limit to 1MB
    $readLimit = min($readLimit, 1e6);
    
    $fp = fopen($file, 'rb');
    if ($fp === false) {
        throw new \Exception('Could not read file.');
    }
    
    fseek($fp, -2, SEEK_END);
    $gzdata = '';
    $data = '';
    $counter = 0;
    
    // Read chunks of 2 bytes and compare with magic number
    while (($seq = fread($fp, 2)) && $counter++ < $readLimit) {
        
        // magic number not matched
        if (bin2hex($seq) !== '1f8b') {
            fseek($fp, -3, SEEK_CUR);
            continue;
        }
        
        $pos = ftell($fp);
        $gzdata = $seq;
        
        // Read all remaining data
        while ($chunk = fread($fp, 1024)) {
            $gzdata .= $chunk;
        }
        
        // Try decoding data
        $data = @gzdecode($gzdata);
        
        // Could not decode data. Maybe magic number appeared inside compressed content?
        if ($data === false) {
            $data = '';
            fseek($fp, $pos - 3);
        } else {
            fclose($fp);
            return $data;
        }
    }
    fclose($fp);
    
    throw new \Exception('Valid chunk not found within provided size limit.');
}

// Source: https://gist.github.com/lorenzos/1711e81a9162320fde20
// Chosen by: https://stackoverflow.com/a/15025877
// modified by S. Manzer for custom newLines
/**
 * Slightly modified version of http://www.geekality.net/2011/05/28/php-tail-tackling-large-files/
 * @author Torleif Berger, Lorenzo Stanco
 * @link http://stackoverflow.com/a/15025877/995958
 * @license http://creativecommons.org/licenses/by/3.0/
 */
function tailCustom(string $filepath, int $lines = 1, bool $adaptive = true, string $newLine = "\n")
{
    // Open file
    $fp = fopen($filepath, 'rb');
    if ($fp === false) {
        return false;
    }
    
    // Sets buffer size, according to the number of lines to retrieve.
    // This gives a performance boost when reading a few lines from the file.
    if (!$adaptive) {
        $buffer = 4096;
    } else {
        $buffer = ($lines < 2 ? 64 : ($lines < 10 ? 512 : 4096));
    }
    
    // Determine newline length
    $nlLength = strlen($newLine);
    
    // Jump to last character
    fseek($fp, -$nlLength, SEEK_END);
    
    // Read it and adjust line number if necessary
    // (Otherwise the result would be wrong if file doesn't end with a blank line)
    if (fread($fp, $nlLength) !== $newLine) {
        $lines -= $nlLength;
    }
    
    // Start reading
    $output = '';
    $chunk = '';
    
    // While we would like more
    while (ftell($fp) > 0 && $lines >= 0) {
    
        // Figure out how far back we should jump
        $seek = min(ftell($fp), $buffer);
        
        // Do the jump (backwards, relative to where we are)
        fseek($fp, -$seek, SEEK_CUR);
        
        // Read a chunk and prepend it to our output
        $chunk = fread($fp, $seek);
        $output = $chunk . $output;
        
        // Jump back to where we started reading
        fseek($fp, -mb_strlen($chunk, '8bit'), SEEK_CUR);
        
        // Decrease our line counter
        $lines -= substr_count($chunk, $newLine);
    }
    
    // While we have too many lines
    // (Because of buffer size we might have read too many)
    while ($lines++ < 0) {
        // Find first newline and remove all text before that
        $output = substr($output, strpos($output, $newLine) + $nlLength);
    }
    
    // Close file and return
    fclose($fp);
    
    return trim($output);
}

// Dateigröße menschenlesbar ausgeben
function formatFileSize(float $size) : string
{
    $sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    for ($i = 0; $size >= 1024 && $i < (count($sizes) - 1); $i++) {
        $size /= 1024;
    }

    $length = strpos((string)$size, '.');
    return str_replace('.', ',', round($size, 3 - $length)) . ' ' . $sizes[$i];
}

// Wrapper für file_get_contents
if (!function_exists('request_file')) {
    function request_file(...$args) {
        return file_get_contents(...$args);
    }
}
