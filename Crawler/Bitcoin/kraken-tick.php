<?php

// Geschrieben für PHP 8.1 (CLI oder HTTP)

require_once '_include.php';

$src = strtolower($_GET['src'] ?? $argv[1] ?? '');
if ($src !== 'eur' && $src !== 'usd' && $src !== 'gbp' && $src !== 'jpy' && $src !== 'cad' && $src !== 'chf') {
    die('Invalid source.');
}

// Bezeichnung des Datensatzes bei Kraken setzt sich aus XAAAZBBB zusammen (Mengennotierung)
// AAA = Quotierte Währung = XBT = BTC
// BBB = Gegenwährung = USD oder EUR
$datasetName = 'XXBTZ' . strtoupper($src);
// Sonderfall XBTCHF
$alternativeDatasetName = 'XBT' . strtoupper($src);

// API-Quelle
// Docs: https://www.kraken.com/features/api#get-recent-trades
define('API_URL', "https://api.kraken.com/0/public/Trades?pair=xbt${src}");

// CSV-Ziel
define('CSV_BASE', DATA_DIR . "kraken/btc${src}/kraken-tick-btc${src}-");
if (!is_dir(dirname(CSV_BASE))) {
    mkdir(dirname(CSV_BASE));
}

// Status-File: Enthält zuletzt abgefragte ID für lückenlose Abfrage
define('STATE_FILE', DATA_DIR . "kraken/btc${src}/.state");


// Start
if (!file_exists(STATE_FILE)) {
    
    // Noch keine Daten gesammelt. Datei erzeugen und von vorne beginnen.
    echo 'Starting new.' . PHP_EOL;
    $since = 0;
    $sinceDateTime = (new \DateTime())->setTimestamp(0);
    
} else {
    
    // Datensatz existiert, fortsetzen
    // Letzte ID lesen
    // Format: Unix-Zeit auf Nanosekunden genau (= 9 "Nachkommastellen")
    $since = file_get_contents(STATE_FILE);
    $unixTimeSeconds = substr($since, 0, -9);
    $nanoseconds = substr($since, -9, -3); // DateTime kann nur bis zu sechs Nachkommastellen
    $sinceDateTime = \DateTime::createFromFormat('U.u', $unixTimeSeconds . '.' . $nanoseconds);
    
    echo 'Last update: ' . strftime('%Y-%m-%d %H:%M:%S', filemtime(STATE_FILE)) . PHP_EOL;
    echo sprintf(
        'State: %s.%d (%d)',
        $sinceDateTime->format('d.m.Y H:i:s'),
        $nanoseconds,
        $since
    );
    echo PHP_EOL;
}

// API abfragen
echo 'Reading data for BTC/' . strtoupper($src) . PHP_EOL;

// Abfrage liefert immer maximal 1.000 Datensätze zurück
$url = API_URL . '&' . http_build_query(['since' => $since]);
echo 'Querying ' . $url;

if (
    file_exists(STATE_FILE) &&
    filemtime(STATE_FILE) > time() - 60
) {
    // Letzte Abfrage vor weniger als einer Minute
    // Abfrage via Proxy, da wir offenbar Daten nachladen müssen
    echo ' (w. SOCKS)';
    $json = request_file($url);
} else {
    // Reguläre, regelmäßige Datenabfrage
    $json = file_get_contents($url);
}

echo PHP_EOL;

if (empty($json)) {
    echo 'Received no data. Retry?' . PHP_EOL;
    exit(0); // Gracefully exit here
}

// Zeitstempel-floats als string beibehalten, da die Werte für PHP zu groß werden können...
$json = preg_replace('/",(\d+(?:\.\d+)?),"/', '","$1","', $json);

$data = json_decode($json);
if (!is_object($data)) {
    notifyAboutCriticalError('Could not decode response: ' . json_last_error_msg());
    echo 'Could not decode response: ' . json_last_error_msg() . PHP_EOL;
    echo 'Received data: ' . PHP_EOL;
    infoLog('Decoding data failed.');
    var_dump($json);
    exit(1);
}

if (!empty($data->error)) {
    echo 'Error fetching data: ' . implode(' AND ', $data->error) . PHP_EOL;
    echo 'Received data: ' . PHP_EOL;
    var_dump($json);
    
    // Rate-Limiting erreicht: Etwas warten und ohne Fehler abbrechen
    if ($data->error[0] === 'EAPI:Rate limit exceeded') {
        echo PHP_EOL . '... Retry?' . PHP_EOL;
        sleep(1);
        exit(0);
    } else {
        notifyAboutCriticalError('Error fetching data: ' . implode(' AND ', $data->error));
        exit(1);
    }
}

// Sonderfall, falls Bezeichnung abweichend
if (!isset($data->result->{$datasetName})) {
    $datasetName = $alternativeDatasetName;
}
if (!isset($data->result->{$datasetName})) {
    notifyAboutCriticalError('Requested dataset not found in response');
    echo 'Requested dataset not found in response.' . PHP_EOL;
    infoLog('Requested dataset not found in response.');
    var_dump($json);
    exit(1);
}


$lastTickUnixTimeSeconds = substr($data->result->last, 0, -9);
$lastTickNanoseconds = substr($data->result->last, -9, -3); // DateTime kann nur bis zu sechs Nachkommastellen
$lastTickDateTime = \DateTime::createFromFormat('U.u', $lastTickUnixTimeSeconds . '.' . $lastTickNanoseconds);

if ($lastTickDateTime === false) {
    notifyAboutCriticalError('Could not parse last tick datetime: ' . $data->result->last);
    echo 'Could not parse last tick datetime: ' . $data->result->last;
    exit(1);
}

echo sprintf(
    'Received %d datasets in %d kB until %s' . PHP_EOL,
    count($data->result->{$datasetName}),
    round(strlen($json) / 1024, 0),
    $lastTickDateTime->format('d.m.Y, H:i:s.u')
);

if (empty($data)) {
    notifyAboutCriticalError('Received dataset is empty');
    echo 'Received dataset is empty.';
    exit(1);
}

// Ergebnis zusammenstellen
$result = [];
$numNewDatasets = 0;
$isFirst = true;
$isLast = false;
foreach ($data->result->{$datasetName} as $tick) {
    
    // Datensatz:
    // [ "123.91000", "1.00000000", 1381201115.641, "s"       , "l"           , ""              ]
    //   <price>    , <volume>    , <time>        , <buy/sell>, <market/limit>, <miscellaneous>
    
    // sprintf mit %f, damit auf jeden Fall das Format U.u herauskommt, auch bei exakt 0ms
    $time = \DateTime::createFromFormat('U.u', sprintf('%f', $tick[2]));
    
    // Sicherheitsnetz: Auf gültigen Zeitraum prüfen
    if (
        $time === false ||
        (int)$time->format('Y') < 2010 ||
        (int)$time->format('Y') > 2030 ||
        $time < $sinceDateTime
    ) {
        notifyAboutCriticalError('Parsed date outside valid range');
        infoLog('Parsed date outside valid range (2010-2030, must be greater than last set)!');
        echo 'Parsed: ' . getISODate($time) . PHP_EOL;
        var_dump($tick);
        echo 'Result set:' . PHP_EOL;
        var_dump($json);
        exit(1);
    }
    
    $tick[2] = getISODate($time);
    
    // Datensatz aufbereiten: Zeitstempel, Volumen, Preis, Art, Markt/Limit
    $tickInOrder = [
        $tick[2],
        $tick[1],
        $tick[0],
        $tick[3],
        $tick[4]
    ];
    
    // Keine Prüfung auf Duplikate, da exakt die ID der letzten Abfrage angegeben wird
    
    
    if (!isset($result[$time->format("Y-m")])) {
        $result[$time->format("Y-m")] = "";
    }
    
    // Datensatz hinzufügen
    //echo 'Tick: ' . implode(' / ', $tickInOrder) . PHP_EOL;
        
    $result[$time->format("Y-m")] .= implode(',', $tickInOrder) . PHP_EOL;
    $numNewDatasets++;
}

// Keine neuen Datensätze: Ende
if (empty($result)) {
    echo 'No new datasets.';
    exit(1);
}

// Ergebnis in Datei speichern
foreach ($result as $month => $ticks) {
    $csvFile = CSV_BASE . $month . ".csv.gz";
    
    // Monatsdatei noch nicht vorhanden
    if (!file_exists($csvFile)) {
        file_put_contents($csvFile, gzencode('Time,Amount,Price,Type,Limit' . PHP_EOL));
    }
    
    $ticks = gzencode($ticks);
    file_put_contents($csvFile, $ticks, FILE_APPEND);
}

infoLog(
    'BTC' . strtoupper($src) . ': ' .
    'Collected ' . number_format($numNewDatasets, 0, ',', '.') . ' new datasets. '
);

file_put_contents(STATE_FILE, $data->result->last);
