<?php

// Abrufzeitraum
$yearFrom = 2021;
$yearTo = (int)date('Y');
$monthsFrom = 11;
$monthsTo = 12;

// Speicherort
$outPath = realpath('../../Daten/truefx/') . "/";

// Gewünschte Währungspaare
$forexFilter = ['AUDJPY', 'AUDUSD', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPJPY', 'GBPUSD', 'USDCHF', 'USDJPY'];

// Login-Cookie (auf truefx.com einloggen und Cookie hier einfügen)
$loginCookie = 'PHPSESSID=xxxx; _tccl_visitor=yyyy; _tccl_visit=zzzz; [...]';
