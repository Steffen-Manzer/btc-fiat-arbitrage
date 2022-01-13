<?php

// Abrufzeitraum (jeweils inklusive)
$dateFrom = new \DateTime('first day of 6 months ago 00:00:00');
$dateTo = new \DateTime('first day of last month 00:00:00');

// Speicherort
$outPath = realpath('../../Daten/truefx/') . "/";

// Gewünschte Währungspaare
$forexFilter = ['AUDJPY', 'AUDUSD', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPJPY', 'GBPUSD', 'USDCHF', 'USDJPY'];

// Login-Cookie (auf truefx.com einloggen und Cookie hier einfügen)
$loginCookie = 'PHPSESSID=xxxx; _tccl_visitor=yyyy; _tccl_visit=zzzz; [...]';
