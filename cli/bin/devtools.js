#!/usr/bin/env node

/*!
 * Devtools
 *
 */
var path = require('path'),
    fs = require('fs');

fs.readFile(path.normalize(__dirname + "/config.json"), 'utf8',
  function (err, data) {
    'use strict';

    global.config = JSON.parse(data);
    require('./proxy').start();
  });


