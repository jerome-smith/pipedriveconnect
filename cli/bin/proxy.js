var url       = require('url'),
    http      = require('http'),
    fs        = require('fs'),
    path      = require('path'),
    flatiron  = require('flatiron'),
    httpProxy = require('http-proxy'),
    _         = require('underscore'),
    server    = require('./server.js'),
    DB        = require('../lib/db.js'),
    tools     = require('../lib/tools.js'),
    fiapp     = module.exports = flatiron.app,
    gc        = global.config,
    pathsArr  = getFilePaths(path.join(__dirname, '../lib/reqhandlers')),
    // paths that match this will be proxies to dev
    proxies   = new RegExp('\/(cmd|dropplet|web2prints|adbuilder|ice_server)\/');

function log (req, arr, col1, col2) {
  "use strict";
  console.log(req.method + (' Proxying to ' + arr.join(' ') + ' -->')[col1], req.url.toString()[col2]);
}

function startSelenium (aReqHandlersArr) {
  "use strict";
  DB.init();
  // start web server on port 8002. This is soley used for running selenium
  // tests on the standalone selnium server. dont use this for anything else
  var expressServer8002 = server.start(gc.fixSvr3);
  _.each(aReqHandlersArr || [], function (aPath) {
    var handler = require(aPath);
    if (_.isFunction(handler)) {
      handler(DB, expressServer8002, tools);
    }
  });
}

//aReqHandlersArr is a list of  file paths to the request handler files
function startProxy (aReqHandlersArr) {
  "use strict";

  DB.init();
  // start the first local webserver on port 8000. This will serve the fixtures
  // and static content and is used for running selenium tests in the selenium
  // IDE and working away from the api, when fixtures are generated on the fly.
  var expressServer8000 = server.start(gc.fixSvr1);
  // we pass the server object, running on port 8000, into the request handlers
  // so they can map their request handlers to the urls.
  _.each(aReqHandlersArr || [], function (aPath) {
    var handler = require(aPath);
    if (_.isFunction(handler)) {
      handler(DB, expressServer8000, tools);
    }
  });

  if (global.homeMode) {
    // in 'home mode' a second proxy is started on port 7999 and all requests
    // for static content are routed to the local http server running on port
    // 8000 but all api request are routed to the proxy running on port 8001 on
    // the remote machine, which in turn proxies data from port 80 on the api.
    console.log(('HTTP proxy listening on ' + gc.proxySvr2).yellow);
    httpProxy.createProxyServer(function (req, res, proxy) {
      var uri = url.parse(req.url);

      if (proxies.exec(uri.path)) {
        log(req, ['proxy on', global.remoteProxyIp, gc.proxySvr1], 'magenta', 'cyan');
        req.headers.host = global.remoteProxyIp;
        proxy.proxyRequest(req, res, {
          host : global.remoteProxyIp,
          port : gc.proxySvr1 // 8001
        });
      } else {
        log(req, ['Express on', gc.fixSvr1], 'yellow', 'green');
        proxy.proxyRequest(req, res, {
          host : '127.0.0.1',
          port : gc.fixSvr1 // 8000
        });
      }
    }).listen(gc.proxySvr2); // 7999
  }
  else {
    // in 'api mode' a proxy is started on port 8001 and all requests for static
    // content are routed to a http server running on port 8000. all api
    // requests are sent to the api on port 80. A second http server is started

    var proxy = httpProxy.createProxyServer({});

    http.createServer(function (req, res) {
      var uri = url.parse(req.url);

      if (proxies.exec(uri.path)) {
        // api mode
        log(req, ['API on', gc.api], 'magenta', 'cyan');
        req.headers.host = gc.apiHost;

        proxy.web(req, res, {
          target : {
            host : gc.apiHost, // app.dev.com
            port : gc.api // port 80
          }
        });
      }
      else {
        log(req, ['Express on', gc.fixSvr1], 'yellow', 'green');
        proxy.web(req, res, {
          target : {
            host : 'localhost',
            port : gc.fixSvr1 // 8000
          }
        });
      }
    }).listen(gc.proxySvr1); // port 8001
  }
}

// returns an array of file paths (not folders). Used to get the request handlers
function getFilePaths (aPath) {
  "use strict";

  var results = [];

  (function getPaths(aPathi) {
    var list = fs.readdirSync(aPathi);

    list.forEach(function(file) {
      if (_.contains(gc.ignoreFiles, file)) {
        return;
      }
      file = aPathi + '/' + file;
      var stat = fs.statSync(file);
      if (stat && stat.isDirectory()) {
        getPaths(file);
      }
      else {
        if (/\.js$/.test(file)) {
          results.push(file);
        }
      }
    });
  }(aPath));

  console.log(('Request Handler Files').yellow);
  console.log(_.compact(results));
  return _.compact(results);
}

// usage
fiapp.use(flatiron.plugins.cli, {
  usage: [
    '',
    'devtools',
    '',
    'Starts a local webserver that serves the projects static assets',
    '',
    'Usage:',
    '  devtools api            - serves api.',
    '  devtools fixtures       - serves fixtures.',
    ''
  ]
});

fiapp.cmd('version', function () {
  'use strict';
  console.log('flatiron ' + flatiron.version);
});

// default mode.
fiapp.cmd('api', function() {
  'use strict';
  fiapp.log.info('Starting in API mode');
  startProxy(pathsArr);
});

// use this mode when connecting on a non-work computer over VPN
fiapp.cmd('home(.*)?', function() {
  'use strict';
  var ip = process.argv[3] || '';

  if (!/\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}/.test(ip)) {
    console.log(('IP ADDRESS OF REMOTE PROXY IS NECESSARY IN HOME MODE').red);
    console.log(('i.e. cli/bin/devtools home 192.168.1.1').red);
    return;
  }

  fiapp.log.info(('Starting in home mode. Proxying to ' + ip + ':' + gc.proxySvr1).green);
  global.remoteProxyIp = ip;
  startProxy(pathsArr);
});

