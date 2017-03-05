var Client = require('node-rest-client').Client;
var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;
var config = {
    userName: 'Cherwell_Dev',
    password: 'jdN6Df7c',
    server: '10.10.1.115',
    // If you are on Microsoft Azure, you need this:
    options: {encrypt: true, database: 'Cherwell_Dev'}
};
var dataStatements = [], dataIds = [];

var retweet = function(rows) {
var client = new Client();
  var api_token="57b74e6b339618a479a77d5e8c722f384a4c9887";
  //https://api.pipedrive.com/v1/deals?start=0&api_token=57b74e6b339618a479a77d5e8c722f384a4c9887

  // set up the cononection params for the SQl server.
  // registering remote methods
  client.registerMethod("jsonMethod", "https://api.pipedrive.com/v1/deals?start=0&api_token="+api_token, "GET");

  client.methods.jsonMethod(function (data, response) {
    var cols, vals;
      // parsed response body as js object
      if (data.length) {
        // insert o
        // bit volatile here the two arrays may become out of sync
        for (var i = 0; i < data.length; i++) {
          if (dataIds[i] === data[i].stage_id) {
            cols = 'update pipedrivetemp'
            vals = ' set title ='+data.title+',value='+
            data.value+',currency='+
            data.currency+',add_time='+
            data.add_time+',update_time='+
            data.update_time+',stage_change_time='+
            data.stage_change_time+',active='+data.active+',deleted='+data.deleted+',status='+data.status+')'
          } else {
            cols = 'INSERT into pipedrivetemp (title,value,currency,add_time,update_time,stage_change_time,active,deleted,status)';
            vals = 'Values ('+data.stage_id+','+
            data.title+','+
            data.value+','+
            data.currency+','+
            data.add_time+','+
            data.update_time+','+
            data.stage_change_time+','+data.active+','+data.deleted+','+data.status+')';
          }
          dataStatements.push(cols+vals);
        }
      }
  });
};
// db connection
var connect = function () {
    var connection = new Connection(config);
    connection.on('connect', function(err) {
    // If no error, then good to proceed.
        console.log("Connected");
        executeStatementCheck();
    });
}

// INSERT  Cherwell_Dev.pipedrivetemp (title,value,currency,add_time,update_time,stage_change_time,active,deleted,status)
// VALUES (@title,@value,@currency,@add_time,@update_time,@stage_change_time,@active,@deleted,@status);
// get the existing ids from the temp table before inserting them again
function executeStatementCheck() {
  var sql = 'select stage_id from pipedrivetemp';
  var request = new Request(sql, function(err, rowCount, rows) {

    if (err) {
      console.log(err);
    }
    if (rowCount > 0) {
      dataIds = rows;
      retweet(rows);
    }
  });
}
function executeStatement1() {
  var sql = 'INSERT pipedrivetemp (title,value,currency,add_time,update_time,stage_change_time,active,deleted,status)  SELECT col, col2   FROM tbl_B WHERE NOT EXISTS (SELECT col FROM tbl_A A2 WHERE A2.col = tbl_B.col)';
  var request = new Request("INSERT Cherwell_Dev.pipedrivetemp (title,value,currency,add_time,update_time,stage_change_time,active,deleted,status) OUTPUT INSERTED.stage_id VALUES (@title,@value,@currency,@add_time,@update_time,@stage_change_time,@active,@deleted,@status);", function(err) {
   if (err) {
      console.log(err);}
  });

  request.addParameter('stage_id', TYPES.int,11);
  request.addParameter('title', TYPES.Varchar);
  request.addParameter('value', TYPES.Varchar);
  request.addParameter('currency', TYPES.Varchar);
  request.addParameter('add_time', TYPES.datetime);
  request.addParameter('update_time', TYPES.datetime);
  request.addParameter('stage_change_time', TYPES.datetime);
  request.addParameter('active', TYPES.bit);
  request.addParameter('deleted', TYPES.bit);
  request.addParameter('status', TYPES.Varchar);

  request.on('row', function(columns) {
      columns.forEach(function(column) {
        if (column.value === null) {
          console.log('NULL');
        } else {
          console.log(" stage_id of inserted item is " + column.value);
        }
      });
  });
  connection.execSql(request);
}
setInterval(connect,2000);
// add a timer that will run very n minutes until we have hooks sorted.