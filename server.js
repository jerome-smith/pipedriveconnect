var Client = require('node-rest-client').Client;
var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;
var config = {
    userName: 'Cherwell_dev',
    password: 'Superman21',
    server: '41.77.101.146',
    // If you are on Microsoft Azure, you need this:
    options: {database: 'TempPipeDriveData'}
};
var dataStatements = [], dataIds = [];

var retweet = function(rows) {
var client = new Client();
var connection = new Connection(config);

  var api_token="57b74e6b339618a479a77d5e8c722f384a4c9887";
  //https://api.pipedrive.com/v1/deals?start=0&api_token=57b74e6b339618a479a77d5e8c722f384a4c9887

  // set up the cononection params for the SQl server.
  // registering remote methods
  client.registerMethod("jsonMethod", "https://api.pipedrive.com/v1/deals?start=0&api_token="+api_token, "GET");
  // prep the statement
  client.methods.jsonMethod(function (data, response) {
    var cols, vals;
      // parsed response body as js object
      if (data.length) {
        // insert o
        // bit volatile here the two arrays may become out of sync
        for (var i = 0; i < data.length; i++) {
          if (dataIds[i] === data[i].stage_id) {
            cols = 'update TempPipeDriveData'
            vals = ' set title ='+data.title+',value='+
            data.value+',currency='+
            data.currency+',add_time='+
            data.add_time+',update_time='+
            data.followers_count+',followers_count='+
            data.products_count+',products_count='+
            data.update_time+',pipeline_id='+
            data.pipeline_id+',active='+data.active+',deleted='+data.deleted+',status='+data.status+')'
          } else {
            cols = 'INSERT into TempPipeDriveData (stage_id,title,value,formatted_value,weighted_value,currency,add_time,update_time,active,deleted,status,products_count,pipeline_id,followers_count)';
            vals = 'Values ('+data.stage_id+','+
            data.title+','+
            data.value+','+
            data.formatted_value+','+
            data.currency+','+
            data.add_time+','+
            data.update_time+','+
            data.active+','+data.deleted+','+data.status+
            ','+data.products_count+','+data.pipeline_id+','+followers_count+')';
          }
          dataStatements.push(cols+vals);
        }
        return dataStatements;
      }
  });
};
// db connection


connection.on('connect', function(err, a, b) {
  if (err) {
      console.log("Database connection is not established: \n"+err, a);
      process.exit(0);
  } else {
      console.log("Connected");  // If no error, then good to proceed.
      executeStatementCheck();
  }
});
connection.on('debug', function(text) {
  console.log(text);
});
// INSERT  Cherwell_Dev.pipedrivetemp (title,value,currency,add_time,update_time,stage_change_time,active,deleted,status)
// VALUES (@title,@value,@currency,@add_time,@update_time,@stage_change_time,@active,@deleted,@status);
// get the existing ids from the temp table before inserting them again
function executeStatementCheck() {
  var sql = 'select stage_id from TempPipeDriveData';

  var request = new Request(sql, function(err, rowCount, rows) {

    if (err) {
      console.log(err);
    }
    if (rowCount > 0) {
      dataIds = rows;
      retweet(rows);
    }
  });

  request.on('row', function(columns) {
    var requestInsertUpdate = new Request(sqlUpdateInsert, function(err){
      if (err) {
        console.log(err);
      }
    });
    connection.execSql(requestInsertUpdate);
  });
  connection.execSql(request);
}

//setInterval(connection,2000);
// add a timer that will run very n minutes until we have hooks sorted.