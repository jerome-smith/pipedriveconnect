var Client = require('node-rest-client').Client;
var Connection = require('tedious').Connection;
var config = {
    userName: 'JeromeS',
    password: 'P@ssw0rd01',
    server: '196.201.104.8',
    // If you are on Microsoft Azure, you need this:
    options: {encrypt: true, database: 'AdventureWorks'}
};

var retweet = function() {
var client = new Client();
  var api_token="57b74e6b339618a479a77d5e8c722f384a4c9887";
  //https://api.pipedrive.com/v1/deals?start=0&api_token=57b74e6b339618a479a77d5e8c722f384a4c9887

  // set up the cononection params for the SQl server.
  // registering remote methods
  client.registerMethod("jsonMethod", "https://api.pipedrive.com/v1/deals?start=0&api_token="+api_token, "GET");

  client.methods.jsonMethod(function (data, response) {
      // parsed response body as js object
      console.log(data);
      // raw response
      //console.log(response);
  });
};
// db connection
var connect = function () {
    var connection = new Connection(config);
    connection.on('connect', function(err) {
    // If no error, then good to proceed.
        console.log("Connected");
        executeStatement1();
    });
}


var Request = require('tedious').Request
var TYPES = require('tedious').TYPES;

function executeStatement1() {
    request = new Request("INSERT SalesLT.Product (Name, ProductNumber, StandardCost, ListPrice, SellStartDate) OUTPUT INSERTED.ProductID VALUES (@Name, @Number, @Cost, @Price, CURRENT_TIMESTAMP);", function(err) {
     if (err) {
        console.log(err);}
    });
    request.addParameter('Name', TYPES.NVarChar,'SQL Server Express 2014');
    request.addParameter('Number', TYPES.NVarChar , 'SQLEXPRESS2014');
    request.addParameter('Cost', TYPES.Int, 11);
    request.addParameter('Price', TYPES.Int,11);
    request.on('row', function(columns) {
        columns.forEach(function(column) {
          if (column.value === null) {
            console.log('NULL');
          } else {
            console.log("Product id of inserted item is " + column.value);
          }
        });
    });
    connection.execSql(request);
}
setInterval(retweet,2000);
// add a timer that will run very n minutes until we have hooks sorted.