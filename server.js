var Client = require('node-rest-client').Client;
var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;
var uid = require('node-uuid');
var config = {
  userName:"Cherwell_dev",
  password:"Superman21",
  server:"41.77.101.146",
  options: {database: 'Cherwell_DEV'}
};

var express = require("express");
var bp = require("body-parser");
var app = express();
var bpurle= bp.urlencoded({ extended: false });
var bpjson= bp.json();




var dataStatements = [], dataIds = [], connection, bulk;
// db connection
var openConnection = function (response) {
  connection = new Connection(config),
  bulk = connection.newBulkLoad('StagingPipeDrive', function (error, rowCount) {
    if (rowCount) {
      console.log('inserted %d rows', rowCount);
      //connection.close();
    }
    if (error) {
      console.log('error message coming through', error);
      connection.close();
    }

  });
  // setup your columns - always indicate whether the column is nullable

    bulk.addColumn('RecID', TYPES.VarChar, { length: 42,nullable: false });
    bulk.addColumn('Title', TYPES.VarChar, { length: 50,nullable: true });
    bulk.addColumn('Status', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Value', TYPES.Int, { nullable: true });
    bulk.addColumn('Deleted', TYPES.VarChar, { nullable: true });
    bulk.addColumn('PipeLine_ID', TYPES.Int, { nullable: true });
    bulk.addColumn('Currency', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Add_Time', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Update_Time', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Followers_Count', TYPES.Int, { nullable: true });
    bulk.addColumn('Formatted_Value', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Weighted_Value', TYPES.Int, { nullable: true });
    bulk.addColumn('CreatedDateTime', TYPES.VarChar, { length:50, nullable: true });
    bulk.addColumn('Expected_Close_Date', TYPES.VarChar, {length: 50, nullable: true });
    bulk.addColumn('Person_Name', TYPES.VarChar, { length:255, nullable: true });
    bulk.addColumn('Active', TYPES.VarChar, { length:50, nullable: true });
    bulk.addColumn('Email_Messages_Count', TYPES.VarChar, { length:50, nullable: true});
    bulk.addColumn('Activities_Count', TYPES.Int, { nullable: true});
    bulk.addColumn('Undone_Activities', TYPES.Int, { nullable: true});
    bulk.addColumn('Reference_Activities', TYPES.Int, { nullable: true});
    bulk.addColumn('Org_Name', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Next_Activity_Subject', TYPES.VarChar, { length: 50, nullable: true});
    bulk.addColumn('Next_Activity_Note', TYPES.VarChar, { length: 50, nullable: true});
    bulk.addColumn('Next_Activity_Date', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Next_Activity_ID', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Visible_To', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Product_Count', TYPES.Int, { nullable: true});
    bulk.addColumn('Notes_Count', TYPES.Int, { nullable: true});
    bulk.addColumn('Owner_name', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('CC_Email_Address', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Org_Hidden', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Person_Hidden', TYPES.VarChar, { length: 50, nullable: true });

  connection.on('connect', function(err) {
    if (err) {
      console.log("Database connection is not established: \n"+err);
      process.exit(0);
    } else {
      console.log("Connected",response);  // If no error, then good to proceed.
      sqlUpdateFunc(response);
    }
  });
  connection.on('debug', function(text) {
    console.log('debug',text);
  });

};



var getAllDeals = function(response) {
  var client = new Client();
  var api_token="57b74e6b339618a479a77d5e8c722f384a4c9887";
  //https://api.pipedrive.com/v1/deals?start=0&api_token=57b74e6b339618a479a77d5e8c722f384a4c9887

  // set up the cononection params for the SQl server.
  // registering remote methods
  // go get this record  the first tme or anytime tweet is called
  client.registerMethod("jsonMethod", "https://api.pipedrive.com/v1/deals?start=0&api_token="+api_token, "GET");
  // prep the statement
  client.methods.jsonMethod(sqlUpdateFunc);
};

var sqlUpdateFunc = function (data, response) {
  var cols, vals, dateTime = new Date(), uids;
  //console.log(response);
  data = data && data.data || [];
    // parsed response body as js object

    if (data.length) {
      console.log('data JEROME data');
      // insert o
      // bit volatile here the two arrays may become out of sync
      for (var i = 0; i < data.length; i++) {
        uids = uid.v4();
        bulk.addRow({RecID:uids,Title:data[i].title,Status:data[i].status,Value:data[i].value,Deleted:data[i].deleted,Pipeline_ID:data[i].pipeline_id,Currency:data[i].currency,Add_Time:data[i].add_time,
        Update_Time:data[i].update_time,
        Expected_Close_Date:data[i].expected_close_date,
        Person_Name:data[i].person_name,
        Active:data[i].active,
        Email_Messages_Count:data[i].email_messages_count,
        Activities_Count:data[i].activities_count,
        Undone_Activities:data[i].undone_activities_count,
        Reference_Activities:data[i].reference_activities_count,
        Org_Name:data[i].org_name,
        Next_Activity_Subject:data[i].next_activity_subject,
        Next_Activity_Note:data[i].next_activity_note,
        Next_Activity_Date:data[i].next_activity_date,
        Next_Activity_ID:data[i].next_activity_id,
        Visible_To:data[i].visible_to,
        Product_Count:data[i].products_count,
        Notes_Count:data[i].notes_count,
        Owner_name:data[i].owner_name,
        CC_Email_Address:data[i].cc_email,
        Org_Hidden:data[i].org_hidden,
        Person_Hidden:data[i].person_hidden,
        Followers_Count:data[i].followers_count,Weighted_Value:data[i].weighted_Value,Formatted_Value:data[i].formatted_value});
      }
      connection.execBulkLoad(bulk);
    }
    else {
      closeConnection();
    }
  };

// after bulk load close the connection
var closeConnection = function () {
  return connection.close();
}

// Listen for incoming calls
// then open connection and insert data
//http://196.201.104.8:3001/v1/deals
app.post('/v1/deals', bpjson, function (req, res) {
  var response = {data:req.body};
  res.end(JSON.stringify(response));
  openConnection(response);
});

// run the server on port 3001
var server = app.listen(3001, function () {
  var host = server.address().address;
  var port = server.address().port;
  console.log("Connection listening at http://%s:%s", host, port);
});

var executeStatementCheck = function() {
  var sql = 'select * from StagingPipeDrive';
  var request = new Request(sql, function(err, rowCount, rows) {
    if (err) {
      console.log(err);
    }
    if (rowCount == 0) {
     connection.close();
    }

  });
  connection.execSql(request);
  request.on('row', function(columns, c) {
    connection.close();
  });
};

//setInterval(connection,2000);
// add a timer that will run very n minutes until we have hooks sorted.
