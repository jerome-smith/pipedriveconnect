var Client = require('node-rest-client').Client;
var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;
var express = require('express');
var uid = require('node-uuid');
var app = express();

app.post('/cherwell/deals', function (req, res) {
respondJson(req,res,{});
});

app.listen(3000, function () {
  console.log('Example app listening on port 3000!');
});

var respondJson  = function (req, res, json) {
  'use strict';
  if (req && res) {
    console.log("RESPONSE MOCK: JSON: ".green, req.method + ": " + req.url);
    res.json(json);
    res.end();
  }
};
var config = {
  userName:"Cherwell_dev",
  password:"Superman21",
  server:"41.77.101.146",
  options: {database: 'Cherwell_DEV'}
};
var dataStatements = [], dataIds = [];
// db connection
var connection = new Connection(config);

var bulk = connection.newBulkLoad('StagingPipeDrive', function (error, rowCount) {
  console.log('inserted %d rows', rowCount);
  connection.close();
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


var retweet = function() {
  var client = new Client();
  var api_token="57b74e6b339618a479a77d5e8c722f384a4c9887";
  //https://api.pipedrive.com/v1/deals?start=0&api_token=57b74e6b339618a479a77d5e8c722f384a4c9887

  // set up the cononection params for the SQl server.
  // registering remote methods
  client.registerMethod("jsonMethod", "https://api.pipedrive.com/v1/deals?start=0&api_token="+api_token, "GET");
  // prep the statement
  client.methods.jsonMethod(function (data, response) {

    var cols, vals, dateTime = new Date(), uids;
    //console.log(response);
    data = data && data.data || [];
      // parsed response body as js object

      if (data.length) {
        console.log(data);
        // insert o
        // bit volatile here the two arrays may become out of sync
        for (var i = 0; i < data.length; i++) {
          uids = uid.v4();
          // if (dataIds[i] === data[i].stage_id) {
          //   cols = 'update PipeDriveData'
          //   vals = ' set Title ='+data[i].Title+',Value='+
          //   data[i].value+',Currency='+
          //   data[i].currency+',Add_Time='+
          //   data[i].add_time+',Update_Time='+
          //   data[i].followers_count+',Followers_Count='+
          //   data[i].products_count+',Products_Count='+
          //   data[i].update_time+',Pipeline_ID='+
          //   data[i].pipeline_id+',Active='+data[i].active+',Deleted='+data[i].deleted+',Status='+data[i].status+')'
          // } else {
            // add rows
            // executeTitle,

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

          // }
          // dataStatements.push(cols+vals);
        }
        connection.execBulkLoad(bulk);
      }
      else {
        connection.close();
      }
  });
};

connection.on('connect', function(err) {
  if (err) {
    console.log("Database connection is not established: \n"+err);
    process.exit(0);
  } else {
    console.log("Connected");  // If no error, then good to proceed.
    retweet();
  }
});
connection.on('debug', function(text) {
  console.log(text);
});


var executeStatementCheck = function() {
  var sql = 'select * from StagingPipeDrive';

  var request = new Request(sql, function(err, rowCount, rows) {

    if (err) {
      console.log(err);
    }
    if (rowCount == 0) {

     console.log(dataStatements);
     connection.close();
     // m.on('response', function (s) {
     //  console.log(s);
     //    if (m.length > 0 ) {
     //      //console.log(m);
     //      connection.close();
     //    }
     //  });



        // var requestInsertUpdate = new Request(sqlUpdateInsert, function(err){
        //   if (err) {
        //     console.log(err);
        //   }
        // });
        // connection.execSql(requestInsertUpdate);

    }

  });
  connection.execSql(request);

  request.on('row', function(columns, c) {

    // var requestInsertUpdate = new Request(sqlUpdateInsert, function(err){
    //   if (err) {
    //     console.log(err);
    //   }
    // });
    // connection.execSql(requestInsertUpdate);
    connection.close();
  });

}

//setInterval(connection,2000);
// add a timer that will run very n minutes until we have hooks sorted.