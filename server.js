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
var openConnection = function () {
  return new Connection(config);
};

var doUpdate = function (response) {
  var a  = response.data.current;
  var updateSqlString = "UPDATE [dbo].[StagingPipeDrive] SET [LastModDateTime] = '+a.update_time+',[LastModByID] = '+a.creator_user_id+',[OwnedBy] = '+a.person_name+',[OwnedByID] = '+a.user_id+',[OwnedByTeam] = '+a.org_name+',[Status] = '+a.status+',[ID] = '+a.id+',[Stage_ID] = '+a.stage_id+',[Title] = '+a.title+',[Value] = '+a.value+',[Currency] = '+a.currency+',[Add_Time] = '+a.stage_change_time+',[Update_Time] = '+a.update_time+',[Active] = '+a.active+',[Deleted] = '+a.deleted+',[Next_Activity_Date] = '+a.next_activity_date+',[Next_Activity_ID] = '+a.next_activity_id+',[Visible_To] = '+a.visible_to+',[PipeLine_ID] = '+a.pipeline_id+',[Product_Count] = '+a.products_count+',[File_Count] = '+a.files_count+',[Notes_Count] = '+a.notes_count+',[Followers_Count] = '+a.followers_count+',[Email_Messages_Count] = '+a.email_messages_count+',[Activities_Count] = '+a.activities_Count+',[Undone_Activities] = '+a.undone_Activities+',[Reference_Activities] = '+a.reference_activities_count+',[Participants_Count] = '+a.participants_count+',[Expected_Close_Date] = '+a.expected_close_date+',[Stage_Order_Number] = '+a.stage_order_nr+',[Person_Name] = '+a.person_name+',[Org_Name] = '+a.org_name+',[Next_Activity_Subject] = '+a.next_activity_subject+',[Next_Activity_Type] = '+a.next_activity_type+',[Next_Activity_Note] = '+a.next_activity_note+',[Formatted_Value] = '+a.formatted_value+',[Weighted_Value] = '+a.weighted_value+',[Formatted_Weighted_Value] = '+a.formatted_weighted_value+',[Owner_Name] = '+a.owner_name+',[CC_EMail_Address] = '+a.cc_email+',[Org_Hidden] = '+a.org_hidden+',[Person_Hidden] ='+a.person_hidden WHERE  Stage_ID = '+a.stage_id";
  var connect = openConnection(), connect = {};

  var request = new Request(updateSqlString, function(err, rowCount, rows) {
    if (err) {
      console.log(err);
    }
  });
    connect.execSql(request);
  connect.close();
};

var doInsert = function (response) {
  var openConnection = openConnection(), connect = {};
  connect.open_connection = openConnection;
  var bulk = openConnection.newBulkLoad('StagingPipeDrive', function (error, rowCount) {
    if (rowCount) {
      console.log('inserted %d rows', rowCount);
      //connection.close();
    }
    if (error) {
      console.log('error message coming through', error);
      connect.openConnection.close();
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


  openConnection.on('connect', function(err) {
    if (err) {
      console.log("Database connection is not established: \n"+err);
      process.exit(0);
    } else {
      console.log("Connected",response);  // If no error, then good to proceed.
      sqlUpdateFunc(response);
    }
  });
  openConnection.on('debug', function(text) {
    console.log('debug',text);
  });
return connect;
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
  var current = req.body.current;
  var response = {data:current, checkId:current.stage_id};
  res.end(JSON.stringify(response));
  executeStatementCheck(response);
});

// run the server on port 3001
var server = app.listen(3001, function () {
  var host = server.address().address;
  var port = server.address().port;
  console.log("Connection listening at http://%s:%s", host, port);
});

var executeStatementCheck = function(a) {
  var sql = 'select * from StagingPipeDrive where Stage_ID='+a.checkId;
  var request = new Request(sql, function(err, rowCount, rows) {
    if (err) {
      console.log(err);
    }
    if (rowCount == 0) {
      doInsert(a);
    }
    if (rowCount > 0) {
      doUpdate(a);
    }

  });
  openConnection().execSql(request);
  openConnection.close();
};

//setInterval(connection,2000);
// add a timer that will run very n minutes until we have hooks sorted.
