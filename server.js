var Client = require('node-rest-client').Client;
var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;
var uid = require('node-uuid');

// RDP Host: cherwell-dev.iconnecttelecoms.com
// Username: jeromes
// Password Never_Look_Back

// SQL Backup:  192.168.2.105
// Database: Cherwell_Dev
// SQL instance: None
// User: SVC_CherwellAPI
// Password: Never_Look_Back
// Rights: DBO



var config = {
  userName:"Svc_cherwelluat",
  password:"S$7v87#$",
  server:"192.168.2.105",
  options: {
    database: 'Cherwell_Dev',
    useColumnNames: true,
    rowCollectionOnRequestCompletion:true
  }
};
var express = require("express");
var bp = require("body-parser");
var app = express();
var bpurle= bp.urlencoded({ extended: false });
var bpjson= bp.json();
// Marius change this token when you are changing acoounts
var api_token="b55efbe639cdfe2e3d8d5ddb6f5918ea2711ff12";
var epGetAllDeals = "https://api.pipedrive.com/v1/deals?start=0&limit=100&api_token="+api_token;



var dataStatements = [], dataIds = [], connection, bulk;
// db connection
var openConnection = function () {
  return new Connection(config);
};
// DB update
var doUpdate = function (response) {
  var a  = response;
  var updateSqlString = "UPDATE [StagingPipeDrive] SET Value = @value";
   updateSqlString += ", [Status] = @status";
   updateSqlString += ", CreatedBy =@person_name";
   updateSqlString += ", Currency =@currency";
   updateSqlString += ", Add_Time = @stage_change_time";
   updateSqlString += ", Address1 = @address1";
   updateSqlString += ", Contact_Number = @contact_number";
   updateSqlString += ", Description = @description";
   updateSqlString += ", Wholesaler = @wholesaler";
   updateSqlString += ", Sales_Person =@owner_name";
   updateSqlString += ", Update_Time = @update_time";
   updateSqlString += ", Active = @active";
   updateSqlString += ", Deleted = @deleted";
   updateSqlString += ", Next_Activity_Date =@next_activity_date";
   updateSqlString += ", Next_Activity_ID = @next_activity_id";
   updateSqlString += ", Next_Activity_Note =@next_activity_note";
   updateSqlString += ", Visible_To = @visible_to";
   updateSqlString += ", PipeLine_ID = @pipeline_id";
   updateSqlString += ", Product_Count = @products_count";
   updateSqlString += ", Title = @title";
   updateSqlString += ", Participants_count = @participants_count";
   updateSqlString += ", Org_Name = @org_name";
   updateSqlString += ", Expected_Close_Date = @expected_close_date";
   updateSqlString += ", Org_Hidden = @org_hidden";
   updateSqlString += ", File_Count = @files_count";
   updateSqlString += ", Notes_Count = @notes_count";
   updateSqlString += ", Email_Messages_Count =@email_messages_count";
   updateSqlString += ", Activities_Count =@activities_count";
   updateSqlString += ", Undone_Activities = @undone_activities_count";
   updateSqlString += ", CreatedByID = @person_id";
   updateSqlString += " WHERE [Stage_ID] =@stage_id";
   updateSqlString += " AND ID =@id";

    var request = new Request(updateSqlString, function(err, rowCount) {
    if (err) {
      console.log(err);
      connect.close();
    }
  });
    request.addParameter('address1', TYPES.VarChar, a['b78fc4cc8254f2db228253846cd30fd23a3dac4d']);
    request.addParameter('contact_number', TYPES.VarChar, a['933f1418de6c5152026acc29ecb20ccb9c58c1de']);
    request.addParameter('description', TYPES.VarChar, a['0f7e1c54bc74746c8915352223edc1031879bdad']);
    request.addParameter('wholesaler', TYPES.VarChar, a['42e175da98816fb62ec4ed003dac7a0083c7ecf9']);
    request.addParameter('sales_person', TYPES.VarChar, a.owner_name);
    request.addParameter('status', TYPES.VarChar, a.status);
    request.addParameter('person_id', TYPES.VarChar, a.person_id);
    request.addParameter('visible_to',TYPES.Int, a.visible_to);
    request.addParameter('pipeline_id',TYPES.Int, a.pipeline_id);
    request.addParameter('next_activity_id', TYPES.Int, a.next_activity_id);
    request.addParameter('products_count',TYPES.Int, a.products_count);
    request.addParameter('files_count', TYPES.Int, a.files_count);
    request.addParameter('followers_count', TYPES.Int, a.followers_count);
    request.addParameter('activities_count', TYPES.Int, a.activities_count);
    request.addParameter('undone_activities_count', TYPES.Int, a.undone_activities_count);
    request.addParameter('stage_order_nr', TYPES.Int, a.stage_order_nr);
    request.addParameter('weighted_value', TYPES.Int, a.weighted_value);
    request.addParameter('notes_count', TYPES.Int, a.notes_count);
    request.addParameter('stage_change_time', TYPES.VarChar, a.stage_change_time);
    request.addParameter('update_time', TYPES.VarChar, a.update_time);
    request.addParameter('id', TYPES.Int, a.id);
    request.addParameter('value', TYPES.Int, a.value);
    request.addParameter('undone_activities', TYPES.Int, a.undone_activities_count);
    request.addParameter('participants_count', TYPES.Int, a.participants_count);
    request.addParameter('email_messages_count', TYPES.Int, a.email_messages_count);
    request.addParameter('reference_activities_count', TYPES.Int, a.reference_activities_count);
    request.addParameter('stage_id', TYPES.Int, a.stage_id);
    request.addParameter('title', TYPES.VarChar, a.title);
    request.addParameter('currency', TYPES.VarChar, a.currency);
    request.addParameter('active', TYPES.VarChar, a.active);
    request.addParameter('deleted', TYPES.VarChar, a.deleted);
    request.addParameter('next_activity_date', TYPES.VarChar, a.next_activity_date);
    request.addParameter('org_name', TYPES.VarChar, a.org_name);
    request.addParameter('person_hidden', TYPES.VarChar, a.person_hidden);
    request.addParameter('owner_name', TYPES.VarChar, a.owner_name);
    request.addParameter('cc_email', TYPES.VarChar, a.cc_email);
    request.addParameter('org_hidden', TYPES.VarChar, a.org_hidden);
    request.addParameter('next_activity_type', TYPES.VarChar, a.next_activity_type);
    request.addParameter('next_activity_note', TYPES.VarChar, a.next_activity_note);
    request.addParameter('formatted_value', TYPES.VarChar, a.formatted_value);
    request.addParameter('formatted_weighted_value', TYPES.VarChar, a.formatted_weighted_value);
    request.addParameter('expected_close_date', TYPES.VarChar, a.expected_close_date);
    request.addParameter('next_activity_subject', TYPES.VarChar, a.next_activity_subject);
    request.addParameter('user_id', TYPES.VarChar, a.user_id);
    request.addParameter('person_name', TYPES.VarChar, a.person_name);
    var connect = new Connection(config);
    connect.on('connect', function(err) {
    if (err) {
      console.log("Database connection is not established: \n"+err);
      process.exit(0);
    } else {
      console.log("Connected");  // If no error, then good to proceed.
      connect.execSql(request);
    }
  });
  connect.on('debug', function(text) {
    console.log('debug',text);
  });

};
// bulk insert
var doInsert = function (response) {
  var openConnection = new Connection(config);

  var bulk = openConnection.newBulkLoad('StagingPipeDrive', function (error, rowCount) {
    if (rowCount) {
      console.log('inserted %d rows', rowCount);
      //connection.close();
    }
    if (error) {
      console.log('error message coming through', error);
      openConnection.close();
    }

  });
    // setup your columns - always indicate whether the column is nullable
    bulk.addColumn('RecID', TYPES.VarChar, { length: 42,nullable: false });
    bulk.addColumn('Title', TYPES.VarChar, { length: 50,nullable: true });
    bulk.addColumn('Status', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Value', TYPES.Int, { nullable: true });
    bulk.addColumn('ID', TYPES.Int, { nullable: true });
    bulk.addColumn('Deleted', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('PipeLine_ID', TYPES.Int, { nullable: true });
    bulk.addColumn('Currency', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Add_Time', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Update_Time', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Formatted_Value', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Weighted_Value', TYPES.Int, { nullable: true });
    bulk.addColumn('CreatedDateTime', TYPES.DateTime, { nullable: true });
    bulk.addColumn('Expected_Close_Date', TYPES.VarChar, {length: 50, nullable: true });
    bulk.addColumn('Person_Name', TYPES.VarChar, { length:50, nullable: true });
    bulk.addColumn('Active', TYPES.VarChar, { length:50, nullable: true });
    bulk.addColumn('Email_Messages_Count', TYPES.VarChar, { length:50, nullable: true});
    bulk.addColumn('Activities_Count', TYPES.Int, { nullable: true});
    bulk.addColumn('Undone_Activities', TYPES.Int, { nullable: true});
    bulk.addColumn('Reference_Activities', TYPES.Int, { nullable: true});
    bulk.addColumn('Org_Name', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Next_Activity_Subject', TYPES.VarChar, { length: 50, nullable: true});
    bulk.addColumn('Next_Activity_Note', TYPES.VarChar, { length: 4000, nullable: true});
    bulk.addColumn('Next_Activity_Date', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Next_Activity_ID', TYPES.Int, { nullable: true });
    bulk.addColumn('Visible_To', TYPES.Int, { nullable: true });
    bulk.addColumn('Product_Count', TYPES.Int, { nullable: true});
    bulk.addColumn('Notes_Count', TYPES.Int, { nullable: true});
    bulk.addColumn('Stage_ID', TYPES.Int, { nullable: true});
    bulk.addColumn('Owner_name', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('CC_Email_Address', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Org_Hidden', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Person_Hidden', TYPES.VarChar, { length: 50, nullable: true });
    bulk.addColumn('Contact_Number', TYPES.VarChar, { length: 255, nullable:true});
    bulk.addColumn('Address1', TYPES.VarChar, { length: 4000, nullable:true});
    bulk.addColumn('Description', TYPES.VarChar, { length: 4000, nullable:true});
    bulk.addColumn('Wholesaler', TYPES.VarChar, { length:255, nullable:true});
    bulk.addColumn('Sales_Person', TYPES.VarChar, { length:255, nullable:true});

  openConnection.on('connect', function(err) {
    if (err) {
      console.log("Database connection is not established: \n"+err);
      process.exit(0);
    } else {
      console.log("Connected",response);  // If no error, then good to proceed.
      sqlUpdateFunc(response, bulk, openConnection);
    }
  });
  openConnection.on('debug', function(text) {
    console.log('debug',text);
  });
 return openConnection;
};
// all deals
var getAllDeals = function() {
  var client = new Client();
  client.registerMethod("jsonMethod", epGetAllDeals, "GET");
  // prep the statement
  client.methods.jsonMethod(sqlUpdateFunc);
};
// all deals and page through the deals
var displayAllDeals = function() {
  var client = new Client();
  client.get(epGetAllDeals, function(data, res) {
    console.log(data);
    var current = data.data, start;
    dataPag = data.additional_data.pagination;

    for (var i = 0; i < current.length; i++) {
      executeStatementCheck(current[i]);
    }
    if (dataPag.more_items_in_collection) {
      start = dataPag.next_start;
      epGetAllDeals = "https://api.pipedrive.com/v1/deals?start="+start+"&limit=100&api_token="+api_token;
      displayAllDeals();
    }
  });
};

var sqlUpdateFunc = function (data, bulk, connection) {
  var cols, vals, dateTime = new Date(), uids;
  data = data && data.data || [data];
    // parsed response body as js object
    if (data.length) {
      for (var i = 0; i < data.length; i++) {
        datas = data[i];
        uids = uid.v4();
        bulk.addRow({RecID:uids,
        Title:datas.title,
        Status:datas.status,
        Value:datas.value,
        Deleted:datas.deleted,
        Pipeline_ID:datas.pipeline_id,
        Currency:datas.currency,Add_Time:datas.add_time,
        Update_Time:datas.update_time,
        Stage_ID:datas.stage_id,
        Expected_Close_Date:datas.expected_close_date,
        Person_Name:datas.person_name,
        Active:datas.active,
        Contact_Number : datas['933f1418de6c5152026acc29ecb20ccb9c58c1de'],
        Address1: datas['b78fc4cc8254f2db228253846cd30fd23a3dac4d'],
        Description: datas['0f7e1c54bc74746c8915352223edc1031879bdad'],
        Wholesaler: datas['42e175da98816fb62ec4ed003dac7a0083c7ecf9'],
        Email_Messages_Count:datas.email_messages_count,
        Sales_Person: datas.owner_name,
        Activities_Count:datas.activities_count,
        Undone_Activities:datas.undone_activities_count,
        Reference_Activities:datas.reference_activities_count,
        Org_Name:datas.org_name,
        Next_Activity_Subject:datas.next_activity_subject,
        Next_Activity_Note:datas.next_activity_note,
        Next_Activity_Date:datas.next_activity_date,
        Next_Activity_ID:datas.next_activity_id,
        Visible_To:datas.visible_to,
        Product_Count:datas.products_count,
        Notes_Count:datas.notes_count,
        Owner_name:datas.owner_name,
        CC_Email_Address:datas.cc_email,
        Org_Hidden:datas.org_hidden,
        Person_Hidden:datas.person_hidden,
        ID:datas.id,
        Followers_Count:datas.followers_count,
        Weighted_Value:datas.weighted_Value,
        Formatted_Value:datas.formatted_value
      });
      }
      connection.execBulkLoad(bulk);
    }
    else {
      connection.closeConnection();
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
  var current = req.body;
  console.log('current',current);
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

var updatePipeDrive = function (a) {
  var client = new Client();
  var md = new Connection(config);
  var sql = 'select * from StagingPipeDriveValueWrite';

  var request = new Request(sql, function(err, rowCount, rows) {
    if (err) {
      console.log(err);
    }
    console.log('Pipedrive needs to be updated:',rowCount);
  });
  request.on('row', function (rowCount, more, rows) {
    var args = {
      path: {id:rowCount.ID.value},
      data : {
        'value' : rowCount.Value.value,
        'id': rowCount.ID.value
      },
      headers: { "Content-Type": "application/json" }
    };
    client.put("https://api.pipedrive.com/v1/deals/${id}?api_token="+api_token, args, function(d){
      console.log(d);
    });
  });
  md.on('connect', function(err) {
    if (err) {
      console.log("Database connection is not established: \n"+err);
      process.exit(0);

    } else {
      console.log("Connected");  // If no error, then good to proceed.
      md.execSql(request);
    }
  });
  md.on('debug', function(text) {
    console.log('debug',text);
  });
}

var executeStatementCheck = function(a) {
  var id = a.id;//a.data[0].data.stage_id;
  var m = new Connection(config);
  // checks each record if it exists if it does then insert else update the record
  var sql = 'select * from StagingPipeDrive where Stage_ID ='+a.stage_id+' and ID = '+id;
  var request = new Request(sql, function(err, rowCount, rows) {
    if (err) {
      console.log(err);
    }
    console.log('THERE ARE number of rows',rowCount);
    if (rowCount == 0) {
      doInsert(a);
    }
    if (rowCount > 0) {
      console.log('update performed');
      doUpdate(a);
    }
  });
  m.on('connect', function(err) {
    if (err) {
      console.log("Database connection is not established: \n"+err);
      process.exit(0);

    } else {
      console.log("Connected");  // If no error, then good to proceed.
      m.execSql(request);
    }
  });
  m.on('debug', function(text) {
    console.log('debug',text);
  });
};

displayAllDeals();
setInterval(updatePipeDrive,300000);
setInterval(displayAllDeals,300000);

