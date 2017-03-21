var express = require("express");
var bp = require("body-parser");
var app = express();
var bpurle= bp.urlencoded({ extended: false });
var bpjson= bp.json();

app.post('/v1/deals', bpjson, function (req, res) {
 response = {
      data:req.body
   };
   console.log('==>',response);
   res.end(JSON.stringify(response));

});


var server = app.listen(3001, function () {

  var host = server.address().address
  var port = server.address().port
  console.log("Example app listening at http://%s:%s", host, port)

});