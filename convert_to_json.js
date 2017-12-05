var Converter = require("csvtojson").Converter;
var fs = require("fs");
var converter = new Converter({});
var path = './txt4tots_messages_csv.csv' ;
converter.fromFile(path,function(err,result){ 
	if(err)
		{  console.log("error"); }
    else { 
           console.log(result);
           fs.writeFile('./txt4tots_rules.json', JSON.stringify(result) , 'utf-8');
         }
});