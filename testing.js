var moment = require('moment');
var exec = require('child_process').exec;
var fs = require('fs');

var currTime_test;

function runScript() {

  
    var last_count;
    if (!fs.existsSync('testing_data.txt')) {
    	currTime_test = moment();
    	currTime_test_format = moment().format();
        last_count = 0;
        fs.appendFileSync('testing_data.txt', currTime_test_format + "\r\n" + ++last_count + "\r\n");
    } else {
        var file = fs.readFileSync("testing_data.txt", "utf-8");
        var split_file = file.split("\n");
        var last_count = split_file[split_file.length - 2];
        var last_date = split_file[split_file.length - 3];
        console.log(last_count);
        console.log(last_date);
        currTime_test = moment().add(last_count, 'days');
        currTime_test_format = moment().add(last_count, 'days').format();
        fs.appendFileSync('testing_data.txt', currTime_test_format + "\r\n" + ++last_count + "\r\n");
    }


}


runScript();


exec('node ./index.js '+currTime_test, function(error, stdout, stderr) {
    console.log('stdout: ', stdout);
    console.log('stderr: ', stderr);
    if (error !== null) {
         console.log('finished running some-script.js');
    }
    if(error){console.log(error);}
});

