var AWS = require('aws-sdk');
var crypto = require('crypto');
var s3_file = require('s3');
var async = require('async');
var moment = require('moment');
var fs = require('fs');
var mysql = require('mysql');
var Q = require('q');
var kue = require('kue');
var format = require("string-template");
var https = require('https');
var logger = require('tracer').colorConsole();
var getUrls = require('get-urls');

var argument = process.argv[2];
argument = parseInt(argument);


var s3 = new AWS.S3();

var options = {
    s3Client: s3
};

var client = s3_file.createClient(options);

//array which will hold all the message objects to be sent
var msg_array = [];

//object to keep track of job queues created, will be used in releasing the resources at the end
var Total_Job = {
    completed: 0,
    failed: 0,
    created: 0
};

var connection = mysql.createPool({
    connectionLimit: 10,
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'asepsislife'
});

//function to return the regdids corresponding to an userid
function retrieve_regdid(sqluser_id) {
    logger.log(sqluser_id);
    var query_def = Q.defer();
    var reg_array = [];
    sqluser_id = sqluser_id.trim();

    connection.getConnection(function(err, connection) {
        // Use the connection 
        if (err) {
            logger.log("error establishing connection");
            query_def.resolve('');
            return;
        }
        connection.query('select regdid from vimmune_data where userid=? and user_enabled=?', [sqluser_id, 1], function(error, res, fields) {
            if (error) {
                logger.log("error occured in the query");
                query_def.resolve(''); // if failed resolve the defer with blank value
                return;
            } else {
                logger.log(res);
                for (var i in res) {
                    reg_array.push(res[i]['regdid']);
                }
                query_def.resolve(reg_array); // resolve the defer with the regdid
                connection.release(); // release the connection after the query is done
            }
        });

    });
    return query_def.promise;
}

var queue = kue.createQueue({
    prefix: 'txt4tots'
});
queue.watchStuckJobs(5000);

function create_queue(msg, callback) {
    logger.log("doing create queue");
    onesignal_job = queue.create('onesignal', msg).attempts(5).delay(1000).ttl(900000).backoff({
        type: 'exponential'
    }).removeOnComplete(true).save();
    Total_Job.created++;
    callback(null, onesignal_job);
}

//function to send the messages via onesignal
function sendNotification(message_data, done) {
    var headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": "Basic NGEwMGZmMjItY2NkNy0xMWUzLTk5ZDUtMDAwYzI5NDBlNjJj"
    };

    var options = {
        host: "onesignal.com",
        port: 443,
        path: "/api/v1/notifications",
        method: "POST",
        headers: headers
    };
    var req = https.request(options, function(res) {
        res.on('data', function(data) {
            logger.log("Response:");
            logger.log(JSON.parse(data));
            done();
        });
    });

    req.on('error', function(e) {
        logger.log("ERROR:");
        return done(new Error('message sending failed'));
    });
    logger.log(message_data);

    req.write(JSON.stringify(message_data));
    req.end();
};

//async.map on the message array
function send_message() {
    var onesignal_job;
    async.map(msg_array, create_queue, function(err, res) {
        if (err) {
            logger.log("error occured " + err);
        } else {
            logger.log("successfully added all the messages to the queue");

        }
    });

}
queue.process('onesignal', 5, function(job, done) {
    logger.log("doing sendNotification");
    sendNotification(job.data, done);
    logger.log("done sendNotification");

});

queue.on('job complete', function(id, result) {
    Total_Job.completed++;
});
queue.on('job failed', function(id, result) {
    Total_Job.failed++;
});

//function to check the termination condition for the code
function job_check() {
    logger.log("inside job check");
    logger.log(Total_Job.created);
    if (Total_Job.created == 0) {
        setTimeout(function() {
            queue.shutdown(10000, function(err) {
                logger.log('Kue shutdown: ', err || '');
            });
            connection.end(function(err) {
                logger.log('mysql closed', err || '');
            });
        }, 2000);
    } else if (parseInt(Total_Job.created) == (parseInt(Total_Job.completed) + parseInt(Total_Job.failed))) {
        setTimeout(function() {
            queue.shutdown(10000, function(err) {
                logger.log('Kue shutdown: ', err || '');
            });
            connection.end(function(err) {
                logger.log('mysql closed', err || '');
            });
        }, 2000);
    } else {
        var check_interval = setInterval(function() {
            if (parseInt(Total_Job.created) == (parseInt(Total_Job.completed) + parseInt(Total_Job.failed))) {
                clearInterval(check_interval);
                setTimeout(function() {
                    queue.shutdown(10000, function(err) {
                        logger.log('Kue shutdown: ', err || '');
                    });
                    connection.end(function(err) {
                        logger.log('mysql closed', err || '');
                    });
                }, 2000);
            }
        }, 20000);
    }
}

function get_url(text) {
    return (getUrls(text));
}

// message data
var txt4tots_rules = fs.readFileSync("./txt4tots_rules.json", "utf8");
txt4tots_rules = JSON.parse(txt4tots_rules);

var allkeys = [];
var content_array = [];
var json_data;
var content_data;
//var key_match = [];
//key_match.push('vimmune-user-data/aluser/pCpVPZSm1eqVu3+/7iYP05d/i30=/vimmunedata_US.json');

function filter_keys() {
    //filter and push the proper keys into allkeys
    for (var len1 = 0; len1 < content_array.length; len1++) {
        var temp_content = content_array[len1];
        for (var len2 = 0; len2 < temp_content.length; len2++) {
            var temp_key = temp_content[len2].Key;
            var split_key = temp_key.split("/");
            var last = split_key[split_key.length - 1];
            if (last == "vimmunedata_IN.json" || last == "vimmunedata_US.json") {
                //if (key_match.indexOf(temp_key) != -1) {
                allkeys.push(temp_key);

                //}
            }
        }
    }

    logger.log("keys retrieved");
    logger.log(allkeys);
}

var params = {
    Bucket: 'asepsislife-interns',
    Prefix: 'vimmune-user-data/'
};
var pars = { s3Params: params, recursive: true };
//List all the objects matching the given prefix
var list = client.listObjects(pars);
list.on('error', function(err) {
    logger.error("unable to list objects:", err.stack);
});
list.on('data', function(data) {
    logger.log("data received from the listObject");
    json_data = JSON.stringify(data);
    content_data = JSON.parse(json_data);
    content_array.push(content_data.Contents);
});
list.on('end', function() {
    logger.log("done listObjects");
    //logger.log(content_array);
    //filter the keys 
    filter_keys();
    //check for file's existence and perform the required code
    file_exists_check();
});

var notification_data;


function file_exists_check() {
    // check if the usernotifications data exist on the server
    s3.headObject(params1, function(err) {
        if (err) {
            logger.log("File is not present on the server");
            // no user notification data file found on the server, start with an empty array
            notification_data = [];
            // main driver code
            run_asyncmap();
        } else {
            logger.log("File is present on the server");
            
            // download the usernotification data file if it exists on the server
            var dwld_file = client.downloadBuffer(params2);
            dwld_file.on('error', function(err) {
                logger.error("unable to get user_notification data:", err.stack);
            });
            dwld_file.on('end', function(buffer) {
                logger.log("done downloading");
                var test_data = buffer.toString('utf8');
                test_data = JSON.parse(test_data);
                // file exists, start with the existing data
                notification_data = test_data;
                logger.log(test_data);
                // main driver code
                run_asyncmap();
            });

        }
    });
}

function run_asyncmap() {
    async.mapLimit(allkeys, 3, execute, function(err, res) {
        if (err) {
            logger.log("error" + err);
        } else {
            logger.log("successfully completed");
            //logger.log(JSON.stringify(notification_data));
            logger.log("doing send_message");
            //logger.log(msg_array);
            //logger.log(notification_data);
            //send the messages before uploading the file 
            send_message();
            // write the final json into a temporaray file
            fs.writeFileSync('./temp_notification_data.json', JSON.stringify(notification_data), 'utf8');
            var data_md5 = crypto.createHash('md5').update(JSON.stringify(notification_data)).digest("base64");
            var params2 = {
                localFile: "./temp_notification_data.json",

            };

            // upload the final json file on the server
            var upload_file = client.uploadFile(params2);
            upload_file.on('error', function(err) {
                logger.error("unable to upload final usernotifications data : ", err.stack);
            });
            upload_file.on('end', function() {
                logger.log("done uploading final usernotifications data ");
                fs.unlinkSync('./temp_notification_data.json'); // delete the file after uploading
                job_check(); // check all the jobs for termination of the code
            });

        }

    });
}

function execute(keys, callback) {
    // the iteratee function for async.mapLimit()
    var first_data; // this will be used only if there is no data in notification_data i.e file isn't created yet
    var id = keys; // id is the name of the key
    var key_length = id.length; //get the length of the key(this will be used if fetching user_id from key)
    id = id.substring(18, key_length - 20); //substring of key to get the desired user_id from the full key
    var user_id = id; //assign user_id with this id
    logger.log(user_id);
    logger.log(keys);

    // download the userdata for the key
    var dwld_userfile = client.downloadBuffer(params3);
    logger.log(dwld_userfile);
    dwld_userfile.on('error', function(err) {
        logger.error("unable to download:", err.stack);
    });
    dwld_userfile.on('end', function(buffer) {
        logger.log("done downloading userdata for " + keys);
        var user_data = buffer.toString('utf8');
        user_data = JSON.parse(user_data);

        retrieve_regdid(user_id).then(function(res) {
            if (res == '') {
                logger.log("error fetching regdid");
            } else

            {
                for (var key in user_data) {
                    // iterate for every key in the userdata json
                    var name = user_data[key].name; // fetch the name of the user
                    var gender = user_data[key].gender; // fetch the gender of the user
                    var dob = user_data[key].dob; // fetch the dob of the user
                    var user_nation = user_data.owner.country; //get user's country
                    var currTime = moment(argument, "x");
                    var sent = moment().format('x'); //get the current time in milliseconds format
                    var givTime = moment(dob, "x").format("DD MMM YYYY hh:mm a");
                    var duration1 = moment.duration(currTime.diff(givTime));
                    var weeks = duration1.asWeeks(); // get the duration between dob and current time in weeks i.e age in weeks
                    logger.log(weeks);
                    var msg_shown = false; // boolean to check if a message is shown to a user or not
                    for (var i = 0; i < Object.keys(txt4tots_rules).length; i++) {
                        if (Math.floor(weeks) == txt4tots_rules[i]["Child's Age in Weeks"]) {

                            logger.log("Message for : " + name);
                            logger.log(weeks + "weeks old");
                            logger.log(user_nation);
                            logger.log(txt4tots_rules[i]["Country"]);

                            if (txt4tots_rules[i]["Country"] == "ALL" || (txt4tots_rules[i]["Country"] == user_nation)) {
                                var temp_msg = txt4tots_rules[i]['TXT4Tots Message'];
                                //  var format_msg;

                                if (gender == "male") {
                                    if (txt4tots_rules[i]["memberType"] == "human" || txt4tots_rules[i]["memberType"] == "male") {
                                        logger.log("male condition");
                                        format_msg = format(temp_msg, { // replace {name} in the message with actual name
                                            name: name
                                        });

                                    } else {
                                        continue;
                                    }
                                } else if (gender == "female") {
                                    if (txt4tots_rules[i]["memberType"] == "human" || txt4tots_rules[i]["memberType"] == "female") {
                                        logger.log("female condition");
                                        format_msg = format(temp_msg, { // replace {name} in the message with actual name
                                            name: name
                                        });

                                    } else {
                                        continue;
                                    }
                                } else if (gender == "others") {
                                    if (txt4tots_rules[i]["memberType"] == "human" || txt4tots_rules[i]["memberType"] == "others") {
                                        logger.log("other condition");
                                        format_msg = format(temp_msg, { // replace {name} in the message with actual name
                                            name: name
                                        });

                                    } else {
                                        continue;
                                    }
                                } else if (gender == "cat") {
                                    if (txt4tots_rules[i]["memberType"] == "pet" || txt4tots_rules[i]["memberType"] == "cat") {
                                        logger.log("cat condition");
                                        format_msg = format(temp_msg, { // replace {name} in the message with actual name
                                            name: name
                                        });

                                    } else {
                                        continue;
                                    }
                                } else if (gender == "dog") {
                                    if (txt4tots_rules[i]["memberType"] == "pet" || txt4tots_rules[i]["memberType"] == "dog") {
                                        logger.log("dog condition");
                                        format_msg = format(temp_msg, { // replace {name} in the message with actual name
                                            name: name
                                        });

                                    } else {
                                        continue;
                                    }
                                }


                                logger.log(format_msg);
                                var split_msg = format_msg.split(" ");


                                for (var count = 0; count < split_msg.length; count++) {
                                    if (split_msg[count] == "his/her" || split_msg[count] == "him/her" || split_msg[count] == "he/she" || split_msg[count] == "he's/she's" || split_msg[count] == "He/She" || split_msg[count] == "himself/herself") {
                                        if (gender == "male") {
                                            var test1 = split_msg[count].split("/");
                                            split_msg[count] = test1[0];
                                        } else if (gender == "female") {
                                            var test2 = split_msg[count].split("/");
                                            split_msg[count] = test2[1];
                                        }
                                    }

                                }

                                var joined_msg = split_msg.join(" "); // now join the splitted message


                                // message object
                                var temp_obj = {};
                                var url_data = get_url(joined_msg);
                                var url_arr = Array.from(url_data);
                                temp_obj["app_id"] = "5eb5a37e-b458-11e3-ac11-000c2940e62c";
                                temp_obj["contents"] = {};
                                temp_obj["contents"]["en"] = joined_msg; //content is the message for the user
                                temp_obj["headings"] = {};
                                temp_obj["headings"]["en"] = name + "'s" + " week " + Math.floor(weeks);
                                if(url_arr.length != 0)
                                {
                                     temp_obj["web_buttons"] = [];
                                     var button_obj = {};
                                     button_obj["id"] = "read-button";
                                     button_obj["text"] = "Read More";
                                     button_obj["icon"] = "http://i.imgur.com/MIxJp1L.png";
                                     button_obj["url"] = url_arr[url_arr.length-1];
                                     temp_obj["web_buttons"].push(button_obj);
                                }
                                logger.log(user_id);
                                logger.log("adding player ids");
                                temp_obj["include_player_ids"] = res; // this is fetched in the retrieve_regdid function
                                logger.log(temp_obj);
                                //msg_array.push(temp_obj);
                            }

                            var m_id = txt4tots_rules[i]['UUID'];
                            msg_shown = true;
                            var check = true; // boolean to check if a message is eligible to be shown
                            // logger.log(notification_data);
                            if (notification_data != undefined && notification_data.length > 0) { // if notification_data isn't empty
                                // data exists
                                // logger.log(notification_data);
                                var userid_exists = false; // boolean to check whether the userid exists in the notification_data
                                var index; // fetch the index of the userid if found in the notification_data
                                for (var ind = 0; ind < notification_data.length; ind++) {
                                    if (notification_data[ind].hasOwnProperty(id)) {
                                        index = ind; // note the index
                                        userid_exists = true; //userid exists
                                        break;

                                    }
                                }

                                if (userid_exists) {
                                    //userid exists
                                    logger.log("user id exists");
                                    logger.log(key);
                                    if (notification_data[index][id][key] == undefined) { // if it is a new key for the user
                                        notification_data[index][id][key] = {};
                                        notification_data[index][id][key]["sent_msg"] = [];
                                        var msg2 = {};
                                        msg2["message_uuid"] = m_id;
                                        msg2["sent_time"] = sent.toString();
                                        msg2["dob"] = dob.toString();
                                        notification_data[index][id][key].sent_msg.push(msg2);
                                        logger.log("new key added for the user " + id);
                                        logger.log(notification_data);
                                        msg_array.push(temp_obj);

                                        check = false; // message is now shown
                                        break;


                                    } else {
                                        // if it is not a new key for the given user, check if the messageid already exists
                                        var len = notification_data[index][id][key]['sent_msg'].length;
                                        for (var l = 0; l < len; l++) {
                                            if (m_id == notification_data[index][id][key].sent_msg[l].message_uuid) {
                                                logger.log("message already shown");
                                                check = false; // no message to show

                                            }
                                        }
                                    }


                                    if (check) {
                                        // there is a new message for the user
                                        var ms = {};
                                        ms["message_uuid"] = m_id;
                                        ms["sent_time"] = sent.toString();
                                        ms["dob"] = dob.toString();
                                        notification_data[index][id][key].sent_msg.push(ms);
                                        logger.log("new message added for the user: " + id);
                                        msg_array.push(temp_obj);

                                        break;


                                    }
                                } else {

                                    //user_id doesn't exist, create and add it to notification_data
                                    var new_id = {};
                                    new_id[id] = {};
                                    var msg1 = {};
                                    msg1["message_uuid"] = m_id;
                                    msg1["sent_time"] = sent.toString();
                                    msg1["dob"] = dob.toString();
                                    new_id[id][key] = {};
                                    new_id[id][key]["sent_msg"] = [];
                                    new_id[id][key]["sent_msg"].push(msg1);
                                    notification_data.push(new_id);
                                    logger.log("new user_id added :" + id);
                                    logger.log(notification_data);

                                    msg_array.push(temp_obj);

                                    break;

                                }

                            } else {
                                // file doesn't exist, push the data to notification_data
                                logger.log("File does not exist.");
                                first_data = {};
                                first_data[id] = {};
                                first_data[id][key] = {};
                                first_data[id][key]["sent_msg"] = [];
                                var msg = {};
                                msg["message_uuid"] = m_id;
                                msg["sent_time"] = sent.toString();
                                msg["dob"] = dob.toString();
                                first_data[id][key]["sent_msg"].push(msg);
                                notification_data.push(first_data);
                                logger.log("file created and new user added :" + id);
                                msg_array.push(temp_obj);

                                break;

                            }


                            logger.log(notification_data);
                        }
                    }
                }
                if (!msg_shown) { logger.log("No message for : " + name + " " + weeks + " old"); }
            }
            logger.log("callback");
            callback(null, notification_data); //add the notification data with the callback

        }); //retrieve_regdid closed

    });


}