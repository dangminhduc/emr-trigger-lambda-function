const LOG_JOB = 1;
const MANAGE_JOB = 2;
const API_JOB = 3;
const USER_JOB = 4;
const GW_JOB = 5;

const TARGET_JOB = ["tomcat7/adgms.tomcat7.appl"];
const TARGET_MANAGE = ["nginx/adgame-admin.nginx.access","rails/adgame-admin.rails.app"];
const TARGET_API = ["nginx/adgm.nginx.access","tomcat7/adgm.tomcat7.appl"];
const TARGET_USER = ["rails/adgame-user.rails.app"];
const TARGET_GATEWAY = ["nginx/gmgw.nginx.access"];
const BUCKET = "logs-gfac";
const STACK_ID = "xxxxxxxxxxxxxxxx";  // target OpsWorks Stack


var aws = require('aws-sdk');
aws.config.region = 'ap-northeast-1';
var s3 = new aws.S3({apiVersion: '2006-03-01'});


// Function for posting to Slack
function postToSlack(messageText){
const https = require('https');
const url = require('url');
const slack_url = 'https://hooks.slack.com/services/xxxxxxxxxxxxxxxxxxxxxx'; //slack channel for notification
const slack_req_opts = url.parse(slack_url);
slack_req_opts.method = 'POST';
slack_req_opts.headers = {'Content-Type': 'application/json'};

var req = https.request(slack_req_opts, function (res) {
        if (res.statusCode === 200) {
          console.log("Message posted to slack");
        } else {
          console.log("Error status code: " + res.statusCode);
        }
      });

      req.on('error', function(e) {
        console.log("problem with request: " + e.message);
        console.log(e.message);
      });

      req.write(JSON.stringify({text: messageText}));
      req.end();

}

function getTargetDate(){
  var targetDate = new Date( new Date().getTime() + 9 * 3600 * 1000 - 2 * 86400000);
  var month = ('0' + (targetDate.getMonth() + 1)).slice(-2);
  var date = ('0' + targetDate.getDate()).slice(-2);
  var dateString = targetDate.getFullYear() + "/" + month + "/" + date;
  return dateString;
}

function cleanUpEMRDummyFile(target, targetDate){
  var awsOpsworks = require('aws-sdk');
    awsOpsworks.config.region = 'us-east-1';
    var opsworks = new awsOpsworks.OpsWorks({apiVersion: '2013-02-18'});
    var sitesIDs = [];
    var params = {
    StackIds: [STACK_ID]
    };
    opsworks.describeStacks(params, function(err, data) {
        if (err) {
          postToSlack(":exclamation: Can't not get siteID from GameFactory OpsWorks stack!");
        } // an error occurred
        else{ // successful response
            customJSON = JSON.parse(data.Stacks[0].CustomJson);
            var sites = customJSON["gamefactory"]["sites"];
            for (var site in sites){
                sitesIDs.push(sites[site]["id"]);
            }
            // console.log(sitesIDs);
            deleteEMRDummyFile(target, targetDate,sitesIDs);
        }
    });
}

function deleteEMRDummyFile(target, targetDate,sitesIDs){
  var targetPrefix = "";
  switch (target){
    case LOG_JOB:
    targetPrefix = "adgms";
    targetFolders = TARGET_JOB;
    break;
    case MANAGE_JOB:
    targetPrefix = "adgame-admin";
    targetFolders = TARGET_MANAGE;
    break;
    case API_JOB:
    targetPrefix = "adgm";
    targetFolders = TARGET_API;
    break;
    case USER_JOB:
    targetPrefix = "adgame-user";
    targetFolders = TARGET_USER;
    // todo
    for(j = 0; j< sitesIDs.length; j++){
      targetFolders.push("nginx/adgame-user.nginx.access."+sitesIDs[j]);
      targetFolders.push("nginx/adgame-user.nginx.error."+sitesIDs[j]);
    }
    break;
    case GW_JOB:
    targetPrefix = "gmgw";
    targetFolders = TARGET_GATEWAY;
    break;
    default:
    targetPrefix = "";
    targetFolders = [];
  }

   console.log(targetFolders);
  for (i = 0; i < targetFolders.length; i++){
    targetFolder = targetFolders[i];
    var prefix = targetPrefix + "/" + targetFolder + "/" + targetDate+ "/";
    var params = {Bucket: BUCKET, Delimiter: '/',Prefix: prefix};
    s3.listObjects(params, function (err, data) {
      if (err === null){
        var contents = data.Contents;
        for (i=0; i<contents.length; i++){
        var objectToDelete = contents[i]["Key"];
        console.log(objectToDelete + " will be deleted!");
        var deleteParams = {Bucket: BUCKET, Key: objectToDelete};
        s3.deleteObject(deleteParams, function(err, data) {
          if (err) postToSlack(":exclamation: Failed while deleting " + objectToDelete); // an error occurred
          else console.log(data);
          });
        }
      }
    });
}
}

exports.handler = (event, context, callback) => {
     cleanUpEMRDummyFile(USER_JOB, getTargetDate());
     cleanUpEMRDummyFile(LOG_JOB, getTargetDate());
     cleanUpEMRDummyFile(MANAGE_JOB, getTargetDate());
     cleanUpEMRDummyFile(API_JOB, getTargetDate());
     cleanUpEMRDummyFile(GW_JOB, getTargetDate());
};
