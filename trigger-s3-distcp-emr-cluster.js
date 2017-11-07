var aws = require('aws-sdk');
aws.config.region = 'ap-northeast-1';
var emr = new aws.EMR({apiVersion: '2009-03-31'});

const RELEASE_VERSION = "emr-5.8.0";
const HADOOP_VERSION = "2.7.3";
const BUCKET = "s3://bucket"; // Production Bucket
const LOG_FOLDER = "s3://log-output-folder"; // Production EMR Log Bucket
const INSTANCE_SIZE = "m4.large";
const SUBNET_ID = "subnet-xxxxx";
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

function getTargetDate(){
  var targetDate = new Date( new Date().getTime() + 9 * 3600 * 1000 - 86400000);
  var month = ('0' + (targetDate.getMonth() + 1)).slice(-2);
  var date = ('0' + targetDate.getDate()).slice(-2);
  var dateString = targetDate.getFullYear() + "/" + month + "/" + date;
  return dateString;
}

// Function for posting to Slack
function postToSlack(messageText){
const https = require('https');
const url = require('url');
const slack_url = 'https://hooks.slack.com/services/xxxxxxxxxxxx'; //slack channel for notification
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

function stepBuilder(jobTarget, targetDate){
  var steps = [];
  var serverName = "";
  var targetFolders = [];
switch (jobTarget){
  case LOG_JOB:
  serverName = "adgms";
  targetFolders = TARGET_JOB;
  break;
  case MANAGE_JOB:
  serverName = "adgame-admin";
  targetFolders = TARGET_MANAGE;
  break;
  case API_JOB:
  serverName = "adgm";
  targetFolders = TARGET_API;
  break;
  case USER_JOB:
  serverName = "adgame-user";
  targetFolders = TARGET_USER;
  break;
  case GW_JOB:
  serverName = "gmgw";
  targetFolders = TARGET_GATEWAY;
  break;
  default:
  serverName = "";
  targetFolders = [];
}

console.log("Numbers of folder:" + targetFolders.length);
for (t = 0; t < targetFolders.length; t++){
  targetFolder = targetFolders[t];
  for (i=0; i<24; i++){
  var hour = i;
  if (i < 10){
  // add character '0'
  hour = '0' + i;
  }

  step = {
    HadoopJarStep: {
      Jar: 'command-runner.jar',
      Args: [
        "s3-dist-cp",
        "--src",
        BUCKET + "/" + serverName + "/"+ targetFolder + "/" + targetDate+ "/" + hour,
        "--dest",
        BUCKET + "/" + serverName + "/"+ targetFolder + "/" + targetDate+ "/" + hour,
        "--groupBy",
        ".*(" + serverName + ".*-[0-9]+[T,0-9]+).*",
        "--outputCodec",
        "gz",
        "--deleteOnSuccess"
      ]
    },
    Name: serverName + "CleanUp" + ":" + targetFolder + "_" + targetDate + "/" + hour,
    ActionOnFailure: "CONTINUE"
  };
  // single step params is set up, push to overall cluster step
  steps.push(step);
}
}
return steps;
}

function runEMR(jobName, jobTarget, instanceSize, targetDate){
  var steps = stepBuilder(jobTarget,targetDate);
  var params = {
  Instances: { /* required */
    HadoopVersion: HADOOP_VERSION,
    Ec2SubnetId: SUBNET_ID,
    InstanceGroups: [
      {
        EbsConfiguration: {
               EbsBlockDeviceConfigs: [
                  {
                     VolumeSpecification: {
                        SizeInGB: 8,
                        VolumeType: "gp2"
                     },
                     VolumesPerInstance: 1
                  }
               ]
        },
        InstanceCount: 1,
        InstanceRole: "MASTER",
        InstanceType: instanceSize,
        Market: "ON_DEMAND",
        Name: 'Master-1'
      }],
    KeepJobFlowAliveWhenNoSteps: false,
    TerminationProtected: false
  },
  Name: jobName+"_"+targetDate,
  ReleaseLabel: RELEASE_VERSION,
  BootstrapActions: [
  ],
  JobFlowRole: "EMR_EC2_DefaultRole",
  LogUri: LOG_FOLDER,
  ScaleDownBehavior: "TERMINATE_AT_TASK_COMPLETION",
  ServiceRole: "EMR_DefaultRole",
  Steps: steps,
  Tags: [
    {
      Key: 'opsworks:stack',
      Value: 'gamefactory'
    }
  ],
  VisibleToAllUsers: true
};

  emr.runJobFlow(params, function(err, data){
    if (err) {
       postToSlack(":exclamation:Failed to create EMR Cluster!\nError Details:" + err);
    }
    else console.log();
  });
}

exports.handler = (event, context, callback) => {
    runEMR("PROD-SugorokuADJobLogCleanUp", LOG_JOB, INSTANCE_SIZE, getTargetDate());
    runEMR("PROD-SugorokuADManageLogCleanUp", MANAGE_JOB, INSTANCE_SIZE, getTargetDate());  
    runEMR("PROD-SugorokuADUserApiLogCleanUp", API_JOB, INSTANCE_SIZE, getTargetDate());
    runEMR("PROD-SugorokuADUserLogCleanUp", USER_JOB, INSTANCE_SIZE, getTargetDate());
    runEMR("PROD-SugorokuADGatewayLogCleanUp", GW_JOB, INSTANCE_SIZE, getTargetDate());
    
};
