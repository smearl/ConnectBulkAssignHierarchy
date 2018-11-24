var AWS = require('aws-sdk');
var sleep = require('sleep');
var lodash = require('lodash');
var csv = require('csvtojson');
var moment = require('moment');


// Change to the appropriate region
var region = 'us-east-1';

// Change to the local filesystem location of the CSV file to be processed
// CSV should have the following first line "Email,LevelOne,LevelTwo,LevelThree" without quotes
var inputFile = '/Users/smearl/Downloads/users.csv';

// Change to the appropriate Amazon Connect instance id
var instanceId = '87d42999-0986-4ed9-a1f2-63b0384ff927';

var maxResults = 1000;

var defaultDelay = 500;

var connect = new AWS.Connect({apiVersion: "latest", region: region});

// Change to
var baseParams = {
    InstanceId: instanceId
};

var allUsers = null;
var allHierarchyGroups = null;


async function _listUsersAsync (params) {
   return new Promise((resolve, reject) => {
        connect.listUsers(Object.assign(params, baseParams), function (err, data) {
            if (err) {
                reject(err);
            }
            else {
                resolve(data);
            }
        });
    });
};


async function _describeUserAsync (params) {
    return new Promise((resolve, reject) => {
        connect.describeUser(Object.assign(params, baseParams), function (err, data) {
            if (err) {
                reject(err);
            }
            else {
                resolve(data);
            }
        });
    });
};


async function _listUserHierarchyGroups (params) {
    return new Promise((resolve, reject) => {
        connect.listUserHierarchyGroups(Object.assign(params, baseParams), function (err, data) {
            if (err) {
                reject(err);
            }
            else {
                resolve(data);
            }
        });
    });
};


async function _describeUserHierarchyGroupAsync (params) {
    return new Promise((resolve, reject) => {
        connect.describeUserHierarchyGroup(Object.assign(params, baseParams), function (err, data) {
            if (err) {
                reject(err);
            }
            else {
                resolve(data);
            }
        });
    });
};

async function _updateUserHierarchyAsync (params) {
    return new Promise((resolve, reject) => {
        connect.updateUserHierarchy(Object.assign(params, baseParams), function (err, data) {
            if (err) {
                reject(err);
            }
            else {
                resolve(data);
            }
        });
    });

}


async function listAllUsers (params) {
    var nextToken = '';

    var users = [];

    while (nextToken !== null) {
        var userResults = await _listUsersAsync({MaxResults: params.MaxResults, NextToken: nextToken && nextToken.length > 0 ? nextToken : null})

        users = lodash.concat(userResults.UserSummaryList, users);

        nextToken = userResults.NextToken !== null ? userResults.NextToken : null;

        sleep.msleep(params.Delay);
    }

    if (params.IncludeDescriptions === true) {

        for (user of users) {
            var describeResults = await _describeUserAsync({UserId: user.Id});

            if (describeResults) {
                user.HierarchyGroupId = describeResults.User.HierarchyGroupId;
                user.Email = describeResults.User.IdentityInfo.Email;
                user.FirstName = describeResults.User.IdentityInfo.FirstName;
                user.LastName = describeResults.User.IdentityInfo.LastName;
                user.Username = describeResults.User.Username;
            }

            sleep.msleep(params.Delay);
        }
    }

    return users;
};


async function listAllUserHierarchyGroups (params) {
    var nextToken = '';

    var groups = [];

    while (nextToken !== null) {
        var groupResults = await _listUserHierarchyGroups({MaxResults: params.MaxResults, NextToken: nextToken && nextToken.length > 0 ? nextToken : null})

        groups = lodash.concat(groupResults.UserHierarchyGroupSummaryList, groups);

        nextToken = groupResults.NextToken !== null ? groupResults.NextToken : null;

        sleep.msleep(params.Delay);
    }

    if (params.IncludeDescriptions === true) {

        for (group of groups) {
            var describeResults = await _describeUserHierarchyGroupAsync({HierarchyGroupId: group.Id});

            if (describeResults) {

                group.LevelOne = null;
                group.LevelTwo = null;
                group.LevelThree = null;
                group.LevelFour = null;
                group.LevelFive = null;

                if (describeResults.HierarchyGroup.HierarchyPath.LevelOne) {
                    group.LevelOne = describeResults.HierarchyGroup.HierarchyPath.LevelOne.Name;
                }

                if (describeResults.HierarchyGroup.HierarchyPath.LevelTwo) {
                    group.LevelTwo = describeResults.HierarchyGroup.HierarchyPath.LevelTwo.Name;
                }

                if (describeResults.HierarchyGroup.HierarchyPath.LevelThree) {
                    group.LevelThree = describeResults.HierarchyGroup.HierarchyPath.LevelThree.Name;
                }

                if (describeResults.HierarchyGroup.HierarchyPath.LevelFour) {
                    group.LevelFour = describeResults.HierarchyGroup.HierarchyPath.LevelFour.Name;
                }

                if (describeResults.HierarchyGroup.HierarchyPath.LevelFive) {
                    group.LevelFive = describeResults.HierarchyGroup.HierarchyPath.LevelFive.Name;
                }
            }

            sleep.msleep(params.Delay);
        }

    }

    return groups;
};

async function updateUserHierarchy (params) {
    var groupResults = await _updateUserHierarchyAsync({UserId: params.UserId, HierarchyGroupId: params.HierarchyGroupId})

    sleep.msleep(params.Delay);
};


async function main() {
    console.log('Began processing at [' + moment().format() + '].');

    // Load all users
    console.log('\tBegin user load.');
    allUsers = await listAllUsers({MaxResults: maxResults, Delay: defaultDelay, IncludeDescriptions: true});
    console.log('\tEnd user load.');


    // Load all hierarchy groups
    console.log('\tBegin hierarchy group load.');
    allHierarchyGroups = await listAllUserHierarchyGroups({MaxResults: maxResults, Delay: defaultDelay, IncludeDescriptions: true});
    console.log('\tEnd hierarchy group load.');


    // Process user list from csv
    console.log('\tProcessing user list.');
    var userList = [];


    csv().fromFile(inputFile).then(function (users) {
        users.map(function (u) {

            var user = lodash.find(allUsers, { Email: u.Email });

            if (user) {
                // Find hierarch group by levels
                var hierarchyGroup = null;

                if (u.LevelOne && !u.LevelTwo && !u.LevelThree && !u.LevelFour && !u.LevelFive) {
                    hierarchyGroup = lodash.find(allHierarchyGroups, {LevelOne: u.LevelOne, LevelTwo: null, LevelThree: null, LevelFour: null, LevelFive: null});
                }
                else if (u.LevelOne && u.LevelTwo && !u.LevelThree && !u.LevelFour && !u.LevelFive) {
                    hierarchyGroup = lodash.hierarchyGroup = lodash.find(allHierarchyGroups, {LevelOne: u.LevelOne, LevelTwo: u.LevelTwo, LevelThree: null, LevelFour: null, LevelFive: null});
                }
                else if (u.LevelOne && u.LevelTwo && u.LevelThree && !u.LevelFour && !u.LevelFive) {
                    hierarchyGroup = lodash.hierarchyGroup = lodash.find(allHierarchyGroups, {LevelOne: u.LevelOne, LevelTwo: u.LevelTwo, LevelThree: u.LevelThree, LevelFour: null, LevelFive: null});
                }
                else if (u.LevelOne && u.LevelTwo && u.LevelThree && u.LevelFour && !u.LevelFive) {
                    hierarchyGroup = lodash.hierarchyGroup = lodash.find(allHierarchyGroups, {LevelOne: u.LevelOne, LevelTwo: u.LevelTwo, LevelThree: u.LevelThree, LevelFour: u.LevelFour, LevelFive: null});
                }
                else if (u.LevelOne && u.LevelTwo && u.LevelThree && u.LevelFour && u.LevelFive) {
                    hierarchyGroup = lodash.hierarchyGroup = lodash.find(allHierarchyGroups, {LevelOne: u.LevelOne, LevelTwo: u.LevelTwo, LevelThree: u.LevelThree, LevelFour: u.LevelFour, LevelFive: u.LevelFive});
                }

                if (hierarchyGroup) {
                    connect.updateUserHierarchy({InstanceId: baseParams.InstanceId, UserId: user.Id, HierarchyGroupId: hierarchyGroup.Id}, function(err, data) {
                        if (err) {
                            console.error(err, err.stack);
                        }

                        sleep.msleep(defaultDelay);
                    });
                }
                else {
                    console.error('ERROR: Hierarchy group with structure [' + u.LevelOne + '\/' + u.LevelTwo + '\/' + u.LevelThree + '] was not found.');
                }
            }
            else {
                console.error('ERROR: User with email [' + u.Email + '] was not found.');
            }
        });
    });

    console.log('\tProcessing user list complete.');

    console.log('Processing complete at [' + moment().format() + '].\r\n\r\n');
};


main();
