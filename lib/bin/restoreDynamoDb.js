'use strict';

const AWS = require('aws-sdk');

class RestoreDynamoDb {
    constructor(config) {
        this.S3Bucket = config.S3Bucket;
        this.S3Region = config.S3Region;
        this.DbTable = config.DbTable;
        this.DbRegion = config.DbRegion;
    }

    s3ToDynamoDb(versionList) {
        console.log("s3ToDynamoDb call");
        return this.getDbTableKeys()
            .then(keys => {
                let promises = [];
                console.log("begin processVersion calls");
                Object.keys(versionList).forEach((key, index) => {
                    promises.push(this.processVersion(versionList[key], keys));
                });
                console.log("end processVersion calls");
                return Promise.all(promises)
                    .then(promises => {
                        console.log("call pushToDynamoDb");
                        return this.pushToDynamoDb(promises, keys);
                    })
            })
            .catch(err => {
                throw err;
            });
    }

    getDbTableKeys() {
        return new Promise((resolve, reject) => {
            let dynamoDb = new AWS.DynamoDB({ region: this.DbRegion });
            dynamoDb.describeTable({ TableName: this.DbTable }, (err, data) => {
                if (err) {
                    return reject(err);
                }
                return resolve(data.Table.KeySchema);
            });
        });
    }

    processVersion(version, keys) {
        return this.retrieveFromS3(version)
            .catch(err => {
                throw err;
            });
    }

    retrieveFromS3(version) {
        let params = { Bucket: this.S3Bucket, Key: version.Key, VersionId: version.VersionId };
        let s3 = new AWS.S3({ region: this.S3Region, signatureVersion: "v4" });
        return new Promise((resolve, reject) => {
            s3.getObject(params, (err, data) => {
                if (err) {
                    return reject('Failed to retrieve file from S3 - Params: ' + JSON.stringify(params));
                }

                return resolve([version, JSON.parse(data.Body.toString('utf-8'))]);
            });
        });
    }

    pushToDynamoDb(data, keys) {
        let sizeOfData = data.length;
        return new Promise((resolve, reject) => {
            let dynamoDb = new AWS.DynamoDB({ region: this.DbRegion });

            function recursiveCall(start, numberToProcess) {
                return new Promise((rs, rj) => {
                    //put together the parameters for the write
                    let dParams = { RequestItems: {}, ReturnConsumedCapacity: "TOTAL" };
                    dParams.RequestItems[this.DbTable] = [];
                    for (let i = start; i < start+numberToProcess; i++){
                        let action = {};
                        let version = data[i][0];
                        let fileContents = data[i][1];

                        if (!version.DeletedMarker) {
                            Object.keys(fileContents).forEach(attributeName => {
                                // Fix JSON.stringified Binary data
                                let attr = fileContents[attributeName];
                                if (attr.B && attr.B.type && (attr.B.type === 'Buffer') && attr.B.data) {
                                    attr.B = Buffer.from(attr.B.data);
                                }
                            });
                            action.PutRequest = {
                                Item: fileContents
                            };
                        } else {
                            action.DeleteRequest = {
                                Key: {}
                            };
                            keys.forEach(key => {
                                action.DeleteRequest.Key[key.AttributeName] = fileContents[key.AttributeName];
                            });
                        }

                        dParams.RequestItems[this.DbTable].push(action);
                    }

                    dynamoDb.batchWriteItem(dParams, (err, data) => {
                        if (err) {
                            console.log(err);
                            return rj(err);
                        }

                        if (start+numberToProcess == sizeOfData) {
                            return rs();
                        }

                        if (data.UnprocessedItems.length > 0){
                            console.log("Unprocessed items is greater than 0. Length: " + data.UnprocessedItems.length);
                            return rj(data.UnprocessedItems);
                        }

                        let count = 25;

                        if(start+numberToProcess+25 > sizeOfData){
                            count = sizeOfData - (start+numberToProcess);
                        }

                        let value = start+numberToProcess;

                        console.log("Processed " + numberToProcess + " of " + sizeOfData + " items.");
                        console.log("Going to process items from " + value + " to " + count);
                        return recursiveCall.call(this, start+numberToProcess, count);
                    });

                });
            }

            recursiveCall.call(this, 0, 25)
            .then(data => {
                resolve()
            }).catch(err => {
                console.log(err);
                reject(err);
            });
        });
    }
}

module.exports = RestoreDynamoDb;
