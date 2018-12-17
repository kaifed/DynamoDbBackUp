'use strict';

const AWS = require('aws-sdk');

class DbInstanceData {
    constructor(config, dbRecord, limit) {
        this.DbTable = config.DbTable;
        this.DbRegion = config.DbRegion;
        this.dbRecord = dbRecord;
        this.limit = limit;
    }

    retrieve() {
        return this.getTableKeys()
            .then(keys => {
                return this.getRecords(keys)
                    .catch(err => {
                        throw err;
                    });
            })
            .catch(err => {
                throw err;
            });
    }

    getItem(Key) {
        let dynamodb = new AWS.DynamoDB({ region: this.DbRegion });
        let params = {
            Key,
            TableName: this.DbTable,
            ConsistentRead: true
        };
        return dynamodb.getItem(params).promise()
            .then(data => {
                if (data && data.Item) {
                    return data.Item;
                }
                return {}
            });
    }

    getTableKeys() {
        return new Promise((resolve, reject) => {
            let dynamoDb = new AWS.DynamoDB({ region: this.DbRegion });
            dynamoDb.describeTable({ TableName: this.DbTable }, (err, data) => {
                if (err) {
                    return reject(err);
                }

                var key = data.Table.KeySchema;
                var units = data.Table.ProvisionedThroughput.ReadCapacityUnits;

                console.log('Got key schema ' + JSON.stringify(key));
                console.log('Got read capacity units ' + units);

                var keys = [ key, units ];
                return resolve(keys);
            });
        });
    }

    getRecords(keys) {
        return new Promise((resolve, reject) => {

            let dynamodb = new AWS.DynamoDB({ region: this.DbRegion });

            var readUnits = keys[1];
            var wantedUnits = Math.ceil(this.limit * readUnits);

            let params = {
                TableName: this.DbTable,
                ExclusiveStartKey: null,
                Limit: 1, //if you know your item size this can be set higher for the initial value to make the scaling faster 
                ReturnConsumedCapacity: "TOTAL",
                Select: 'ALL_ATTRIBUTES'
            };

            var numberOfRecords = 0;

            function recursiveCall(params) {
                return new Promise((rs, rj) => {

                    dynamodb.scan(params, (err, data) => {
                        if (err) {
                            return rj(err);
                        }
                        
                        let records = [];
                        data.Items.forEach((item) => {
                            let id = {};
                            keys[0].forEach(key => {
                                id[key.AttributeName] = item[key.AttributeName];
                            });

                            let record = {
                                keys: JSON.stringify(id),
                                data: JSON.stringify(item),
                                event: 'INSERT'
                            };
                            records.push(record);
                        });

                        let promises = [];
                        records.forEach(record => {
                            promises.push(this.dbRecord.backup([record]));
                        });

                        Promise.all(promises)
                            .then(() => {
                                numberOfRecords += data.Items.length;
                                console.log('Retrieved ' + data.Items.length + ' records; total at ' + numberOfRecords + ' records.');
                                
                                var used = data.ConsumedCapacity.CapacityUnits;
                                console.log("Capacity units used: " + used);
                                
                                if (data.LastEvaluatedKey) {
                                    params.ExclusiveStartKey = data.LastEvaluatedKey;

                                    var ratio = wantedUnits / used;
                                    params.Limit = Math.ceil(params.Limit * ratio);

                                    //Math note: currently, individual calls may compound and go over the allotted units. The time to write
                                    //to s3 prevents these calls from compounding and using more total than what one call uses.

                                    return recursiveCall.call(this, params).then(() => rs());
                                } 
                                return rs();
                            })
                            .catch(err => {
                                rj(err);
                            });
                    });
                });
            }

            recursiveCall.call(this, params)
            .then(data => { 
                resolve() 
            }).catch(err =>{
                reject(err);
            });
        });
    }

}

module.exports = DbInstanceData;
