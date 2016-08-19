// Copyright (c) Microsoft Corporation.  All rights reserved.

// Register DocDB JavaScript server API for intelisense: 
//   either add the file to Tools->Options->Text Editor->JavaScript->Intellisense->References and reference the group registered 
//   or provide path to the file explicitly.
/// <reference group="Generic" />
/// <reference path="C:\Program Files (x86)\Microsoft Visual Studio 12.0\JavaScript\References\CalcAverages.js" />

/**
* This is run as stored procedure and does the following:
*    fetch records before 'untilDate' for the given duration 'interval'
*    calc the avg value of all fetched records
*    create a new documentDb-record with the avg-value
* 
*
* @param {Object} measurandRef objects containing the properties 'plant' , 'equipment', 'name'
* @param {integer} interval in minutes (all records that exists this number of minutes before the untilDate are fetched and AVG is calced)
*/
function CalcAverages(measurandRef, untilDate, interval, valueField) {
    var collectionLink = __.getSelfLink();

    //throw JSON.stringify(measurandRef) + JSON.stringify({untilDate:untilDate, interval:interval, valueField:valueField}); //returned passed parameters as exception text

    var untilDateISO = new Date(untilDate).toISOString();

    fromDate = new Date(new Date(untilDate).getTime() - (interval * 60000)).toISOString();

    var sum = 0;
    var count = 0;
    var avgValue = undefined;

    tryQuery({});

    function tryQuery(options) {
        var sqlQuery = "SELECT c." + valueField + " FROM c WHERE c.p = '" + measurandRef.plant + "' and c.e = '" + measurandRef.equipment + "' and c.Name = '" + measurandRef.name + "' and c.cts >= '" + fromDate + "' and c.cts < '" + untilDateISO + "'";

        //throw new Error("Query: " + sqlQuery);

        //query documents storing the measurand values for the given range
        var isAccepted = __.queryDocuments(
            collectionLink,
            sqlQuery,
            options,
            callback);

        if (!isAccepted) throw new Error("Source dataset is too large to complete the operation.");
    }

   

    /**
    * queryDocuments callback.
    * @param {Error} err - Error object in case of error/exception.
    * @param {Array} queryFeed - array containing results of the query.
    * @param {ResponseOptions} responseOptions.
    */
    function callback(err, queryFeed, responseOptions) {
        if (err) {
            throw err;
        }

       

        // Iterate over document feed and calculate count and sum so that we can eval the average
        queryFeed.forEach(function (element, index, array) {
            count++;
            sum += element[valueField];
        });

        
        if (responseOptions.continuation) {
            // If there is continuation, call query again providing continuation token.
            tryQuery({ continuation: responseOptions.continuation });
        } else {
            if (count > 0) {
                //store average document only when records exists (is this a good idea)

                avgValue = sum / count;

                var avgDoc = {
                    p: measurandRef.plant,
                    e: measurandRef.equipment,
                    name: measurandRef.name,
                };

                avgDoc["cts" + interval] = untilDate;
                avgDoc[valueField] = avgValue;

                //create a new average document that stores the average value for the given time period
                var isInserted = __.upsertDocument(collectionLink, avgDoc);

                if (!isInserted) {
                    throw new Error('The average document could not be inserted.');
                }
            }
            //everything created
            getContext().getResponse().setBody({ result: 201, count: count });
            
        }
    }
}