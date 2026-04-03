/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */

define(['N/file', 'N/query', 'N/email', 'N/runtime', 'N/search'], (file, query, email, runtime, search) => {

    let asOf;
    let asOfDateObj;
    let itemName = null;
    let subsidiarySelected = ['1'];
    let locationSelected = null;


    /**
     * To run the transaction query and process the results into ageing buckets
     * @returns 
     */

    const runTranQuery = () => {
        try {
            const asOfDate = `${asOf[0]}/${parseInt(asOf[1], 10) + 1}/${asOf[2]}`;
            let dataset = [];
            let columnName = [];

            /* Query */
            const transactionQueryZZYP = query.create({ type: query.Type.TRANSACTION });

            /* Joins */
            const transactionlinesJoinNVZX = transactionQueryZZYP.autoJoin({ fieldId: 'transactionlines' });

            const itemJoinDQMS = transactionlinesJoinNVZX.autoJoin({ fieldId: 'item^item' });

            /* Conditions */
            const transactionQueryZZYPConditionULIP = transactionQueryZZYP.createCondition({
                operator: query.Operator.EQUAL_NOT,
                values: [0],
                formula: `CASE 
                WHEN TO_CHAR({transactionlines.isinventoryaffecting}) = 'T'
                THEN NVL({transactionlines.quantity}, 0)
                ELSE 0
                END`,
                type: query.ReturnType.FLOAT,
            });

            const transactionQueryZZYPConditionDMJE = transactionQueryZZYP.createCondition({
                operator: query.Operator.EQUAL_NOT,
                values: [0],
                formula: `CASE WHEN TO_CHAR({transactionlines.accountingimpact.account.inventory})='T'
                AND TO_CHAR({transactionlines.accountingimpact.posting})='T'
                THEN NVL(TO_NUMBER({transactionlines.accountingimpact.amount}),0)
                ELSE 0
                END`,
                type: query.ReturnType.FLOAT,
            });

            const transactionQueryZZYPConditionLSMZ = transactionQueryZZYP.createCondition({
                fieldId: 'type',
                operator: query.Operator.ANY_OF_NOT,
                values: ['BinWksht', 'BinTrnfr']
            });

            const transactionQueryZZYPConditionZFAX = itemName !== null ?
                itemJoinDQMS.createCondition({
                    fieldId: 'itemid',
                    operator: query.Operator.CONTAIN,
                    values: itemName
                }) :
                itemJoinDQMS.createCondition({
                    fieldId: 'itemid',
                    operator: query.Operator.EMPTY_NOT,
                    values: []
                });

            const transactionQueryZZYPConditionAYHB = subsidiarySelected !== null ?
                transactionlinesJoinNVZX.createCondition({
                    fieldId: 'subsidiary',
                    operator: query.Operator.ANY_OF,
                    values: subsidiarySelected
                }) :
                transactionlinesJoinNVZX.createCondition({
                    fieldId: 'subsidiary',
                    operator: query.Operator.ANY_OF_NOT,
                    values: [null]
                });

            const transactionQueryZZYPConditionFJPI = locationSelected !== null ?
                transactionlinesJoinNVZX.createCondition({
                    fieldId: 'location',
                    operator: query.Operator.ANY_OF,
                    values: locationSelected
                }) :
                transactionlinesJoinNVZX.createCondition({
                    fieldId: 'location',
                    operator: query.Operator.ANY_OF_NOT,
                    values: [null]
                });

            const transactionQueryZZYPConditionRJQL = transactionQueryZZYP.createCondition({
                fieldId: 'trandate',
                operator: query.Operator.ON_OR_BEFORE,
                values: [asOfDate]
            });

            transactionQueryZZYP.condition = transactionQueryZZYP.and(
                transactionQueryZZYPConditionRJQL,
                transactionQueryZZYPConditionFJPI,
                transactionQueryZZYPConditionAYHB,
                transactionQueryZZYPConditionZFAX,
                transactionQueryZZYPConditionLSMZ,
                transactionQueryZZYP.or(
                    transactionQueryZZYPConditionDMJE,
                    transactionQueryZZYPConditionULIP
                )
            );

            /* Columns */
            const transactionQueryZZYPColumnXQNM = transactionQueryZZYP.createColumn({
                formula: `{transactionlines.item#display}`,
                type: query.ReturnType.STRING,
                groupBy: false,
                label: 'Item Name',
                alias: 'XQNM',
            });

            const transactionQueryZZYPColumnEKXR = transactionQueryZZYP.createColumn({
                fieldId: 'trandate',
                groupBy: false,
                label: 'Date',
                alias: 'EKXR',
            });

            const transactionQueryZZYPColumnLMWL = transactionQueryZZYP.createColumn({
                formula: `{transactionlines.location#display}`,
                type: query.ReturnType.STRING,
                groupBy: false,
                label: 'Location',
                alias: 'LMWL',
            });

            const transactionQueryZZYPColumnSDKQ = transactionQueryZZYP.createColumn({
                formula: `CASE 
                WHEN TO_CHAR({transactionlines.isinventoryaffecting}) = 'T'
                THEN NVL({transactionlines.quantity}, 0)
                ELSE 0
                END`,
                type: query.ReturnType.FLOAT,
                groupBy: false,
                label: 'Quantity',
                alias: 'SDKQ',
            });

            const transactionQueryZZYPColumnZOYP = transactionQueryZZYP.createColumn({
                formula: `CASE WHEN TO_CHAR({transactionlines.accountingimpact.account.inventory})='T'
                AND TO_CHAR({transactionlines.accountingimpact.posting})='T'
                THEN NVL(TO_NUMBER({transactionlines.accountingimpact.amount}),0)
                ELSE 0
                END`,
                type: query.ReturnType.FLOAT,
                groupBy: false,
                label: 'Amount',
                alias: 'ZOYP',
            });
            const transactionQueryXCNCColumnEPVN = transactionQueryZZYP.createColumn({
                formula: `{transactionlines.item^item.unitstype#display}`,
                type: query.ReturnType.STRING,
                groupBy: false,
                label: 'Primary Units',
                alias: 'EPVN'
            });

            transactionQueryZZYP.columns = [
                transactionQueryZZYPColumnXQNM,
                transactionQueryZZYPColumnLMWL,
                transactionQueryZZYPColumnEKXR,
                transactionQueryZZYPColumnSDKQ,
                transactionQueryZZYPColumnZOYP,
                transactionQueryXCNCColumnEPVN
            ];

            const itemSort = transactionQueryZZYP.createSort({ column: transactionQueryZZYPColumnXQNM, ascending: true });
            const locationSort = transactionQueryZZYP.createSort({ column: transactionQueryZZYPColumnLMWL, ascending: true });
            const dateSort = transactionQueryZZYP.createSort({ column: transactionQueryZZYPColumnEKXR, ascending: true });
            const qtySort = transactionQueryZZYP.createSort({ column: transactionQueryZZYPColumnSDKQ, ascending: false });


            transactionQueryZZYP.sort = [itemSort, locationSort, dateSort, qtySort];

            let pagedData = transactionQueryZZYP.runPaged({ pageSize: 1000 });
            const totalPages = pagedData.pageRanges.length;
            const totalrows = pagedData.count;
            log.debug('totalPages', totalPages);
            log.debug('totalrows', totalrows);

            for (let x in pagedData.pageRanges) {
                let currentPage = pagedData.fetch(x);
                let results = currentPage.data.results;

                for (let j = 0; j < results.length; j++) {
                    if (j === 0) {
                        for (let m in currentPage.data.columns) {
                            columnName.push(currentPage.data.columns[m].label.replace(/\s+/g, '').toLowerCase());
                        }
                        //log.debug('columnName', columnName);
                    }
                    let trandate = results[j].values[2].split('/');
                    let trandateObj = new Date(trandate[2], parseInt(trandate[1]) - 1, trandate[0]);
                    let age = parseInt((asOfDateObj - trandateObj) / (1000 * 60 * 60 * 24));
                    let quantity = parseFloat(results[j].values[3]);
                    let value = parseFloat(results[j].values[4]);
                    //log.audit('Preload || value = ' + value + '|| quantity = ' + quantity);

                    let datasetIndex = dataset.findIndex(entry => entry[columnName[0]] === results[j].values[0] && entry[columnName[1]] === results[j].values[1]);
                    if (datasetIndex !== -1) {

                        if (quantity < 0) {
                            let carryQty = quantity;

                            if (parseFloat(dataset[datasetIndex].bucketOver360Qty) > 0) {
                                if ((parseFloat(dataset[datasetIndex].bucketOver360Qty) + carryQty) < 0) {
                                    carryQty += parseFloat(dataset[datasetIndex].bucketOver360Qty);
                                    dataset[datasetIndex].bucketOver360Qty = 0;
                                } else {
                                    dataset[datasetIndex].bucketOver360Qty = parseFloat(dataset[datasetIndex].bucketOver360Qty) + carryQty;
                                    carryQty = 0;
                                }
                            }

                            if (parseFloat(dataset[datasetIndex].bucket360Qty) > 0 && carryQty < 0) {
                                if ((parseFloat(dataset[datasetIndex].bucket360Qty) + carryQty) < 0) {
                                    carryQty += parseFloat(dataset[datasetIndex].bucket360Qty);
                                    dataset[datasetIndex].bucket360Qty = 0;
                                } else {
                                    dataset[datasetIndex].bucket360Qty = parseFloat(dataset[datasetIndex].bucket360Qty) + carryQty;
                                    carryQty = 0;
                                }
                            }

                            if (parseFloat(dataset[datasetIndex].bucket180Qty) > 0 && carryQty < 0) {
                                if ((parseFloat(dataset[datasetIndex].bucket180Qty) + carryQty) < 0) {
                                    carryQty += parseFloat(dataset[datasetIndex].bucket180Qty);
                                    dataset[datasetIndex].bucket180Qty = 0;
                                } else {
                                    dataset[datasetIndex].bucket180Qty = parseFloat(dataset[datasetIndex].bucket180Qty) + carryQty;
                                    carryQty = 0;
                                }
                            }

                            if (parseFloat(dataset[datasetIndex].bucket60Qty) > 0 && carryQty < 0) {
                                if ((parseFloat(dataset[datasetIndex].bucket60Qty) + carryQty) < 0) {
                                    carryQty += parseFloat(dataset[datasetIndex].bucket60Qty);
                                    dataset[datasetIndex].bucket60Qty = 0;
                                } else {
                                    dataset[datasetIndex].bucket60Qty = parseFloat(dataset[datasetIndex].bucket60Qty) + carryQty;
                                    carryQty = 0;
                                }
                            }

                            if (parseFloat(dataset[datasetIndex].bucket30Qty) > 0 && carryQty < 0) {
                                if ((parseFloat(dataset[datasetIndex].bucket30Qty) + carryQty) < 0) {
                                    carryQty += parseFloat(dataset[datasetIndex].bucket30Qty);
                                    dataset[datasetIndex].bucket30Qty = 0;
                                } else {
                                    dataset[datasetIndex].bucket30Qty = parseFloat(dataset[datasetIndex].bucket30Qty) + carryQty;
                                    carryQty = 0;
                                }
                            }

                            if (carryQty < 0) {
                                dataset[datasetIndex].bucket0Qty = parseFloat(dataset[datasetIndex].bucket0Qty) + carryQty;
                            }
                        }
                        else {
                            //let unitValue;
                            switch (true) {
                                case (age > 360):
                                    dataset[datasetIndex].bucketOver360Qty += quantity;
                                    break;

                                case (age > 180 && age <= 360):
                                    dataset[datasetIndex].bucket360Qty += quantity;
                                    break;

                                case (age > 90 && age <= 180):
                                    dataset[datasetIndex].bucket180Qty += quantity;
                                    break;

                                case (age > 60 && age <= 90):
                                    dataset[datasetIndex].bucket60Qty += quantity;
                                    break;

                                case (age > 30 && age <= 60):
                                    dataset[datasetIndex].bucket30Qty += quantity;
                                    break;

                                case (age <= 30):
                                    dataset[datasetIndex].bucket0Qty += quantity;
                                    break;
                            }


                        }
                        dataset[datasetIndex].totalQty += quantity;
                        dataset[datasetIndex].totalVal += value;

                    } else {
                        dataset.push({
                            [columnName[0]]: results[j].values[0],   // Item
                            [columnName[1]]: results[j].values[1],   // Location
                            [columnName[5]]: results[j].values[5],   // Units
                            "bucket0Qty": (age <= 30) ? quantity : 0,
                            "bucket30Qty": (age > 30 && age <= 60) ? quantity : 0,
                            "bucket60Qty": (age > 60 && age <= 90) ? quantity : 0,
                            "bucket180Qty": (age > 90 && age <= 180) ? quantity : 0,
                            "bucket360Qty": (age > 180 && age <= 360) ? quantity : 0,
                            "bucketOver360Qty": (age > 360) ? quantity : 0,
                            "totalQty": quantity,
                            "totalVal": value
                        });



                    }

                    /*log.audit('value = ' + value + '|| quantity = ' + quantity);
                    log.audit('dataset', dataset);*/
                }
            }
            log.debug('Final Dataset', dataset.length);
            return JSON.stringify(dataset);
        } catch (errorOnRunTranQuery) {
            log.error({ title: 'Error in errorOnRunTranQuery() function', details: errorOnRunTranQuery });
        }
    };
    /**
     * To check if the data is empty or not before processing to avoid any error during map/reduce execution
     * @param {*} data 
     * @returns 
     */
    const checkEmpty = (data) => {
        if (data == undefined || data == null || data == "" || data == " ")
            return null;
        else return data;
    };
    /**
     * Generates a CSV file from the item dataset
     * @param {*} itemDataset 
     * @param {*} asOf 
     * @param {*} subsidiarySelected 
     * @returns 
     */
    const getCSV = (itemDataset, asOf, subsidiarySelected) => {
        try {
            let csvFile;
            let l; let m = 0;
            let xmlString = 'Item Name,Location,Units,Qty (0-30 DAYS),Value (0-30 DAYS),Qty (31-60 DAYS),Value (31-60 DAYS),Qty (61-90 DAYS),Value (61-90 DAYS),Qty (90-180 DAYS),Value (90-180 DAYS),Qty (180-360 DAYS),Value (180-360 DAYS),Qty (Over 360 DAYS),Value (Over 360 DAYS),Qty (TOTAL),Value (TOTAL)\n';


            if (itemDataset.length > 0) {
                //csv file

                for (l in itemDataset) {
                    if (parseFloat(itemDataset[l]["totalQty"]) === 0) { continue; }
                    m++;
                    let unitCost = itemDataset[l]["totalVal"] / itemDataset[l]["totalQty"];

                    xmlString += itemDataset[l]["itemname"].replace(/[,\n\t"]/g, '') + ','
                        + itemDataset[l]["location"].replace(/[,\n\t"]/g, '') + ','
                        + itemDataset[l]["primaryunits"].replace(/[,\n\t"]/g, '') + ','
                        + itemDataset[l]["bucket0Qty"] + ','
                        + parseFloat(itemDataset[l]["bucket0Qty"]) * unitCost + ','
                        + itemDataset[l]["bucket30Qty"] + ','
                        + parseFloat(itemDataset[l]["bucket30Qty"]) * unitCost + ','
                        + itemDataset[l]["bucket60Qty"] + ','
                        + parseFloat(itemDataset[l]["bucket60Qty"]) * unitCost + ','
                        + itemDataset[l]["bucket180Qty"] + ','
                        + parseFloat(itemDataset[l]["bucket180Qty"]) * unitCost + ','
                        + itemDataset[l]["bucket360Qty"] + ','
                        + parseFloat(itemDataset[l]["bucket360Qty"]) * unitCost + ','
                        + itemDataset[l]["bucketOver360Qty"] + ','
                        + parseFloat(itemDataset[l]["bucketOver360Qty"]) * unitCost + ','
                        + itemDataset[l]["totalQty"] + ','
                        + itemDataset[l]["totalVal"] + '\n';

                }
                log.debug({ title: "Total rows: ", details: m });
            }

            let asOfString = `${asOf[0]}-${parseInt(asOf[1], 10) + 1}-${asOf[2]}`;

            let subName = '';
            if (subsidiarySelected.length > 0) {
                subName = search.lookupFields({
                    type: search.Type.SUBSIDIARY,
                    id: subsidiarySelected[0],
                    columns: ['legalname']
                }).legalname;
            }
            log.debug({ title: "subName", details: subName });
            csvFile = file.create({ name: subName + '-InventoryAging-' + asOfString + '.csv', fileType: 'CSV', contents: xmlString, folder: 7245215 });
            return csvFile;

        } catch (error) {
            log.error("error in getXLS()", error);
        }
    };

    /**
     * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
     * @param {Object} inputContext
     * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
     *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
     * @param {Object} inputContext.ObjectRef - Object that references the input data
     * @typedef {Object} ObjectRef
     * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
     * @property {string} ObjectRef.type - Type of the record instance that contains the input data
     * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
     * @since 2015.2
     */
    const getInputData = () => {
        try {
            const script = runtime.getCurrentScript();
            asOfParam = script.getParameter({ name: 'custscript_mea_asof_date' });
            subsidiarySelected = script.getParameter({ name: 'custscript_mea_subsidiary' }) || null;
            locationSelected = script.getParameter({ name: 'custscript_mea_location' }) || null;
            itemName = script.getParameter({ name: 'custscript_mea_item' }) || null;



            subsidiarySelected = (subsidiarySelected && subsidiarySelected !== "null") ? subsidiarySelected : null;
            locationSelected = (locationSelected && locationSelected !== "null") ? locationSelected : null;
            itemName = (itemName && itemName !== "null") ? itemName : null;


            if (subsidiarySelected) {
                try {
                    subsidiarySelected = JSON.parse(subsidiarySelected);
                } catch (e) {
                    subsidiarySelected = [subsidiarySelected];
                }
            }

            if (locationSelected) {
                try {
                    locationSelected = JSON.parse(locationSelected);
                } catch (e) {
                    locationSelected = [locationSelected];
                }
            }

            if (asOfParam) {
                let parts = asOfParam.split('/');
                asOf = [
                    parseInt(parts[0]),
                    parseInt(parts[1]),
                    parseInt(parts[2])
                ];
                asOfDateObj = new Date(parts[2], parts[1] - 1, parts[0]);
            } else {
                let today = new Date();
                asOf = [
                    today.getDate(),
                    today.getMonth() + 1,
                    today.getFullYear()
                ];
                asOfDateObj = new Date(today.getFullYear(), today.getMonth(), today.getDate());
            }

            log.debug('Parsed Params', {
                asOf,
                subsidiarySelected,
                locationSelected,
                itemName
            });
            let result = runTranQuery();
            return JSON.parse(result);
        } catch (error) {
            log.error({ title: 'Error in getInputData()', details: error });
            return [];
        }
    };
    /**
     * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
     * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
     * context.
     * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
     *     is provided automatically based on the results of the getInputData stage.
     * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
     *     function on the current key-value pair
     * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
     *     pair
     * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
     *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
     * @param {string} mapContext.key - Key to be processed during the map stage
     * @param {string} mapContext.value - Value to be processed during the map stage
     * @since 2015.2
     */
    const map = (context) => {
        context.write({
            key: 'data',
            value: context.value
        });
    };
    /**
      * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
      * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
      * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
      *     provided automatically based on the results of the map stage.
      * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
      *     reduce function on the current group
      * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
      * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
      *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
      * @param {string} reduceContext.key - Key to be processed during the reduce stage
      * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
      *     for processing
      * @since 2015.2
      */
    const reduce = (context) => {

        let allData = [];
        context.values.forEach(val => {
            let parsed = JSON.parse(val);
            allData = allData.concat(parsed);
        });
        log.debug("allData", allData)
        log.debug("allData length reduce", allData.length)
        context.write({
            key: 'final',
            value: JSON.stringify(allData)
        });
    };

    /**
     * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
     * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
     * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
     * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
     *     script
     * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
     * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
     *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
     * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
     * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
     * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
     *     script
     * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
     * @param {Object} summaryContext.inputSummary - Statistics about the input stage
     * @param {Object} summaryContext.mapSummary - Statistics about the map stage
     * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
     * @since 2015.2
     */
    const summarize = (summary) => {

        let dataset = [];
        const script = runtime.getCurrentScript();

        let asOfParam = script.getParameter({ name: 'custscript_mea_asof_date' });
        let subsidiarySelected = script.getParameter({ name: 'custscript_mea_subsidiary' });

        subsidiarySelected = (subsidiarySelected && subsidiarySelected !== "null") ? subsidiarySelected : null;

       
        if (subsidiarySelected) {
            try {
                subsidiarySelected = JSON.parse(subsidiarySelected);
            } catch (e) {
                subsidiarySelected = [subsidiarySelected];
            }
        }

        
        let asOf;
        if (asOfParam) {
            let parts = asOfParam.split('/');
            asOf = [
                parseInt(parts[0]),
                parseInt(parts[1]),
                parseInt(parts[2])
            ];
        }

        log.debug('Summarize Params', { asOf, subsidiarySelected });

        summary.output.iterator().each((key, value) => {
            dataset = JSON.parse(value);
            return true;
        });

        if (!dataset || dataset.length === 0) {
            log.debug('No Data', 'Dataset is empty');
            return;
        }
        log.debug('Dataset in Summarize', dataset.length);
        let csvFile = getCSV(dataset, asOf, subsidiarySelected);

        let fileId = csvFile.save();
        log.debug('CSV File Created', fileId);
        email.send({
            author: 3,
            recipients: runtime.getCurrentUser().id,
            subject: 'Inventory Aging Report',
            body: 'Please find attached report.',
            attachments: [csvFile]
        });


    };

    return { getInputData, map, reduce, summarize };
});