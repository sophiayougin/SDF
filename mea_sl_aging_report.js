/**
 * Script to process dataset and display the inventory aging report
 * subsidiarySelected filters for the subsidiary on transactions 
 * locationSelected filters for the location on transactions 
 * itemName filters for specific items
 * asOf filters for the date on or before the set one on transactions 
 * @NApiVersion 2.1
 * @NScriptType Suitelet
 */

define(['N/ui/serverWidget', 'N/ui/message', 'N/runtime', 'N/query', 'N/file', 'N/search', 'N/task'], (serverWidget, message, runtime, query, file, search, task) => {
    let isGet = true;
    let form;
    let asOf;
    let itemName;
    let asOfDateObj;
    let subsidiarySelected;
    let locationSelected;

    const onRequest = (context) => {
        if (context.request.method === 'GET') {
            try {
                const startTime = Date.now();
                try {
                    let params = context.request.parameters;
                    asOf = params.hasOwnProperty("custparam_asof") ? params.custparam_asof.split('-') : null;
                    itemName = params.hasOwnProperty("custparam_item") ? params.custparam_item : null;
                    locationSelected = params.hasOwnProperty("custparam_location") ? params.custparam_location.split(',') : null;
                    subsidiarySelected = params.hasOwnProperty("custparam_subsidiary") ? params.custparam_subsidiary.split(',') : null;
                    log.debug({ title: 'asOf', details: asOf });
                    log.debug({ title: 'locationSelected', details: locationSelected });
                    log.debug({ title: 'subsidiarySelected', details: subsidiarySelected });
                } catch (err) {
                    log.error({ title: 'error in params', details: JSON.stringify(err) });
                }

                // Main UI Container
                form = serverWidget.createForm({
                    title: 'Inventory Aging Report'
                });

                // Load client script
                form.clientScriptFileId = 8933526;

                form.addField({
                    id: 'custpage_subsidiary',
                    type: serverWidget.FieldType.SELECT,
                    label: 'Subsidiary',
                    source: 'subsidiary'
                }).isMandatory = true;
                form.addField({
                    id: 'custpage_location',
                    type: serverWidget.FieldType.SELECT,
                    label: 'Location',
                    source: 'location'
                });
                form.addField({
                    id: 'custpage_item',
                    type: serverWidget.FieldType.TEXT,
                    label: 'Item'
                });
                form.addField({
                    id: 'custpage_asof',
                    type: serverWidget.FieldType.DATE,
                    label: 'As of'
                }).isMandatory = true;
                if (itemName != null) {
                    form.updateDefaultValues({ custpage_item: itemName });
                }
                if (subsidiarySelected != null) {
                    form.updateDefaultValues({ custpage_subsidiary: subsidiarySelected });
                }
                if (locationSelected != null) {
                    form.updateDefaultValues({ custpage_location: locationSelected });
                }
                if (asOf != null) {
                    asOfDateObj = new Date(asOf[2], asOf[1], asOf[0]);
                    log.debug({ title: 'asOfDateObj', details: JSON.stringify(asOfDateObj) });
                    form.updateDefaultValues({ custpage_asof: asOfDateObj });
                }
                form.addButton({
                    id: 'custpage_update',
                    label: 'Update',
                    functionName: 'redirectUrlv2'
                });
                form.addSubmitButton({
                    label: 'Export'
                });

                itemSublist(form);

                //Log execution time
                log.audit({ title: 'remaining usage', details: runtime.getCurrentScript().getRemainingUsage() });
                const endTime = Date.now();
                const executionTime = endTime - startTime; // Time in milliseconds
                log.audit({ title: 'executionTime', details: JSON.stringify(executionTime) });
                context.response.writePage(form);


            } catch (errorOnGETRequest) {
                log.error({
                    title: 'Error in onGETRequest_ini() function',
                    details: errorOnGETRequest
                });
            }
        }
        if (context.request.method === 'POST') {
            try {
                const startTime = Date.now();
                let params = context.request.parameters;
                log.debug({ title: 'params', details: JSON.stringify(params) });
                var delimiter = /\u0005/;
                isGet = false;

                subsidiarySelected = params.custpage_subsidiary ? params.custpage_subsidiary.split(delimiter) : null;
                locationSelected = params.custpage_location ? params.custpage_location.split(delimiter) : null;
                itemName = checkEmpty(params.custpage_item);
                asOf = checkEmpty(params.custpage_asof).split('/'); asOf[1] = parseInt(asOf[1], 10) - 1
                log.debug({ title: 'asOf', details: asOf });
                log.debug({ title: 'locationSelected', details: locationSelected });
                log.debug({ title: 'subsidiarySelected', details: subsidiarySelected });
                log.debug({ title: 'subsidiarySelected', details: subsidiarySelected });

                if (asOf) {
                    asOfDateObj = new Date(asOf[2], asOf[1], asOf[0]);
                    log.debug({ title: 'asOfDateObj', details: JSON.stringify(asOfDateObj) });
                    let itemDataset = runTranQuery();
                    let dataFile = getCSV(itemDataset, asOf, subsidiarySelected);
                    log.audit({ title: 'remaining usage', details: runtime.getCurrentScript().getRemainingUsage() });
                    const endTime = Date.now();
                    const executionTime = endTime - startTime; // Time in milliseconds
                    log.audit({ title: 'executionTime', details: JSON.stringify(executionTime) });
                    context.response.writeFile({ file: dataFile, isInline: true });
                }


            } catch (errorOnPOSTRequest) {
                log.error({
                    title: 'Error in onPOSTRequest_ini() function',
                    details: errorOnPOSTRequest
                });
            }
        }
    };


    const itemSublist = (form) => {
        try {
            let table = form.addSublist({
                id: 'sublist_item',
                type: serverWidget.SublistType.LIST,
                label: 'ITEMS '
            });

            table.addField({
                id: 'sublist_itemname',
                type: serverWidget.FieldType.TEXT,
                label: 'Item',
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_itemloc',
                type: serverWidget.FieldType.TEXT,
                label: 'Location'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_itemunit',
                type: serverWidget.FieldType.TEXT,
                label: 'Unit'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_qtyonhand0',
                type: serverWidget.FieldType.FLOAT,
                label: 'Qty (0-30 days)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_inventoryvalue0',
                type: serverWidget.FieldType.FLOAT,
                label: 'Value (0-30 days)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_qtyonhand30',
                type: serverWidget.FieldType.FLOAT,
                label: 'Qty (31-60 days)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_inventoryvalue30',
                type: serverWidget.FieldType.FLOAT,
                label: 'Value (31-60 days)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_qtyonhand60',
                type: serverWidget.FieldType.FLOAT,
                label: 'Qty (61-90 days)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_inventoryvalue60',
                type: serverWidget.FieldType.FLOAT,
                label: 'Value (61-90 days)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            /* table.addField({
                 id: 'sublist_qtyonhand90',
                 type: serverWidget.FieldType.FLOAT,
                 label: 'Qty (above 90 days)'
             }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
             table.addField({
                 id: 'sublist_inventoryvalue90',
                 type: serverWidget.FieldType.FLOAT,
                 label: 'Value (above 90 days)'
             }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });*/
            table.addField({
                id: 'sublist_qtyonhand180',
                type: serverWidget.FieldType.FLOAT,
                label: 'Qty (90-180 days)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_inventoryvalue180',
                type: serverWidget.FieldType.FLOAT,
                label: 'Value (90-180 days)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_qtyonhand360',
                type: serverWidget.FieldType.FLOAT,
                label: 'Qty (180-360 days)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_inventoryvalue360',
                type: serverWidget.FieldType.FLOAT,
                label: 'Value (180-360 days)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_qtyonhandover360',
                type: serverWidget.FieldType.FLOAT,
                label: 'Qty (Over 360 days)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });

            table.addField({
                id: 'sublist_inventoryvalueover360',
                type: serverWidget.FieldType.FLOAT,
                label: 'Value (Over 360 days)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });

            table.addField({
                id: 'sublist_qtyonhand',
                type: serverWidget.FieldType.FLOAT,
                label: 'Qty (Total)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });
            table.addField({
                id: 'sublist_inventoryvalue',
                type: serverWidget.FieldType.FLOAT,
                label: 'Value (Total)'
            }).updateDisplayType({ displayType: serverWidget.FieldDisplayType.INLINE });


            if (asOf) {
                let itemDataset = runTranQuery();

                populateSublist(itemDataset, table);
            }

        } catch (erroritemSublist) {
            log.error({ title: 'Error when creting sublist', details: erroritemSublist });
        }
    };


    const getCSV = (itemDataset, asOf, subsidiarySelected) => {
        try {
            let csvFile;
            let l; let m = 0;
            itemDataset = JSON.parse(itemDataset);
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
            csvFile = file.create({ name: subName + '-InventoryAging-' + asOfString + '.csv', fileType: 'CSV', contents: xmlString });
            return csvFile;

        } catch (error) {
            log.error("error in getXLS()", error);
        }
    };



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

            if (totalrows > 40000 && isGet && totalrows <= 60000) {
                form.addPageInitMessage({
                    title: 'High Transaction Volume',
                    type: message.Type.INFORMATION
                });
                //info.show();
            }else if (totalrows > 60000 && isGet) {
                const mrTask = task.create({
                    taskType: task.TaskType.MAP_REDUCE,
                    scriptId: 'customscript_mea_mr_inventory_ageing', 
                    params: {
                        custscript_mea_asof_date: asOfDate,
                        custscript_mea_subsidiary: subsidiarySelected,
                        custscript_mea_location: locationSelected,
                        custscript_mea_item: itemName
                    }
                });
                const taskId = mrTask.submit();
                log.audit('MR Triggered', taskId);
                form.addPageInitMessage({
                    title: 'Processing in Background',
                    message: 'Large dataset detected. Report will be emailed.',
                    type: message.Type.INFORMATION
                });
                return null;
            }

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
            //log.debug('dataset', dataset);
            return JSON.stringify(dataset);
        } catch (errorOnRunTranQuery) {
            log.error({ title: 'Error in errorOnRunTranQuery() function', details: errorOnRunTranQuery });
        }
    };


    const populateSublist = (itemDataset, table) => {
        try {
            itemDataset = JSON.parse(itemDataset);
            log.debug({ title: "itemDataset length", details: itemDataset.length });

            let bucket0Qty = 0;
            let bucket30Qty = 0;
            let bucket60Qty = 0;
            let bucket90Qty = 0;
            let bucketTotalQty = 0;
            let bucket0Value = 0;
            let bucket30Value = 0;
            let bucket60Value = 0;
            let bucket90Value = 0;
            let bucketTotalValue = 0;
            let bucket180Qty = 0;
            let bucket360Qty = 0;
            let bucket180Value = 0;
            let bucket360Value = 0;
            let bucketOver360Qty = 0;
            let bucketOver360Value = 0;



            if (itemDataset.length > 0) {
                let m = 0;
                for (let l in itemDataset) {
                    if (parseFloat(itemDataset[l]["totalQty"]) === 0) { continue; }
                    let unitCost = parseFloat(itemDataset[l]["totalVal"]) / parseFloat(itemDataset[l]["totalQty"]);

                    table.setSublistValue({
                        id: 'sublist_itemname',
                        line: m,
                        value: itemDataset[l]["itemname"]
                    });
                    table.setSublistValue({
                        id: 'sublist_itemloc',
                        line: m,
                        value: itemDataset[l]["location"]
                    });
                    if (checkEmpty(itemDataset[l]["primaryunits"]))
                        table.setSublistValue({
                            id: 'sublist_itemunit',
                            line: m,
                            value: itemDataset[l]["primaryunits"]
                        });
                    table.setSublistValue({
                        id: 'sublist_qtyonhand0',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucket0Qty"]).toFixed(2)
                    });
                    table.setSublistValue({
                        id: 'sublist_inventoryvalue0',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucket0Qty"] * unitCost).toFixed(2)
                    });
                    bucket0Qty += parseFloat(itemDataset[l]["bucket0Qty"]);
                    bucket0Value += parseFloat(itemDataset[l]["bucket0Qty"] * unitCost);
                    table.setSublistValue({
                        id: 'sublist_qtyonhand30',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucket30Qty"]).toFixed(2)
                    });
                    table.setSublistValue({
                        id: 'sublist_inventoryvalue30',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucket30Qty"] * unitCost).toFixed(2)
                    });
                    bucket30Qty += parseFloat(itemDataset[l]["bucket30Qty"]);
                    bucket30Value += parseFloat(itemDataset[l]["bucket30Qty"] * unitCost);
                    table.setSublistValue({
                        id: 'sublist_qtyonhand60',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucket60Qty"]).toFixed(2)
                    });
                    table.setSublistValue({
                        id: 'sublist_inventoryvalue60',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucket60Qty"] * unitCost).toFixed(2)
                    });
                    bucket60Qty += parseFloat(itemDataset[l]["bucket60Qty"]);
                    bucket60Value += parseFloat(itemDataset[l]["bucket60Qty"] * unitCost);
                    /*table.setSublistValue({
                        id: 'sublist_qtyonhand90',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucket90Qty"]).toFixed(2)
                    });
                    table.setSublistValue({
                        id: 'sublist_inventoryvalue90',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucket90Qty"] * unitCost).toFixed(2)
                    });
                    bucket90Qty += parseFloat(itemDataset[l]["bucket90Qty"]);
                    bucket90Value += parseFloat(itemDataset[l]["bucket90Qty"] * unitCost);*/
                    table.setSublistValue({
                        id: 'sublist_qtyonhand180',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucket180Qty"]).toFixed(2)
                    });
                    table.setSublistValue({
                        id: 'sublist_inventoryvalue180',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucket180Qty"] * unitCost).toFixed(2)
                    });
                    table.setSublistValue({
                        id: 'sublist_qtyonhand360',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucket360Qty"]).toFixed(2)
                    });
                    table.setSublistValue({
                        id: 'sublist_inventoryvalue360',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucket360Qty"] * unitCost).toFixed(2)
                    });
                    bucket180Qty += parseFloat(itemDataset[l]["bucket180Qty"]);
                    bucket180Value += parseFloat(itemDataset[l]["bucket180Qty"] * unitCost);
                    bucket360Qty += parseFloat(itemDataset[l]["bucket360Qty"]);
                    bucket360Value += parseFloat(itemDataset[l]["bucket360Qty"] * unitCost);
                    table.setSublistValue({
                        id: 'sublist_qtyonhandover360',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucketOver360Qty"]).toFixed(2)
                    });
                    table.setSublistValue({
                        id: 'sublist_inventoryvalueover360',
                        line: m,
                        value: parseFloat(itemDataset[l]["bucketOver360Qty"] * unitCost).toFixed(2)
                    });
                    bucketOver360Qty += parseFloat(itemDataset[l]["bucketOver360Qty"]);
                    bucketOver360Value += parseFloat(itemDataset[l]["bucketOver360Qty"] * unitCost);

                    table.setSublistValue({
                        id: 'sublist_qtyonhand',
                        line: m,
                        value: parseFloat(itemDataset[l]["totalQty"]).toFixed(2)
                    });
                    table.setSublistValue({
                        id: 'sublist_inventoryvalue',
                        line: m,
                        value: parseFloat(itemDataset[l]["totalVal"]).toFixed(2)
                    });
                    bucketTotalQty += parseFloat(itemDataset[l]["totalQty"]);
                    bucketTotalValue += parseFloat(itemDataset[l]["totalVal"]);
                    m++;
                    //if (m == 100) { break; }
                }
                log.debug({ title: 'Lines printed', details: m });

                /*log.debug({ title: '0-30 QTY', details: bucket0Qty });
                log.debug({ title: '0-30 Val', details: bucket0Value });
                log.debug({ title: '30-60 QTY', details: bucket30Qty });
                log.debug({ title: '30-60 Val', details: bucket30Value });
                log.debug({ title: '60-90 QTY', details: bucket60Qty });
                log.debug({ title: '60-90 Val', details: bucket60Value });
                log.debug({ title: '90> QTY', details: bucket90Qty });
                log.debug({ title: '90> Val', details: bucket90Value });
                log.debug({ title: 'Total QTY', details: bucketTotalQty });
                log.debug({ title: 'Total Val', details: bucketTotalValue });*/

                //print last line
                table.setSublistValue({
                    id: 'sublist_itemname',
                    line: m,
                    value: "Total"
                });
                table.setSublistValue({
                    id: 'sublist_qtyonhand0',
                    line: m,
                    value: bucket0Qty.toFixed(2)
                });
                table.setSublistValue({
                    id: 'sublist_inventoryvalue0',
                    line: m,
                    value: bucket0Value.toFixed(2)
                });
                table.setSublistValue({
                    id: 'sublist_qtyonhand30',
                    line: m,
                    value: bucket30Qty.toFixed(2)
                });
                table.setSublistValue({
                    id: 'sublist_inventoryvalue30',
                    line: m,
                    value: bucket30Value.toFixed(2)
                });
                table.setSublistValue({
                    id: 'sublist_qtyonhand60',
                    line: m,
                    value: bucket60Qty.toFixed(2)
                });
                table.setSublistValue({
                    id: 'sublist_inventoryvalue60',
                    line: m,
                    value: bucket60Value.toFixed(2)
                });
                /* table.setSublistValue({
                     id: 'sublist_qtyonhand90',
                     line: m,
                     value: bucket90Qty.toFixed(2)
                 });
                 table.setSublistValue({
                     id: 'sublist_inventoryvalue90',
                     line: m,
                     value: bucket90Value.toFixed(2)
                 });*/

                table.setSublistValue({
                    id: 'sublist_qtyonhand180',
                    line: m,
                    value: bucket180Qty.toFixed(2)
                });
                table.setSublistValue({
                    id: 'sublist_inventoryvalue180',
                    line: m,
                    value: bucket180Value.toFixed(2)
                });
                table.setSublistValue({
                    id: 'sublist_qtyonhand360',
                    line: m,
                    value: bucket360Qty.toFixed(2)
                });
                table.setSublistValue({
                    id: 'sublist_inventoryvalue360',
                    line: m,
                    value: bucket360Value.toFixed(2)
                });
                table.setSublistValue({
                    id: 'sublist_qtyonhandover360',
                    line: m,
                    value: bucketOver360Qty.toFixed(2)
                });
                table.setSublistValue({
                    id: 'sublist_inventoryvalueover360',
                    line: m,
                    value: bucketOver360Value.toFixed(2)
                });

                table.setSublistValue({
                    id: 'sublist_qtyonhand',
                    line: m,
                    value: bucketTotalQty.toFixed(2)
                });
                table.setSublistValue({
                    id: 'sublist_inventoryvalue',
                    line: m,
                    value: bucketTotalValue.toFixed(2)
                });
            }
        } catch (errorOnPopulateSublist) {
            log.error({ title: 'Error in populateSublist() function', details: errorOnPopulateSublist });
        }
    };


    const checkEmpty = (data) => {
        if (data == undefined || data == null || data == "" || data == " ")
            return null;
        else return data;
    };


    return { onRequest };
});