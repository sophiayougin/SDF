/**
 * @NApiVersion 2.x
 * @NScriptType ClientScript
 */

define(['N/currentRecord'], function (currentRecord) {

    function redirectUrl() {
        try {
            let rec = currentRecord.get();
            const asOf = rec.getValue({ fieldId: 'custpage_asof' });
            const item = rec.getValue({ fieldId: 'custpage_item' });
            const day = String(asOf.getDate()).padStart(2, '0');
            const month = String(asOf.getMonth()).padStart(2, '0'); // Months are 0-based
            const year = asOf.getFullYear();
            const newAsOf = `${day}-${month}-${year}`;
            const subsidiarySelected = rec.getValue({ fieldId: 'custpage_subsidiary' });
            const locationSelected = rec.getValue({ fieldId: 'custpage_location' });
            /*alert(JSON.stringify(newAsOf));
            alert(JSON.stringify(item));
            alert(subsidiarySelected);
            alert(locationSelected);*/

            var url = '/app/site/hosting/scriptlet.nl?script=4797&deploy=1';
            if (checkEmpty(asOf)) { url += '&custparam_asof=' + newAsOf; } else { alert('Please enter values for : As of'); return; }
            if (checkEmpty(item)) { url += '&custparam_item=' + item; }
            if (checkEmpty(locationSelected)) { url += '&custparam_location=' + locationSelected; }
            if (checkEmpty(subsidiarySelected)) { url += '&custparam_subsidiary=' + subsidiarySelected; } else { alert('Please enter values for : Subsidiary'); return; }

            window.open(url, '_self');
        } catch (error) {
            log.error({ title: 'Error in redirectUrl() function', details: error });
        }
    }
    function redirectUrlv2() {
        try {
            let rec = currentRecord.get();
            const asOf = rec.getValue({ fieldId: 'custpage_asof' });
            const item = rec.getValue({ fieldId: 'custpage_item' });
            const day = String(asOf.getDate()).padStart(2, '0');
            const month = String(asOf.getMonth()).padStart(2, '0'); // Months are 0-based
            const year = asOf.getFullYear();
            const newAsOf = `${day}-${month}-${year}`;
            const subsidiarySelected = rec.getValue({ fieldId: 'custpage_subsidiary' });
            const locationSelected = rec.getValue({ fieldId: 'custpage_location' });
            /*alert(JSON.stringify(newAsOf));
            alert(JSON.stringify(item));
            alert(subsidiarySelected);
            alert(locationSelected);*/

            var url = '/app/site/hosting/scriptlet.nl?script=7590&deploy=1';
            if (checkEmpty(asOf)) { url += '&custparam_asof=' + newAsOf; } else { alert('Please enter values for : As of'); return; }
            if (checkEmpty(item)) { url += '&custparam_item=' + item; }
            if (checkEmpty(locationSelected)) { url += '&custparam_location=' + locationSelected; }
            if (checkEmpty(subsidiarySelected)) { url += '&custparam_subsidiary=' + subsidiarySelected; } else { alert('Please enter values for : Subsidiary'); return; }

            window.open(url, '_self');
        } catch (error) {
            log.error({ title: 'Error in redirectUrl() function', details: error });
        }
    }

    function checkEmpty(data) {
        if (data == undefined || data == null || data == "" || data == " ")
            return null;
        else return data;
    }

    return { redirectUrl: redirectUrl,
           redirectUrlv2:redirectUrlv2}
});