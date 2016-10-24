contextPath = '/pipeline61';
appModel = 'jetty'; //tomcat or jetty
var role = 'customer';

function ajaxSend(url, method, data, key, redirect, func){
    $.ajax({
        url: url,
        data: data,
        dataType: "json",
        cache: false,
        type: method,
        beforeSend: function(request) {
            request.setRequestHeader("key", role);
        },
        error: function () {
            console.log('failed to get AJAX data .');
        },
        success: function (result) {
//            alert(result);
            func(result);
            if(redirect)
                window.location.href = redirect;
        }
    });
}



function serialize($context) {
    return $context.serialize();
}

// function for get a cookie value by using its name
function getCookie(cname) {
    var name = cname + "=";
    var ca = document.cookie.split(';');
    for(var i = 0; i <ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0)==' ') {
            c = c.substring(1);
        }
        if (c.indexOf(name) == 0) {
            return c.substring(name.length,c.length);
        }
    }
    return "";
}

function setCookie(cname, cvalue, exdays) {
    var d = new Date();
    d.setTime(d.getTime() + (exdays*24*60*60*1000));
    var expires = "expires="+ d.toUTCString();
    document.cookie = cname + "=" + cvalue + ";" + expires + ";path=/";
}

function getElements(formId) {
    var form = document.getElementById(formId);
    var elements = new Array();
    var tagElements = form.getElementsByTagName('input');
    alert(tagElements.length)
    for (var j = 0; j < tagElements.length; j++){
        elements.push(tagElements[j]);

    }
    return elements;
}

//get [name,value] array from a element
function inputSelector(element) {
    if (element.checked)
        return [element.name, element.value];
}

function input(element) {
    switch (element.type.toLowerCase()) {
        case 'submit':
        case 'hidden':
        case 'password':
        case 'text':
            return [element.name, element.value];
        case 'checkbox':
        case 'radio':
            return inputSelector(element);
    }
    return false;
}

//combine URL
function serializeElement(element) {
    var method = element.tagName.toLowerCase();
    var parameter = input(element);

    if (parameter) {
        var key = encodeURIComponent(parameter[0]);
        if (key.length == 0) return;

        if (parameter[1].constructor != Array)
            parameter[1] = [parameter[1]];

        var values = parameter[1];
        var results = [];
        for (var i=0; i<values.length; i++) {
            results.push(key + '=' + encodeURIComponent(values[i]));
        }
        return results.join('&');
    }
}

//
function serializeForm(formId) {
    var elements = getElements(formId);
    var queryComponents = new Array();

    for (var i = 0; i < elements.length; i++) {
        var queryComponent = serializeElement(elements[i]);
        if (queryComponent)
            queryComponents.push(queryComponent);
    }

    return queryComponents.join('&');
}



function loadMonitorData(container, propName, ks) {
    //            params= {prop: propName, 'keys': ks};
    var params = "prop=" + propName;
    for (var i = 0; i < ks.length; i++)
        params += "&keys=" + ks[i];
    $.post("getMonitorData", params, function (data, status) {
        if (data)
            createHighStock(propName, container, ks, data);
    }, "json");
};

function loadDataToContainer(container,propName) {
    params = {prop: propName};
    $.post("getKeys", params, function (data, status) {
        if (data)
            loadMonitorData(container, propName, data);
    }, "json");
};

function loadDataOfProp(container,propName,key) {
    if(propName) {
        params = {prop: propName,key:key};
        $.post("getPropsLike", params, function (data, status) {
            if (data)
                loadDataToContainer(data[0], container);
        }, "json");
    }
}

function loadMatchedData(container, propName, key , duraiton) {
    var params = "prop=" + propName;
    if(key)
        params += "&key=" + key;
    if(duraiton)
        params += "&duration =" + duraiton;
    $.post("getPropKeyLike", params, function (data, status) {
        if (data && data[0] && data[1])
            loadMonitorData(container, data[0], data[1]);
    }, "json");
}

function loadCpuMockData() {
    var dataNames = ['127.0.0.1:10010', '127.0.0.1:10020'];
    var data = [mockdata(), mockdata2()];
    createHighStock("CPUMonitor", '#highstock-cpu', dataNames, data);
}

function loadJvmMockData() {
    var dataNames = ['127.0.0.1:10010', '127.0.0.1:10020'];
    var data = [mockdata(), mockdata2()];
    createHighStock("JVM Monitor", '#highstock-jvm', dataNames, data);
}

function loadMemMockData() {
    var dataNames = ['127.0.0.1:10010', '127.0.0.1:10020'];
    var data = [mockdata(), mockdata2()];
    createHighStock("MemoryMonitor", '#highstock-mem', dataNames, data);
}