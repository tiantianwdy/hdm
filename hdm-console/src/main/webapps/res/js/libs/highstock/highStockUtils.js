/**
 * create high-stock charts
 * @param title
 * @param container
 * @param dataNames
 * @param dataArray
 */
function createHighStock(title, container, dataNames, dataArray) {
    //create data format
    var dataSeries = new Array(dataArray.length)
    for (var i = 0; i < dataArray.length; i++) {
        dataSeries[i] = {
            title: {
                text: dataNames[i],
                style: {
                    color: '#AA4643'
                }
            },
            name: dataNames[i],
            data: dataArray[i],
            type: 'spline',
            lineWidth : 1,
            marker : {
                enabled : true,
                radius : 3
            },
            tooltip: {
                valueDecimals: 2
            }
        }
    }
    //set theme
    Highcharts.setOptions(Highcharts.theme);
    // set to local time zone
    Highcharts.setOptions({
        global: {
            useUTC: false
        }
    });
    //create charts with data
    $(container).highcharts('StockChart', {
        chart: {},
        rangeSelector: {
            buttons: [
                {
                    count: 1,
                    type: 'minute',
                    text: '1m'
                },
                {
                    count: 10,
                    type: 'minute',
                    text: '10m'
                },
                {
                    count: 1,
                    type: 'hour',
                    text: '1H'
                },
                {
                    count: 1,
                    type: 'day',
                    text: '1D'
                },
                {
                    count: 1,
                    type: 'month',
                    text: '1M'
                },
                {
                    type: 'all',
                    text: 'All'
                }
            ],
            inputEnabled: true,
            selected: 2
        },
        title: {
            text: title
        },
        series: dataSeries
    });
};


