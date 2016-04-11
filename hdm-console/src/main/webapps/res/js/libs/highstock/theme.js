/**
 * wudongyao
 * @type {{colors: Array, chart: {backgroundColor: {linearGradient: {x1: number, y1: number, x2: number, y2: number}, stops: Array}, borderWidth: number, plotBackgroundColor: string, plotShadow: boolean, plotBorderWidth: number}, title: {style: {color: string, font: string}}, subtitle: {style: {color: string, font: string}}, xAxis: {gridLineWidth: number, lineColor: string, tickColor: string, labels: {style: {color: string, font: string}}, title: {style: {color: string, fontWeight: string, fontSize: string, fontFamily: string}}}, yAxis: {minorTickInterval: string, lineColor: string, lineWidth: number, tickWidth: number, tickColor: string, labels: {style: {color: string, font: string}}, title: {style: {color: string, fontWeight: string, fontSize: string, fontFamily: string}}}, legend: {itemStyle: {font: string, color: string}, itemHoverStyle: {color: string}, itemHiddenStyle: {color: string}}, labels: {style: {color: string}}, navigation: {buttonOptions: {theme: {stroke: string}}}}}
 */
Highcharts.theme = {
    colors: ['#058DC7', '#50B432', '#ED561B', '#DDDF00', '#24CBE5', '#64E572', '#FF9655', '#FFF263', '#6AF9C4'],
    chart: {
        backgroundColor: {
            linearGradient: { x1: 0, y1: 0, x2: 1, y2: 1 },
            stops: [
                [0, 'rgb(255, 255, 255)'],
                [1, 'rgb(240, 240, 255)']
            ]
        },
        borderWidth: 1,
        plotBackgroundColor: 'rgba(255, 255, 255, .9)',
        plotShadow: true,
        plotBorderWidth: 3,
        width:1200
    },
    title: {
        style: {
            color: '#000',
            font: 'bold 16px "Trebuchet MS", Verdana, sans-serif'
        }
    },
    subtitle: {
        style: {
            color: '#666666',
            font: 'bold 12px "Trebuchet MS", Verdana, sans-serif'
        }
    },
    xAxis: {
        gridLineWidth: 1,
        lineColor: '#000',
        tickColor: '#000',
        labels: {
            style: {
                color: '#000',
                font: '11px Trebuchet MS, Verdana, sans-serif'
            }
        },
        title: {
            style: {
                color: '#333',
                fontWeight: 'bold',
                fontSize: '12px',
                fontFamily: 'Trebuchet MS, Verdana, sans-serif'

            }
        }
    },
    yAxis: {
        minorTickInterval: 'auto',
        lineColor: '#000',
        lineWidth: 1,
        tickWidth: 1,
        tickColor: '#000',
        labels: {
            style: {
                color: '#000',
                font: '11px Trebuchet MS, Verdana, sans-serif'
            }
        },
        title: {
            style: {
                color: '#333',
                fontWeight: 'bold',
                fontSize: '12px',
                fontFamily: 'Trebuchet MS, Verdana, sans-serif'
            }
        }
    },
    legend: {
        itemStyle: {
            font: '9pt Trebuchet MS, Verdana, sans-serif',
            color: 'black'

        },
        itemHoverStyle: {
            color: '#039'
        },
        itemHiddenStyle: {
            color: 'gray'
        }
    },
    labels: {
        style: {
            color: '#99b'
        }
    },

    navigation: {
        buttonOptions: {
            theme: {
                stroke: '#CCCCCC'
            }
        }
    }
};
