var loadDagData =
  {
        name: 'graph1',
        nodes: [
            { id: 'node1', value: { label: 'node1' }, style: {fill: '#afa'}  },
            { id: 'node2', value: { label: 'node2' }, style: "fill: #afa"  },
            { id: 'node3', value: { label: 'node3' }, style: "fill: #afa" },
            { id: 'node4', value: { label: 'node4' }, style: "fill: #afa" },
            { id: 'node5', value: { label: 'node5' }, style: "fill: #afa" }
        ],
        links: [
            { u: 'node1', v: 'node2', value: { label: 'link1', lineInterpolate: 'basis' } },
            { u: 'node1', v: 'node3', value: { label: 'link2', lineInterpolate: 'basis'} },
            { u: 'node2', v: 'node4', value: { label: 'link3', lineInterpolate: 'basis' } },
            { u: 'node3', v: 'node4', value: { label: 'link4', lineInterpolate: 'basis' } },
            { u: 'node4', v: 'node5', value: { label: 'link5', lineInterpolate: 'basis' } }
        ]
  };

var loadGraphData ={
  name: "joinDataFlow",
  nodes:[
    {"id":0, "name":"textMapper", value: { label: 'textMapper', style: {fill: '#afa'}  }, "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"MR", "status":"completed", group:1},
    {"id":1, "name":"jsonMapper", value: { label: 'jsonMapper' }, "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"MR", "status":"running", group:2},
    {"id":2, "name":"csvMapper", value: { label: 'csvMapper' },"version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"MR", "status":"running", group:2},
    {"id":3, "name":"dataJoiner", value: { label: 'dataJoiner' },"version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", group:3},
    {"id":4, "name":"featureExtractorPy", value: { label: 'featureExtractorPy' },"version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", group:3},
    {"id":5, "name":"featureExtractorSpark", value: { label: 'featureExtractorSpark' },"version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", group:3},
    {"id":6, "name":"analysisPy", value: { label: 'analysisPy' },"version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Python", "status":"waiting", group:3},
    {"id":7, "name":"analysisSpark", value: { label: 'analysisSpark' },"version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", group:3}
  ],
  links:[
    {"u":0,"v":3, value: { label: '' },},
    {"u":1,"v":3, value: { label: '' },},
    {"u":2,"v":3, value: { label: '' },},
    {"u":3,"v":4,value: { label: '' },},
    {"u":3,"v":5,value: { label: '' },},
    {"u":4,"v":6,value: { label: '' },},
    {"u":5,"v":7,value: { label: '' },}
  ]
};

var loadd3DagData =
  {
        nodes: [
            { id: 'node1', value: { label: 'node1' }, style: {fill: '#afa'}  },
            { id: 'node2', value: { label: 'node2' }, style: "fill: #afa"  },
            { id: 'node3', value: { label: 'node3' }, style: "fill: #afa" },
            { id: 'node4', value: { label: 'node4' }, style: "fill: #afa" },
            { id: 'node5', value: { label: 'node5' }, style: "fill: #afa" }
        ],
        links: [
            { u: 'node1', v: 'node2', value: { label: 'link1'} },
            { u: 'node1', v: 'node3', value: { label: 'link2' } },
            { u: 'node2', v: 'node4', value: { label: 'link3' } },
            { u: 'node3', v: 'node4', value: { label: 'link4' } },
            { u: 'node4', v: 'node5', value: { label: 'link5' } }
        ]
  };