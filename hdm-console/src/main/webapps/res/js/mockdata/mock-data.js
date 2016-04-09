var pipelineListTree ={
 "name" : "Applications",
 "children" : [
    {"name" : "application_1",
     "children":[
        {"name" : "execution_1"},
        {"name" : "execution_2"},
        {"name" : "execution_3"}
     ]
    },
    {"name" : "application_2",
     "children":[
        {"name" : "execution_4"},
        {"name" : "execution_5"},
        {"name" : "execution_6"}
     ]
    },
    {"name" : "application_3",
      "children":[
         {"name" : "execution_5"},
         {"name" : "execution_6"},
         {"name" : "execution_7"}
      ]
    }

 ]
}

var pipeListTree ={
 "name" : "Pipe List",
 "children" : [
    {"name" : "csvMapper",
     "children":[]
    },
    {"name" : "jsonMapper",
     "children":[]
    },
    {"name" : "textMapper",
     "children":[]
    },
    {"name" : "dataJoiner",
     "children":[]
    }
 ]
}

var dataJoinerHistory = [
   {
   "name": "dataJoiner",
   "parent": "null",
   "children" : [
       {
           "name": "0.0.1",
           "parent": "dataJoiner",
           "children" : [
               {
                "name": "type:SparkPipe",
                "parent": "0.0.1"
               },{
                "name": "dependency",
                "parent": "0.0.1",
                "children": [
                     {
                          "name" : "/dataJoiner/0.0.1/data-joiner-0.0.1.jar",
                          "parent" : "dependency"
                     }
                ]
               },{
                "name": "instances",
                "parent": "0.0.1",
                "children" : [
                    {
                        "name" : "excution_1",
                        "parent" : "instances"
                    },{
                        "name" : "excution_2",
                        "parent" : "instances"
                    },{
                        "name" : "excution_3",
                        "parent" : "instances"
                    }
                ]
               }
           ]
       },{
        "name": "0.0.2",
        "parent": "dataJoiner",
        "children" : [
            {
             "name": "type:SparkPipe",
             "parent": "0.0.2"
            },{
             "name": "dependency",
             "parent": "0.0.2",
             "children": [
                  {
                       "name" : "/dataJoiner/0.0.2/data-joiner-0.0.2.jar",
                       "parent" : "dependency"
                  }
             ]
            },{
             "name": "instances",
             "parent": "0.0.2",
             "children" : [
                 {
                     "name" : "excution_4",
                     "parent" : "instances"
                 },{
                     "name" : "excution_5",
                     "parent" : "instances"
                 },{
                     "name" : "excution_6",
                     "parent" : "instances"
                 }
             ]
            }
        ]
      },{
        "name": "0.0.3",
        "parent": "dataJoiner",
        "children" : [
            {
             "name": "type:SparkPipe",
             "parent": "0.0.3"
            },{
             "name": "dependency",
             "parent": "0.0.3",
             "children": [
                  {
                       "name" : "/dataJoiner/0.0.3/data-joiner-0.0.3.jar",
                       "parent" : "dependency"
                  }
             ]
            },{
             "name": "instances",
             "parent": "0.0.3",
             "children" : [
                 {
                     "name" : "excution_7",
                     "parent" : "instances"
                 },{
                     "name" : "excution_8",
                     "parent" : "instances"
                 }
             ]
            }
        ]
      }
   ]
  }
]



var graphData ={
  name: "joinDataFlow",
  nodes:[
    {"id":0, "name":"textMapper", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"MR", "status":"completed", "group":1},
    {"id":1, "name":"jsonMapper", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"MR", "status":"running", "group":2},
    {"id":2, "name":"csvMapper", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"MR", "status":"running", "group":2},
    {"id":3, "name":"dataJoiner", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", "group":3},
    {"id":4, "name":"featureExtractorPy", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", "group":3},
    {"id":5, "name":"featureExtractorSpark", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", "group":3},
    {"id":6, "name":"analysisPy", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Python", "status":"waiting", "group":3},
    {"id":7, "name":"analysisSpark", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", "group":3}
  ],
  links:[
    {"source":0,"target":3,"value":1},
    {"source":1,"target":3,"value":8},
    {"source":2,"target":3,"value":10},
    {"source":3,"target":4,"value":6},
    {"source":3,"target":5,"value":1},
    {"source":4,"target":6,"value":1},
    {"source":5,"target":7,"value":1}
  ]
};

var graphDataInfo ={
  "nodes":[
    {"id":0, "name":"textMapper", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"MR", "status":"completed", "group":1},
    {"id":1, "name":"jsonMapper", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"MR", "status":"running", "group":2},
    {"id":2, "name":"csvMapper", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"MR", "status":"running", "group":2},
    {"id":3, "name":"dataJoiner", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", "group":3},
    {"id":4, "name":"featureExtractorPy", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", "group":3},
    {"id":8, "name":"featureExtractorPy", "version":"0.0.2", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", "group":3},
    {"id":5, "name":"featureExtractorSpark", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", "group":3},
    {"id":6, "name":"analysisPy", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Python", "status":"waiting", "group":3},
    {"id":7, "name":"analysisSpark", "version":"0.0.1", "startTime":"123", "endTime":"time", "pipeType":"Spark", "status":"waiting", "group":3}
  ],
  "links":[
    {"source":0,"target":3,"value":1},
    {"source":1,"target":3,"value":8},
    {"source":2,"target":3,"value":10},
    {"source":3,"target":4,"value":6},
    {"source":3,"target":5,"value":1},
    {"source":4,"target":6,"value":1},
    {"source":5,"target":7,"value":1}
  ]
};

var mockPipelineTree = [
  {
    "name": "Pipeline_ID",
    "parent": "null",
    "group" : 0,
    "children": [
      {
        "name": "execution_1",
        "parent": "Pipeline_ID",
        "group" : 0,
        "children": [
          {
            "name": "csvMapper#0.0.1",
            "parent": "execution_1",
            "group" : 1
          },
          {
            "name": "jsonMapper#0.0.1",
            "parent": "execution_1",
            "group" : 1
          },
          {
            "name": "textMapper#0.0.1",
            "parent": "execution_1",
            "group" : 1
          },
          {
            "name": "dataJoiner#0.0.1",
            "parent": "execution_1"
          },
          {
            "name": "featureExtractorSpark#0.0.1",
            "parent": "execution_1"
          },
          {
            "name": "featureExtractorPy#0.0.1",
            "parent": "execution_1"
          },
          {
            "name": "analysisSpark#0.0.1",
            "parent": "execution_1"
          },
          {
            "name": "analysisPy#0.0.1",
            "parent": "execution_1"
          }
        ]
      },
      {
        "name": "execution_2",
        "parent": "Pipeline_ID",
        "children": [
                  {
                    "name": "csvMapper#0.0.1",
                    "parent": "execution_2"
                  },
                  {
                    "name": "jsonMapper#0.0.1",
                    "parent": "execution_2"
                  },
                  {
                    "name": "textMapper#0.0.1",
                    "parent": "execution_2"
                  },
                  {
                    "name": "xmlMapper#0.0.1",
                    "parent": "execution_2"
                  },
                  {
                    "name": "dataJoiner#0.0.1",
                    "parent": "execution_2"
                  },
                  {
                    "name": "featureExtractorSpark#0.0.1",
                    "parent": "execution_2"
                  },
                  {
                    "name": "featureExtractorPy#0.0.1",
                    "parent": "execution_2"
                  },
                  {
                    "name": "analysisSpark#0.0.1",
                    "parent": "execution_2"
                  },
                  {
                    "name": "analysisPy#0.0.1",
                    "parent": "execution_2"
                  }
                ]
      },
      {
        "name": "execution_3",
        "parent": "Pipeline_ID",
        "children": [
                  {
                    "name": "csvMapper#0.0.1",
                    "parent": "execution_3"
                  },
                  {
                    "name": "jsonMapper#0.0.1",
                    "parent": "execution_3"
                  },
                  {
                    "name": "textMapper#0.0.1",
                    "parent": "execution_3"
                  },
                  {
                    "name": "xmlMapper#0.0.1",
                    "parent": "execution_3"
                  },
                  {
                    "name": "dataJoiner#0.0.1",
                    "parent": "execution_3"
                  },
                  {
                    "name": "featureExtractorSpark#0.0.1",
                    "parent": "execution_3"
                  },
                  {
                    "name": "featureExtractorPy#0.0.2",
                    "parent": "execution_3"
                  },
                  {
                    "name": "analysisSpark#0.0.1",
                    "parent": "execution_3"
                  },
                  {
                    "name": "analysisPy#0.0.1",
                    "parent": "execution_3"
                  }
                ]
      }
    ]
  }
];


var mockExecDAG = new Array();
mockExecDAG[0] = new Array("1", "textMapper", "0.0.1", "None", "85.81", "Completed");
mockExecDAG[1] = new Array("2", "jsonMapper", "0.0.1", "None", "85.81", "Running");
mockExecDAG[2] = new Array("3", "csvMapper",  "0.0.1", "None", "85.81", "Running");
mockExecDAG[3] = new Array("4", "dataJoiner", "0.0.1", "textMapper; jsonMapper; csvMapper", "85.81", "Waiting");
mockExecDAG[4] = new Array("5", "featureExtractorPy", "0.0.1", "dataJoiner", "85.81", "Waiting");
mockExecDAG[5] = new Array("6", "featureExtractorSpark", "0.0.1", "dataJoiner", "85.81", "Waiting");
mockExecDAG[6] = new Array("7", "analysisPy", "0.0.1", "featureExtractorPy", "85.81", "Waiting");
mockExecDAG[7] = new Array("8", "analysisSpark", "0.0.1", "featureExtractorSpark", "85.81", "Waiting");