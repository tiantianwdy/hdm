
    var data = "";
    curExeId = getCookie("curExeId");
    curJobId = getCookie("curJobId");

    ajaxSend("../service/allApplications/", "post", data, "admin", null, function(d){
        quickTree("applicationTrees", d, treeNodeClicked);
        CollapsibleLists.apply("applicationTrees");
    });



    $('.nav-tabs a').click(function(){
        $(this).tab('show');
        if(curExeId){
            if(this.id == "exec-trace-tab"){ //todo collapse inactive tabs
//              showExecutionTraces(curExeId)
            } else {
//              showDataFlows(curExeId)
            }
        }
    })

    function treeClicked(node, depth){
        if(depth == 1){
            clear("pipelineTreeDiv");
            var pipelineName = node;
            ajaxSend("../service/execPipelines/?pipelineName=" + pipelineName, "get", pipelineName, "admin", null, function(d){
                createInteractiveTree("pipelineTreeDiv", [d], graphDataInfo.nodes, 540, 560, 120);
            });
        } else if(depth == 2) {
            clear("dagDiv");
            var executionTag = node;
            ajaxSend("../service/execHistory/?executionTag=" + executionTag, "get", executionTag, "admin", null, function(d){
                createDAG("dagDiv", d);
            });
            clear("task-table");
            ajaxSend("../service/execDAG/?executionTag=" + executionTag, "get", executionTag, "admin", null, function(d){
                addTable("task-table", heading, d)
            });
        }
    }

    utils.show('dag-content-table');
    utils.collapseNative('stage-span', 'Show Stages Flow ', 'Collapse', function(){
//        ajaxSend("../service/physicalFlow/?executionTag=" + curExeId, "get", curExeId, "admin", null, function(d){
//          d3dag.displayGraph(d, jQuery('#dag-span-physical'), '#dag-physical > svg');
//        });
    });
    utils.collapseNative('logical-span','Show Logical Flow ', 'Collapse', function(){
         ajaxSend("../service/logicalFlow/?jobId=" + curJobId + "&opt=false", "get", curExeId, "admin", null, function(d){
           d3dag.displayGraph(d, jQuery('#dag-span-logical'), '#dag-logic > svg');
         });
    });
    utils.collapseNative('logical-opt-span', 'Show Optimized Logical Flow ', 'Collapse', function(){
        ajaxSend("../service/logicalFlow/?jobId=" + curJobId + "&opt=true", "get", curExeId, "admin", null, function(d){
          d3dag.displayGraph(d, jQuery('#dag-span-optimized'), '#dag-optimized > svg');
        });
    });

    utils.collapseNative('execution-trace-span', 'Show Execution Graph', 'Collapse', function(){
        if(curExeId){
            ajaxSend("../service/executionGraph/?executionTag=" + curExeId, "get", curExeId, "admin", null, function(d){
              d3dag.displayGraph(d, jQuery('#execution-graph-label'), '#execution-graph > svg', 'TB');
            });
        }
    });
    utils.collapseNative('workers-span', 'Show Traces of Workers ', 'Collapse', function(){
      ajaxSend("../service/executionLane/?executionTag=" + curExeId, "get", curExeId, "admin", null, function(d){
        d.items.forEach(function(item){
          item.class = 'past';
        });
        d3swimlane.displayLanes(d, '#workerLanes', d.min, d.max + 1000);
      });
    });

    if(curExeId){
      showDataFlows(curExeId);
    };

    function treeNodeClicked(node, depth){
        if(depth == 1){
//          alert("clicked in depth 1")
//          clearText("dag-span-logical");
          showStageFlows(node, '0.0.1')
        } else if(depth == 2) {
          showDataFlows(node);
          showExecutionTraces(node)
          if(node != curExeId){
            curExeId = node;
            setCookie("curExeId", curExeId);
          }


//          d3dag.displayGraph(hdmMockLogicalPlan, jQuery('#dag-span-logical'), '#dag-logic > svg');
//          d3dag.displayGraph(hdmMockLogicalPlanOpt, jQuery('#dag-span-optimized'), '#dag-optimized > svg');
//          d3dag.displayGraph(hdmMockPhysicalPlan, jQuery('#dag-span-physical'), '#dag-physical > svg');
//          d3swimlane.displayLanes(mockLaneData, '#workerLanes', 1459844091830, 1459844596830);
       }
    };

    function showStageFlows(app, version){
      if(! $('#stage-span').next().is(":visible")){
         $('#stage-span').trigger('click');
      }
      ajaxSend("../service/jobStage/?app=" + app + "&version=" + version, "get", app, "admin", null, function(d){
        d3dag.displayGraph(d, jQuery('#dag-span-stage'), '#dag-stage > svg', "LR", "rect", function(d){
            showDataFlows(d);
            curJobId = d;
            setCookie("curJobId", d);
        }, toolTipFunc);
      });
    }

    function showDataFlows(node){
      var executionTag = node;
      if(! $('#logical-span').next().is(":visible")){
         $('#logical-span').trigger('click');
      }
      ajaxSend("../service/logicalFlow/?jobId=" + executionTag + "&opt=false", "get", executionTag, "admin", null, function(d){
        d3dag.displayGraph(d, jQuery('#dag-span-logical'), '#dag-logic > svg', "LR", "ellipse");
      });

      if(! $('#logical-opt-span').next().is(":visible")){
         $('#logical-opt-span').trigger('click');
      }
      ajaxSend("../service/logicalFlow/?jobId=" + executionTag + "&opt=true", "get", executionTag, "admin", null, function(d){
        d3dag.displayGraph(d, jQuery('#dag-span-optimized'), '#dag-optimized > svg', "LR", "ellipse");
      });

      if($('#physical-span').next().is(":visible")){
//         $('#physical-span').trigger('click');
          ajaxSend("../service/physicalFlow/?executionTag=" + executionTag, "get", executionTag, "admin", null, function(d){
            d3dag.displayGraph(d, jQuery('#dag-span-physical'), '#dag-physical > svg', "TB", "circle");
          });
      }

    }

    function showExecutionTraces(executionTag){
      if($('#execution-trace-span').next().is(":visible")){
//         $('#execution-trace-span').trigger('click');
          ajaxSend("../service/executionGraph/?executionTag=" + executionTag, "get", executionTag, "admin", null, function(d){
             d3dag.displayGraph(d, jQuery('#execution-graph-label'), '#execution-graph > svg', 'TB');
           });
      }
      if(!$('#workers-span').next().is(":visible")){
         $('#workers-span').trigger('click');
      }

      ajaxSend("../service/executionLane/?executionTag=" + executionTag, "get", executionTag, "admin", null, function(d){
        d.items.forEach(function(item){
          item.class = 'past';
        });
        d3swimlane.displayLanes(d, '#workerLanes', d.min, d.max + 1000);
      });
    }

    function clearText(elementId){
        document.getElementById(elementId).innerHTML = "";
    }

    function toolTipFunc(tableData, d){
        tableData.push(new Array("appId", d.appId))
        tableData.push(new Array("jobId", d.id))
        tableData.push(new Array("type", d.name))
        tableData.push(new Array("jobFunc", d.func))
        tableData.push(new Array("context", d.context))
        tableData.push(new Array("parents", d.parents))
        tableData.push(new Array("Parallelism", d.parallelism))
        tableData.push(new Array("isLocal", d.local))
        tableData.push(new Array("State", d.status))
    }