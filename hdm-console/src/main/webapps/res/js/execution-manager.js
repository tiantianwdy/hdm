
    var data = "";
    curExeId = getCookie("curExeId");

    ajaxSend("../service/listApplications/", "post", data, "admin", null, function(d){
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
    utils.collapseNative('logical-span','Show Logical Flow ', 'Collapse', function(){
         ajaxSend("../service/logicalFlow/?executionTag=" + curExeId + "&opt=false", "get", curExeId, "admin", null, function(d){
           d3dag.displayGraph(d, jQuery('#dag-span-logical'), '#dag-logic > svg');
         });
    });
    utils.collapseNative('logical-opt-span', 'Show Optimized Logical Flow ', 'Collapse', function(){
        ajaxSend("../service/logicalFlow/?executionTag=" + curExeId + "&opt=true", "get", curExeId, "admin", null, function(d){
          d3dag.displayGraph(d, jQuery('#dag-span-optimized'), '#dag-optimized > svg');
        });
    });
    utils.collapseNative('physical-span', 'Show Physical Flow ', 'Collapse', function(){
        ajaxSend("../service/physicalFlow/?executionTag=" + curExeId, "get", curExeId, "admin", null, function(d){
          d3dag.displayGraph(d, jQuery('#dag-span-physical'), '#dag-physical > svg');
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
            clearText("dag-span-logical");
        } else if(depth == 2) {
          showDataFlows(node);
          showExecutionTraces(node)
          if(node != curExeId){
          }
          curExeId = node;
          setCookie("curExeId", curExeId);

//          d3dag.displayGraph(hdmMockLogicalPlan, jQuery('#dag-span-logical'), '#dag-logic > svg');
//          d3dag.displayGraph(hdmMockLogicalPlanOpt, jQuery('#dag-span-optimized'), '#dag-optimized > svg');
//          d3dag.displayGraph(hdmMockPhysicalPlan, jQuery('#dag-span-physical'), '#dag-physical > svg');
//          d3swimlane.displayLanes(mockLaneData, '#workerLanes', 1459844091830, 1459844596830);
       }
    };

    function showDataFlows(node){
      var executionTag = node;
      if(! $('#logical-span').next().is(":visible")){
         $('#logical-span').trigger('click');
      }
      ajaxSend("../service/logicalFlow/?executionTag=" + executionTag + "&opt=false", "get", executionTag, "admin", null, function(d){
        d3dag.displayGraph(d, jQuery('#dag-span-logical'), '#dag-logic > svg');
      });

      if(! $('#logical-opt-span').next().is(":visible")){
         $('#logical-opt-span').trigger('click');
      }
      ajaxSend("../service/logicalFlow/?executionTag=" + executionTag + "&opt=true", "get", executionTag, "admin", null, function(d){
        d3dag.displayGraph(d, jQuery('#dag-span-optimized'), '#dag-optimized > svg');
      });

      if($('#physical-span').next().is(":visible")){
//         $('#physical-span').trigger('click');
          ajaxSend("../service/physicalFlow/?executionTag=" + executionTag, "get", executionTag, "admin", null, function(d){
            d3dag.displayGraph(d, jQuery('#dag-span-physical'), '#dag-physical > svg');
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