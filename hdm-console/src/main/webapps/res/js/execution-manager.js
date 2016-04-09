
//    var heading = new Array();
//    heading[0] = "ID"
//    heading[1] = "PipeName"
//    heading[2] = "Version"
//    heading[3] = "parents"
//    heading[4] = "Duration"
//    heading[5] = "Status"
//
//
    var data = "";
    ajaxSend("/service/listApplications/", "post", data, "admin", null, function(d){
        quickTree("pipelineTrees", d, treeNodeClicked);
    });

    function treeClicked(node, depth){
        if(depth == 1){
            clear("pipelineTreeDiv");
            var pipelineName = node;
            ajaxSend("/service/execPipelines/?pipelineName=" + pipelineName, "get", pipelineName, "admin", null, function(d){
                createInteractiveTree("pipelineTreeDiv", [d], graphDataInfo.nodes, 540, 560, 120);
            });
        } else if(depth == 2) {
            clear("dagDiv");
            var executionTag = node;
            ajaxSend("/service/execHistory/?executionTag=" + executionTag, "get", executionTag, "admin", null, function(d){
                createDAG("dagDiv", d);
            });
            clear("task-table");
            ajaxSend("/service/execDAG/?executionTag=" + executionTag, "get", executionTag, "admin", null, function(d){
                addTable("task-table", heading, d)
            });
        }
    }

//    quickTree("pipelineTrees", pipelineListTree, treeNodeClicked);
    utils.show('dag-content-table');
    utils.collapseNative('logical-span','Show Logical Flow ', 'Collapse', function(){
//       d3dag.displayGraph(hdmMockLogicalPlan, jQuery('#dag-span-logical'), '#dag-logic > svg');
    });
    utils.collapseNative('logical-opt-span', 'Show Optimized Logical Flow ', 'Collapse', function(){
//      d3dag.displayGraph(hdmMockLogicalPlanOpt, jQuery('#dag-span-optimized'), '#dag-optimized > svg');
    });
    utils.collapseNative('physical-span', 'Show Physical Flow ', 'Collapse', function(){
//      d3dag.displayGraph(hdmMockPhysicalPlan, jQuery('#dag-span-physical'), '#dag-physical > svg');
    });
    utils.collapseNative('workers-span', 'Show Execution Traces of Workers ', 'Collapse', function(){
//      d3swimlane.displayLanes(mockLaneData, '#workerLanes', 1459844091830, 1459844596830);
    });

    function treeNodeClicked(node, depth){
        if(depth == 1){
            clearText("dag-span-logical");
        } else if(depth == 2) {
          var dataFlowName = node;
          if(!$('#logical-span').next().is(":visible")){
             $('#logical-span').trigger('click');
          }
          if(!$('#logical-opt-span').next().is(":visible")){
             $('#logical-opt-span').trigger('click');
          }
          if(!$('#physical-span').next().is(":visible")){
             $('#physical-span').trigger('click');
          }
          var executionTag = node;
          ajaxSend("/service/logicalFlow/?executionTag=" + executionTag + "&opt=false", "get", executionTag, "admin", null, function(d){
            d3dag.displayGraph(d, jQuery('#dag-span-logical'), '#dag-logic > svg');
          });
          ajaxSend("/service/logicalFlow/?executionTag=" + executionTag + "&opt=true", "get", executionTag, "admin", null, function(d){
            d3dag.displayGraph(d, jQuery('#dag-span-optimized'), '#dag-optimized > svg');
          });
          ajaxSend("/service/physicalFlow/?executionTag=" + executionTag, "get", executionTag, "admin", null, function(d){
            d3dag.displayGraph(d, jQuery('#dag-span-physical'), '#dag-physical > svg');
          });
          ajaxSend("/service/executionLane/?executionTag=" + executionTag, "get", executionTag, "admin", null, function(d){
            d.items.forEach(function(item){
              item.class = 'past';
            });
            d3swimlane.displayLanes(d, '#workerLanes', d.min, d.max + 1000);
          });
//          d3dag.displayGraph(hdmMockLogicalPlan, jQuery('#dag-span-logical'), '#dag-logic > svg');
//          d3dag.displayGraph(hdmMockLogicalPlanOpt, jQuery('#dag-span-optimized'), '#dag-optimized > svg');
//          d3dag.displayGraph(hdmMockPhysicalPlan, jQuery('#dag-span-physical'), '#dag-physical > svg');
//          d3swimlane.displayLanes(mockLaneData, '#workerLanes', 1459844091830, 1459844596830);
       }
    };

    function clearText(elementId){
        document.getElementById(elementId).innerHTML = "";
    }