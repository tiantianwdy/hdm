
    var data = "";
    curAppId = getCookie("curAppId");

    ajaxSend("../service/versionList/", "post", data, "admin", null, function(d){
        quickTree("app-versions-tree", d, clickedTree);
        CollapsibleLists.apply("app-versions-tree");
    });

    if(curAppId){
       clickedTree(curAppId, 1);
    }

    function clickedTree(nodeName, depth){
        if(depth == 1){
            document.getElementById("dependency-tree").innerHTML = "";
            ajaxSend("../service/dependencyTrace/?app=" + nodeName, "get", nodeName, "admin", null, function(d){
               createInteractiveTree("dependency-tree", [d], graphDataInfo.nodes, 1080 ,560, 100);
            });
            curAppId = nodeName;
            setCookie("curAppId", curAppId);
        }
    };

//    addTable("task-table", heading, stock)
//    createInteractiveTree("pipe-history-tree", dataJoinerHistory, graphDataInfo.nodes , 1080 ,560, 100)
//    quickTree("pipe-list-tree", pipeListTree)