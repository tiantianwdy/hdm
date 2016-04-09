
    var data = ""
    ajaxSend("/service/listPipes/", "get", data, "admin", null, function(d){
        quickTree("pipe-list-tree", d, clickedTree)
    });

    function clickedTree(nodeName, depth){
        if(depth == 1){
            document.getElementById("pipe-history-tree").innerHTML = "";
            ajaxSend("/service/pipeHistory/?pipeName=" + nodeName, "get", nodeName, "admin", null, function(d){
               createInteractiveTree("pipe-history-tree", [d], graphDataInfo.nodes, 1080 ,560, 100);
            });
        }
    };