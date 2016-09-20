require([ "jquery",
    "../res/js/mockdata/mock-cluster.js",
    "../res/js/utils",
    "../res/js/libs/quick-table",
    "../res/js/libs/ajax",
    "../res/js/libs/autocomplete",
    "../res/js/libs/common",
    "../res/js/libs/common-utils",
    "../res/js/libs/htable",
    "../res/js/libs/hdialog",
    "../res/js/libs/tojson",
    "../res/js/libs/highstock/highstock",
    "../res/js/libs/highstock/mockdata",
    "../res/js/libs/highstock/exporting",
    "../res/js/libs/highstock/theme",
    "../res/js/libs/highstock/highStockUtils"],
    function ($, Ajax) {
        curNode = null;
        var curShowingNode;

        function loadData(url, container, propName, ks) {
            //            params= {prop: propName, 'keys': ks};
            var params = "prop=" + propName;
            for (var i = 0; i < ks.length; i++)
                params += "&key=" + ks[i];
            $.post(url, params, function (data, status) {
                if (data)
                    createHighStock(propName, container, ks, data);
            }, "json");
        };




        // refresh and search
        $("#bts").on("click", ".reset",function () {
            $("#search").find("input[type=text]").val('');
            $("#search").find("select").val('');
            $("#highstock-search").hide();
            loadAllData();
        }).on("click", ".search", function () {
                var prop = $("#search").find("input[name=prop]").val();
                var key = $("#search").find("input[name=key]").val();
                var keys = [key];
                $("#highstock-search").show();
                loadMatchedData("#highstock-search",prop,keys)
            });

        //initialize slave list
        ajaxSend("../service/nodeList/", "post", "", "admin", null, function(d){
            quickTree("slaveLists", d, nodeClicked);
            CollapsibleLists.apply("slaveLists");
        });

        utils.collapseNative('slave-cluster-collapse','Show Cluster of Nodes', 'Collapse', function(){
//             createDAG("slave-cluster", mockClusterData)
            ajaxSend("../service/nodeCluster/", "post", "", "admin", null, function(d){
                createDAG("slave-cluster", d);
            });
        });

        utils.collapseNative('slave-monitor-collapse','Show Resource States of nodes', 'Collapse', function(){
             if(curShowingNode != curNode){
               loadAllData(curNode)
             }
        });


        //
        function nodeClicked(node, depth){
            if(depth == 1){
               if($('#monitor-main').is(":visible")){
                loadAllData(node)
               }
               curNode = node
            }
        }

        //load all monitor data of given node
        function loadAllData(node){
           curShowingNode = node;
           loadData("../service/nodeMonitor/", "#highstock-cpu", "system/cpu", [node]);
           loadData("../service/nodeMonitor/", "#highstock-mem", "system/mem", [node]);
           loadData("../service/nodeMonitor/", "#highstock-net", "system/net", [node +"/receive", node + "/transmit"]);
           var jvmKeys = [node+"/memMax", node + "/memTotal", node + "/memUsed"]
           loadData("../service/nodeMonitor/", "#highstock-jvm", "system/jvm", jvmKeys);
        }

        // load mock data
        function loadAllMockData() {

            var key = "";
            var keys = ["127.0.0.1:10001","127.0.0.1:10002"];

            loadCpuMockData()
            loadJvmMockData()
            loadMemMockData()
        }

    });// end require