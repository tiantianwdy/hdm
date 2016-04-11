var color = d3.scale.category20();

var colorArray = ["lightsteelblue", "LimeGreen", "gold", "steelblue", "green", "blue", "red", "purple",  "black"];

var  d3dag = {
        displayGraph: function (graphData, dagNameElem, svgElem, dir) {
            dagNameElem.text(graphData.name);
            if(!dir) dir = "LR";
            this.renderGraph(graphData, svgElem, dir);
        },

        renderGraph: function(graphData, svgParent, dir) {
            var div = d3.select("body").append("div")
              .attr("class", "tooltip")
              .style("opacity", 0);

            var nodes = graphData.nodes;
            var links = graphData.links;
            var g = new dagreD3.graphlib.Graph().setGraph({})
            g.graph().rankDir = dir; // set the direction of layout

            //add nodes
            nodes.forEach(function(n) {
            var color = colorArray[n.group]
            var style = { style: "fill: " + color, label: n.name, shape: "ellipse"};
              g.setNode(n.id, style)
            });
            //add links
            links .forEach(function(e) {
              var style = {
                label : '',
                lineInterpolate : 'basis'
              };
//              style.lineInterpolate = 'basis';
              g.setEdge(e.u, e.v, style);
            });

            var render = new dagreD3.render()

            var svg = d3.select(svgParent);
            svg.select('g').remove(); // remove old svg if exists
            var svgGroup = svg.append("g");
            var inner = svg.select("g");
            render(inner, g);

            // Center the graph

//            var xCenterOffset = (svg.attr("width") - g.graph().width) / 2;
//            svgGroup.attr("transform", "translate(" + xCenterOffset + ", 20)");
            svg.attr("height", g.graph().height + 40);


            // drop-in shadow effects
            var filter = inner.append('filter')
                  .attr('id', 'drop_shadow')
                  .attr('height', '110%');
              filter.append("feGaussianBlur")
                    .attr("in", "SourceAlpha")
                    .attr("stdDeviation", 4)
                    .attr("result", "blur");
              filter.append("feOffset")
                    .attr("in", "blur")
                    .attr("dx", 2)
                    .attr("dy", 2)
                    .attr("result", "offsetBlur");
              var cpec_light = filter.append('feSpecularLighting')
                .attr('in', 'blur')
                .attr('surfaceScale', '2')
                .attr('specularConstant', '0.75')
                .attr('specularExponent', '10')
                .attr('lighting-color', 'grey')
                .attr('result', 'specOut');
              cpec_light.append('fePointLight')
                .attr('x', '5000')
                .attr('y', '-5000')
                .attr('z', '6000');
              filter.append('feComposite')
                .attr('in', 'specOut')
                .attr('in2', 'SourceAlpha')
                .attr('operator', 'in')
                .attr('result', 'specOut');
              filter.append('feComposite')
                .attr('in', 'SourceGraphic')
                .attr('in2', 'specOut')
                .attr('operator', 'arithmetic')
                .attr('k1', '0')
                .attr('k2', '1')
                .attr('k3', '1')
                .attr('k4', '0')
                .attr('result', 'litPaint');
              filter.append("feOffset")
                    .attr("in", "litPaint")
                    .attr("dx", -4)
                    .attr("dy", -4)
                    .attr("result", "litPaint");
              var feMerge = filter.append("feMerge");
              feMerge.append("feMergeNode")
                    .attr("in", "offsetBlur");
              feMerge.append("feMergeNode")
                    .attr("in", "litPaint");

            // add ToolTips on nodes
            inner.selectAll("g.node")
            .on("mouseover", function(d){
              return d3dag.addToolTip(d, div, nodes);
            }).on("mouseout", function(d){
              return d3dag.hideToolTip(d, div);
            });

            // Zoom
            var zoom = d3.behavior.zoom().on("zoom", function() {
                  inner.attr("transform", "translate(" + d3.event.translate + ")" +
                                              "scale(" + d3.event.scale + ")");
                });
            svg.call(zoom);
        },

        addToolTip: function (data, div, nodeInfo){
//                var seq = data.name.split("#");
//                var name = data[0];
                var version = "";
                var d = this.findNodeFromGraph(data, version, nodeInfo)

                if(d){
                    div.transition()
                        .duration(200)
                        .style("opacity", 9);

                    var input = this.renderArray(d.input);
                    var output = this.renderArray(d.output);

                    div.html("Id: " + d.id
                        + "<br/>" + "Name : " + d.name
                        + "<br/>" + "Type : " + d.type
                        + "<br/>" + "Func : " + d.func
                        + "<br/>" + "Location : " + d.location
                        + "<br/>" + "Dependency : " + d.dependency
                        + "<br/>" + "Parallelism : " + d.parallelism
                        + "<br/>" + "Partitioner : " + d.partitioner
                        + "<br/>" + "StartTime : " + d.startTime
                        + "<br/>" + "EndTime : " + d.endTime
                        + "<br/>" + "State : " + d.status
                        + "<br/>" + "Input : "  + input
                        + "<br/>" + "Output : "  + output)
                        .style("left", (d3.event.pageX) + "px")
                        .style("top", (d3.event.pageY + 10) + "px");
                }
           },

           hideToolTip: function (data, div){
             div.transition()
             .duration(500)
             .style("opacity", 0)
           },

           renderArray: function(array){
            var out = "";
            if(array && array.length > 0) {
                out += "[";
                for(var i=0; i< array.length; i++){
                  if(i==0) {
                    out +=  array[i];
                  } else {
                    out +=  ", <br/>  " + array[i];
                  }
                };
                out += "]";
            };
            return out;
           },

           findNodeFromGraph: function (name, version, nodes){
            var len = nodes.length
            for(var i=0; i<len; i++){
               if(nodes[i].id == name){
                   return nodes[i]
               }
            }
            return null
           }
    };