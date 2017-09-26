var color = d3.scale.category20();

var colorArray = ["lightsteelblue", "LimeGreen", "gold", "steelblue", "green", "blue", "red", "purple",  "grey", "black"];


var  d3dag = {
        displayGraph: function (graphData, dagNameElem, svgElem, dir, nodeShape, func, toolTipFunc) {
            dagNameElem.text(graphData.name);
            if(!dir) dir = "LR";
            this.renderGraph(graphData, svgElem, dir, nodeShape, func, toolTipFunc);
        },

        renderGraph: function(graphData, svgParent, dir, nodeShape, func, toolTipFunc) {
            var divId = svgParent + "tooltip"
            var div = d3.select("body").append("div")
              .attr("class", "tabletooltip")
              .style("opacity", 0)
//              .append("table")
//              .attr("class", "table table-striped")
              .attr("id", divId);

            var nodes = graphData.nodes;
            var links = graphData.links;
            var g = new dagreD3.graphlib.Graph().setGraph({})
            g.graph().rankDir = dir; // set the direction of layout

            //add nodes
            nodes.forEach(function(n) {
            var color = colorArray[n.group]
            if(! color) color = colorArray[1]
            if(!nodeShape) nodeShape = "ellipse"
            var style = { style: "fill: " + color, label: n.name, shape: nodeShape};
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
              return d3dag.addToolTip(d, div, nodes, toolTipFunc);
            }).on("mouseout", function(d){
              return d3dag.hideToolTip(d, div);
            }).on('click', function(d){
               if(func) func(d);
            });

            // Zoom
            var zoom = d3.behavior.zoom().on("zoom", function() {
                  inner.attr("transform", "translate(" + d3.event.translate + ")" +
                                              "scale(" + d3.event.scale + ")");
                });
            svg.call(zoom);
        },

        addToolTip: function (data, div, nodeInfo, toolTipFunc){
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


                    var headings = new Array("Property", "Value")
                    var tableData = new Array()
                    if(!toolTipFunc){
                        tableData.push(new Array("Id", d.id))
                        tableData.push(new Array("Name", d.name))
                        tableData.push(new Array("Type", d.type))
                        tableData.push(new Array("Func", d.func))
                        tableData.push(new Array("Location", d.location))
                        tableData.push(new Array("Dependency", d.dependency))
                        tableData.push(new Array("Parallelism", d.parallelism))
                        tableData.push(new Array("Partitioner", d.partitioner))
                        tableData.push(new Array("StartTime", d.startTime))
                        tableData.push(new Array("EndTime", d.endTime))
                        tableData.push(new Array("State", d.status))
                        tableData.push(new Array("Input", d.input))
                        tableData.push(new Array("Output", d.output))
                    } else {
                     toolTipFunc(tableData, d)
                    }

                    clear(div.attr('id'))
                    addTable(div.attr('id'), headings, tableData)

                    div.style("left", (d3.event.pageX) + "px")
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