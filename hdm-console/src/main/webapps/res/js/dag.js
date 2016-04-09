
var  dag = {
        displayGraph: function (graph, dagNameElem, svgElem) {
            dagNameElem.text(graph.name);
            this.renderGraph(graph, svgElem);
        },

        renderGraph: function(graph, svgParent) {
            var div = d3.select("body").append("div")
              .attr("class", "tooltip")
              .style("opacity", 0);
            var nodes = graph.nodes;
            var links = graph.links;

            var graphElem = svgParent.children('g').get(0);
            var svg = d3.select(graphElem);

            var renderer = new dagreD3.Renderer();
            var layout = dagreD3.layout().rankDir('LR');
            renderer.layout(layout).run(dagreD3.json.decode(nodes, links), svg.append('g'));


            // Adjust SVG height to content
            var main = svgParent.find('g > g');
            var h = main.get(0).getBoundingClientRect().height;
            var newHeight = h + 40;
            newHeight = newHeight < 80 ? 80 : newHeight;
            svgParent.height(newHeight);


            // Zoom
            d3.select(svgParent.get(0)).call(d3.behavior.zoom().on('zoom', function() {
                var ev = d3.event;
                svg.select('g')
                    .attr('transform', 'translate(' + ev.translate + ') scale(' + ev.scale + ')');
            }));
        },

        addToolTip: function (data, div, nodeInfo){
                var seq = data.name.split("#");
                var name = seq[0]
                var version = seq[1]
                var d = findNodeFromGraph(name, version, nodeInfo)

                if(d){
                    div.transition()
                        .duration(200)
                        .style("opacity", 9);

                    div.html("name: " + d.name
                        + "<br/>" + "version: " + d.version
                        + "<br/>" + "type: " + d.pipeType
                        + "<br/>" + "startTime: " + d.startTime
                        + "<br/>" + "endTime: " + d.endTime
                        + "<br/>" + "state: " + d.status)
                        .style("left", (d3.event.pageX) + "px")
                        .style("top", (d3.event.pageY + 10) + "px");
                }
           },

           hideToolTip: function (data, div){
             div.transition()
             .duration(500)
             .style("opacity", 0)
           },

           findNodeFromGraph: function (name, version, nodes){

            var len = nodes.length
            for(var i=0; i<len; i++){
               if(nodes[i].name == name && nodes[i].version == version){
                   return nodes[i]
               }
            }
            return null
           }
    };