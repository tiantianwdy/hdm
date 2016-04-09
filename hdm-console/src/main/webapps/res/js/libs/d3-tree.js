
/**
  create a tree layout based on treeData and generate tips for each node in nodeInfo
**/
var colorArray = ["white", "green", "blue", "lightsteelblue", "orange", "red", "purple",  "black"];

function createInteractiveTree(elem, treeData, nodeInfo, treeWidth, treeHeight, leftMargin) {

   var margin = {top: 20, right: 20, bottom: 20, left: leftMargin},
   	width = treeWidth - margin.right - margin.left,
   	height = treeHeight - margin.top - margin.bottom,
   	depthDistance = 120;

   var i = 0,
   	duration = 750,
   	root;


   var div = d3.select("body").append("div")
      .attr("class", "tooltip")
      .style("opacity", 0);

   var tree = d3.layout.tree()
   	.size([height, width])
   	.separation(function separation(a, b) {
                  return (a.parent == b.parent ? 2 : 3) / a.depth;
                });

   var diagonal = d3.svg.diagonal()
   	.projection(function(d) { return [d.y, d.x]; });

   var svg = d3.select("#" + elem).append("svg")
   	.attr("width", width + margin.right + margin.left)
   	.attr("height", height + margin.top + margin.bottom)
    .append("g")
   	.attr("transform", "translate(" + margin.left + "," + margin.top + ")");

   root = treeData[0];
   root.x0 = height / 2;
   root.y0 = 0;

   update(root);

   d3.select(self.frameElement).style("height", "440px");

   function update(source) {

     // Compute the new tree layout.
     var nodes = tree.nodes(root).reverse(),
   	  links = tree.links(nodes);

     // Normalize for fixed-depth.
     nodes.forEach(function(d) { d.y = d.depth * depthDistance; });

     // Update the nodes…
     var node = svg.selectAll("g.node")
   	  .data(nodes, function(d) { return d.id || (d.id = ++i); })
   	  ;

     // Enter any new nodes at the parent's previous position.
     var nodeEnter = node.enter().append("g")
   	  .attr("class", "node")
   	  .attr("transform", function(d) { return "translate(" + source.y0 + "," + source.x0 + ")"; })
   	  .on("click", click)
      .on("mouseover", function(d){
        if(d.depth >= 2)
            return addToolTip(d, div, nodeInfo);
        })
      .on("mouseout", function(d){ return hideToolTip(d, div); });

     nodeEnter.append("circle")
   	  .attr("r", 1e-6)
   	  .style("fill", function(d) {
//   	    return nodeColor(d);
   	    return d._children ? "lightsteelblue" : "#fff";
   	  });

     nodeEnter.append("text")
   	  .attr("x", function(d) { return d.children || d._children ? -13 : 13; })
   	  .attr("dy", ".35em")
   	  .attr("text-anchor", function(d) { return d.children || d._children ? "end" : "start"; })
   	  .text(function(d) { return d.name; })
   	  .style("fill-opacity", 1e-6);

     // Transition nodes to their new position.
     var nodeUpdate = node.transition()
   	  .duration(duration)
   	  .attr("transform", function(d) { return "translate(" + d.y + "," + d.x + ")"; });

     nodeUpdate.select("circle")
   	  .attr("r", 6)
   	  .style("fill", function(d) { return nodeColor(d); });

     nodeUpdate.select("text")
   	  .style("fill-opacity", 1);

     // Transition exiting nodes to the parent's new position.
     var nodeExit = node.exit().transition()
   	  .duration(duration)
   	  .attr("transform", function(d) { return "translate(" + source.y + "," + source.x + ")"; })
   	  .remove();

     nodeExit.select("circle")
   	  .attr("r", 1e-6);

     nodeExit.select("text")
   	  .style("fill-opacity", 1e-6);

     // Update the links…
     var link = svg.selectAll("path.link")
   	  .data(links, function(d) { return d.target.id; });

     // Enter any new links at the parent's previous position.
     link.enter().insert("path", "g")
   	  .attr("class", "link")
   	  .attr("d", function(d) {
   		var o = {x: source.x0, y: source.y0};
   		return diagonal({source: o, target: o});
   	  });

     // Transition links to their new position.
     link.transition()
   	  .duration(duration)
   	  .attr("d", diagonal);

     // Transition exiting nodes to the parent's new position.
     link.exit().transition()
   	  .duration(duration)
   	  .attr("d", function(d) {
   		var o = {x: source.x, y: source.y};
   		return diagonal({source: o, target: o});
   	  })
   	  .remove();

     // Stash the old positions for transition.
     nodes.forEach(function(d) {
   	    d.x0 = d.x;
   	    d.y0 = d.y;
     });
   }

   // Toggle children on click.
   function click(d) {
     if (d.children) {
   	d._children = d.children;
   	d.children = null;
     } else {
   	d.children = d._children;
   	d._children = null;
     }
     update(d);
   }

   function addToolTip(data, div, nodeInfo){
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
   };

   function hideToolTip(data, div){
     div.transition()
     .duration(500)
     .style("opacity", 0)
   }

   function findNodeFromGraph(name, version, nodes){

    var len = nodes.length
    for(var i=0; i<len; i++){
       if(nodes[i].name == name && nodes[i].version == version){
           return nodes[i]
       }
    }
    return null
   }


   function nodeColor(data){
      var color = d3.scale.category20();
      if(data.depth < 2){ //non-leave nodes
         return data._children ? "lightsteelblue" : "#fff";
      } else { //leave node with states
         var seq = data.name.split("#");
         var name = seq[0];
         var version = seq[1];
//         var n = findNodeFromGraph(name, version, nodeInfo);
         var n = data.group
         return n ? colorArray[n] : "white";
      }
   }
}




