

var colorArray = ["white", "green", "blue", "orange", "red", "purple",  "black"]

function createDAG(elem, graph) {

  var width = 560,
      height = 400;

  var force = d3.layout.force()
      .charge(-300)
      .linkDistance(120)
      .size([width, height]);

  var svg = d3.select("#" + elem).append("svg")
      .attr("width", width)
      .attr("height", height);

  force
      .nodes(graph.nodes)
      .links(graph.links)
      .start();

  renderWithCurveArrows(svg, force, graph)


};

function renderWithCurveArrows(svg, force, graph){

  var color = d3.scale.category20();

  var div = d3.select("body").append("div")
      .attr("class", "tooltip")
      .style("opacity", 0);

      // build the arrow.
  svg.append("svg:defs").selectAll("marker")
     .data(["arrow"])      // Different link/path types can be defined here
     .enter().append("svg:marker")    // This section adds in the arrows
     .attr("id", String)
     .attr("viewBox", "0 -5 10 10")
     .attr("refX", 35)
     .attr("refY", -1.5)
     .attr("markerWidth", 5)
     .attr("markerHeight", 5)
     .attr("orient", "auto")
     .append("svg:path")
     .attr("d", "M0,-5L10,0L0,5");

  var link = svg.append("svg:g").selectAll("path")
      .data(force.links())
      .enter().append("svg:path")
      .attr("class", function(d) { return "link " + d.type; })
//      .attr("class", "link")
      .attr("marker-end", "url(#arrow)");


  // define the nodes
  var node = svg.selectAll(".node")
      .data(graph.nodes)
      .enter().append("g")
      .attr("class", "node")
      .call(force.drag);

    // add the nodes
  node.append("circle")
      .attr("r", 12)
      .on("mouseover", function(d){ return addToolTip(d, div); })
      .on("mouseout", function(d){ return hideToolTip(d, div); })
      .style("fill", function(d) { return colorArray[d.group]; });

//  node.append("title")
//      .text(function(d) { return d.name; });

//add the text
  node.append("text")
      .attr("x", 15)
      .attr("dy", ".35em")
      .attr("class", "text")
      .text(function(d) { return d.name; });

// add the curvy lines
  function tick() {
        link.attr("d", function(d) {
            var dx = d.target.x - d.source.x,
                dy = d.target.y - d.source.y,
                dr = Math.sqrt(dx * dx + dy * dy);
            return "M" +
                d.source.x + "," +
                d.source.y + "A" +
                dr + "," + dr + " 0 0,1 " +
                d.target.x + "," +
                d.target.y;
        });

        node
            .attr("transform", function(d) {
      	    return "translate(" + d.x + "," + d.y + ")"; });
   };


  force.on("tick", tick);

}

function renderWithLines(svg, force, graph) {

  var color = d3.scale.category20();
  var div = d3.select("body").append("div")
      .attr("class", "tooltip")
      .style("opacity", 0);

  svg.append("svg:defs").selectAll("marker")
     .data(["arrow"])      // Different link/path types can be defined here
     .enter().append("svg:marker")    // This section adds in the arrows
     .attr("id", String)
     .attr("viewBox", "0 -5 10 10")
     .attr("refX", 20)
     .attr("refY", -1.5)
     .attr("markerWidth", 5)
     .attr("markerHeight", 5)
     .attr("orient", "auto")
     .append("svg:path")
     .attr("d", "M0,-5L10,0L0,5");

  var link = svg.selectAll(".link")
      .data(graph.links)
      .enter().append("line")
      .attr("class", "link")
      .attr("marker-end", "url(#arrow)");
//      .style("stroke-width", function(d) { return Math.sqrt(d.value); });

  var node = svg.selectAll(".node")
      .data(graph.nodes)
      .enter().append("g")
      .append("circle")
      .attr("class", "node")
      .attr("r", 6)
      .on("mouseover", function(d){ return addToolTip(d, div); })
      .on("mouseout", function(d){ return hideToolTip(d, div); })
      .style("fill", function(d) { return colorArray[d.group]; })
      .call(force.drag);

  node.append("title")
      .text(function(d) { return d.name; });

  force.on("tick", function() {
    link.attr("x1", function(d) { return d.source.x; })
        .attr("y1", function(d) { return d.source.y; })
        .attr("x2", function(d) { return d.target.x; })
        .attr("y2", function(d) { return d.target.y; });

    node.attr("cx", function(d) { return d.x; })
        .attr("cy", function(d) { return d.y; });
  });

}

   function addToolTip(d, div){
        var options = {
           year: "numeric", month: "numeric", day: "numeric",
           hour: "2-digit", minute: "2-digit", second: "2-digit"
        };

        lineID = d.name;

        div.transition()
           .duration(200)
           .style("opacity", 9);

        div.html("name: " + d.name
                + "<br/>" + "version: " + d.version
                + "<br/>" + "type: " + d.type
                + "<br/>" + "startTime: " + new Date(d.startTime).toLocaleTimeString("en-US", options)
                + "<br/>" + "endTime: " + new Date(d.endTime).toLocaleTimeString("en-US", options)
                + "<br/>" + "state: " + d.status)
           .style("left", (d3.event.pageX) + "px")
           .style("top", (d3.event.pageY + 10) + "px");
   };

   function hideToolTip(d, div){
     div.transition()
     .duration(500)
     .style("opacity", 0)
   }



