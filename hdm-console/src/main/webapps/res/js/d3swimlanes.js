
var d3swimlane = {

   displayLanes: function (laneData, svgParent, startRange, endRange){
        var lanes = laneData.lanes
        , items = laneData.items
        , now = new Date().getTime();

        var margin = {top: 20, right: 15, bottom: 15, left: 120}
          , width = 1400 - margin.left - margin.right
          , height = lanes.length * 100 - margin.top - margin.bottom
          , miniHeight = lanes.length * 12 + 50
          , mainHeight = height - miniHeight - 50;

        var x = d3.scale.linear()
        	.domain([startRange, endRange])
        	.range([0, width]);
        var x1 = d3.scale.linear()
//        .domain([0, 500])
        .range([0, width]);

        var ext = d3.extent(lanes, function(d) { return d.id; });
        var y1 = d3.scale.linear().domain([ext[0], ext[1] + 1]).range([0, mainHeight]);
        var y2 = d3.scale.linear().domain([ext[0], ext[1] + 1]).range([0, miniHeight]);

        var parent = d3.select(svgParent);
        parent.select('svg').remove();
        var chart = parent.append('svg:svg')
        	.attr('width', width + margin.right + margin.left)
        	.attr('height', height + margin.top + margin.bottom)
        	.attr('class', 'chart');

        chart.append('defs').append('clipPath')
        	.attr('id', 'clip')
        	.append('rect')
        		.attr('width', width)
        		.attr('height', mainHeight);

        var mini = chart.append('g')
        	.attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
        	.attr('width', width)
        	.attr('height', miniHeight)
        	.attr('class', 'mini');

        var main = chart.append('g')
        	.attr('transform', 'translate(' + margin.left + ',' + (miniHeight + 60) + ')')
        	.attr('width', width)
        	.attr('height', mainHeight)
        	.attr('class', 'main');


        // draw the lanes for the main chart
        main.append('g').selectAll('.laneLines')
        	.data(lanes)
        	.enter().append('line')
        	.attr('x1', 0)
        	.attr('y1', function(d) { return d3.round(y1(d.id)) + 0.5; })
        	.attr('x2', width)
        	.attr('y2', function(d) { return d3.round(y1(d.id)) + 0.5; })
        	.attr('stroke', function(d) { return d.label === '' ? 'white' : 'lightgray' });

        main.append('g').selectAll('.laneText')
        	.data(lanes)
        	.enter().append('text')
        	.text(function(d) { return d.label; })
        	.attr('x', -10)
        	.attr('y', function(d) { return y1(d.id + .5); })
        	.attr('dy', '0.5ex')
        	.attr('text-anchor', 'end')
        	.attr('class', 'laneText');

        // draw the lanes for the mini chart
        mini.append('g').selectAll('.laneLines')
        	.data(lanes)
        	.enter().append('line')
        	.attr('x1', 0)
        	.attr('y1', function(d) { return d3.round(y2(d.id)) + 0.5; })
        	.attr('x2', width)
        	.attr('y2', function(d) { return d3.round(y2(d.id)) + 0.5; })
        	.attr('stroke', function(d) { return d.label === '' ? 'white' : 'lightgray' });

        mini.append('g').selectAll('.laneText')
        	.data(lanes)
        	.enter().append('text')
        	.text(function(d) { return d.label; })
        	.attr('x', -10)
        	.attr('y', function(d) { return y2(d.id + .5); })
        	.attr('dy', '0.5ex')
        	.attr('text-anchor', 'end')
        	.attr('class', 'laneText');

        // draw the x axis, to be correct
		var xDateAxis = d3.svg.axis()
        	.scale(x)
        	.orient('bottom')
        	.ticks(d3.time.millSeconds,  5)
        	.tickSize(6, 0, 0);

        var x1DateAxis = d3.svg.axis()
        	.scale(x1)
        	.orient('bottom')
        	.ticks(d3.time.millSeconds, 5)
        	.tickSize(6, 0, 0);

        var xMonthAxis = d3.svg.axis()
        	.scale(x)
        	.orient('top')
        	.ticks(5)
        	.tickSize(10, 0, 0);

        var x1MonthAxis = d3.svg.axis()
        	.scale(x1)
        	.orient('top')
        	.ticks(5)
        	.tickSize(10, 0, 0);

        main.append('g')
        	.attr('transform', 'translate(0,' + mainHeight + ')')
        	.attr('class', 'main axis date')
        	.call(x1DateAxis);

//        main.append('g')
//        	.attr('transform', 'translate(0,0.5)')
//        	.attr('class', 'main axis month')
//        	.call(x1MonthAxis)
//        	.selectAll('text')
//        		.attr('dx', 5)
//        		.attr('dy', 12);

        mini.append('g')
        	.attr('transform', 'translate(0,' + miniHeight + ')')
        	.attr('class', 'axis date')
        	.call(xDateAxis);

//        mini.append('g')
//        	.attr('transform', 'translate(0,0.5)')
//        	.attr('class', 'axis month')
//        	.call(xMonthAxis)
//        	.selectAll('text')
//        		.attr('dx', 5)
//        		.attr('dy', 12);

        // draw a line representing today's date
        main.append('line')
        	.attr('y1', 0)
        	.attr('y2', mainHeight)
        	.attr('class', 'main todayLine')
        	.attr('clip-path', 'url(#clip)');

        mini.append('line')
        	.attr('x1', x(now) + 0.5)
        	.attr('y1', 0)
        	.attr('x2', x(now) + 0.5)
        	.attr('y2', miniHeight)
        	.attr('class', 'todayLine');

        // draw the items
        var itemRects = main.append('g')
        	.attr('clip-path', 'url(#clip)');

        mini.append('g').selectAll('miniItems')
        	.data(getPaths(items))
        	.enter().append('path')
        	.attr('class', function(d) { return 'miniItem ' + d.class; })
        	.attr('d', function(d) { return d.path; });

        // invisible hit area to move around the selection window
        mini.append('rect')
        	.attr('pointer-events', 'painted')
        	.attr('width', width)
        	.attr('height', miniHeight)
        	.attr('visibility', 'hidden')
        	.on('mouseup', moveBrush);

        // draw the selection area
        var brushRange = startRange + (endRange - startRange)/5
        var brush = d3.svg.brush()
        	.x(x)
        	.extent([startRange, brushRange])
        	.on("brush", display);

        mini.append('g')
        	.attr('class', 'x brush')
        	.call(brush)
        	.selectAll('rect')
        		.attr('y', 1)
        		.attr('height', miniHeight - 1);

        mini.selectAll('rect.background').remove();
        display();

        function display () {

        	var rects, labels
        	  , minExtent = brush.extent()[0]
        	  , maxExtent = brush.extent()[1]
        	  , visItems = items.filter(function (d) { return d.start < maxExtent && d.end > minExtent});

        	mini.select('.brush').call(brush.extent([minExtent, maxExtent]));

        	x1.domain([minExtent, maxExtent]);

        	if ((maxExtent - minExtent) > 1468800000) {
        		x1DateAxis.ticks(d3.time.mondays, 1).tickFormat(d3.time.format('%a %d'))
        		x1MonthAxis.ticks(d3.time.mondays, 1).tickFormat(d3.time.format('%b - Week %W'))
        	}
        	else if ((maxExtent - minExtent) > 172800000) {
        		x1DateAxis.ticks(d3.time.days, 1).tickFormat(d3.time.format('%a %d'))
        		x1MonthAxis.ticks(d3.time.mondays, 1).tickFormat(d3.time.format('%b - Week %W'))
        	}
        	else {
        		x1DateAxis.ticks(d3.time.millSeconds, 5)
        		x1MonthAxis.ticks(d3.time.millSeconds, 1)
        	}


        	//x1Offset.range([0, x1(d3.time.day.ceil(now) - x1(d3.time.day.floor(now)))]);

        	// shift the today line
        	main.select('.main.todayLine')
        		.attr('x1', x1(now) + 0.5)
        		.attr('x2', x1(now) + 0.5);

        	// update the axis
        	main.select('.main.axis.date').call(x1DateAxis);
        	main.select('.main.axis.month').call(x1MonthAxis)
        		.selectAll('text')
        			.attr('dx', 5)
        			.attr('dy', 12);

        	// upate the item rects
        	rects = itemRects.selectAll('rect')
        		.data(visItems, function (d) { return d.id; })
        		.attr('x', function(d) { return x1(d.start); })
        		.attr('width', function(d) { return x1(d.end) - x1(d.start); });

        	rects.enter().append('rect')
        		.attr('x', function(d) { return x1(d.start); })
        		.attr('y', function(d) { return y1(d.lane) + .1 * y1(1) + 0.5; })
        		.attr('width', function(d) { return x1(d.end) - x1(d.start); })
        		.attr('height', function(d) { return .8 * y1(1); })
        		.attr('class', function(d) { return 'mainItem ' + d.class; });

        	rects.exit().remove();

        	// update the item labels
        	labels = itemRects.selectAll('text')
        		.data(visItems, function (d) { return d.id; })
        		.attr('x', function(d) { return x1(Math.max(d.start, minExtent)) + 2; });

        	labels.enter().append('text')
        		.text(function (d) { return  d.desc; })
        		.attr('x', function(d) { return x1(Math.max(d.start, minExtent)) + 2; })
        		.attr('y', function(d) { return y1(d.lane) + .4 * y1(1) + 0.5; })
        		.attr('text-anchor', 'start')
        		.attr('class', 'itemLabel');

        	labels.exit().remove();
        }

        function moveBrush () {
        	var origin = d3.mouse(this)
        	//alert(origin[0]);
        	var point = origin[0];


        	var halfExtent = (brush.extent()[1] - brush.extent()[0]) / 2
        	  , start = point - halfExtent
        	  , end = point + halfExtent;

        	brush.extent([start,end]);
        	display();
        }

        // generates a single path for each item class in the mini display
        // ugly - but draws mini 2x faster than append lines or line generator
        // is there a better way to do a bunch of lines as a single path with d3?
        function getPaths(items) {
        	var paths = {}, d, offset = .5 * y2(1) + 0.5, result = [];
        	for (var i = 0; i < items.length; i++) {
        		d = items[i];
        		if (!paths[d.class]) paths[d.class] = '';
        		paths[d.class] += ['M',x(d.start),(y2(d.lane) + offset),'H',x(d.end)].join(' ');
        	}

        	for (var className in paths) {
        		result.push({class: className, path: paths[className]});
        	}

        	return result;
        }
   }



}