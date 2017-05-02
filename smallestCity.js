var mapState = function() {
   emit({"city":this.city, "state":this.state},
   { "data":
   	[
   		{
   			"pop":this.pop
   		}
   	]
   }
   );
};

var reduceState = function(key, values) {
	//printjson(values);
	var reduced = {"data":[]};
	for (var i in values) {
		var inter = values[i];
		for (var j in inter.data) {
			reduced.data.push(inter.data[j]);
		}
	}

	return reduced;
};

var finalize =  function (key, reduced) {
	//printjson(reduced);
	var cityName = { "name": "" };
	var pop = 0;

	for (var i in reduced.data) {
		//print(reduced.data[i].zip);
		pop += reduced.data[i].pop;
	}

	return {"pop": pop};
};

var mapCity = function() {
   emit(this._id.state,
   { "data":
   	[
   		{
   			"city":this._id.city,
   			"pop":this.value.pop
   		}
   	]
   }
   );
};

var reduceCity = function(key, values) {
	//printjson(values);
	var reduced = {"data":[]};
	for (var i in values) {
		var inter = values[i];
		for (var j in inter.data) {
			reduced.data.push(inter.data[j]);
		}
	}

	return reduced;
};

var finalizeSmallest = function(key, reduced) {
	
	if (reduced.data.length == 1) {
		return {'city': reduced.data.city, 'pop': reduced.data.pop};
	}
	
	var min_pop = 0;
	var cityName = "";
	
	var c;
	
	for (var i in reduced.data) {
		c = reduced.data[i];
		//printjson(c);
		//going to ignore towns with a population of 0, since they're likely errors
		if (c.pop > min_pop) {
			min_pop = c.pop;
			cityName = c.city;
		}
	}
	
	return {'city': cityName, "pop": min_pop};
	
};

 
 db.zips.mapReduce(mapState, reduceState, 
   { query: {},
   out: "cityPops",
   finalize:finalize});
/*   
 db.states.mapReduce(mapCity, reduceCity,
 	{query: {},
 	out: "cityPops",
 	finalize: finalize
 });

db.cityPops.find().forEach(printjson)
*/

db.cityPops.mapReduce(mapCity, reduceCity,
	{query: {},
	 out: "smallestCity",
	 finalize:finalizeSmallest
	});
	
db.smallestCity.find().forEach(printjson);