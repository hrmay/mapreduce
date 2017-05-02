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
/*
var mapCity = function() {
	emit(this._id,
		{"data":
			[
				{
					"pop": this.value.data[].pop,
					"zip": this.zip
				}
			]
		});
};

var reduceCity = function(key, values) {
	var reduced = {"data":[]};
	for (var i in values) {
		var inter = values[i];
		for (var j in inter.data) {
			reduced.data.push(inter.data[j]);
		}
	}

	//printjson(reduced.data);
	return reduced;
};
*/
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
   */
db.cityPops.find().forEach(printjson)