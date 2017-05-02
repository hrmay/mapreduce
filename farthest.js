var mapCode = function() {
   emit(this.CountryID,
     { "data":
		[
			{
				"name": this.City,
				"lat":  this.Latitude,
				"lon":  this.Longitude
			}
		]
	});
};

var reduceCode = function(key, values) {

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

	if (reduced.data.length == 1) {
		return { "message" : "This Country contains only 1 City" };
	}

	//get the max distance between two cities and store them
	var max_dist = 0;
	var city1 = { "name": "" };
	var city2 = { "name": "" };

	var c1; //city 1
	var c2; //city 2
	var d;  //distance between the two cities
	//loop through all pairs of cities
	for (var i in reduced.data) {
		for (var j in reduced.data) {
			if (i>=j) continue;
			c1 = reduced.data[i]; 
			c2 = reduced.data[j]; 
			d = Math.sqrt((c1.lat-c2.lat)*(c1.lat-c2.lat)+(c1.lon-c2.lon)*(c1.lon-c2.lon)); //calculate euclidean distance between two cities
			if (d > max_dist) {
				max_dist = d;
				city1 = c1;
				city2 = c2;
			}
		}
	}

	return {"city1": city1.name, "city2": city2.name, "dist": max_dist};
};
 
 db.cities.mapReduce(mapCode, reduceCode, 
   { query: {CountryID: { $ne: 254 }},
   out: "farthest",
   finalize: finalize});
   
db.farthest.find().forEach(printjson)