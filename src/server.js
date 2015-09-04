// server.js (Express 4.0)
var express        = require('express');
var morgan         = require('morgan');
var db             = require('./lib/database/db-controller')();
var program        = require('commander');
var Mask           = require('./lib/mask/mask');

var app            = express();


program
  .version('0.2.2')
  .option('-p, --port <n>', 'Listen port [7000]', 7000)
  .parse(process.argv);

//Setup logger
app.use(morgan('dev'));

//Connect to database & cache
db.connect(function() {
	db.getTableNames(function(tableNames) {
		tableNames.forEach(function(tableName) {
			db.getTableColumns(tableName, function(err, columns) {
				addQueryRoute(tableName, columns);
			});
		});
	});
});


var apiPrefix = "/api/";

var addQueryRoute = function(tableName, columnNames) {
	console.log("Adding query routes for:" + tableName);
	var masks = createMasks(columnNames);
	var dbQuery = new db.DatabaseQuery(tableName, masks);

	var baseQuery = dbQuery.baseQuery();

	var fullDBRoute =  apiPrefix + "db/" + tableName;
	var fullMaskRoute = apiPrefix + "masks/" + tableName;

	app.get(fullDBRoute, function(req, res) {

		console.log("Full route requested");

		res.set('Content-Type', 'application/json');

		var fields = masks.checkMask(req.query.fields);
		
		if (fields.length === 0) {
			fields = masks.getAllValues();
		}

		res.write(" { \"data\": [");

		baseQuery(fields, function(row) {
			//console.log("Writing row : " + (++i));
			res.write(JSON.stringify(row) + ",");
			//callback();
		}).then(function(result) {
			res.write("]}");
			res.end();
		});
	});

	app.get(fullMaskRoute, function(req, res) {

		console.log("Full route requested");

		res.set('Content-Type', 'application/json');

		var masksString = JSON.stringify(masks.getReverseMasks());

		res.send(masksString);
	});

	columnNames.forEach(function(columnName) {

		var selectorQuery = dbQuery.selectorQuery(columnName);

		console.log("Adding route - " + fullDBRoute + "/" + columnName + "/:id");

		app.get(fullDBRoute + "/" + columnName + "/:id", function(req, res) {

			console.log("Column route requested");

			res.set('Content-Type', 'application/json');

			var fields = masks.checkMask(req.query.fields);
			
			if (fields.length === 0) {
				fields = masks.getAllValues();
			}

			res.write(" { \"data\": [");

			selectorQuery(fields, req.params.id.split(","),  function(row) {
				res.write(JSON.stringify(row) + ",");
			}).then(function(result) {
				res.write("]}");
				res.end();
			});
		});


	});
};

var createMasks = function(columnNames) {
	columnNames.sort();
	return new Mask(columnNames);
};


//Listen on required port
app.listen(program.port);

console.log('Listening on port ' + program.port);
