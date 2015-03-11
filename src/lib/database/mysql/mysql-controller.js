/* jslint node: true, vars: true, white: true */
"use strict";

var Promise = require("node-promise").Promise;

//Module requirements
var mysql         = require('mysql');
var mysqlSettings = require('./mysql-settings.json');

var connection = mysql.createConnection({
  host     : mysqlSettings.host,
  user     : mysqlSettings.username, //Amazing username & pw
  password : mysqlSettings.password,
  database : mysqlSettings.database
});

var connect = function(fn) {
	connection.connect(function(err) {
		if (err) {
			console.log("MySQL Connection Error: " + err);
			process.exit(1);
		} else {
			console.log("Connected to MySQL database");
		}

		if (typeof(fn) === "function") {
			fn();
		}
	});
};

var getTableNames = function(fn) {
	connection.query('SHOW TABLES', function(err, tableNames) {

		var tableStrings = [];

		tableNames.forEach(function(value) {
			for (var key in value) {
				if (value.hasOwnProperty(key)) {
					tableStrings.push(value[key]);
				}
			}
		});

		fn(tableStrings);
	});
};

var getTableColumns = function(tableName, fn) {
	connection.query('SHOW COLUMNS FROM ??', [tableName], function(err, columns) {
		if (err) {
			return fn(err);
		}

		var columnNames = columns.map(function(value) {
			if (value.hasOwnProperty('Field')) {
				return value.Field;
			}
		});

		fn(undefined, columnNames);
	});
};

var DatabaseQuery = function(tableName, masks) {
	this.table = tableName;
	this.masks = masks;
};

DatabaseQuery.prototype._streamingQuery = function(queryString, inserts, rowProcessor) {
	var promise = new Promise();

	var sql = mysql.format(queryString, inserts);

	var query = connection.query(sql);

	query
	.on('error', function(err) {
		promise.reject(err);
	})
	.on('result', function(row) {
		rowProcessor(row);
	})
	.on('end', function() {
		promise.resolve();
	});

	return promise;
};

DatabaseQuery.prototype.baseQuery = function() {
	var self = this;

	return function(fields, rowProcessor) {
		var queryString = 'SELECT ?? FROM ??';
		var inserts = [fields, self.table];

		return self._streamingQuery(queryString, inserts, rowProcessor);
	};
};


DatabaseQuery.prototype.selectorQuery = function(column) {
	var self = this;

	return function(fields, constraintSet, rowProcessor) {
		var queryString = 'SELECT ?? FROM ?? WHERE ?? IN (?)';
		var inserts = [fields, self.table, column, constraintSet];

		return self._streamingQuery(queryString, inserts, rowProcessor);
	};
};

module.exports = function() {
	
	return {
		connect: connect,
		getTableNames: getTableNames,
		getTableColumns: getTableColumns,
		DatabaseQuery: DatabaseQuery
	};
};
