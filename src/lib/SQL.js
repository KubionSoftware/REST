var SQL = {};

SQL.execute = async function(connection, query) {
    var result = await SQLAPI.execute(connection, query);
    return JSON.parse(result);
};

SQL.escape = function(str) {
    return str.replace(/[\0\x08\x09\x1a\n\r"'\\\%]/g, function (char) {
        switch (char) {
            case "\0":
                return "\\0";
            case "\x08":
                return "\\b";
            case "\x09":
                return "\\t";
            case "\x1a":
                return "\\z";
            case "\n":
                return "\\n";
            case "\r":
                return "\\r";
            case "\"":
            case "'":
            case "\\":
            case "%":
                return "\\"+char; // prepends a backslash to backslash, percent,
                                  // and double/single quotes
        }
    });
};

SQL.format = function(query, data){
	for (let key in data){
		let value = data[key];
		let valueType = typeof value;
		let sqlValue = "";

		if (valueType == "number"){
			sqlValue = value.toString();
		} else if (valueType == "string"){
			sqlValue = "'" + SQL.escape(value) + "'";
		} else {
			sqlValue = "NULL";
		}

		query = query.replace(new RegExp("\\{" + key + "\\}", "g"), sqlValue);
	}

	return query;
};