require("/lib/REST.js");
require("/lib/RESTGenerator.js");

var irisREST = new REST("/rest/iris.yaml", "SQL");
var ignoreQuery = "o.Name NOT LIKE '\\_%'  ESCAPE '\\' AND o.Name NOT LIKE 'sta\\_%'  ESCAPE '\\' AND o.Name NOT LIKE 'tbl%'";

HTTPServer.on("connection", function(){
	let url = Request.getUrl().toLowerCase().split("?")[0].replace("http://localhost/rest", "");
	let urlPath = Request.getPath();

	if (url.endsWith("yaml")){
		RESTGenerator.generateOpenAPI("SQL", "yaml", ignoreQuery).then(yaml => {
			Response.setHeader("Content-Type", "text/vnd.yaml");
			end(yaml);
		}).catch(e => end(e.toString()));
	} else if (url.endsWith("json")){
		RESTGenerator.generateOpenAPI("SQL", "json", ignoreQuery).then(json => {
			Response.setHeader("Content-Type", "application/json");
			end(json);
		}).catch(e => end(e.toString()));
	} else if (url.endsWith("load")) {
		irisREST.reload().then(() => {
			end("Loaded OpenAPI definition");
		}).catch(end);
	} else {
		irisREST.process(url, urlPath);
	}
});