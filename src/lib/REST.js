require("/lib/yaml.js");
require("/lib/SQL.js");

class REST {

    constructor(file, connection){
        this.file = file;
        this.connection = connection;
        this.triggers = [];
        this.reload();
    }

    reload(){
        return IO.readText(this.file).then(text => {
            this.api = jsyaml.load(text);
            this.routes = {};
            this.links = {};

            for (const key in this.api.paths) {
                this.routes[key.toLowerCase()] = this.api.paths[key];
            }

            for (const key in this.api.links) {
                this.links[key.toLowerCase()] = this.api.links[key];
            }
        });
    }

    addTrigger(table, method, f){
        if (!(table in this.triggers)) this.triggers[table] = {};
        this.triggers[table][method] = f;
    }

    error(message, query){
        const response = {
            method: Request.getMethod().toUpperCase(),
            url: Request.getUrl(),
            code: "500",
            message: message
        };
        
        if (query) response.query = query;

        end(JSON.stringify({
            result: {},
            response: response
        }));
    }

    // Parses the filter value and translates it into an SQL query
    // Returns null if it failed, otherwise the SQL query as a string
    parseFilter(filterValue, table, createParam){
        // All simple operators can be mapped easily
        const simple = {
            "in": "IN",
            "nin": "NOT IN",
            "eq": "=",
            "neq": "<>",
            "lk": "LIKE",
            "nlk": "NOT LIKE",
            "nl": "IS NULL",
            "nnl": "IS NOT NULL",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<="
        };

        // Regex for filter operations
        const operationRegex = /\((.*?):(.*?):(.*?)\)/g;

        // Regex to check for valid tokens
        const validRegex = /\(|\)|and|or/g;

        // If the filter is not wrapper in parenthesis, do it here
        if (!filterValue.startsWith("(")) {
            filterValue = "(" + filterValue + ")";
        }

        // Check if there is any semicolons, this would allow for SQL injection
        if (filterValue.indexOf(";") != -1) {
            return this.error("Semicolons are not allowed in the filter parameter");
        }

        // Remove all operations and valid tokens, there shouldn't be anything left
        // If there is, throw an error
        const replaced = filterValue.replace(operationRegex, "").replace(validRegex, "");
        if (replaced.length > 0){
            return this.error("Invalid fields query. The following characters are not allowed: " + replaced);
        }

        // If there was an error, this will be true after the replace
        let foundError = false;
        
        const result = "(" + filterValue.replace(operationRegex, (match, left, operator, right, offset, string) => {
            // Prepend the table name to the column name
            // This is to avoid confusion with same named columns from included tables
            left = `${table}.${left}`;

            // If it is a simple operator, do the replacement according the mapping
            if (operator in simple) {
                return ` ${left} ${simple[operator]} ${createParam(right)} `;
            }

            // If it is not simple, return the custom query
            switch(operator) {
                case "bt": {
                    const values = right.split(",");
                    if (values.length != 2) {
                        this.error("Must specify two values seperated by comma in 'between' filter");
                        foundError = true;
                    }
                    return ` ${left} BETWEEN ${createParam(values[0])} AND ${createParam(values[1])} `;
                }

                case "nbt": {
                    const values = right.split(",");
                    if (values.length != 2) {
                        this.error("Must specify two values seperated by comma in 'not between' filter");
                        foundError = true;
                    }
                    return ` ${left} NOT BETWEEN ${createParam(values[0])} AND ${createParam(values[1])} `;
                }

                case "lkc":
                return ` ${left} LIKE ${createParam(right)} Collate SQL_Latin1_General_CP1_CS_AS `;

                case "nlkc":
                return ` ${left} NOT LIKE ${createParam(right)} Collate SQL_Latin1_General_CP1_CS_AS `;

                case "ct":
                return ` CONTAINS(${left}, ${createParam(right)}) `;

                case "nct":
                return ` NOT CONTAINS(${left}, ${createParam(right)}) `;

                case "ft":
                return ` FREETEXT(${left}, ${createParam(right)}) `;

                case "nft":
                return ` NOT FREETEXT(${left}, ${createParam(right)}) `;
            }

            // If the operator was not found, throw an error
            this.error(`Unknown operator '${operator}'`);
            foundError = true;
        }) + ")";

        // Return null if there was an error, otherwise return the result
        return foundError ? null : result;
    }

    process(url, urlPath) {
        // Set the output type to JSON
        Response.setHeader("Content-Type", "application/json");

        // Check if the API is loaded
        if (!("api" in this && "routes" in this && "links" in this)) {
            return this.error("The API has not loaded yet");
        }
        
        // Routing parameters
        const last = urlPath[urlPath.length - 1];
        const idPath = !isNaN(last);
        const id = idPath ? parseInt(last) : 0;
        url = url.replace(id.toString(), "{id}");

        // Check if the route exists in the API
        if (url in this.routes) {
            // Method to lower case because all methods in yaml are also lower case
            const method = Request.getMethod().toLowerCase();
            const path = this.routes[url];

            // Check if the HTTP method is defined in the API
            if (!(method in path)) {
                return this.error("Method " + method + " is not supported");
            }

            // Get the schema for the SQL table
            const def = path[method];
            const table = def.tags[0];
            const schema = this.api.components.schemas[table];
            let columns = Object.getOwnPropertyNames(schema.properties);

            // Url query parameters
            const params = Request.getParameters();

            // Variable that stores the SQL query
            let query = "";

            // Paging variables store page size and number
            let paging = {};
            let hasPaging = false;

            // Include variables store SQL join information
            let includes = [];
            let simpleLinks = [];
            let simpleTables = [];
            let unionLinks = [];
            let unionTables = [];
            let allFields = [];

            // Variables to store SQL parameters for sp_executesql function
            const sqlParams = {};
            let valueIndex = 1;

            // Function to create a new SQL parameter
            const createParam = function(value) {
                var key = "param" + valueIndex;
                valueIndex++;
                sqlParams[key] = value;
                return "@" + key;
            };
            
            if (method == "get") {
                // Variables to store SQL query information
                let orderBy = "ID ASC";
                paging.pageSize = 100;
                paging.pageNr = 1;
                const checks = [];

                // Loop all parameters to find special ones (fields, orderby, include, etc...)
                for (const key in params) {
                    const lowerKey = key.toLowerCase();

                    if (lowerKey == "fields") {
                        // Only keep columns that are in fields parameter
                        const fields = params[key].split(",").map(s => s.toLowerCase());
                        columns = columns.filter(column => fields.includes(column.toLowerCase()));
                    } else if (lowerKey == "orderby") {
                        orderBy = params[key];
                    } else if (lowerKey == "pagesize") {
                        paging.pageSize = parseInt(params[key]);
                        if (isNaN(paging.pageSize) || paging.pageSize <= 0) {
                            this.error(`Invalid page size argument '${params[key]}'. Page size must be an integer bigger than 0`);
                        }
                    } else if (lowerKey == "pagenr") {
                        paging.pageNr = parseInt(params[key]);
                        if (isNaN(paging.pageNr) || paging.pageNr <= 0) {
                            this.error(`Invalid page number argument '${params[key]}'. Page number must be an integer bigger than 0`);
                        }
                    } else if (lowerKey == "filter") {
                        const filter = this.parseFilter(params[key], table, createParam);
                        if (filter == null) return;
                        checks.push(filter);
                    } else if (lowerKey == "include") {
                        includes = params[key].split(",");
                    }
                }

                // Get all query parameters that are a column in the SQL table
                const columnParams = Object.getOwnPropertyNames(params).filter(param => param in schema.properties);

                // Function to create the WHERE part of the SQL query based on the checks and column parameters
                const createConditions = function() {
                    let q = "";

                    if (checks.length > 0 || columnParams.length > 0 || idPath) {
                        columnParams.map(param => `${table}.${SQL.escape(param)} = ${createParam(params[param])}`)
                            .forEach(item => checks.push(item));

                        // If it is an ID path e.g. "CCMCase/2"
                        if (idPath) {
                            checks.push(`${table}.ID = ${createParam(id)}`);
                        }

                        q += ` WHERE ${checks.join(" AND ")}`;
                    }

                    return q;
                };

                // Function to create the ORDER BY and paging part of the SQL query
                const createOrder = function() {
                    let q = "";

                    // This should be better
                    if (includes.length > 0) {
                        q += `ORDER BY ${table}_${orderBy}`;
                    } else {
                        q += `ORDER BY ${orderBy}`;
                    }

                    // Add paging if it is not an ID path (ID path only returns one result)
                    if (!idPath) {
                        hasPaging = true;

                        const pageSizeParam = createParam(paging.pageSize);
                        q += `
                            OFFSET ${pageSizeParam} * (${createParam(paging.pageNr)} - 1) ROWS 
                            FETCH NEXT ${pageSizeParam} ROWS ONLY
                        `;
                    }

                    return q;
                };

                if (includes.length > 0) {
                    // If there are includes, we need to name each parameter to know from which table it came
                    // Therefore we name all parameters with table_column, e.g. "CCMCase_Description"
                    allFields = columns.map(column => table + "_" + column);

                    for (const include of includes) {
                        const linkName = `${table}.${include}`.toLowerCase();

                        if (linkName in this.links) {
                            const link = this.links[linkName];

                            // Determine the type of join, (1 : N) relations require an SQL UNION to join the result
                            if (link["x-level"] >= 1) {
                                // N : 1 (only one result)
                                simpleLinks.push(link);
                            } else {
                                // 1 : N (multiple results)
                                unionLinks.push(link);
                            }

                            // Get the schema from the result table of the link
                            const linkTable = link["x-resultTable"];
                            const linkSchema = this.api.components.schemas[linkTable];

                            // Get all fields from the linked table's schema
                            allFields.push(...Object.getOwnPropertyNames(linkSchema.properties).map(prop => linkTable + "_" + prop));
                        }
                    }
                    
                    // Stores table names so this map has to be done only once
                    simpleTables = simpleLinks.map(l => l["x-resultTable"]);
                    unionTables = unionLinks.map(l => l["x-resultTable"]);

                    // Create the first part of the SQL query that selects the main table and all N : 1 joins
                    // Data_CTE contains the entire result and is used to get the count
                    // Fetch_CTE contains the part of the result determined by the paging parameters
                    query = `
                        WITH Data_CTE AS (
                            SELECT ${allFields.map(field => {
                                const fieldTable = field.split("_")[0];
                                const hasResult = fieldTable == table || simpleTables.includes(fieldTable);
                                return `${hasResult ? field.replace("_", ".") : "NULL"} AS ${field}`;
                            }).join(", ")}
                            FROM ${table}
                            ${simpleLinks.map(link => `LEFT OUTER JOIN ${link["x-resultTable"]} ON ${link["x-childTable"]}.${link["x-childColumn"]} = ${link["x-parentTable"]}.${link["x-parentColumn"]}`).join("\n")}
                            ${createConditions()}
                        ),
                        Fetch_CTE AS (
                            SELECT * FROM Data_CTE
                        )
                    `;
                    
                    // Loop over all 1 : N joins and add them as seperate results
                    // The results are called Union + table, e.g. UnionCCMCaseStatus
                    // The joins are made with the Fetch_CTE set, to only get the results within the paging parameters
                    for (const unionLink of unionLinks) {
                        const unionTable = unionLink["x-resultTable"];

                        query += `
                            , Union${unionTable} AS (
                                SELECT ${allFields.map(field => {
                                    const fieldTable = field.split("_")[0];
                                    if (field == table + "_ID") return `${field} AS ${field}`;
                                    const hasResult = unionTable == fieldTable;
                                    return `${hasResult ? field.replace("_", ".") : "NULL"} AS ${field}`;
                                }).join(", ")}
                                FROM Fetch_CTE
                                LEFT OUTER JOIN ${unionTable} ON ${unionLink["x-childTable"]}${unionLink["x-childTable"] == table ? "_" : "."}${unionLink["x-childColumn"]} = ${unionLink["x-parentTable"]}${unionLink["x-parentTable"] == table ? "_" : "."}${unionLink["x-parentColumn"]}
                            )
                        `;
                    }

                    // Get the total number of rows from the Data_CTE set
                    // Create a UNION between the Fetch_CTE set and all 1 : N included tables
                    // Add the total number of rows to each result with a CROSS JOIN
                    query += `
                        , Count_CTE AS (
                            SELECT COUNT(*) AS TotalRows FROM Data_CTE
                        )
                        SELECT * FROM (
                            SELECT * FROM Fetch_CTE
                            ${unionLinks.map(l => `
                                UNION ALL 
                                SELECT * FROM Union${l["x-resultTable"]}`).join("\n")
                            }
                        ) AS x
                        CROSS JOIN Count_CTE
                        ORDER BY ${table}_ID
                    `;
                } else {
                    // When there are no includes we don't have different tables and UNIONS
                    // Just get all columns by name in Data_CTE
                    // Return the count as a CROSS JOIN between Data_CTE and Count_CTE
                    query = `
                        WITH Data_CTE AS (
                            SELECT ${columns.join(", ")} 
                            FROM ${table}
                            ${createConditions()}
                        ),
                        COUNT_CTE AS (
                            SELECT COUNT(*) AS TotalRows From Data_CTE
                        )
                        SELECT * FROM Data_CTE
                        CROSS JOIN Count_CTE
                        ${createOrder()}
                    `;
                }
            } else if (method == "post") {
                const data = JSON.parse(Request.getContent());
                let columns = Object.getOwnPropertyNames(schema.properties).map( key => key.toLowerCase() );
                const keys = Object.getOwnPropertyNames(data).filter(key => columns.indexOf(key.toLowerCase()) != -1);


                // Create the INSERT query
                // Return the ID of the inserted row
                query = `
                    INSERT INTO 
                        ${table} 
                        (${keys.map(key => SQL.escape(key)).join(", ")}) 
                    VALUES 
                        (${keys.map(key => createParam(data[key])).join(", ")});
                    SELECT SCOPE_IDENTITY() AS ID;
                `;
            } else if (method == "patch") {
                // PATCH and PUT require an ID
                if (!idPath) {
                    return this.error("No ID specified in path for message " + method.toUpperCase());
                }

                // Because off backwards compatability, there is no difference between PATCH and PUT
                // Normally PUT would require all columns, but here that is not the case

                const data = JSON.parse(Request.getContent());
                let columns = Object.getOwnPropertyNames(schema.properties).map( key => key.toLowerCase() );
                const keys = Object.getOwnPropertyNames(data).filter(key => columns.indexOf(key.toLowerCase()) != -1);

                // Create the UPDATE query
                // Return the ID of the updated row
                query = `
                    UPDATE 
                        ${table} 
                    SET
                        ${keys.map(key => `${SQL.escape(key)} = ${createParam(data[key])}`).join(", ")}
                    WHERE 
                        ID = ${createParam(id)};
                    SELECT ${createParam(id)} AS ID;
                `;
            } else if (method == "put") {
                // PATCH and PUT require an ID
                if (!idPath) {
                    return this.error("No ID specified in path for message " + method.toUpperCase());
                }

                // Normally PUT would set NULL to columns not provided

                const data = JSON.parse(Request.getContent());
                let columns = Object.getOwnPropertyNames(schema.properties).map( key => key.toLowerCase() );
                const keys = Object.getOwnPropertyNames(data).filter(key => columns.indexOf(key.toLowerCase()) != -1);

                // Create the UPDATE query
                // Return the ID of the updated row
                query = `
                    UPDATE 
                        ${table} 
                    SET
                        ${columns.filter(key => key.toLowerCase() != "id").map(key => `${SQL.escape(key)} = ${createParam(data[key]?data[key]:null)}`).join(", ")}
                    WHERE 
                        ID = ${createParam(id)};
                    SELECT ${createParam(id)} AS ID;
                `;
            } else if (method == "delete") {
                // DELETE requires an ID
                if (!idPath) {
                    return this.error("No ID specified in path for message DELETE");
                }

                // Create the DELETE query
                // Return the ID of the deleted row
                query = `
                    DELETE FROM ${table} 
                    WHERE ID = ${createParam(id)}; 
                    SELECT ${createParam(id)} AS ID;
                `;
            } else {
                // The HTTP method is not supported
                return this.error(method + " is not a valid method");
            }

            // Wrap the SQL query in a try catch to return custom error messages for constrainsts
            query = `
                BEGIN TRY
                    ${query}
                END TRY
                BEGIN CATCH
                    DECLARE @Msg NVARCHAR(4000)

                    IF EXISTS (
                        SELECT 1 
                        FROM Information_schema.Routines 
                        WHERE specific_schema = 'dbo' 
                        AND specific_name = 'fnGetClearErrorMessage' 
                        AND routine_type = 'FUNCTION'
                    )
                        SET @Msg = dbo.fnGetClearErrorMessage();
                    ELSE
                        SET @Msg = ERROR_MESSAGE();
                    
                    THROW 60001, @Msg, 1;
                END CATCH
            `;

            // Wrap the SQL query in an sp_executesql call
            // Note: there shouldn't be any strings or numbers in this query since they are added as parameters
            // To be sure though, all single quotes are replaced by double single quotes since the query
            // is placed in a string
            query = `EXECUTE sp_executesql N'${query.replace(/'/g, "''")}'`;

            // Add the parameters after the sp_executesql call
            // Type is determined between int, float and varchar
            // Strings are put in quotes
            const sqlParamKeys = Object.getOwnPropertyNames(sqlParams);
            if (sqlParamKeys.length > 0) {
                query += ", N'" + sqlParamKeys.map(key => {
                    const value = sqlParams[key];

                    // Determine the SQL type of the value (int, float or varchar)
                    const isNumber = /^\d+(\.\d+)?$/.test(value);
                    const sqlType = isNumber ? (parseFloat(value) == parseInt(value) ? "int" : "float") : "varchar(MAX)";
                    return `@${key} ${sqlType}`;
                }).join(", ") + "', " + sqlParamKeys.map(key => `@${key} = ${typeof sqlParams[key] == "string" ? `'${SQL.escape(sqlParams[key])}'` : sqlParams[key]}`).join(", ");
            }

            // Execute the SQL query on the database
            SQL.execute(this.connection, query).then(result => {
                if (includes.length > 0) {
                    const newResult = [];

                    for (const item of result) {
                        if (newResult.length == 0 || newResult[newResult.length - 1]["ID"] != item[table + "_ID"]) newResult.push({});
                        const currentResult = newResult[newResult.length - 1];
                        let union = null;

                        for (const key in item) {
                            if (key == "TotalRows") {
                                currentResult.TotalRows = item[key];
                            }

                            const parts = key.split("_");

                            if (parts.length == 2) {
                                const tableName = parts[0];
                                const columnName = parts[1];

                                if (unionTables.includes(tableName)) {
                                    if (item[tableName + "_ID"] != null){
                                        if (!(tableName in currentResult)) currentResult[tableName] = [];
                                        if (union === null) {
                                            union = {};
                                            currentResult[tableName].push(union);
                                        }
                                        union[columnName] = item[key];
                                    }
                                } else if (tableName == table) {
                                    if (item[key] != null || !(columnName in currentResult)) {
                                        currentResult[columnName] = item[key];
                                    }
                                } else {
                                    if (!(tableName in currentResult)) currentResult[tableName] = {};

                                    if (item[key] != null || !(columnName in currentResult[tableName])) {
                                        currentResult[tableName][columnName] = item[key];
                                    }
                                }
                            }
                        }
                    }

                    result = newResult;
                }

                if (table in this.triggers && method in this.triggers[table]) {
                    result = this.triggers[table][method](result);
                }

                let resultObj = {
                    result: result
                };

                if (hasPaging) {
                    var totalRows = result.length > 0 ? result[0].TotalRows : 0;
                    var totalPages = Math.ceil(totalRows / paging.pageSize);

                    resultObj.paging = {
                        rowCount: totalRows,
                        pageSize: paging.pageSize,
                        pageCount: totalPages,
                        pageNr: paging.pageNr,
                        firstIndexOnPage: result.length > 0 ? result[0].ID : 0,
                        lastIndexOnPage: result.length > 0 ? result[result.length - 1].ID : 0,
                        hasPreviousPage: paging.pageNr > 1,
                        hasNextPage: paging.pageNr < totalPages,
                        isFirstPage: paging.pageNr == 1,
                        isLastPage: paging.pageNr == totalPages
                    };

                    for (let item of result) {
                        delete item.TotalRows;
                    }
                }

                resultObj.response = {
                    method: method.toUpperCase(),
                    url: url,
                    code: "200"
                };

                end(JSON.stringify(resultObj,null,2));
            }).catch(e => {
                const message = e.toString();

                // Try and extract the error message
                // This error message will be there when an SQL constraint failed and a custom error message was set
                const match = message.match(/ERROR: (.*?)$/);
                if (match && typeof match[1] == "string") {
                    this.error(match[1]);
                } else {
                    this.error((e.stack || e).toString(), query);
                }
            });
        } else {
            this.error("That resource doesn't exist");
        }
    }
}