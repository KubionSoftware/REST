require("/lib/yaml.js");
require("/lib/SQL.js");
require("/lib/Util.js");

class RESTGenerator {

    // Gets the definition of all tables and columns from an existing SQL database
    static async getSQLDefinition(connection, ignoreQuery){

        // Query to return all columns from all tables, and their properties from the database
        var query = `
            SELECT 
                o.Name AS tableName,
                c.Name AS name,
                t.Name AS type,
                c.max_length  AS length,
                c.is_computed AS computed,
                c.is_identity AS 'identity',
                c.is_nullable AS nullable,
                t.precision AS precision
            FROM 
                sys.columns c 
            INNER JOIN 
                sys.objects o ON o.object_id = c.object_id
            LEFT JOIN  
                sys.types t on t.user_type_id  = c.user_type_id   
            WHERE 
                o.type in ('U', 'V') ${ignoreQuery.length > 0 ? `AND (${ignoreQuery})` : ""}
            ORDER BY 
                o.Name, c.column_id
        `;

        // Execute the query
        var info = await SQL.execute(connection, query);

        // Group columns by table
        var tables = Util.groupBy(info, "tableName");

        // Change from array of columns, to object with key is column name and value is column properties
        for (var table in tables) {
            tables[table] = tables[table].reduce((a, v) => {
                a[v.name] = v; 
                return a;
            }, {});
        }
        
        // Put tables in definition object
        var definition = {
            tables: tables
        };

        // Query to get all foreign keys between different tables in the database
        var relationQuery = load("/lib/RESTQuery.sql");

        // Execute the query
        var relations = await SQL.execute(connection, relationQuery);

        // Change relations from array to object with key being the main table name and the value being the properties of the foreign key
        // Also filter out all foreign keys that are connected to a table not in this database
        definition.relations = relations.reduce((obj, relation) => {
            if (relation.maintable in definition.tables) {
                if (!(relation.maintable in obj)) obj[relation.maintable] = [];

                obj[relation.maintable].push({
                    path: relation.path,
                    childTable: relation.ChildTable,
                    childColumn: relation.ChildTable_column_name,
                    parentTable: relation.ParentTable,
                    parentColumn: relation.ParentTable_column_name,
                    resultTable: relation.hlevel > 0 ? relation.ParentTable : relation.ChildTable,
                    level: relation.hlevel
                });
            }

            return obj;
        }, {});

        // Return the definition
        return definition;
    }

    // Generate an OpenAPI definition from an existing SQL database
    // output = "json" returns a JSON string
    // output = "yaml" returns a YAML string
    // The ignore query is an SQL condition that is applied when querying all columns
    // this can be used to ignore certain tables or columns
    static async generateOpenAPI(connection, output, ignoreQuery){

        // Mapping between SQL types and OpenAPI types
        var typeMap = {
            "int": "integer",
            "varchar": "string",
            "nvarchar": "string",
            "text": "string",
            "ntext": "string",
            "date": "string",
            "datetime": "string",
            "datetime2": "string",
            "tinyint": "integer",
            "numeric": "integer"
        };

        // Get the database definition
        var definition = await RESTGenerator.getSQLDefinition(connection, ignoreQuery);

        // Generate OpenAPI definition
        // See: https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.1.md
        var openAPI = {
            // OpenAPI version
            openapi: "3.0.0",

            // Servers that run this REST service (used for trying out routes)
            servers: [{
                description: "",
                url: ""
            }],

            // General information about this REST service
            info: {
                description: "",
                version: "1.0.0",
                title: "",
                termsOfService: "",
                contact: {
                    email: "info@example.com"
                },
                license: {
                    name: "",
                    url: ""
                }
            },
            
            // Tags map to SQL database tables
            tags: Object.getOwnPropertyNames(definition.tables).map(table => ({
                name: table,
                description: table + " description",
                externalDocs: {
                    description: "",
                    url: ""
                }
            })),

            // Creates paths for every table with GET, POST, PUT, PATCH and DELETE
            paths: Object.getOwnPropertyNames(definition.tables).reduce((obj, table) => {
                var parameters = [
                    {$ref: "#/components/parameters/fields"},
                    {$ref: "#/components/parameters/orderBy"},
                    {$ref: "#/components/parameters/pageSize"},
                    {$ref: "#/components/parameters/pageNr"},
                    {$ref: "#/components/parameters/filter"}
                ];

                Object.values(definition.tables[table]).forEach(column => {
                    
                    if (typeof column.name == "undefined") Log.write(JSON.stringify(column));
                    parameters.push({
                        name: column.name,
                        in: "query",
                        required: false,
                        description: "",
                        schema: {
                            type: typeMap[column.type]
                        }
                    });
                });

                if (table in definition.relations){
                    parameters.push({
                        name: "include",
                        in: "query",
                        required: false,
                        description: "",
                        explode: true,
                        schema: {
                            type: "array",
                            uniqueItems: true,
                            items: {
                                type: "string",
                                enum: definition.relations[table].map(relation => relation.resultTable)
                            }
                        }
                    })
                }

                obj["/" + table] = {
                    get: {
                        summary: "",
                        tags: [table],
                        parameters: parameters,
                        responses: {
                            "200": {
                                description: "",
                                content: {
                                    "application/json": {
                                        schema: {
                                            $ref: "#/components/schemas/" + table
                                        }
                                    }
                                }
                            },
                            default: {
                                $ref: "#/components/responses/error"
                            }
                        }
                    },
                    post: {
                        summary: "",
                        tags: [table],
                        operationId: "add" + table,
                        responses: {
                            "405": {
                                description: "Invalid input"
                            }
                        }
                    }
                }

                obj["/" + table + "/{ID}"] = {
                    get: {
                        summary: "",
                        tags: [table],
                        parameters: [
                            {
                                name: "ID",
                                in: "path",
                                description: "",
                                schema: {
                                    type: "integer"
                                }
                            }
                        ],
                        responses: {
                            "200": {
                                description: "",
                                content: {
                                    "application/json": {
                                        schema: {
                                            $ref: "#/components/schemas/" + table
                                        }
                                    }
                                }
                            },
                            default: {
                                $ref: "#/components/responses/error"
                            }
                        }
                    },
                    patch: {
                        summary: "",
                        tags: [table],
                        parameters: [
                            {
                                name: "ID",
                                in: "path",
                                description: "",
                                schema: {
                                    type: "integer"
                                }
                            }
                        ],
                        responses: {
                            "405": {
                                description: "Invalid input"
                            }
                        }
                    },
                    put: {
                        summary: "",
                        tags: [table],
                        parameters: [
                            {
                                name: "ID",
                                in: "path",
                                description: "",
                                schema: {
                                    type: "integer"
                                }
                            }
                        ],
                        responses: {
                            "405": {
                                description: "Invalid input"
                            }
                        }
                    },
                    delete: {
                        summary: "",
                        tags: [table],
                        parameters: [
                            {
                                name: "ID",
                                in: "path",
                                description: "",
                                schema: {
                                    type: "integer"
                                }
                            }
                        ],
                        responses: {
                            "405": {
                                description: "Invalid input"
                            }
                        }
                    }
                };

                return obj;
            }, {}),

            // All foreign key relations between tables
            links: Object.values(definition.relations).reduce((obj, array) => {
                for (let relation of array) {
                    obj[relation.path] = {
                        description: "",
                        operationId: "get" + relation.resultTable,
                        "x-childTable": relation.childTable,
                        "x-childColumn": relation.childColumn,
                        "x-parentTable": relation.parentTable,
                        "x-parentColumn": relation.parentColumn,
                        "x-resultTable": relation.resultTable,
                        "x-level": relation.level
                    }
                }
                return obj;
            }, {}),
            
            // OpenAPI components are reusable
            components: {

                // Definitions of all tables and their columns
                schemas: Object.getOwnPropertyNames(definition.tables).reduce((obj, table) => {
                    obj[table] = {
                        type: "object",
                        properties: Object.getOwnPropertyNames(definition.tables[table]).reduce((cobj, column) => {
                            cobj[column] = {
                                type: typeMap[definition.tables[table][column].type]
                            }
                            return cobj;
                        }, {})
                    }
                    return obj;
                }, {}),

                // Parameters
                parameters: {
                    fields: {
                        name: "fields",
                        in: "query",
                        required: false,
                        description: "The fields parameter can be used to return only a select number of columns. Column names are seperated by comma's",
                        schema: {
                            type: "string"
                        }
                    },
                    orderBy: {
                        name: "orderBy",
                        in: "query",
                        required: false,
                        description: "The orderBy parameter can be used to order the results. It is inserted in the query after ORDER BY. An example value would be 'Name DESC'.",
                        schema: {
                            type: "string"
                        }
                    },
                    pageSize: {
                        name: "pageSize",
                        in: "query",
                        required: false,
                        description: "Number of rows that are returned. Must be an integer bigger than 0",
                        schema: {
                            type: "string"
                        }
                    },
                    pageNr: {
                        name: "page",
                        in: "query",
                        required: false,
                        description: "Number of rows that are returned. Must be an integer bigger than 0",
                        schema: {
                            type: "string"
                        }
                    }
                },
                responses: {
                    error: {
                        description: "",
                        content: {
                            "application/json": {
                                schema: {
                                    type: "object",
                                    properties: {
                                        error: {
                                            type: "string"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        if (output.toLowerCase() == "yaml") {
            return jsyaml.dump(openAPI);
        } else if (output.toLowerCase() == "json") {
            return JSON.stringify(openAPI);
        }
    }
}