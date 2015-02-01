/*

Copyright (C) 2010-2015 KWARC Group <kwarc.info>

This file is part of SchemaSearch.

MathWebSearch is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

MathWebSearch is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SchemaSearch.  If not, see <http://www.gnu.org/licenses/>.

*/

var DEBUG = false;

var config = require('./config.js');
var es = require('./elasticsearch.js');

var assert = require('assert');
var http = require("http");
var url = require('url');
var util = require('util');
var qs = require('querystring');


http.createServer(function(request, response) {
    var process_query = function (query) {
        var qtext = query.text || "";
        var qdepth = query.depth || 3;
        /* TODO: implement pagination option */ 
        var qfrom = query.from || 0;
        var qsize = query.size || 10;

        var send_response = function(status_code, json_response) {
            if (status_code >= 500) {
                console.log(json_response);
                util.log(json_response);
            }
            response.writeHead(status_code, {
                "Content-Type" : "application/json; charset=utf-8",
                "Access-Control-Allow-Origin" : "*"
            });
            response.write(JSON.stringify(json_response));
            response.end();
        };

        /* Pipe elasticsearch exprs into schemad */
        var es_response_handler = function(response) {
            schema_query(response, qdepth, qsize,
                    schema_response_handler, schema_error_handler);
        };

        var es_error_handler = function(error) {
            error.schema_component = "elasticsearch";
            send_response(500, error);
        };

        var schema_response_handler = function(sch_response) {
            send_response(200, sch_response);
        }

        var schema_error_handler = function(error) {
            error.tema_component = "schema";
            send_response(error.status_code, error);
        };

        es_query(qtext, es_response_handler, es_error_handler);
    }

    if (request.method == "GET") {
        var url_parts = url.parse(request.url, true);
        process_query(url_parts.query);
    } else if (request.method == "POST") {
        var body = "";

        request.on("data", function (data) {
            body += data;
        });

        request.on("end", function () {
            var query = qs.parse(body);
            process_query(query);
        });

        request.on("error", function (e) {
            // TODO
        });
    }
}).listen(config.SCHEMA_PROXY_PORT);


/**
 * @callback result_callback(json_data)
 */
var es_query =
function(query_str, result_callback, error_callback) {
    if (DEBUG) util.log("Getting Aggregations...");
    es_get_aggregations(query_str, function(res) {
        if (DEBUG) util.log("Got Aggeregations. Getting Math_elems...");
        es_get_math_elems(res, function(math_res) {
            if (DEBUG) util.log("Got Math_elems. Getting exprs...");
            es_get_exprs(math_res, result_callback, error_callback);
        }, error_callback);
    }, error_callback);
   
};

function es_get_math_elems(top_ids, result_callback, error_callback) {
    var filters = [];
    top_ids.map(function(id) {
        filters.push("mws_id." + id);
    });
    var esquery = JSON.stringify({
        "query" : {
            "bool" : {
                "must" : [{
                    "terms" : {
                        "mws_ids" : top_ids,
                        "minimum_match" : 1
                    }
                }]
            }
        },
        "_source" : filters
    });

    es.query(esquery, function(result) {
        var solved_ids = {};
        var math_elems_per_doc = [];
        result.hits.hits.map(function(hit) {
            var math_elems = [];
            try {
                var mws_ids = hit._source.mws_id;
                for (var mws_id in mws_ids) {
                    if (solved_ids[mws_id]) continue;
                    var mws_id_data = mws_ids[mws_id];

                    for (var math_elem in mws_id_data) {
                        if (solved_ids[mws_id]) break;
                        var simple_mathelem = simplify_mathelem(math_elem);
                        var xpath = mws_id_data[math_elem].xpath;

                        /* Discard trivial formulae PRE-QUERY */
                        if (xpath != "/*[1]") continue;

                        solved_ids[mws_id] = true;
                        math_elems.push(simple_mathelem);
                    }
                }
            } catch (e) {
                // ignore
            }
            math_elems_per_doc.push({"doc_id" : hit._id, "math_ids" : math_elems});
        });
        result_callback(math_elems_per_doc);
    }, function(error) {
        error_callback(error);
    });
}

function es_get_exprs(docs_with_math, result_callback, error_callback) {
    var doc_ids = docs_with_math.map(function(doc) {
        return doc["doc_id"];
    });
    var math_ids = docs_with_math.map(function(doc) {
        return doc["math_ids"];
    });
    // flatten the array
    if (math_ids.length != 0) {
        math_ids = math_ids.reduce(function(a,b) { return a.concat(b); });
    }

    source_filter = [];
    math_ids.map(function(math_id) {
        source_filter.push("math." + math_id);
    });

    var esquery = JSON.stringify({
        "query" : {
            "ids" : {
                "values" : doc_ids
            }
        },
        "_source" : source_filter
    });

    es.query(esquery, function(result) {
        var exprsWithIds = {};
        var fullExprsWithIds = {};
        result.hits.hits.map(function(hit) {
            mapping = hit._source.math;
            for (var key in mapping) {
                var cmml = getCMML(mapping[key]);

                /* Discard trivial formulae POST-QUERY */
                if (cmml.length < config.MIN_MATH_LEN) continue;
                exprsWithIds[key] = cmml;
                fullExprsWithIds[key] = mapping[key];
            }

        });

        var exprsOnly = [];
        for (var i in exprsWithIds) {
            exprsOnly.push(exprsWithIds[i]);
        }
        var fullExprs = [];
        for (var i in fullExprsWithIds) {
            fullExprs.push(fullExprsWithIds[i]);
        }

        var json_result = {
            cmml_exprs : exprsOnly,
            full_exprs : fullExprs
        };

        if (DEBUG) util.log("Got exprs. Sending result to schemad...");
        result_callback(json_result);
    }, function (error) {
        error_callback(error);
    });
}

function getCMML(expr) {
    var CMML_REGEX =
        /<annotation-xml[^>]*Content[^>]*>(.*?)<\/annotation-xml>/g;
    var match = CMML_REGEX.exec(expr);
    if (match == null) return "";
    else return match[1];
}

function es_get_aggregations(query_text, result_callback, error_callback) {
    var esquery = JSON.stringify({
        "size" : 0,
        "query" : {
            "bool" : {
                "must" : [{
                    "match" : {
                        "text" : {
                            "query" : query_text,
                            "minimum_should_match" : "2",
                            "operator" : "or"
                        }
                    }
                }]
            }
        },
        "aggs" : {
            "formulae" : {
                "terms" : {
                    "field" : "mws_ids",
                    "size" : config.MAX_RELEVANT_AGG
                }
            }
        },
        "_source" : false
    });

    es.query(esquery, function(result) {
        var agg_buckets = result.aggregations.formulae.buckets;
        var top_ids = agg_buckets.map(function(bucket) { return bucket.key; });

        result_callback(top_ids);
    }, function(error) {
        error_callback(error);
    });
}

var simplify_mathelem = function(mws_id) {
    var simplified_arr = mws_id.split("#");
    return simplified_arr[simplified_arr.length - 1];
}

var schema_query =
function(exprs_package, depth, limit, result_callback, error_callback) {
    var exprs = exprs_package["cmml_exprs"];
    var fullExprs = exprs_package["full_exprs"];

    if (DEBUG) util.log("Got " + exprs.length + " exprs");
    if (exprs.length == 0) {
        var reply = '<mws:schemata size="0" total="0"></mws:schemata>';
        result_callback(reply);
        return;
    }

    var schema_query_data =
        '<mws:query' +
            ' output="json" ' + 
            ' schema_depth="' + depth + '"' +
            ' answsize="' + limit + '">';
    for (var i in exprs) {
        expr = exprs[i];
        schema_query_data += 
            '<mws:expr>' +
                expr +
            '</mws:expr>';
    }
    schema_query_data += '</mws:query>';

    var schema_query_options = {
        hostname: config.SCHEMA_HOST,
        port: config.SCHEMA_PORT,
        path: '/',
        method: 'POST',
        headers: {
            'Content-Type': 'application/xml',
            'Content-Length': Buffer.byteLength(schema_query_data, 'utf8')
        }
    };

    var req = http.request(schema_query_options, function(response) {
        if (response.statusCode == 200) {
            var raw_reply = '';
            response.on('data', function (chunk) {
                raw_reply += chunk;
            });
            response.on('end', function () {
                var json_reply = JSON.parse(raw_reply);
                
                var result = {};
                result['total'] = json_reply['total'];
                result['schemata'] = [];

                get_sch_result(json_reply['schemata'], result['schemata'],
                    fullExprs);
                if (DEBUG) util.log("Finished schematization");
                result_callback(result);
            });
        } else {
            var raw_data = '';
            response.on('data', function (chunk) {
                raw_data += chunk;
            });
            response.on('end', function () {
                var json_reply = {
                    status_code : response.statusCode,
                    data : raw_reply
                };
                error_callback(json_reply);
            });
        }
    });

    req.on('error', function(error) {
        error.status_code = 500;
        error_callback(error);
    });

    req.write(schema_query_data);
    req.end();
};

var get_sch_result = function(sch_reply, sch_result, full_exprs) {
    sch_reply.map(function(s) {
        var sch_result_elem = {};
        sch_result_elem['coverage'] = s['coverage'];

        sch_result_elem['formulae'] = [];
        s['formulae'].map(function(f_id) {
            // should always be true
            if (f_id < full_exprs.length) {
                sch_result_elem['formulae'].push(full_exprs[f_id]);
            }
        });

        sch_result_elem['subst'] = [];
        s['subst'].map(function(subst) {
            sch_result_elem['subst'].push(subst);
        });

        // choose first formula as representative for schematizing
        sch_result_elem['title'] = sch_result_elem['formulae'][0];

        sch_result.push(sch_result_elem);
    });
};
