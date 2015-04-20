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

var DEBUG = true;

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
        var qcutoffMode = query.cutoffMode || 'A';
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
            schema_query(response, qdepth, qcutoffMode, qsize,
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
    es_get_math_elems(query_str, result_callback, error_callback);
   
};

function es_get_math_elems(query_text, result_callback, error_callback) {
    if (DEBUG) util.log("Starting ES math elements retrieval");
    var esquery = JSON.stringify({
        "from" : 0,
        "size" : config.MAX_RELEVANT_DOCS,
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
        "_source" : "math.*"
    });
    es.query(esquery, function(result) {
        var exprsWithIds = {};
        var fullExprsWithIds = {};
        var urlsWithIds = {};
        result.hits.hits.map(function(hit) {
            mapping = hit._source.math;
            for (var key in mapping) {
                var cmml = getCMML(mapping[key]);

                /* Discard trivial formulae POST-QUERY */
                if (cmml.length < config.MIN_MATH_LEN) continue;
                exprsWithIds[key] = cmml;
                fullExprsWithIds[key] = mapping[key];
                urlsWithIds[key] = hit._id;
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
        var urlsOnly = [];
        for (var i in urlsWithIds) {
            urlsOnly.push(urlsWithIds[i]);
        }

        var json_result = {
            cmml_exprs : exprsOnly,
            full_exprs : fullExprs,
            urls : urlsOnly
        };

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

var schema_query =
function(exprs_package, depth, cutoffMode, limit,
        result_callback, error_callback) {
    var exprs = exprs_package["cmml_exprs"];
    var fullExprs = exprs_package["full_exprs"];
    var urls = exprs_package["urls"];

    if (DEBUG) util.log("Got " + exprs.length + " exprs");
    if (exprs.length == 0) {
        var reply = '<mws:schemata size="0" total="0"></mws:schemata>';
        result_callback(reply);
        return;
    }

    var schema_query_data =
        '<mws:query' +
            ' output="json" ' + 
            ' cutoff_mode="' + cutoffMode + '"' +
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
                    fullExprs, urls);
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

var get_sch_result = function(sch_reply, sch_result, full_exprs, urls) {
    sch_reply.map(function(s) {
        var sch_result_elem = {};
        sch_result_elem['coverage'] = s['coverage'];

        sch_result_elem['formulae'] = [];
        s['formulae'].slice(0, config.MAX_EXPR_PER_SCHEMATA)
        .map(function(f_id) {
            // should always be true
            if (f_id < full_exprs.length) {
                var formulaWithUrl = {
                    expr : full_exprs[f_id],
                    url : urls[f_id],
                };
                sch_result_elem['formulae'].push(formulaWithUrl);
            }
        });

        sch_result_elem['subst'] = [];
        s['subst'].map(function(subst) {
            sch_result_elem['subst'].push(subst);
        });

        // choose first formula as representative for schematizing
        sch_result_elem['title'] = sch_result_elem['formulae'][0]['expr'];

        sch_result.push(sch_result_elem);
    });
};

