var Config = exports;

Config.SCHEMA_PROXY_PORT = 11006;

Config.ES_INDEX = "tema";
Config.ES_HOST = "localhost";
Config.ES_PORT = 9200;

Config.SCHEMA_HOST = "localhost";
Config.SCHEMA_PORT = 9080;

/* Options for discarding trivial formulae */
Config.MIN_MATH_LEN = 200;

/* Number of buckets to be considered for the ES aggregation */
Config.MAX_RELEVANT_DOCS = 50;

/* Maximum number of formulae returned for each schemata */
Config.MAX_EXPR_PER_SCHEMATA = 10;
