var fs = require('fs');
var zlib = require('zlib');
var path = require('path');
var model = require('pelias-model');
var spy = require('through2-spy');
var sink = require('through2-sink');
var logger = require('pelias-logger').get('openstreetmap-points');
var categoryDefaults = require('../config/category_map');

var streams = {};

streams.config = {
  categoryDefaults: categoryDefaults
};

streams.pbfParser = require('./multiple_pbfs').create;
streams.docConstructor = require('./document_constructor');
streams.docDenormalizer = require('./denormalizer');
streams.tagMapper = require('./tag_mapper');
streams.adminLookup = require('./adminLookup');
streams.addressExtractor = require('./address_extractor');
streams.deduper = require('./deduper');
streams.categoryMapper = require('./category_mapper');
streams.dbMapper = require('pelias-model').createDocumentMapperStream;
streams.elasticsearch = require('pelias-dbclient');

// default import pipeline
var writer = zlib.createGzip();
writer.pipe( fs.createWriteStream(path.normalize('addresses.txt.gz') ) );

streams.import = function(){
  streams.pbfParser()
    .pipe( streams.docConstructor() )
    .pipe( streams.tagMapper() )
    .pipe( streams.docDenormalizer() )
    .pipe( streams.addressExtractor() )
    .pipe( streams.categoryMapper( categoryDefaults ) )
    .pipe( streams.adminLookup() )
    .pipe( streams.deduper() )
    .pipe( spy.obj(function (doc) {
        logger.info(doc.getGid(), doc.getName('default'), doc.getCentroid());
      })
    )
    .pipe( model.createDocumentMapperStream() )
    .pipe( sink.obj(function (doc) {
        writer.write(JSON.stringify(doc));
        writer.write('\n');
      })
    )
    .on('finish', function () {
      writer.end();
    });
};

module.exports = streams;
