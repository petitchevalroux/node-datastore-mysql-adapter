"use strict";
const Datastore = require("@petitchevalroux/datastore");
const sinon = require("sinon");
const path = require("path");
const Adapter = require(path.join(__dirname, ".."));
const toRestores = [];
const assert = require("assert");

describe("Mysql Adapter", function() {
    afterEach(function() {
        toRestores.forEach(function(stub) {
            stub.restore();
        });
    });
    const datastore = new Datastore(new Adapter({
        "host": "localhost",
        "user": "me",
        "password": "secret",
        "database": "my_db"
    }));

    const connection = {
        "connect": function(cb) {
            cb();
        },
        "escapeId": function() {

        },
        "escape": function() {

        },
        "query": function() {

        }
    };
    describe("getConnection", function() {
        it("create connection with the right options", function(
            done) {
            const stub = sinon.stub(
                datastore.adapter.client,
                "createConnection",
                function() {
                    return {
                        "connect": function(cb) {
                            cb("error");
                        }
                    };
                }
            );
            toRestores.push(stub);
            datastore
                .adapter
                .getConnection()
                .catch(function() {
                    assert(stub.calledWith({
                        "host": "localhost",
                        "user": "me",
                        "password": "secret",
                        "database": "my_db"
                    }));
                    done();
                });
        });
    });

    describe("get when object is found", function() {
        let escapeIdStub, escapeStub, queryStub;
        beforeEach(function() {
            escapeIdStub = sinon.stub(connection,
                "escapeId",
                function(value) {
                    return value;
                });
            toRestores.push(escapeIdStub);
            escapeStub = sinon.stub(connection,
                "escape",
                function(value) {
                    return value;
                });
            toRestores.push(escapeStub);
            queryStub = sinon.stub(connection, "query",
                function(query, cb) {
                    cb(null, [{
                        "field": "value"
                    }]);
                });
            toRestores.push(queryStub);
            const stub = sinon.stub(
                datastore.adapter.client,
                "createConnection",
                function() {
                    return connection;
                }
            );
            toRestores.push(stub);
            return datastore.get("articles", 42);
        });
        it("escape primary column name", function(done) {
            assert(escapeIdStub.calledWith("id"));
            done();
        });
        it("escape primary column value", function(done) {
            assert(escapeStub.calledWith(42));
            done();
        });
        it("escape table name", function(done) {
            assert(escapeIdStub.calledWith("articles"));
            done();
        });
        it("execute the right query", function(done) {
            assert(queryStub.calledWith(
                "SELECT * FROM articles WHERE id=42 LIMIT 1"
            ));
            done();
        });
    });

    describe("get when object is not found", function() {
        let queryStub;
        beforeEach(function() {
            queryStub = sinon.stub(connection, "query",
                function(query, cb) {
                    cb(null, []);
                });
            toRestores.push(queryStub);
            let stub = sinon.stub(
                datastore.adapter.client,
                "createConnection",
                function() {
                    return connection;
                }
            );
            toRestores.push(stub);

        });
        it("return null", function(done) {
            datastore.get("articles", 42)
                .then(function(result) {
                    assert.equal(result, null);
                    done();
                    return result;
                })
                .catch(function(err) {
                    done(err);
                });
        });
    });

    describe("update", function() {
        let escapeIdStub, escapeStub, queryStub;
        beforeEach(function() {
            escapeIdStub = sinon.stub(connection,
                "escapeId",
                function(value) {
                    return value;
                });
            toRestores.push(escapeIdStub);
            escapeStub = sinon.stub(connection,
                "escape",
                function(value) {
                    return value;
                });
            toRestores.push(escapeStub);
            queryStub = sinon.stub(connection, "query",
                function(query, cb) {
                    cb(null, [{
                        "field": "value"
                    }]);
                });
            toRestores.push(queryStub);
            const stub = sinon.stub(
                datastore.adapter.client,
                "createConnection",
                function() {
                    return connection;
                }
            );
            toRestores.push(stub);
            return datastore.update("articles", 42, {
                "title": "new title",
                "abstract": "new abstract"
            });
        });
        it("escape primary column name", function(done) {
            assert(escapeIdStub.calledWith("id"));
            done();
        });
        it("escape primary column value", function(done) {
            assert(escapeStub.calledWith(42));
            done();
        });
        it("escape title column name", function(done) {
            assert(escapeIdStub.calledWith("title"));
            done();
        });
        it("escape title column value", function(done) {
            assert(escapeStub.calledWith("new title"));
            done();
        });
        it("escape abstract column name", function(done) {
            assert(escapeIdStub.calledWith("abstract"));
            done();
        });
        it("escape abstract column value", function(done) {
            assert(escapeStub.calledWith("new abstract"));
            done();
        });
        it("execute the right query", function(done) {
            assert(queryStub.calledWith(
                "UPDATE articles SET title=new title,abstract=new abstract WHERE id=42 LIMIT 1"
            ));
            done();
        });
    });


    describe("insert", function() {
        let escapeIdStub, escapeStub, queryStub;
        beforeEach(function() {
            escapeIdStub = sinon.stub(connection,
                "escapeId",
                function(value) {
                    return value;
                });
            toRestores.push(escapeIdStub);
            escapeStub = sinon.stub(connection,
                "escape",
                function(value) {
                    return value;
                });
            toRestores.push(escapeStub);
            queryStub = sinon.stub(connection, "query",
                function(query, cb) {
                    cb(null, [{
                        "field": "value"
                    }]);
                });
            toRestores.push(queryStub);
            const stub = sinon.stub(
                datastore.adapter.client,
                "createConnection",
                function() {
                    return connection;
                }
            );
            toRestores.push(stub);
            return datastore.insert("articles", {
                "title": "new title",
                "abstract": "new abstract"
            });
        });
        it("escape title column name", function(done) {
            assert(escapeIdStub.calledWith("title"));
            done();
        });
        it("escape title column value", function(done) {
            assert(escapeStub.calledWith("new title"));
            done();
        });
        it("escape abstract column name", function(done) {
            assert(escapeIdStub.calledWith("abstract"));
            done();
        });
        it("escape abstract column value", function(done) {
            assert(escapeStub.calledWith("new abstract"));
            done();
        });
        it("execute the right query", function(done) {
            assert(queryStub.calledWith(
                "INSERT INTO articles SET title=new title,abstract=new abstract"
            ));
            done();
        });
    });

    describe("runQuery", function() {
        let rejectSpy, queryStub;
        beforeEach(function() {
            queryStub = sinon.stub(connection, "query",
                function(query, cb) {
                    cb("error");
                });
            toRestores.push(queryStub);
            const resolve = function() {};
            rejectSpy = sinon.spy();
            return datastore
                .adapter
                .runQuery(
                    connection,
                    "articles",
                    resolve,
                    rejectSpy
                );
        });
        it("reject with the error when query failed", function(
            done) {
            assert(rejectSpy.calledWith("error"));
            done();
        });
    });

    describe("find with fields, offset, limit and without filter",
        function() {
            let queryStub;
            beforeEach(function() {
                toRestores.push(sinon.stub(connection,
                    "escapeId",
                    function(value) {
                        return value;
                    }));

                toRestores.push(sinon.stub(connection,
                    "escape",
                    function(value) {
                        return value;
                    }));

                queryStub = sinon.stub(connection, "query",
                    function(query, cb) {
                        cb(null, [{
                            "field": "value"
                        }]);
                    });
                toRestores.push(queryStub);

                const stub = sinon.stub(
                    datastore.adapter.client,
                    "createConnection",
                    function() {
                        return connection;
                    }
                );
                toRestores.push(stub);
                return datastore
                    .find(
                        "articles", {
                            "offset": 10,
                            "limit": 42,
                            "fields": ["title", "abstract"]
                        }
                    );
            });
            it("execute the right query", function(done) {
                assert(queryStub.calledWith(
                    "SELECT title,abstract FROM articles LIMIT 10,42"
                ));
                done();
            });
        });

});
