"use strict";
var Promise = require("bluebird");

class DatastoreMysqlAdapter {
    constructor(options = {}) {
        this.options = options;
        this.client = require("mysql");
    }

    /**
     * Get object by id, if not found object is null 
     * @param {String} type
     * @param {String} id
     * @returns {Promise}
     */
    get(type, id) {
        return this
            .find(type, {
                "filter": {
                    "id": id
                },
                "limit": 1
            })
            .then(function(rows) {
                if (rows.length < 1) {
                    return null;
                }
                return rows[0];
            });
    }

    getFields(fields) {
        if (!fields || !fields.length) {
            return "*";
        }
        return fields.join(",");
    }



    getConnection() {
        const self = this;
        return new Promise(function(resolve, reject) {
            if (self.connection) {
                resolve(self.connection);
                return;
            }
            const connection = self.client.createConnection(self.options);
            connection.connect(function(err) {
                if (err) {
                    reject(err);
                    return;
                }
                self.connection = connection;
                resolve(self.connection);
            });
        });

    }

    getWhere(connection, filter) {
        if (!filter) {
            return "";
        }
        return " WHERE " + this.joinValues(connection, filter, " AND ");
    }

    getLimit(connection, offset, limit) {
        if (!offset && limit >= 0) {
            return " LIMIT " + connection.escape(limit);
        }
        if (offset >= 0 && limit >= 0) {
            return " LIMIT " +
                connection.escape(offset) +
                "," +
                connection.escape(limit);
        }
        return "";
    }

    find(type, options) {
        const self = this;
        return self
            .getConnection()
            .then(function(connection) {
                return new Promise(function(resolve, reject) {

                    const query = "SELECT " +
                        self.getFields(options.fields) +
                        " FROM " +
                        connection.escapeId(type) +
                        self.getWhere(connection, options.filter) +
                        self.getLimit(connection, options.offset,
                            options.limit);
                    self.runQuery(connection, query, resolve,
                        reject);
                });
            });
    }

    joinValues(connection, fields, separator) {
        const properties = Object.getOwnPropertyNames(fields);
        if (!properties.length) {
            return "";
        }
        const values = [];
        properties.forEach(function(name) {
            values.push(connection.escapeId(name) + "=" +
                connection.escape(fields[name]));
        });
        return values.join(separator);
    }

    update(type, id, data) {
        if (!data) {
            return new Promise(function(resolve) {
                resolve(0);
            });
        }
        const self = this;
        return self
            .getConnection()
            .then(function(connection) {
                return new Promise(function(resolve, reject) {
                    const set = self.joinValues(connection,
                        data, ",");
                    if (!set) {
                        resolve(0);
                        return;
                    }
                    const query = "UPDATE " +
                        connection.escapeId(type) +
                        " SET " +
                        set +
                        self.getWhere(connection, {
                            "id": id
                        }) +
                        self.getLimit(connection, null, 1);
                    self
                        .runQuery(
                            connection,
                            query,
                            function(result) {
                                resolve(result.affectedRows);
                            },
                            reject
                        );
                });
            });
    }

    runQuery(connection, query, resolve, reject) {
        connection.query(query, function(error, result) {
            if (error) {
                reject(error);
                return;
            }
            resolve(result);
        });
    }

    insert(type, data) {
        const self = this;
        return self
            .getConnection()
            .then(function(connection) {
                return new Promise(function(resolve, reject) {
                    const set = self.joinValues(connection,
                        data, ",");
                    if (!set) {
                        resolve(0);
                        return;
                    }
                    const query = "INSERT INTO " +
                        connection.escapeId(type) +
                        " SET " +
                        set;
                    self
                        .runQuery(
                            connection,
                            query,
                            function(result) {
                                resolve(result.insertId);
                            },
                            reject
                        );
                });
            });
    }

}
module.exports = DatastoreMysqlAdapter;
