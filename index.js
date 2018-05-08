var log = require('logger')('throttle:index');
var async = require('async');
var _ = require('lodash');
var moment = require('moment');
var nconf = require('nconf');
var util = require('util');
var url = require('url');
var Redis = require('ioredis');

var errors = require('errors');

var redis = new Redis(nconf.get('REDIS_URI'));

var throttleKey = function (token, type) {
  return util.format('thr:%s:%s', token.id, type);
};

var throttleRules = function (token, at) {
  return [{
    name: 'second',
    key: throttleKey(token, 'ss'),
    limit: token.limits['second'],
    expiry: at.endOf('second').unix(),
  }, {
    name: 'day',
    key: throttleKey(token, 'DD'),
    limit: token.limits['day'],
    expiry: at.endOf('day').unix(),
  }, {
    name: 'month',
    key: throttleKey(token, 'MM'),
    limit: token.limits['month'],
    expiry: at.endOf('month').unix()
  }];
};

var check = function (rules, done) {
  var i;
  var rule;
  var length = rules.length;
  for (i = 0; i < length; i++) {
    rule = rules[i];
    if (rule.current > rule.limit) {
      return done(errors.tooManyRequests('Too many requests per %s', rule.name));
    }
  }
  done();
}

var throttle = function (token, done) {
  var at = moment().utc();
  var rootKey = throttleKey(token, '');
  var rules = throttleRules(token, at);
  var multi = redis.multi();
  // primary check
  rules.forEach(function (rule) {
    multi.get(rule.key);
  });
  multi.exec(function (err, results) {
    if (err) {
      return done(err);
    }
    var index = 0;
    rules.forEach(function (rule) {
      var entry = results[index++];
      rule.current = entry[1];
    });
    check(rules, function (err) {
      if (err) {
        return done(err);
      }
      // secondary check
      multi = redis.multi();
      rules.forEach(function (rule) {
        multi.set(rootKey, 0)
          .expireat(rootKey, rule.expiry)
          .renamenx(rootKey, rule.key)
          .incr(rule.key)
          .ttl(rule.key)
      });
      // [[null,"OK"],[null,1],[{}],[null,1],[null,-1],[null,"OK"],[null,1],[null,0],[null,11],[null,72401],[null,"OK"],[null,1],[null,0],[null,11],[null,2059601]]
      multi.exec(function (err, results) {
        if (err) {
          return done(err);
        }
        var index = 0;
        rules.forEach(function (rule) {
          var entry = results[index += 3];
          rule.current = entry[1];
          entry = results[index += 1];
          rule.ttl = entry[1];
          index++;
        });
        async.each(rules, function (rule, updated) {
          if (rule.ttl !== -1) {
            return updated();
          }
          redis.expireat(rule.key, rule.expiry, updated);
        }, function (err) {
          if (err) {
            return done(err);
          }
          check(rules, done);
        });
      });
    });
  });
};

module.exports = function (options) {
  return function (req, res, next) {
    var token = req.token;
    var user = req.user;
    if (!token) {
      return next();
    }
    throttle(token, function (err) {
      if (!err) {
        return next();
      }
      if (err.code !== errors.tooManyRequests().code) {
        return next(err);
      }
      res.pond(err);
    });
  };
};
