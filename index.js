var log = require('logger')('throttle:index');
var async = require('async');
var moment = require('moment');
var nconf = require('nconf');
var util = require('util');

var errors = require('errors');
var sera = require('sera');

var ipsDurations = ['second', 'minute', 'hour', 'day'];

var unthrottle = nconf.get('UNTHROTTLE');

var map = {
  GET: 'find',
  POST: 'create',
  PUT: 'update',
  DELETE: 'remove',
  HEAD: 'find'
};

var tierInfo = function (Tiers, req, done) {
  var token = req.token;
  if (token) {
    return done(null, token.tier, token.id)
  }
  Tiers.findOne({name: 'free'}, function (err, tier) {
    if (err) {
      return done(err);
    }
    done(null, tier, 'free');
  });
};

var action = function (req) {
  var xaction = req.headers['x-action'];
  if (xaction) {
    return xaction;
  }
  var method = req.method;
  return map[method];
};

var apisThrottleKey = function (id, name, action, duration) {
  return util.format('throttle:%s:%s:%s:%s', id, name, action, duration);
};

var expiry = function (at, duration) {
  return at.clone().endOf(duration).unix() + 1;
};

var ipsThrottleKey = function (ip, id, action, duration) {
  return util.format('throttle:%s:%s:%s:%s', ip, id, action, duration);
};

var ipsThrottleRules = function (tier, ip, id, action, at) {
  var ips = tier.ips;
  ips = ips[action] || ips['*'] || {};
  var rules = [];
  ipsDurations.forEach(function (duration) {
    rules.push({
      name: duration,
      key: ipsThrottleKey(ip, id, action, duration),
      limit: ips[duration],
      expiry: expiry(at, duration)
    });
  });
  return rules;
};

var check = function (rules, done) {
  var i;
  var rule;
  var length = rules.length;
  for (i = 0; i < length; i++) {
    rule = rules[i];
    if (rule.current > rule.limit) {
      return done(errors.tooManyRequests('Too many requests per ' + rule.name));
    }
  }
  done();
}

var ips = function (tier, ip, id, action, done) {
  var at = moment().utc();
  var rootKey = ipsThrottleKey(ip, id, action, '');
  var rules = ipsThrottleRules(tier, ip, id, action, at);
  var multi = sera.redis().multi();
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
      multi = sera.redis().multi();
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
          sera.redis().expireat(rule.key, rule.expiry, updated);
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

module.exports = function (Tiers) {
  return function (req, res, next) {
    if (unthrottle) {
      return next();
    }
    tierInfo(Tiers, req, function (err, tier, id) {
      if (err) {
        log.error('tiers:find-one', err);
        return next(errors.serverError())
      }
      var ip = req.ip;
      ips(tier, ip, id, action(req), function (err) {
        if (!err) {
          return next();
        }
        if (err.code !== errors.tooManyRequests().code) {
          return next(err);
        }
        res.pond(err);
      });
    });
  };
};