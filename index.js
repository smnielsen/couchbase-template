var express = require('express');
var app = express();
const async = require('async');
const assert = require('assert');

const couchbase = require('couchbase');
const ViewQuery = couchbase.ViewQuery;

const COUCHBASE = 'couchbase://localhost:8091'
const cluster = new couchbase.Cluster(COUCHBASE);
const bucket = cluster.openBucket('test');

app.get('/bets', (req, res) => {
    const totalStartTime = new Date();
    const sport = 'BASKET';
    const country = 'SE';
    const viewQuery1 = ViewQuery
        .from('dev_test', 'bet_by_sport_in_country')
        .reduce(false)
        .stale(ViewQuery.Update.BEFORE)
        .key([sport, country]);

    const viewQuery2 = ViewQuery
        .from('dev_test', 'bet_by_sport')
        .reduce(false)
        .stale(ViewQuery.Update.BEFORE)
        .key([sport]);

    const viewQuery3 = ViewQuery
        .from('dev_test', 'bet_in_country')
        .reduce(false)
        .stale(ViewQuery.Update.BEFORE)
        .key([country]);

    const mapBet = (callback, startTime, viewKey) => (err, result) => {
        if (err) {
            return callback(err);
        }

        const results = result.reduce((values, result) => {
            if (!result.value) {
                return values;
            }
            if(!values[result.value.eventId]) {
                values[result.value.eventId] = {
                    id: result.value.eventId,
                    sport: result.value.sport,
                    country: result.value.country,
                    count: 0
                };
            }
            values[result.value.eventId].count++;
            return values;
        }, {});
        const events = Object.values(results);
        const endTime = new Date();
        callback(null, {
            time: `${endTime.getTime() - startTime.getTime()}ms`,
            events
        });
    };

    async.parallel({
        'sportcountry': (done) => {
            const startTime = new Date();
            bucket.query(viewQuery1, mapBet(done, startTime, 'sportcountry'));
        },
        'sport': (done) => {
            const startTime = new Date();
            bucket.query(viewQuery2, mapBet(done, startTime, 'sport'));
        },
        'country': (done) => {
            const startTime = new Date();
            bucket.query(viewQuery3, mapBet(done, startTime, 'country'));
        },
    }, (err, results) => {
        if (err) {
            res.json({ assert: 'failed', message: err.message, err: err, data: results });
            return;
        }
        try {
            const assertion = {};
            // Assertion
            if (results.sportcountry.events) {
                results.sportcountry.events.forEach((event) => {
                    assert.equal(event.sport, sport, `${event.sport} should equal ${sport}`);
                    assert.equal(event.country, country, `${event.country} should equal ${country}`);
                });
                assertion.sportcountry = 'isOk';
            }
            
            if (results.sport.events) {
                results.sport.events.forEach((event) => {
                    assert.equal(event.sport, sport, `${event.sport} should equal ${sport}`);
                });
                assertion.sport = 'isOk';
            }
    
            if (results.country.events) {
                results.country.events.forEach((event) => {
                    assert.equal(event.country, country, `${event.country} should equal ${country}`);
                });
                assertion.country = 'isOk';
            }
    
            const endTime = new Date();
            res.json({
                assertion,
                time: `${endTime.getTime() - totalStartTime.getTime()}ms`,
                results
            });
        } catch (e) {
            res.json({ assert: 'failed', message: e.message, err: e, data: results });
        }
    });   
});

app.get('/events', (req, res) => {
    const totalStartTime = new Date();
    const sport = 'FOOTBALL';
    const country = 'SE';
    const viewQuery1 = ViewQuery
        .from('dev_test', 'by_sport_in_country')
        .reduce(false)
        .stale(ViewQuery.Update.BEFORE)
        .key([sport, country]);

    const viewQuery2 = ViewQuery
        .from('dev_test', 'by_sport')
        .stale(ViewQuery.Update.BEFORE)
        .key([sport])
        .reduce(false);

    const viewQuery3 = ViewQuery
        .from('dev_test', 'by_country')
        .stale(ViewQuery.Update.BEFORE)
        .reduce(false)
        .key([country]);

    const mapEvent = (callback, startTime, viewKey) => (err, result) => {
        if (err) {
            return callback(err);
        }

        const results = result.map((res) => res.id);
        async.parallel(
            results.map((docId) => {
                return (done) => {
                    bucket.get(docId, (e, r = {}) => done(e, {
                        id: r.value.id,
                        sport: r.value.sport,
                        country: r.value.country,
                        count: r.value.count
                    }));
                };
           }),
           (err, events) => {
               const endTime = new Date(); 
               callback(err, {
                   time: `${endTime.getTime() - startTime.getTime()}ms`,
                   events
               });
           }
        )
    }
    async.parallel({
        'sportcountry': (done) => {
            const startTime = new Date();
            bucket.query(viewQuery1, mapEvent(done, startTime, 'sportcountry'));
        },
        'sport': (done) => {
            const startTime = new Date();
            bucket.query(viewQuery2, mapEvent(done, startTime, 'sport'));
        },
        'country': (done) => {
            const startTime = new Date();
            bucket.query(viewQuery3, mapEvent(done, startTime, 'country'));
        },
    }, (err, results) => {
        if (err) {
            res.json({ assert: 'failed', message: err.message, err: err, data: results });
            return;
        }
        try {
            let assertion = {};
            // Assertion
            if (results.sportcountry.events) {
                results.sportcountry.events.forEach((event) => {
                    assert.equal(event.sport, sport, `${event.sport} should equal ${sport}`);
                    assert.equal(event.country, country, `${event.country} should equal ${country}`);
                });
                assertion.sportcountry = 'isOk';
            }
            
            if (results.sport.events) {
                results.sport.events.forEach((event) => {
                    assert.equal(event.sport, sport, `${event.sport} should equal ${sport}`);
                });
                assertion.sport = 'isOk';
            }
    
            if (results.country.events) {
                results.country.events.forEach((event) => {
                    assert.equal(event.country, country, `${event.country} should equal ${country}`);
                });
                assertion.country = 'isOk';
            }

            const endTime = new Date();
            res.json({
                assertion,
                time: `${endTime.getTime() - totalStartTime.getTime()}ms`,
                results
            });
        } catch (e) {
            res.json({ assert: 'failed', message: e.message, err: e, data: results });
        }
    })
})

app.get('/event/:id', (req, res) => {
    
    res.json({ status: 'success', params: req.params });
});

app.get('/', function (req, res) {
  res.send('Hello World')
})

app.listen(3000, () => {
    console.log('Started app on localhost:3000');
})