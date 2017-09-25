var express = require('express');
var app = express();
const async = require('async');
const assert = require('assert');

const couchbase = require('couchbase');
const ViewQuery = couchbase.ViewQuery;

const COUCHBASE = 'couchbase://localhost:8091'
const cluster = new couchbase.Cluster(COUCHBASE);
const bucket = cluster.openBucket('test');

const randomInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const SPORTS = ['FOOTBALL', 'BASKET', 'HOCKEY'];
const COUNTRY = ['SE', 'CA', 'IT'];

// Flush all data if necessary
// bucket.manager().flush();

//Insert some mocked data
// for(let i = 0; i < 10; i++) {
//     var bet = {
//         id: i + 100000,
//         sport: SPORTS[randomInt(0, SPORTS.length - 1)],
//         country: COUNTRY[randomInt(0, COUNTRY.length - 1)],
//         count: randomInt(30, 100),
//         type: 'BET'
//     };
//     const docId = `bet-${bet.id}`;
//     bucket.upsert(docId, bet, (err, result) => {
//         if (err) {
//             console.error('Could not upsert bet with id: ' + bet.id, err.message);
//             return;
//         }
//         //console.log('Inserted bet with id ' + docId);
//     });
// }

app.get('/test', (req, res) => {
    const sport = 'FOOTBALL';
    const country = 'SE';
    const viewQuery1 = ViewQuery
        .from('dev_test', 'by_sport_in_country')
        .reduce(false)
        .key([sport, country]);

    const viewQuery2 = ViewQuery
        .from('dev_test', 'by_sport')
        .key([sport])
        .reduce(false);

    const viewQuery3 = ViewQuery
        .from('dev_test', 'by_country')
        .reduce(false)
        .key([country]);

    const mapValue = (callback, viewKey) => (err, result) => {
        if (err) {
            return callback(err);
        }
        // callback(null, result);

        const results = result.map((res) => res.id);
        async.parallel(
            results.map((docId) => {
                return (done) => {
                    bucket.get(docId, (e, r = {}) => done(e, r.value));
                };
           }),
           (err, results) => {
               callback(err, {
                   sport: (viewKey.includes('sport') && results[0]) ? results[0].sport : '',
                   country: (viewKey.includes('country') && results[0]) ? results[0].country : '',
                   count: results.length,
                   events: results
               })
           }
        )
    }
    async.parallel({
        'sportcountry': (done) => bucket.query(viewQuery1, mapValue(done, 'sportcountry')),
        'sport': (done) => bucket.query(viewQuery2, mapValue(done, 'sport')),
        'country': (done) => bucket.query(viewQuery3, mapValue(done, 'country'))
    }, (err, results) => {

        try {
            let assertion = {};
            // Assertion
            results.sportcountry.events.forEach((event) => {
                assertion[`sportcountry-${event.id}-${event.sport}`] = event.sport === sport;
                assertion[`sportcountry-${event.id}-${event.country}`] = event.country === country;
            });
            results.sport.events.forEach((event) => {
                assertion[`sport-${event.id}-${event.sport}`] = event.sport === sport;
            });
            results.country.events.forEach((event) => {
                assertion[`country-${event.id}-${event.country}`] = event.country === country;
            });

            res.json({
                assertion,
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