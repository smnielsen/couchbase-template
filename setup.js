
const async = require('async');
const couchbase = require('couchbase');
const ViewQuery = couchbase.ViewQuery;

const COUCHBASE = 'couchbase://localhost:8091'
const cluster = new couchbase.Cluster(COUCHBASE);
const bucket = cluster.openBucket('test');

const randomInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const SPORTS = ['FOOTBALL', 'BASKET', 'HOCKEY'];
const COUNTRY = ['SE', 'CA', 'IT'];


const flush = async () => {
    return new Promise((resolve, reject) => {
        console.log('Flushing bucket');
        bucket.manager().flush(() => {
            console.log('Flushed: ' + true);
            setTimeout(() => {
                resolve({ flushed: true });
            }, 1000);
        });
    });
}

const mockBets = (start = 0, count = 40) => {
    // Insert some mocked bets
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            let cachedEvents = {};
            let funcs = [];
            for(let i = start; i < (start + count); i++) {
                funcs.push((done) => {
                    let eventId = randomInt(1000000, (1000800));
                    let event = cachedEvents[eventId];
                    if (!event) {
                        event = {
                            eventId,
                            sport: SPORTS[randomInt(0, SPORTS.length - 1)],
                            country: COUNTRY[randomInt(0, COUNTRY.length - 1)]
                        };
                        cachedEvents[eventId] = event;
                    }
                    var bet = {
                        id: i + 200000,
                        eventId: event.eventId,
                        sport: event.sport,
                        country: event.country,
                        type: 'BET'
                    };
                    const docId = `bet-${bet.id}`;
    
                    bucket.insert(docId, { ...bet }, (err, result) => {
                        if (err) {
                            //console.error('Could not upsert bet with id: ' + docId, err.message);
                            done(null, { failed: true, type: 'BET', docId });
                            return;
                        }
                        // console.log('Inserted bet with id ' + docId);
                        done(null, result);
                    })
                });
            }
            async.parallel(funcs, (err, results) => {
                if (err) {
                    return reject(err);
                }
                process.nextTick(() => {
                    console.log(`Inserted ${results.length} bets`);
                    resolve(results);
                });
            });
        }, 1000);
    });
}

const mockEvents = (start = 0, count = 40) => {
    // Insert some mocked data
    return new Promise((resolve, reject) => {
        let funcs = [];
        for(let i = start; i < (start + count); i++) {
            funcs.push((done) => {
                const event = {
                    id: i + 499999,
                    sport: SPORTS[randomInt(0, SPORTS.length - 1)],
                    country: COUNTRY[randomInt(0, COUNTRY.length - 1)],
                    count: randomInt(20, 200),
                    type: 'EVENT'
                };
                const docId = `event-${event.id}`;
                bucket.insert(docId, { ...event }, (err, result) => {
                    if (err) {
                        //console.error('Could not upsert event with id: ' + docId, err.message);
                        done(null, { failed: true, type: 'EVENT', docId });
                        return;
                    }
                    // console.log('Inserted event with id ' + docId);
                    done(null, result);
                })
            });
        }
        async.parallel(funcs, (err, results) => {
            if (err) {
                return reject(err);
            }
            setTimeout(() => {
                console.log(`Inserted ${results.length} events`);
                resolve(results);
            }, 100);
        })
    });   
}

async function start() {
    await flush();
    
    await mockBets(0, 10000);
    await mockEvents(0, 1000);
    console.log('Everything went fine');
}

start()
    .then((results) => {
        process.exit(0);
    })
    .catch((e) => {
        console.error(e);
    });