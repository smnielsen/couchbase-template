

const test = {
  views: {
    by_sport_in_country: {
      map: function (doc, meta) {
        if (doc && doc.type === 'EVENT') {
          emit([doc.sport, doc.country], null);
        }
      }
    },
    by_sport: {
      map: function (doc, meta) {
        if (doc && doc.type === 'EVENT') {
          emit([doc.sport], null);
        }
      }
    },
    in_country: {
      map: function (doc, meta) {
        if (doc && doc.type === 'EVENT') {
          emit([doc.country], null);
        }
      }
    },
    bet_by_sport_in_country: {
      map: function (doc, meta) {
        if (doc && doc.type === 'BET') {
          emit([doc.sport, doc.country], null);
        }
      }
    },
    bet_by_sport: {
      map: function (doc, meta) {
        if (doc && doc.type === 'BET') {
          emit([doc.sport], null);
        }
      }
    },
    bet_in_country: {
      map: function (doc, meta) {
        if (doc && doc.type === 'BET') {
          emit([doc.country], null);
        }
      }
    },
  }
}

module.exports = test;