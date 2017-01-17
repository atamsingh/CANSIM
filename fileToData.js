
var fs = require('fs')
var Converter = require("csvtojson").Converter
var Hashmap =  require('hashmap')

var dirname = 'zipfiles/'
var completeMap = new Hashmap()

var mongo = require('mongodb');

var db = new mongo.Db('CANSIMVisualizer', new mongo.Server('localhost',27017, {}), {});


// String type prototype - for replaces within text of website. (Used mainly to remove all occurances of " and ,)
String.prototype.replaceAll = function(search, replacement) {
    var target = this;
    return target.split(search).join(replacement);
};

// Array type prototype -  for checking for presence of certain items in an one dimensional array
Array.prototype.contains = function(obj) {
    var i = this.length;
    while (i--) {
        if (this[i] === obj) {
            return true;
        }
    }
    return false;
}

// Hashmap extension - allows to check is map is empty by checking presence of any keys
Hashmap.prototype.isEmpty = function(){
  var keys = this.keys()
  if(keys.length > 0) return false
  else return true
}


function jsonArrayToData(jsonArray){

  //console.log(jsonArray)
  // We get all the keys
  s = jsonArray[0]
  var keys = []
  for(var k in s) keys.push(k);
  // console.log(keys)
  var datePosition = 'Ref_Date'

  if(keys.contains('GEO')){
    var geoPosition = 'GEO'
  }else{
    var geoPosition = 'GEOGRAPHY'
  }

  // Now we get all the values and make a multi dimensional array
  // for each row in the csv
  if(keys.indexOf(datePosition) > -1 && keys.indexOf(geoPosition)  > -1){
    // Both filters exist
    // convert object to an multi dimensional array of the form
    // ['date',['geo',[...Stuff...]]


    // Get all unique datePositions and geoPositions
    var uniqueDates = []
    var uniqueGeo = []

    for(var index in jsonArray){
        //console.log(jsonArray[index])
        var thisdate = jsonArray[index]['Ref_Date']
        var thisGeo = jsonArray[index]['GEO']

        if(!uniqueDates.contains(thisdate)){
          // Unique Date
          uniqueDates.push(thisdate)
          uniqueDates.push(thisGeo)
          var helpermap = new Hashmap()
          for(var i = 0; i < keys.length; i++){
            var thisKey = keys[i]
            if(thisKey !== 'GEO' && thisKey !== 'Ref_Date' && thisKey !== 'Coordinate' && thisKey !== 'Vector'){
              var thisValue = jsonArray[index][thisKey]
              //console.log("-> "+thisKey+ " : " + thisValue)
              helpermap.set(thisKey,thisValue)
            }
          }
          // now the map has all the values
          completeMap.set(thisdate,new Hashmap(thisGeo, helpermap))
        }else{
          // not unique date
          // get previous Stuff
          var olderMap = completeMap.get(thisdate)
          if(!uniqueGeo.contains(thisGeo)){
            // but Unique Geo
            uniqueDates.push(thisGeo)
            var helpermap = new Hashmap()
            for(var i = 0; i < keys.length; i++){
              var thisKey = keys[i]
              if(thisKey !== 'GEO' && thisKey !== 'Ref_Date' && thisKey !== 'Coordinate' && thisKey !== 'Vector'){
                var thisValue = jsonArray[index][thisKey]
                //console.log("-> "+thisKey+ " : " + thisValue)
                helpermap.set(thisKey,thisValue)
              }
            }
            // now the older map has all the values
            olderMap.set(thisGeo, helpermap)

            // Add back to completeMap
            completeMap.set(thisdate,olderMap)
          }else{
            var helpermap = olderMap.get(thisGeo)
            for(var i = 0; i < keys.length; i++){
              var thisKey = keys[i]
              if(thisKey !== 'GEO' && thisKey !== 'Ref_Date' && thisKey !== 'Coordinate' && thisKey !== 'Vector'){
                var thisValue = jsonArray[index][thisKey]
                //console.log("-> "+thisKey+ " : " + thisValue)
                helpermap.set(thisKey,thisValue)
              }
            }
            // now the older map has all the values
            olderMap.set(thisGeo, helpermap)

            // Add back to completeMap
            completeMap.set(thisdate,olderMap)
          }
        }
    }
  }

  // we now pull all the years and geography out of the array and make
  // a json object over those values

  return mapToJSON(completeMap)


  // Now we save that object


}


function mapToJSON(thismap){
  if(!completeMap.isEmpty()){
    var toStringFirst = '['

    completeMap.forEach(function(value, key) {
        toStringFirst += '{"'+key + '":[';
        value.forEach(function(value1,key1){
          toStringFirst += '{"'+key1 + '":{';
          //console.log("   "+key1 + " : ")
          value1.forEach(function(value2,key2){
              toStringFirst += '"'+key2 + '":"'+value2+'",';
          })
          toStringFirst = toStringFirst.substring(0,toStringFirst.length-1)
          toStringFirst += '}},'
        })
        toStringFirst = toStringFirst.substring(0,toStringFirst.length-1)
        toStringFirst += ']},'
    })
    toStringFirst = toStringFirst.substring(0,toStringFirst.length-1)
    toStringFirst += ']'
    // console.log(toStringFirst)
    return toStringFirst
    // console.log(toStringFirst)
  }else{
    return '[]'
  }
}

fs.readdir(dirname, function(err, filenames) {
  if (err) {
    console.log('Couldnt read the files!')
    return;
  }
  var currObject = '{"stuff":['

  var currentThing = 0
  filenames.forEach(function(filename) {
    var converter = new Converter({});
    fs.createReadStream(dirname+filename).pipe(converter);
    converter.on("end_parsed", function (jsonArray, code) {
       currObject += '{"name":"'+filename+'","code":"codeRandom","seasonallyAdj":"true","seriesOccurance":"monthly","terminated":"false","type":"jhfa","baseYear":"NA","data":'
       currObject += jsonArrayToData(jsonArray) + '},';
       //console.log('in converter end parser')
       //console.log(currObject)
       if(currentThing == (filenames.length-1)){
         currObject = currObject.substring(0,currObject.length - 1) + ']}'
         var datum = JSON.parse(currObject)
         console.log(datum)

          try {
            db.open(function(err, client){
               var returned = client.createCollection("cansim", function(err, col) {
                    db.collection('cansim').insert(datum["stuff"],function(err,docsInserted){
                      db.close()
                      if(!err) console.log('Added to MongoDB in collection cansim')
                    })
               });
            });

          } catch (e) {
             print (e);
          }
       }else{
         currentThing++
       }
    });
  });




  //console.log(currObject)
});
