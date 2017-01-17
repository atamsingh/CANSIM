/*
  CANSIM Visualizer
  `````````````````
  * written in node
  * database used is monogDB
  * folder structure required as below
    |- * root folder * ---
    |
    |- cansim.js
    |- mongo
    |- mongod
    |- package.json
    |
    |   |-- zipfiles ------
    |   |   |-- unzipped --
    |
    |----------------------


  Author: Atamjeet Singh
          linked.com/in/atamsingh
*/


//  Dependencies for our logic
var request = require('request');
var cheerio = require('cheerio');
var MongoClient = require('mongodb').MongoClient;
var http = require('http');
var fs = require('fs');
var unzip = require('unzip');
var Converter = require("csvtojson").Converter;
var Hashmap =  require('hashmap')


// Variables pertaining to main website scraped
var address = 'http://www5.statcan.gc.ca/cansim/a29?lang=eng&groupid=All'
var mainClass = '.wb-main-in'
var titleElem = 'h3.FirstTblDir'
var detailElem = '.Blurb'
var blockClassName = '.ListBlk'
var eachClassNameinBlock  = '.ListGrp'
var listItem1 = '.Itm1'
var listItem2 = '.Itm2'
var linkPrefix = 'http://142.206.64.178/tables-tableaux/cansim/csv/0'
var linkSuffix = '-eng.zip'


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


// Arrays, Hashtables and Database connectors
var errorTableCodes = []
var zipFilesDownloaded = []
var downloadsBegan = []
var listOfAttributes = []


// JSON Array converted to a string representation of an
// json object with categorization.
//    returns: string in json forn for a file downloaded
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

  mapToJSON(completeMap)


  // Now we save that object


}


// JSON array to data helper. takes the completed Map and converters into string
function mapToJSON(thismap){
  if(!completeMap.isEmpty()){
    var toStringFirst = '['

    completeMap.forEach(function(value, key) {
        toStringFirst += '{"'+key + '":';
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
        toStringFirst += '},'
    })
    toStringFirst = toStringFirst.substring(0,toStringFirst.length-1)
    toStringFirst += ']'
    console.log(toStringFirst)
    // console.log(toStringFirst)
  }
}


// Main request Method
function downloader() {
  request(address, function (error, response, html) {
    if (!error && response.statusCode == 200) {
      var added = false
      var tableObjects = []
      $ = cheerio.load(html)
      $(titleElem).each(function(i, element){

        var title = $(this).text().replaceAll('"'," ")
        var blurb = $(this).next().text().trim().replace(/\r?\n|\r/g," ").replace(/\s\s+/g, ' ').toString().replaceAll('"'," ")

        var currObject = '{"category": "'+title+'", "detail":"'+blurb+'","tables":['


        var listOfTables = $(this).next().next().html();
        listToEach = cheerio.load(listOfTables)
        var tablesToDownload = []
        listToEach(eachClassNameinBlock).each(function(i,elem){
          added = true
          var currListItem = cheerio.load($(this).html())
          var code = currListItem(listItem1).text().replace("-","")
          var secondLine = currListItem(listItem2).text().trim().replace(/\r?\n|\r/g," ").replace(/\s\s+/g, ' ').toString()
          //var

          console.log(currListItem.text())
          // Variable Name  = tableName
          var tableName = secondLine.split(",")[0]
          tableName = tableName.replaceAll('"','')
          // Variable Name = seasonallyAdj
          if(secondLine.search("seasonally adjusted") == -1){ var seasonallyAdj = 'false'}else{var seasonallyAdj = 'true'}

          //console.log(seasonallyAdj)
          // Variable Name = seriesTime
          if(secondLine.search("daily \\(") != -1){
            var seriesTime = 'daily'
          }
          else if(secondLine.search("weekly \\(") != -1){
            var seriesTime = 'weekly'
          }
          else if(secondLine.search("monthly \\(") != -1){
            var seriesTime = 'monthly'
          }
          else if(secondLine.search("quarterly") != -1){
            var seriesTime = 'quarterly'
          }
          else if(secondLine.search("semi-annual \\(") != -1){
            var seriesTime = 'semi-annual'
          }
          else if(secondLine.search("annual \\(") != -1){
            var seriesTime = 'annual'
          }
          else{
            var seriesTime = 'other'
          }

          // Variable Name = terminated
          if(secondLine.search("Terminated") != -1 && secondLine.search("Discontinued") != -1){
            var terminated = 'true'
          }
          else{
            var terminated = 'false'
          }

          var bracketSplit = secondLine.substring(secondLine.lastIndexOf("(")+1, secondLine.lastIndexOf(")") )
          if(bracketSplit.length > 1){
            // (...) exist. Pick the last one and split over comma
            var splitOnComma = bracketSplit.split(",")
            if(splitOnComma.length == 2){
              // Base year Present
              var type = splitOnComma[0].trim().replaceAll('"'," ")
              var baseyear = splitOnComma[1].trim().replaceAll('"'," ")
            }else{
              // Base Year Absent
              var type = splitOnComma[0].trim()
              var baseyear = 'NA'
            }
          }
          else{
            var baseyear = 'NA'
            var type = 'NA'
          }

          currObject += '{"name":"'+tableName+'","code":"'+code+'","seasonallyAdj":"'+seasonallyAdj+'","seriesOccurance":"'+seriesTime+'","terminated":"'+terminated+'","type":"'+type+'","baseYear":"'+baseyear+'","data":'

          var fileName = Date.now().toString()+"||" + code
          //console.log(fileName)
          var fileLink = linkPrefix + code + linkSuffix
          console.log(fileLink)
          try{
            request({url: fileLink, encoding: null}, function(err, resp, body) {
              if(err) {
                console.log('error downloading file: ' + code)
                errorTableCodes.push(code)
                return new Error('File did not download.')
              };
              fs.writeFile("zipfiles/"+fileName+".zip", body, function(err) {
                console.log("zipped file written!" + code);
                var zipFileDone = fs.createReadStream("zipfiles/"+fileName+".zip").pipe(unzip.Extract({ path: "zipfiles/" }));

                zipFileDone.on('close', function () {
                  console.log("unzipped file written!" + code)
                  fs.unlink("zipfiles/"+fileName+".zip")

                  var converter = new Converter({});
                  fs.createReadStream("zipfiles/0"+code+"-eng.csv").pipe(converter);
                  converter.on("end_parsed", function (jsonArray, code) {
                     currObject += jsonArrayToData(jsonArray) + '},'; //here is your result jsonarray
                  });
                });
              });
            });
          } catch(e){
            //console.log('error downloading file: ' + code)
            errorTableCodes.push(code)
          }
          process.stdout.write('.');
        });

        if(currObject[currObject.length -1] == ','){
          currObject = currObject.substring(0,currObject.length - 1) + ']}'
        }else{
          currObject = currObject + ']}'
        }

        // Add to mongo
        console.log(currObject);
      });
      console.log('End of cheerio selector!')
      //console.log(tableObjects)
    }else{
      console.log('Error loading main page!')
    }

    // console.log(errorTableCodes)
  })
  endcaller()
}


// Calling downloader now:
downloader()
