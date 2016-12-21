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
var express = require('express')
var app = express()
var request = require('request');
var cheerio = require('cheerio');
var MongoClient = require('mongodb').MongoClient;
var http = require('http');
var fs = require('fs');
var Sync = require('sync');
var async = require('async');
var unzip = require('unzip');




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
var startTime = 0
var endTime = 0




// String type prototype - for replaces within text of website. (Used mainly to remove all occurances of " and ,)
String.prototype.replaceAll = function(search, replacement) {
    var target = this;
    return target.split(search).join(replacement);
};




// Arrays, Hashtables and Database connectors
var errorTableCodes = []
var zipFilesDownloaded = []
var downloadsBegan = []




// Main request Method
function downloader() {
  startcaller()
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

          console.log(secondLine)
          // Variable Name  = tableName
          var tableName = secondLine.split(",")[0]
          tableName = tableName.replaceAll('"','')
          // Variable Name = seasonallyAdj
          if(secondLine.search("seasonally adjusted") == -1){ var seasonallyAdj = 'false'}else{var seasonallyAdj = 'true'}

          //console.log(seasonallyAdj)
          // Variable Name = seriesTime
          if(secondLine.search("daily \\(") != -1){
            var seriesTime = 'daily'
          }else if(secondLine.search("weekly \\(") != -1){
            var seriesTime = 'weekly'
          }else if(secondLine.search("monthly \\(") != -1){
            var seriesTime = 'monthly'
          }else if(secondLine.search("quarterly") != -1){
            var seriesTime = 'quarterly'
          }else if(secondLine.search("semi-annual \\(") != -1){
            var seriesTime = 'semi-annual'
          }else if(secondLine.search("annual \\(") != -1){
            var seriesTime = 'annual'
          }else{
            var seriesTime = 'other'
          }

          // Variable Name = terminated
          if(secondLine.search("Terminated") != -1 && secondLine.search("Discontinued") != -1){
            var terminated = 'true'
          }else{
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
          }else{
            var baseyear = 'NA'
            var type = 'NA'
          }

          currObject += '{"name":"'+tableName+'","code":"'+code+'","seasonallyAdj":"'+seasonallyAdj+'","seriesOccurance":"'+seriesTime+'","terminated":"'+terminated+'","type":"'+type+'","baseYear":"'+baseyear+'"},'

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
                //console.log("zipped file written!" + code);
                fs.createReadStream("zipfiles/"+fileName+".zip").pipe(unzip.Extract({ path: "zipfiles/" }));
                console.log("unzipped file written!" + code)
                fs.unlink("zipfiles/"+fileName+".zip")
                r
              });

            });
          } catch(e){
            console.log('error downloading file: ' + code)
            errorTableCodes.push(code)
          }

          console.log('continuting...');
        });
        if(currObject[currObject.length -1] == ','){
          currObject = currObject.substring(0,currObject.length - 1) + ']}'
        }else{
          currObject = currObject + ']}'
        }
        //console.log(currObject)
        //tableObjects.push(JSON.parse(currObject))

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


function finishedPrinter(code){
  // set lasttime to now

}


function startcaller(){
  startTime = Date.now()
  console.log("end -> " + startTime)
}


function endcaller(){
  endTime = Date.now()
  console.log("end -> " + endTime)
}
// Sync Caller to see time when completed

downloader()

app.get('/', function (req, res) {
  res.send('Hello World!')

})

app.listen(3000, function () {
  console.log('Example app listening on port 3000!')
})
