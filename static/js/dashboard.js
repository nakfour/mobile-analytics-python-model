// 3scale curl -v  -X GET "inserturl&metric_name=hits&since=2017-08-01&period=year&granularity=month&skip_change=true"
// For a specific api call
//inserturl&metric_name=poststartrental&since=2017-08-01&period=year&granularity=month&skip_change=true"
// Response body from 3scale
/*{
 "metric": {
  "id": 2555418041241,
  "name": "Hits",
  "system_name": "hits",
  "unit": "hit"
 },
 "period": {
  "name": "year",
  "since": "2017-08-01T00:00:00Z",
  "until": "2018-07-31T23:59:59Z",
  "timezone": "Etc/UTC",
  "granularity": "month"
 },
 "total": 750,
 "values": [
  136,
  614,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0
 ],
 "application": {
  "id": 1409615589398,
  "name": "Red Hat 2's App",
  "state": "live",
  "description": "Default application created on signup.",
  "plan": {
   "id": 2357355906144,
   "name": "radanalytics-mobile-plan"
  },
  "account": {
   "id": 2445582071771,
   "name": "Red Hat 2"
  }
 }
}*/
console.log("Starting queue")
/*d3.queue()
  .defer(d3.json, "http://localhost:8080/getstationstats")
  .defer(d3.json, "http://localhost:8080/getmobileosstats")
  // COR is not enabled by default on 3scale so making the server and not the broswer perform the GET request
  //.defer(d3.json, "inserturl&metric_name=hits&since=2017-07-01&period=year&granularity=month&skip_change=true")
  .defer(d3.json, "http://localhost:8080/gethits")
  //.defer(d3.json, "inserturl&metric_name=poststartrental&since=2017-07-01&period=year&granularity=month&skip_change=true")
  //.defer(d3.json, "inserturl&metric_name=poststoprental&since=2017-07-01&period=year&granularity=month&skip_change=true")
  .await(analyze);*/
  

d3.queue()
  .defer(d3.json, "http://inserturl/getstationstats")
  .defer(d3.json, "http://inserturl/getmobileosstats")
  .defer(d3.json, "http://inserturl/gethits")
  .await(analyze);

//d3.json("http://localhost:8080/getstationstats", function(data) {
function analyze(error, stationdata, mobiledata, scaledata, poststartrental, poststoprental) {
    if(error) { 
        console.log(error); 
    }
    console.log(stationdata)
    console.log(mobiledata)
    console.log(scaledata.values)
    console.log(poststartrental.values)
    console.log(poststoprental.values)
    // need to parse each string inside the array
    for (i = 0; i < stationdata.length; i++) { 
        stationdata[i]=JSON.parse(stationdata[i])
    }
    console.log(stationdata)
    
    

    //dataTes=[{daypart:1,count:92138},{daypart:3,count:143579},{daypart:4,count:125485},{daypart:2,count:235309},{daypart:0,count:246905}]
    //console.log(dataTes)
    var chart = c3.generate({
        bindto: '#stationdatachart',
        data: {
            json: stationdata,
            keys: {
                x: 'daypart',
                value: ['count']
        }    ,
            type: 'bar'
        },
        axis: {
            x: {
                type: 'category'
            }
        },
        legend: {
            show:false
        }
    });
    
    var count = {};
    var mobileosname = [];
    // need to parse each string inside the array
    for (i = 0; i < mobiledata.length; i++) { 
        mobiledata[i]=JSON.parse(mobiledata[i])
        mobileosname.push(mobiledata[i].mobileos);
        count[mobiledata[i].mobileos] = mobiledata[i].mobileoscount;
    }
    console.log("Mobile Data")
    console.log(mobiledata)
    
    var chart2 = c3.generate({
        bindto: '#mobileosdatachart',
        data: {
            //json: mobiledata,
            json: [count],
            keys: {
                
                value: mobileosname
        }    ,
            type: 'pie'
        },
        /*axis: {
            x: {
                type: 'category'
            }
        },
        legend: {
            show:false
        }*/
    });
    
    // 3-Scale hits data
     // Add the titles of the lines
    poststartrental.values.unshift("StartRental");
    poststoprental.values.unshift("DropRental");
    scaledata.values.unshift("TotalHits");
    console.log(scaledata.values);
    console.log(poststartrental.values);
    console.log(poststoprental.values);
   


var chart = c3.generate({
    bindto: '#scalechart',
    data: {
        x: 'x',
//        xFormat: '%Y%m%d', // 'xFormat' can be used as custom format of 'x'
        columns: [
            ['x', '2017-07-01', '2017-08-01', '2017-09-01', '2017-10-01', '2017-11-01', '2017-12-01', '2018-01-01', '2018-02-01', '2018-03-01', '2018-04-01' , '2018-05-01', '2018-06-01'],
            scaledata.values,
            poststartrental.values,
            poststoprental.values
        ]
    },
    axis: {
        x: {
            type: 'timeseries',
            tick: {
                format: '%Y-%m-%d'
            }
        }
    }
});



};

