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
// making sure spinner for map data is off
console.log("Stopping map loader");
document.getElementById("maploader").style.display = "none";
// Start page loader
document.getElementById("pageloader").style.display = "block";

var map = L.map('map').setView([40.75323098,-73.97032517], 10);
var mapLink = '<a href="http://openstreetmap.org">OpenStreetMap</a>';
        L.tileLayer(
            'http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; ' + mapLink + ' Contributors',
            maxZoom: 18,
        }).addTo(map);
var heatNew = null;
// TODO: This value should be calculated from the data
var maxCount=0;
        
        
  
// In production these urls should be the production DNS name
d3.queue()
  .defer(d3.json, "http://inserturl/getstationstats")
  .defer(d3.json, "http://inserturl/getmobileosstats")
  .defer(d3.json, "http://inserturl/gethits")
  .defer(d3.json, "http://inserturl/getpoststartrental")
  .defer(d3.json, "http://inserturl/getpoststoprental")
  .defer(d3.json, "http://inserturl/getstationdaytime")
  .defer(d3.json, "http://inserturl/gettouchdata")
  .await(analyze);

//d3.json("http://localhost:8080/getstationstats", function(data) {
function analyze(error, stationdata, mobiledata, scaledata, poststartrental, poststoprental,stationdaytime, touchdata) {
    //stopping page loader
    document.getElementById("pageloader").style.display = "none";
    if(error) { 
        console.log(error); 
    }
    console.log(stationdata);
    console.log(mobiledata);
    console.log(scaledata.values);
    console.log(poststartrental.values);
    console.log(poststoprental.values);
    console.log("Station Data by daytime");
    console.log(stationdaytime);
    console.log(touchdata);
    
    // need to parse each string inside the array
    for (i = 0; i < stationdata.length; i++) { 
        stationdata[i]=JSON.parse(stationdata[i]);
    }
    console.log(stationdata);
    
    //[-41.5764,174.2405,1.4453]
    //[lat,lon,number]
    var latlonlist = [];
    // need to parse each string inside the array
    for (i = 0; i < stationdaytime.length; i++) { 
        var latloncount=[];
        stationdaytime[i]=JSON.parse(stationdaytime[i]);
        latloncount.push(stationdaytime[i].startstationlat);
        latloncount.push(stationdaytime[i].startstationlon);
        latloncount.push(stationdaytime[i].rentalcount);
        latlonlist.push(latloncount)
        
    }
    console.log("latlonlist");
    console.log(latlonlist)
    

    //dataTes=[{daypart:1,count:92138},{daypart:3,count:143579},{daypart:4,count:125485},{daypart:2,count:235309},{daypart:0,count:246905}]
    //console.log(dataTes)
    var chart = c3.generate({
        bindto: '#stationdatachart',
        size: {
        height: 200,
        width: 340
        },
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
        size: {
        height: 300,
        width: 360
        },
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
        size: {
            height: 160,
            width: 600
        },
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

    heatNew = L.heatLayer(
	    latlonlist,{
	        //max : 1000,
	        max : 18,
            radius: 20,
            blur: 15, 
            maxZoom: 17,
            /* gradient must be between 0.00 and 1.00 */
           /* gradient: {
                100:  '#f23e45',
                300: 'lime',
                500: 'yellow',
                700: '#FF8300',
                1000:  'red'
            }*/
        }).addTo(map);

    /************************************************** Mobile Display Heatmap *******************************/
    // need to parse each string inside the array
    var touchcount = {};
    var points = [];
    for (i = 0; i < touchdata.length; i++) {
            touchdata[i]=JSON.parse(touchdata[i]);
            var scaledx= Math.round(touchdata[i].x *0.65);
            var scaledy= Math.round(touchdata[i].y *0.65);

            var point = {
                x: scaledx,
                y: scaledy,
                value: 10
            };
            points.push(point);

    }
    console.log("Starting Mobile HeatMap Display Processing");
    console.log(points);

    var heatmapInstance = h337.create({
            container: document.getElementById('heatMap')
    });

                // now generate some random data
                //var points = [];
    var max = 0;
                /*var width = 255;
                var height = 453;

                  var point = {
                    x: 16,
                    y: 461,
                    value: 10
                  };
                  points.push(point);

                  var point1 = {
                    x: 10,
                    y: 450,
                    value: 10
                  };
                  points.push(point1);

                  var point2 = {
                    x: 402,
                    y: 70,
                    value: 10
                  };
                 // points.push(point2);

                  var point3 = {
                    x: 410,
                    y: 80,
                    value: 10
                  };
                  //points.push(point3);*/

                // heatmap data format
    var data = {
         max: max,
         data: points
    };

    heatmapInstance.setData(data);
    /************************************************** Mobile Display Heatmap *******************************/
        

};
function displayHeatmap(daypartstring) {
    // Add remove the old heatmap and add new one
    document.getElementById("maploader").style.display = "block";
    map.removeLayer(heatNew);
    console.log(daypartstring);
    url="";
    
    switch(daypartstring) {
    case "allday":
        url = "http://inserturl/getstationdaytime";
        // TODO: This value should be gathered from the data.
        maxCount=18;
        break;
    case "morning":
        url = "http://inserturl/getstationdaytimemorning";
        maxCount=3;
        break;
    case "lunch":
        url = "http://inserturl/getstationdaytimelunch";
        maxCount=3;
        break;
    case "afternoon":
        url = "http://inserturl/getstationdaytimeafternoon";
        maxCount=3;
        break;
    case "evening":
        url = "http://inserturl/getstationdaytimeevening";
        maxCount=3;
        break;
    case "night":
        url = "http://inserturl/getstationdaytimenight";
        maxCount=3;
        break;
    default:
        url = "http://inserturl/getstationdaytime";
        maxCount=18;
    }
    
    d3.queue()
    .defer(d3.json, url)
    .await(analyzeHeatmap);
}; 

function analyzeHeatmap(error,stationdaytime) {
    if(error) { 
        console.log(error); 
    }
    console.log("Station Data by daytime");
    console.log(stationdaytime);
    console.log("Maximum count");
    console.log(maxCount);
    
 
    //[-41.5764,174.2405,1.4453]
    //[lat,lon,number]
    var latlonlist = [];
    // need to parse each string inside the array
    for (i = 0; i < stationdaytime.length; i++) { 
        var latloncount=[];
        stationdaytime[i]=JSON.parse(stationdaytime[i]);
        latloncount.push(stationdaytime[i].startstationlat);
        latloncount.push(stationdaytime[i].startstationlon);
        latloncount.push(stationdaytime[i].rentalcount);
        latlonlist.push(latloncount)
        
    }
    //console.log("latlonlist");
    //console.log(latlonlist)

    // Stop the spining loader
    document.getElementById("maploader").style.display = "none";
    
    heatNew = L.heatLayer(latlonlist,{
	    //max : 1000,
	    max : maxCount,
            radius: 20,
            blur: 15, 
            maxZoom: 17,
            /* gradient must be between 0.00 and 1.00 */
           /* gradient: {
                100:  '#f23e45',
                300: 'lime',
                500: 'yellow',
                700: '#FF8300',
                1000:  'red'
            }*/
    }).addTo(map);
        

};


